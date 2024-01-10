/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;

import sun.misc.Unsafe;

public final class CalculateAverage_fwbrasil {

    static final Unsafe UNSAFE = initUnsafe();

    static final long TABLE_BASE_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
    static final long TABLE_SCALE = UNSAFE.arrayIndexScale(long[].class);
    static final int TABLE_SIZE = (int) Math.pow(2, 15);
    static final int TABLE_MASK = TABLE_SIZE - 1;

    static final int PARTITIONS = (int) (Runtime.getRuntime().availableProcessors());
    static final long PRIME = (long) (Math.pow(2, 31) - 1);

    public static void main(String[] args) throws Throwable {
        new CalculateAverage_fwbrasil().run();
    }

    CalculateAverage_fwbrasil() throws Throwable {
    }

    final RandomAccessFile file = new RandomAccessFile("measurements.txt", "r");
    final long length = file.length();
    final MemorySegment segment = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, length, Arena.global());
    final long address = segment.address();

    final long partitionSize = length / PARTITIONS;
    final Partition[] partitions = new Partition[PARTITIONS];
    final CountDownLatch cdl = new CountDownLatch(PARTITIONS);

    void run() throws Throwable {
        runPartitions();
        Results results = mergeResults();
        System.out.println(results);
    }

    void runPartitions() throws Throwable {
        for (int i = 0; i < PARTITIONS; i++) {
            var p = new Partition();
            p.id = i;
            partitions[i] = p;
        }
        var i = 0;
        var start = 0L;
        var pos = partitionSize;
        while (pos < length) {
            pos = findNextEntry(pos);
            if (pos > length) {
                pos = length;
            }
            partitions[i].run(start, pos);
            start = pos;
            pos += partitionSize;
            i++;
        }
        if (start < length) {
            partitions[i].run(start, length);
            i++;
        }
        for (int j = 0; j < PARTITIONS - i; j++) {
            cdl.countDown();
        }
        cdl.await();
    }

    long findNextEntry(long pos) {
        while (pos < length && read(pos) != '\n') {
            pos++;
        }
        pos++;
        return pos;
    }

    Results mergeResults() {
        Results results = partitions[0].results;
        for (int i = 1; i < PARTITIONS; i++) {
            results.merge(partitions[i].results);
        }
        return results;
    }

    byte read(long pos) {
        return UNSAFE.getByte(address + pos);
    }

    final class Partition implements Runnable {
        Results results = new Results();

        int id;
        long start = 0;
        long end = 0;
        long pos = 0;

        public void run(long start, long end) {
            this.start = start;
            this.end = end;
            (new Thread(this)).start();
        }

        @Override
        public void run() {
            pos = start - 1;
            while (pos < end - 1) {
                pos++;
                readEntry();
            }
            cdl.countDown();
        }

        void readEntry() {
            var keyStart = pos;
            var hash = readKeyHash();
            var keyEnd = pos;
            pos++;
            var value = readValue();
            results.add(hash, keyStart, keyEnd, value);
        }

        long readKeyHash() {
            long hash = 0L;
            byte b = 0;
            for (; (b = read(pos)) != ';'; pos++) {
                hash += b;
                hash += (hash << 10);
                hash ^= (hash >> 6);
            }
            hash += (hash << 3);
            hash ^= (hash >> 11);
            hash += (hash << 15);
            var r = Math.abs(hash * PRIME);
            return r;
        }

        short readValue() {
            var value = 0;
            var negative = false;
            if (read(pos) == '-') {
                negative = true;
                pos++;
            }
            byte maybeDot = read(pos + 1);
            if (maybeDot == '.') {
                value = read(pos) * 10 + read(pos + 2) - 528;
                pos += 3;
            } else {
                value = read(pos) * 100 + maybeDot * 10 + read(pos + 3) - 5328;
                pos += 4;
            }
            return (short) (negative ? -value : value);
        }
    }

    final class Results {

        static final int FIELDS = 6;
        static final long ENTRY_SIZE = FIELDS * TABLE_SCALE;

        // keyHash | keyStart | keyEnd | 0 | state(min, max, count) | sum

        final ByteBuffer table = new DirectByteBuffer((int) (TABLE_SIZE * ENTRY_SIZE));

        final long tableStart = table.;
        final long tableEnd = tableStart + TABLE_SIZE * ENTRY_SIZE;

        long offset = tableStart;
        Key key = new Key();
        State state = new State();

        public void add(long keyHash, long keyStart, long keyEnd, short value) {
            if (!findKey(keyHash)) {
                key.init(keyHash, keyStart, keyEnd);
                state.init(value);
            } else {
                state.update(value);
            }
        }

        boolean findKey(long keyHash) {
            long l = keyHash & TABLE_MASK;
            offset = tableStart + l * FIELDS;
            var h = 0L;
            if ((h = UNSAFE.getLong(offset)) != keyHash && h != 0) {
                next();
                while ((h = UNSAFE.getLong(offset)) != keyHash && h != 0) {
                    next();
                }
            }
            return h != 0;
        }

        void next() {
            var o = offset;
            o += ENTRY_SIZE;
            if (o > tableEnd) {
                o = tableStart;
            }
            offset = o;
        }

        public void merge(Results other) {
        }

        double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        String readKey() {
            var start = key.start;
            var size = (int) (key.end - start);
            var bytes = new byte[size];
            UNSAFE.copyMemory(null, address + start, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, size);
            try {
                return new String(bytes, "UTF-8");
            } catch (Throwable ex) {
                throw new RuntimeException(ex);
            }
        }

        final class Key {
            long hash = 0;
            long start = 0;
            long end = 0;

            public void init(long hash, long start, long end) {
                this.hash = hash;
                this.start = start;
                this.end = end;
                write();
            }

            public void read() {
                var o = offset;
                hash = UNSAFE.getLong(o);
                o += TABLE_SCALE;
                start = UNSAFE.getLong(o);
                o += TABLE_SCALE;
                end = UNSAFE.getLong(o);
            }

            void write() {
                var o = offset;
                UNSAFE.putLong(o, hash);
                o += TABLE_SCALE;
                UNSAFE.putLong(o, start);
                o += TABLE_SCALE;
                UNSAFE.putLong(o, end);
            }
        }

        final class State {
            short min = Short.MAX_VALUE;
            short max = Short.MIN_VALUE;
            int count = 0;
            long sum = 0;

            public long init(short value) {
                min = value;
                max = value;
                count = 1;
                sum = value;
                return write();
            }

            public long update(short value) {
                read();
                if (value < min) {
                    min = value;
                }
                if (value > max) {
                    max = value;
                }
                count++;
                sum += value;
                return write();
            }

            long write() {
                var o = offset + 4;
                long st = ((long) min << 48) | ((long) max << 32) | (count & 0xffffffffL);
                UNSAFE.putLong(o, st);
                o += TABLE_SCALE;
                UNSAFE.putLong(o, sum);
                return o + TABLE_SCALE;
            }

            void read() {
                var o = offset + 4;
                var st = UNSAFE.getLong(o);
                min = (short) (st >> 48);
                max = (short) (st >> 32);
                count = (int) st;
                offset += TABLE_SCALE;
                sum = UNSAFE.getLong(o);
            }
        }
    }

    static Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
