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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CalculateAverage_PanagiotisDrakatos {
    private static final String FILE = "./measurements.txt";
    private static TreeMap<String, MeasurementObject> sortedCities;

    public static void main(String[] args) throws IOException {
        SeekableByteRead(FILE);
        System.out.println(sortedCities);
    }

    private static void SeekableByteRead(String path) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(FILE);
        FileChannel fileChannel = fileInputStream.getChannel();
        Optional<Map<String, MeasurementObject>> optimistic = SplitSeekableByteChannel(fileChannel)
                .parallel()
                .map(CalculateAverage_PanagiotisDrakatos::MappingByteBufferToData)
                .reduce(CalculateAverage_PanagiotisDrakatos::combineMaps);
        fileChannel.close();
        sortedCities = new TreeMap<>(optimistic.orElseThrow());

    }

    private static Stream<ByteBuffer> SplitSeekableByteChannel(FileChannel channel) throws IOException {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<ByteBuffer>() {
            private static final long MAP_SIZE = 1024 * 1024 * 10L;

            private long position = 0;
            private long length = channel.size();

            @Override
            public boolean hasNext() {
                while (position < length) {
                    return true;
                }
                return false;
            }

            @Override
            public ByteBuffer next() {
                try {
                    MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, position, Math.min(MAP_SIZE, length - position));
                    int end = buffer.limit() - 1;
                    while (buffer.get(end) != '\n') {
                        end--;
                    }
                    position += end + 1;
                    return buffer.slice(0, end);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, Spliterator.IMMUTABLE), false);
    }

    public static ByteBuffer concat(ByteBuffer[] buffers) {
        int overAllCapacity = 0;
        for (int i = 0; i < buffers.length; i++)
            overAllCapacity += buffers[i].limit() - buffers[i].position();
        overAllCapacity += buffers[0].limit() - buffers[0].position();
        ByteBuffer all = ByteBuffer.allocate(overAllCapacity);
        for (int i = 0; i < buffers.length; i++) {
            ByteBuffer curr = buffers[i];
            all.put(curr);
        }

        all.flip();
        return all;
    }

    private static Map<String, MeasurementObject> combineMaps(Map<String, MeasurementObject> map1, Map<String, MeasurementObject> map2) {
        for (var entry : map2.entrySet()) {
            map1.merge(entry.getKey(), entry.getValue(), MeasurementObject::combine);
        }

        return map1;
    }

    private static Map<String, MeasurementObject> MappingByteBufferToData(ByteBuffer byteBuffer) {
        Map<String, MeasurementObject> cities = new HashMap<>();
        ByteBuffer bb = byteBuffer.duplicate();
        int start = 0;
        int end = 0;
        while (start < bb.limit()) {
            while (bb.get(end) != ';') {
                end++;
            }
            int temp_counter = 0;
            int temp_end = end;
            try {
                bb.position(end);
                while (bb.get(temp_end) != '\n') {
                    temp_counter++;
                    temp_end++;
                }
            }
            catch (IndexOutOfBoundsException e) {
                temp_counter--;
                temp_end--;
            }
            ByteBuffer city = bb.slice(start, end - start);
            ByteBuffer temp = bb.slice(end + 1, temp_counter);
            int tempPointer = 0;
            int abs = 1;
            if (temp.get(0) == '-') {
                abs = -1;
                tempPointer++;
            }
            int measuredValue;
            if (temp.get(tempPointer + 1) == '.') {
                measuredValue = abs * ((temp.get(tempPointer)) * 10 + (temp.get(tempPointer + 2)) - 528);
            }
            else {
                measuredValue = abs * (temp.get(tempPointer) * 100 + temp.get(tempPointer + 1) * 10 + temp.get(tempPointer + 3) - 5328);
            }

            byte[] citybytes = new byte[city.limit()];
            city.get(citybytes);
            String cityName = new String(citybytes, StandardCharsets.UTF_8);

            // update the map with the new measurement
            MeasurementObject agg = cities.get(cityName);
            if (agg == null) {
                cities.put(cityName, new MeasurementObject(measuredValue, measuredValue, 0, 0).updateWith(measuredValue));
            }
            else {
                cities.put(cityName, agg.updateWith(measuredValue));
            }
            start = temp_end + 1;
            end = temp_end;
        }
        return cities;
    }

    private static final class MeasurementObject {

        private int MAX;
        private int MIN;

        private long SUM;

        private int REPEAT;

        public MeasurementObject(int MAX, int MIN, long SUM, int REPEAT) {
            this.MAX = MAX;
            this.MIN = MIN;
            this.SUM = SUM;
            this.REPEAT = REPEAT;
        }

        public MeasurementObject() {
        }

        public MeasurementObject(int MAX, int MIN, long SUM) {
            this.MAX = MAX;
            this.MIN = MIN;
            this.SUM = SUM;
        }

        public MeasurementObject(int MAX, int MIN) {
            this.MAX = MAX;
            this.MIN = MIN;
        }

        public static MeasurementObject combine(MeasurementObject m1, MeasurementObject m2) {
            var mres = new MeasurementObject();
            mres.MIN = MeasurementObject.min(m1.MIN, m2.MIN);
            mres.MAX = MeasurementObject.max(m1.MAX, m2.MAX);
            mres.SUM = m1.SUM + m2.SUM;
            mres.REPEAT = m1.REPEAT + m2.REPEAT;
            return mres;
        }

        public MeasurementObject updateWith(int measurement) {
            MIN = MeasurementObject.min(MIN, measurement);
            MAX = MeasurementObject.max(MAX, measurement);
            SUM += measurement;
            REPEAT++;
            return this;
        }

        private static int max(final int a, final int b) {
            final int diff = a - b;
            final int dsgn = diff >> 31;
            return a - (diff & dsgn);
        }

        private static int min(final int a, final int b) {
            final int diff = a - b;
            final int dsgn = diff >> 31;
            return b + (diff & dsgn);
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            MeasurementObject that = (MeasurementObject) o;
            return MAX == that.MAX && MIN == that.MIN && REPEAT == that.REPEAT;
        }

        @Override
        public int hashCode() {
            return Objects.hash(MAX, MIN, REPEAT);
        }

        @Override
        public String toString() {
            return round(MIN) + "/" + round((1.0 * SUM) / REPEAT) + "/" + round(MAX);
        }
    }
}
