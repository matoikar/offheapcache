/**
 * Copyright 2014 Matti Oikarinen
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.elegantscale.util.cache.offheap;

import com.elegantscale.util.hash.HashFunction;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

/**
 *
 */
public class ConstantSizeCacheTest {
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidSize() {
        ConstantSizeCache c = new ConstantSizeCache(0, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidKeySize() {
        ConstantSizeCache c = new ConstantSizeCache(1, 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidValueSize() {
        ConstantSizeCache c = new ConstantSizeCache(1, 1, 0);
    }

    @Test
    public void testZeroKeyBehavior() {
        ConstantSizeCache cMin = new ConstantSizeCache(100, 1, 1);
        ConstantSizeCache cLarge = new ConstantSizeCache(200 * 1024, 50*1024, 1);
        Assert.assertArrayEquals(new byte[1], cMin.get(new byte[1]));
        Assert.assertArrayEquals(new byte[1], cLarge.get(new byte[50 * 1024]));
    }

    @Test
    public void testKeyValueSizes() {
        byte[][] oneByte = new byte[10][];
        byte[][] hundredK = new byte[10][];
        for (int i = 0; i < oneByte.length; i++) {
            oneByte[i] = new byte[]{(byte) (i + 1)};
            hundredK[i] = new byte[100000];
            for (int j = 0; j < hundredK[i].length; j++) {
                hundredK[i][j] = (byte) (((j + i) % 256) - 128);
            }

        }

        testCollisions(oneByte, oneByte);
        testCollisions(oneByte, hundredK);
        testCollisions(hundredK, oneByte);
        testCollisions(hundredK, hundredK);
        testUpdates(oneByte, oneByte);
        testUpdates(oneByte, hundredK);
        testUpdates(hundredK, oneByte);
        testUpdates(hundredK, hundredK);
    }

    public void testCollisions(byte[][] keys, byte[][] values) {
        ConstantSizeCache c = new ConstantSizeCache(9  * (keys[0].length + values[0].length), keys[0].length,
                values[0].length, new HashFunction() {
            @Override
            public long hash(byte[] value) {
                return 0;
            }

            @Override
            public long hash(byte[] value, int offset, int length) {
                return 0;
            }
        });

        for (int i = 0; i < keys.length; i++) {
            byte[] key = keys[i];
            byte[] value = values[i];

            // check that older than 3 latest are not in cache
            for (int j = 0; j < i - 3; j++) {
                byte[] oldKey = keys[j];
                Assert.assertNull(c.get(oldKey));
            }

            // check that 3 latest are in cache
            for (int j = i - 3; j >= 0 && j < i; j++) {
                byte[] oldKey = keys[j];
                Assert.assertArrayEquals(values[j], c.get(oldKey));
            }

            // check that future keys are not in cache
            for (int j = i; j < keys.length; j++) {
                Assert.assertNull(c.get(keys[j]));
            }

            c.put(key, value);
        }
    }

    public void testUpdates(byte[][] keys, byte[][] values) {
        // minimum of 3 latest values are always in cache, in most cases close to 9 latest.
        ConstantSizeCache c = new ConstantSizeCache(9 * (keys[0].length + values[0].length), keys[0].length, values[0].length);
        Random r = new Random(0);
        byte[][] valuesNew = new byte[values.length][];

        for (int i = 0; i < valuesNew.length; i++) {
            valuesNew[i] = Arrays.copyOf(values[i], values[i].length);
        }

        for (int i = 0; i < keys.length; i++) {
            c.put(keys[i], values[i]);

            for (int j = i - 3; j >= 0 && j < i; j++) {
                byte[] oldKey = keys[j];
                Assert.assertArrayEquals(valuesNew[j], c.get(oldKey));
                r.nextBytes(valuesNew[j]);
                c.put(keys[j], valuesNew[j]);
                Assert.assertArrayEquals(valuesNew[j], c.get(oldKey));
            }
        }
    }


}
