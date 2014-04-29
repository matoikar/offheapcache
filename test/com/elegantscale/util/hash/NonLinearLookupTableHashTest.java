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
package com.elegantscale.util.hash;

import com.elegantscale.util.cache.offheap.ConstantSizeCache;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class NonLinearLookupTableHashTest {
    @Test
    public void testOffsets() {
        NonLinearLookupTableHash h = new NonLinearLookupTableHash();
        byte[] value = "value".getBytes();
        byte[] nvalue = "nvalue".getBytes();
        Assert.assertEquals(h.hash(value), h.hash(nvalue, 1, value.length));
        Assert.assertNotEquals(0, h.hash(value));
    }

    @Test
    public void testNewRandom() {
        NonLinearLookupTableHash h1 = new NonLinearLookupTableHash();
        NonLinearLookupTableHash h2 = new NonLinearLookupTableHash(true);
        byte[] value = "value".getBytes();
        Assert.assertNotEquals(0, h1.hash(value));
        Assert.assertNotEquals(0, h2.hash(value));
        Assert.assertNotEquals(h1.hash(value), h2.hash(value));
    }
}
