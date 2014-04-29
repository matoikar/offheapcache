/**
 * Copyright 2014 Matti Oikarinen
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.elegantscale.util.cache.offheap;

import com.elegantscale.util.cache.Map;
import com.elegantscale.util.hash.HashFunction;
import com.elegantscale.util.hash.NonLinearLookupTableHash;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Hash table based cache for constant length values and keys. Key with 0 bytes is not recommended as it is
 * distinguished from unset value. This implementation is thread safe and does not use locks, but may lose data in race
 * conditions. Multiple writes to same hashtable bucket may cause updates to be missed. As a cache may lose data anyway
 * due to hash collisions, this behavior should not cause problems in typical cache use cases.
 */
public class ConstantSizeCache implements Map {
    private static final Unsafe unsafe;
    private static final int ELEMENTS_PER_SLOT = 3;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
        } catch (NoSuchFieldException|IllegalAccessException  e) {
            throw new RuntimeException("Failed to get unsafe instance, are you running Oracle JDK?", e);
        }
    }

    private final int keySz;
    private final int valueSz;
    private final long sz;
    private final long startPtr;
    private final int slotSz;
    private final long slotCount;
    private HashFunction hashFunction;

    public ConstantSizeCache(long size, int keySize, int valueSize) {
        this(size, keySize, valueSize, new NonLinearLookupTableHash());
    }

    public ConstantSizeCache(long size, int keySize, int valueSize, HashFunction hash) {
        if(keySize < 1 || valueSize < 1) throw new IllegalArgumentException("Key and value size has to be larger than zero");
        if(size < (keySize + valueSize) * ELEMENTS_PER_SLOT) throw new IllegalArgumentException("Too small size.");
        if(hash == null) throw new NullPointerException("Hash algorithm instance is required.");

        this.keySz = keySize;
        this.valueSz = valueSize;
        this.sz = size;

        startPtr = unsafe.allocateMemory(size);
        byte[] fourkb = new byte[4096];
        for (int i = 0; i < sz;) {
            int copySz = i + fourkb.length > sz ? (int) sz - i : fourkb.length;
            unsafe.copyMemory(fourkb, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, startPtr, copySz);
            i += fourkb.length;
        }

        slotSz = ELEMENTS_PER_SLOT * (keySize + valueSize);
        slotCount = size / slotSz;
        hashFunction = hash;
    }

    @Override
    public void put(byte[] key, byte[] value) {
        if(key.length != keySz) throw new IllegalArgumentException("Invalid key length " + key.length + " expected " + keySz);
        if(value.length != valueSz) throw new IllegalArgumentException("Invalid value length " + value.length + " expected " + valueSz);

        long hash = Math.abs(hashFunction.hash(key));
        long slotPtr = (hash % slotCount) * slotSz + startPtr;
        byte[] copy = new byte[slotSz];
        unsafe.copyMemory(null, slotPtr, copy, Unsafe.ARRAY_BYTE_BASE_OFFSET, slotSz);

        int keyOffset = containsKey(key, copy);

        if(keyOffset < 0) {
            // move existing values right
            System.arraycopy(copy, 0, copy, keySz + valueSz, 2 * (keySz + valueSz));
            System.arraycopy(key, 0, copy, 0, keySz);
            System.arraycopy(value, 0, copy, keySz, valueSz);
        } else {
            // no reordering on update
            System.arraycopy(value, 0, copy, keyOffset + keySz, valueSz);
        }

        unsafe.copyMemory(copy, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, slotPtr, slotSz);
    }

    private int containsKey(byte[] key, byte[] slot) {
        for(int i = 0; i < ELEMENTS_PER_SLOT; i++) {
            int keyOffset = i * (keySz + valueSz);
            boolean match = true;
            for (int j = 0; j < key.length; j++) {
                if(slot[j + keyOffset] != key[j]) {
                    match = false;
                    break;
                }
            }

            if(match) {
                return keyOffset;
            }
        }

        return -1;
    }

    @Override
    public byte[] get(byte[] key) {
        if(key.length != keySz) throw new IllegalArgumentException("Invalid key length " + key.length + " expected " + keySz);

        long hash = Math.abs(hashFunction.hash(key));
        long slotPtr = (hash % slotCount) * slotSz + startPtr;
        byte[] copy = new byte[slotSz];
        unsafe.copyMemory(null, slotPtr, copy, Unsafe.ARRAY_BYTE_BASE_OFFSET, slotSz);

        int keyOffset = containsKey(key, copy);

        if(keyOffset < 0) return null;

        byte[] value = new byte[valueSz];
        System.arraycopy(copy, keyOffset + keySz, value, 0, valueSz);

        return value;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        unsafe.freeMemory(startPtr);
    }
}
