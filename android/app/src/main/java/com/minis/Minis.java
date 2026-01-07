package com.minis;

import java.util.Map;

public class Minis implements AutoCloseable {
    // This holds the pointer to the C 'Minis' struct
    private long nativeHandle;

    // Load the .so library when the class is loaded
    static {
        System.loadLibrary("minis");
    }

    public Minis() {
        // Call C to allocate memory and start the thread
        this.nativeHandle = nativeInit();
    }

    @Override
    public void close() {
        if (this.nativeHandle != 0) {
            nativeFree(this.nativeHandle);
            this.nativeHandle = 0;
        }
    }

    private void checkHandle() {
        if (this.nativeHandle == 0) {
            throw new IllegalStateException("Minis DB is closed");
        }
    }

    public void set(String key, String value) {
        checkHandle();
        nativeSet(this.nativeHandle, key, value);
    }

    public String get(String key) {
        checkHandle();
        return nativeGet(this.nativeHandle, key);
    }

    public boolean del(String key) {
        checkHandle();
        return nativeDel(this.nativeHandle, key) > 0;
    }

    /**
     * Atomic Batch Set.
     * Usage: db.mset("key1", "val1", "key2", "val2");
     */
    public void mset(String... keysAndValues) {
        checkHandle();
        if (keysAndValues == null || keysAndValues.length == 0) return;

        if (keysAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("mset requires pairs of keys and values (e.g. k1, v1, k2, v2)");
        }

        nativeMSet(this.nativeHandle, keysAndValues);
    }

    /**
     * Atomic Batch Get.
     * Returns an array of Strings. If a key is missing, that slot will be null.
     */
    public String[] mget(String... keys) {
        checkHandle();
        if (keys == null || keys.length == 0)
            return new String[0];
        return nativeMGet(this.nativeHandle, keys);
    }

    /**
     * Atomic Batch Delete.
     * Returns the number of keys actually deleted.
     */
    public long mdel(String... keys) {
        checkHandle();
        if (keys == null || keys.length == 0)
            return 0;
        return nativeMDel(this.nativeHandle, keys);
    }

    public boolean pexpire(String key, long ttlMs) {
        checkHandle();
        return nativePexpire(this.nativeHandle, key, ttlMs) == 1;
    }

    public long pttl(String key) {
        checkHandle();
        return nativePttl(this.nativeHandle, key);
    }

    public boolean save(String absolutePath) {
        checkHandle();
        return nativeSave(this.nativeHandle, absolutePath);
    }

    public boolean load(String absolutePath) {
        checkHandle();
        return nativeLoad(this.nativeHandle, absolutePath);
    }

    public int hset (String key, String field, String value) {
        checkHandle();
        return nativeHSet(this.nativeHandle, key, field, value);
    }

    public String hget (String key, String field) {
        checkHandle();
        return nativeHGet(this.nativeHandle, key, field);
    }

    public int hdel (String key, String[] fields) {
        checkHandle();
        return nativeHDel(this.nativeHandle, key, fields);
    }

    public Map<String, String> hgetall (String key) {
        checkHandle();
        return nativeHGetall(this.nativeHandle, key);
    }

    public boolean hexists(String key, String field) {
        checkHandle();
        return nativeHExists(this.nativeHandle, key, field);
    }

    /**
     * Returns the number of fields contained in the hash stored at key.
     */
    public long hlen(String key) {
        checkHandle();
        return nativeHLen (this.nativeHandle, key);
    }

    /**
     * Sets field in the hash stored at key to value.
     * @return 1 if field is a new field in the hash and value was set. 0 if field already existed and was updated.
     */
    private native int nativeHSet(long ptr, String key, String field, String value);

    /**
     * Returns the value associated with field in the hash stored at key.
     * @return The value or null if field or key does not exist.
     */
    private native String nativeHGet(long ptr, String key, String field);

    /**
     * Removes the specified fields from the hash stored at key.
     * @return The number of fields that were removed.
     */
    private native int nativeHDel(long ptr, String key, String[] fields);

    /**
     * Returns all fields and values of the hash stored at key.
     * @return A Map containing all field-value pairs.
     */
    private native Map<String, String> nativeHGetall(long ptr, String key);

    /**
     * Returns if field is an existing field in the hash stored at key.
     */
    private native boolean nativeHExists(long ptr, String key, String field);

    /**
     * Returns the number of fields contained in the hash stored at key.
     */
    private native long nativeHLen(long ptr, String key);

    private native long nativeInit();

    private native void nativeFree(long ptr);

    private native void nativeSet(long ptr, String key, String val);

    private native String nativeGet(long ptr, String key);

    private native int nativeDel(long ptr, String key);

    // New Batch Native Methods
    private native void nativeMSet(long ptr, String[] keyVals);

    private native String[] nativeMGet(long ptr, String[] keys);

    private native long nativeMDel(long ptr, String[] keys);

    private native int nativePexpire(long ptr, String key, long ttlMs);

    private native long nativePttl(long ptr, String key);

    private native boolean nativeSave(long ptr, String path);

    private native boolean nativeLoad(long ptr, String path);
}
