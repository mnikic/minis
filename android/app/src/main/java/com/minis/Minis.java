package com.minis;

import java.io.File;

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

    // --- Public API ---

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

    // --- Native Definitions (The Bridge) ---
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
