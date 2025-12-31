package com.minis;

import android.content.Context;
import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.ext.junit.runners.AndroidJUnit4;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(AndroidJUnit4.class)
public class MinisInstrumentedTest {

    // Helper to get a safe writeable path for the test
    private String getDbPath(String name) {
        Context appContext = InstrumentationRegistry.getInstrumentation().getTargetContext();
        return new File(appContext.getFilesDir(), name).getAbsolutePath();
    }

    @Test
    public void testBasicOperations() {
        try (Minis db = new Minis()) {
            db.set("foo", "bar");
            assertEquals("bar", db.get("foo"));
            
            db.set("foo", "baz");
            assertEquals("baz", db.get("foo"));
            
            assertTrue(db.del("foo"));
            assertNull(db.get("foo"));
        }
    }

    @Test
    public void testPersistenceLifecycle() {
        String path = getDbPath("persistence_test.rdb");
        int itemCount = 50000;

        // --- PHASE 1: FILL & SAVE ---
        try (Minis db1 = new Minis()) {
            // Write a lot of data
            for (int i = 0; i < itemCount; i++) {
                db1.set("key:" + i, "val:" + i);
            }
            
            // Verify RAM state
            assertEquals("val:49999", db1.get("key:49999"));

            // Save to Disk
            boolean saveResult = db1.save(path);
            assertTrue("Save failed!", saveResult);
        } 
        // db1.close() is called automatically here, destroying the C struct.
        // The data now ONLY exists on disk.

        // --- PHASE 2: LOAD & VERIFY ---
        try (Minis db2 = new Minis()) {
            // Verify it's empty initially (new C struct)
            assertNull("New DB should be empty", db2.get("key:0"));

            // Load from Disk
            boolean loadResult = db2.load(path);
            assertTrue("Load failed!", loadResult);

            // Verify integrity
            for (int i = 0; i < itemCount; i++) {
                String val = db2.get("key:" + i);
                assertEquals("Data corruption at index " + i, "val:" + i, val);
            }
        }
    }
}
