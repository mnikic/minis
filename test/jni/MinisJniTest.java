// Drives the Android JNI library, built for the host, through the app's
// own com.minis.Minis class: make jni-test
import com.minis.Minis;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;

public class MinisJniTest {
    private static int failures = 0;

    private static void check(boolean ok, String what) {
        System.out.println((ok ? "  PASS " : "  FAIL ") + what);
        if (!ok)
            failures++;
    }

    private static String freshDir(String name) throws Exception {
        File dir = Files.createTempDirectory("minis-jni-" + name).toFile();
        dir.deleteOnExit();
        return new File(dir, "shards").getAbsolutePath();
    }

    // Snapshot files of a directory, sorted
    private static String[] files(String dir) {
        String[] names = new File(dir).list();
        if (names == null)
            return new String[0];
        Arrays.sort(names);
        return names;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("TEST: save and load");
        String dir = freshDir("save");
        try (Minis db = new Minis()) {
            db.set("greeting", "hello");
            db.hset("user", "name", "jdoe");
            check(db.save(dir), "save returns true");
        }
        try (Minis db = new Minis()) {
            check(db.load(dir), "load returns true");
            check("hello".equals(db.get("greeting")), "string survives");
            check("jdoe".equals(db.hget("user", "name")), "hash survives");
        }

        System.out.println("TEST: sync writes what load reads");
        dir = freshDir("sync");
        try (Minis db = new Minis(dir)) {
            db.set("a", "1");
            db.set("b", "2");
            db.pexpire("b", 100_000);
            db.sync();
            check(files(dir).length > 0, "sync wrote shard files");
            for (String f : files(dir))
                check(f.endsWith(".mdb"), "shard file " + f + " is what load reads");
        }
        try (Minis db = new Minis()) {
            check(db.load(dir), "load finds the synced shards");
            check("1".equals(db.get("a")), "synced key survives");
            long ttl = db.pttl("b");
            check(ttl > 90_000 && ttl <= 100_000, "synced TTL survives (" + ttl + ")");
        }

        System.out.println("TEST: only changes reach the disk, and later ones too");
        dir = freshDir("update");
        try (Minis db = new Minis(dir)) {
            for (int i = 0; i < 100; i++)
                db.set("k" + i, "v1");
            db.sync();
            db.set("k1", "v2");
            db.del("k2");
            db.sync();
        }
        try (Minis db = new Minis()) {
            db.load(dir);
            check("v2".equals(db.get("k1")), "update survives");
            check(db.get("k2") == null, "delete survives");
            check("v1".equals(db.get("k99")), "untouched key survives");
        }

        System.out.println("TEST: the background thread saves without sync");
        dir = freshDir("background");
        try (Minis db = new Minis(dir)) {
            db.set("snake:frame", "42");
            Map<String, String> none = db.hgetall("nothing");
            check(none == null || none.isEmpty(), "hgetall of a missing key");
            // It saves changed shards every second
            Thread.sleep(1500);
        }
        try (Minis db = new Minis()) {
            check(db.load(dir), "load finds the background save");
            check("42".equals(db.get("snake:frame")), "key saved in the background survives");
        }

        System.out.println("TEST: no background saving without a path");
        try (Minis db = new Minis()) {
            db.set("x", "y");
            db.sync();
            check("y".equals(db.get("x")), "sync without a path does nothing harmful");
        }

        if (failures > 0) {
            System.out.println(failures + " FAILED");
            System.exit(1);
        }
        System.out.println("All JNI tests PASSED");
    }
}
