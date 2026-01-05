package com.example.minisandroidtest

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import com.example.minisandroidtest.ui.theme.MinisAndroidTestTheme
import com.minis.Minis
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.concurrent.Volatile
// --- CONFIGURATION ---
const val BATCH_SIZE = 200
const val THREAD_COUNT = 1
const val ITEMS_PER_THREAD = 3_000_000

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()

        // 1. GET THE PATH HERE (Activity is a Context)
        val storagePath = filesDir.absolutePath

        setContent {
            MinisAndroidTestTheme {
                Scaffold(modifier = Modifier.fillMaxSize()) { innerPadding ->
                    // 2. Pass it to the screen
                    BenchmarkScreen(
                        modifier = Modifier.padding(innerPadding),
                        storagePath = storagePath
                    )
                }
            }
        }
    }
}

// 3. Update the function to accept the path String
private fun testPersistenceIntegrity(storagePath: String, onUpdate: (String) -> Unit) {
    val sb = StringBuilder()
    fun log(msg: String) {
        sb.append(msg).append("\n")
        onUpdate(sb.toString())
    }

    // Use the passed path
    val path = "$storagePath/test_dump.rdb"
    val itemCount = 100000

    log("=== Persistence Integrity Test ===")

    try {
        // 1. Populate Data
        log("Step 1: Filling DB with $itemCount items...")
        Minis().use { db1 ->
            for (i in 0 until itemCount) {
                db1.set("key:$i", "value:$i")
            }
            // 2. Save to Disk
            log("Step 2: Saving to $path")
            val saved = db1.save(path)
            if (!saved) throw RuntimeException("Save failed!")
        }

        // 3. Re-Initialize (Simulate App Restart)
        log("Step 3: Reloading from disk...")
        Minis().use { db2 ->
            val loaded = db2.load(path)
            if (!loaded) throw RuntimeException("Load failed!")

            // 4. Verify Data
            log("Step 4: Verifying data integrity...")
            for (i in 0 until itemCount) {
                val `val` = db2.get("key:$i")
                if (`val` == null || `val` != "value:$i") {
                    throw RuntimeException("Data corruption at index $i")
                }
            }
        }
        log("SUCCESS: Data survived the lifecycle!")
        val file = File(path)
        if (file.exists())
            if (!file.delete())
                log ("WARNING: Didn't manage to clean the file at $path")
            else
                log ("Successfully cleaned up the db dump.")
    } catch (e: Exception) {
        log("FAILURE: ${e.message}")
        e.printStackTrace()
    }
}

@Volatile
private var keepRunning = true

private fun testConcurrentPersistence(storagePath: String, onUpdate: (String) -> Unit) {
    val sb = StringBuilder()
    fun log(msg: String) {
        sb.append(msg).append("\n")
        onUpdate(sb.toString())
    }
    val path = "$storagePath/test_dump.mdb"
    val minis = Minis()

    log("=== Concurrent Save Test (Torture Test) ===")

    // Thread A: Writes data continuously
    val writer = Thread(Runnable {
        var i = 0
        while (keepRunning) {
            minis.set("stress:$i", "data_$i")
            i++
            if (i % 100000 == 0) log( "db contains $i items now")
            if (i % 1000 == 0) Thread.yield() // Be nice to the CPU
        }
    })

    // Thread B: Tries to save every 500ms
    val saver = Thread(Runnable {
        var saves = 0
        while (keepRunning) {
            try {
                Thread.sleep(500)
                val start = System.currentTimeMillis()
                val res = minis.save(path)
                val end = System.currentTimeMillis()

                log(
                    "Save #" + saves + ": " + (if (res) "OK" else "FAIL") +
                            " (" + (end - start) + "ms)"
                )
                saves++
            } catch (e: InterruptedException) {
                break
            }
        }
    })

    // Run for 10 seconds
    writer.start()
    saver.start()

    try {
        Thread.sleep(30000)
    } catch (e: InterruptedException) {
    }

    keepRunning = false
    try {
        writer.join()
        saver.join()
    } catch (e: InterruptedException) {
    }

    minis.close()
    val file = File(path)
    if (file.exists())
        if (!file.delete())
            log ("WARNING: Didn't manage to clean the file at $path")
        else
            log ("Successfully cleaned up the db dump.")
    log("Torture Test Complete. If app didn't freeze/crash, we passed.")
}

@Composable
fun BenchmarkScreen(modifier: Modifier = Modifier, storagePath: String) {
    var outputText by remember { mutableStateOf("Initializing...") }
    val scope = rememberCoroutineScope()

    LaunchedEffect(Unit) {
        scope.launch(Dispatchers.Default) {
            // A. Run Persistence Test
            //testPersistenceIntegrity(storagePath) { outputText = it }
            testConcurrentPersistence(storagePath){ outputText = it }

            // Add a visual separator
            outputText += "\n\n------------------------\n\n"

            // B. Run Benchmark (Appending to existing text)
            runBatchBenchmark { newLog ->
                // We append the new log to the existing text so we don't lose the persistence result
                // (Note: This simple logic re-appends the whole benchmark builder,
                // so we just take the last update effectively.
                // A cleaner way is typically a shared StringBuilder or a list of logs)
                outputText += newLog.replace(outputText.substringAfter("------------------------\n\n", ""), "")
            }
        }
    }

    Box(modifier = modifier
        .fillMaxSize()
        .verticalScroll(rememberScrollState())) {
        Text(text = outputText)
    }
}

suspend fun runBatchBenchmark(onUpdate: (String) -> Unit) {
    val sb = StringBuilder()
    fun log(msg: String) {
        sb.append(msg).append("\n")
        onUpdate(sb.toString())
    }

    // Calculate total operations
    // 1 Batch = (1 MSET + 1 MGET + 1 MDEL)
    // But internally that represents BATCH_SIZE * 3 logical item operations
    val totalItemsProcessed = THREAD_COUNT * ITEMS_PER_THREAD * 3L

    log("== Minis Batch Showdown (Android) ==")
    log("Threads: $THREAD_COUNT")
    log("Items/Thread: $ITEMS_PER_THREAD")
    log("Batch Size: $BATCH_SIZE")
    log("Total Ops (Logical): $totalItemsProcessed")

    // --- 1. JAVA BENCHMARK (Baseline) ---
    log("\n[Java ConcurrentHashMap] Running...")
    val javaMap = HashMap<String, String>()

    val javaStart = System.nanoTime()
    var aha = 0;
    runBatchWorkload(THREAD_COUNT, ITEMS_PER_THREAD, BATCH_SIZE) { i, j ->
        // Simulating a batch by running a tight loop
        // Java has no native "MSET", so this is the fastest way it can go.
        val prefix = "key_${i}_"
        val value = "val_${i}"

        // 1. "MSET" equivalent loop
        for (b in 0 until BATCH_SIZE) {
            val key = prefix + (j + b)
            javaMap[key] = value
        }

        // 2. "MGET" equivalent loop
        for (b in 0 until BATCH_SIZE) {
            val key = prefix + (j + b)
            val value = javaMap[key]
            if (i % 10 == 0) {
                log ("value == $value")
                aha++;
            }
        }

        log ("aha was $aha")
        // 3. "MDEL" equivalent loop
        for (b in 0 until BATCH_SIZE) {
            val key = prefix + (j + b)
            javaMap.remove(key)
        }
    }

    log ("Java map has ${javaMap.size} items.")
    javaMap.clear()
    val javaDuration = System.nanoTime() - javaStart
    val javaSec = javaDuration / 1_000_000_000.0
    val javaOps = (totalItemsProcessed / javaSec).toLong()
    log("  Time: %.3fs".format(javaSec))
    log("  Throughput: %,d items/sec".format(javaOps))


    // --- 2. MINIS BENCHMARK (The Challenger) ---
    log("\n[Minis JNI Batch] Running...")

    try {
        Minis().use { minis ->
            val minisStart = System.nanoTime()

            runBatchWorkload(THREAD_COUNT, ITEMS_PER_THREAD, BATCH_SIZE) { i, j ->
                val prefix = "key_${i}_"
                val value = "val_${i}"

                // PREPARE ARRAYS
                // (Allocation cost is included in the benchmark because it's part of using the API)
                val msetArgs = Array(BATCH_SIZE * 2) { "" } // [k1, v1, k2, v2...]
                val keys = Array(BATCH_SIZE) { "" }         // [k1, k2...]

                for (b in 0 until BATCH_SIZE) {
                    val key = prefix + (j + b)
                    keys[b] = key

                    // Populate MSET interleaved array
                    msetArgs[b * 2] = key
                    msetArgs[b * 2 + 1] = value
                }

                // 1. ATOMIC MSET (One JNI crossing)
                minis.mset(*msetArgs)

                // 2. ATOMIC MGET (One JNI crossing)
                minis.mget(*keys)

                // 3. ATOMIC MDEL (One JNI crossing)
                minis.mdel(*keys)
            }

            val minisDuration = System.nanoTime() - minisStart
            val minisSec = minisDuration / 1_000_000_000.0
            val minisOps = (totalItemsProcessed / minisSec).toLong()

            log("  Time: %.3fs".format(minisSec))
            log("  Throughput: %,d items/sec".format(minisOps))

            log("\n--------------------------------")
            if (minisOps > 0) {
                val ratio = javaOps.toDouble() / minisOps.toDouble()
                if (minisOps > javaOps) {
                    log("WINNER: Minis is %.2fx FASTER than Java!".format(minisOps.toDouble() / javaOps.toDouble()))
                } else {
                    log("Java is %.2fx faster than Minis".format(ratio))
                }
            }
        }
    } catch (e: Exception) {
        log("\nFATAL ERROR: ${e.message}")
        e.printStackTrace()
    }
}

/**
 * Handles the threading and chunking logic.
 * @param action Callback receiving (threadIndex, currentBaseIndex)
 */
fun runBatchWorkload(threads: Int, totalItems: Int, batchSize: Int, action: (Int, Int) -> Unit) {
    val executor = Executors.newFixedThreadPool(threads)
    val latch = CountDownLatch(threads)

    for (i in 0 until threads) {
        executor.submit {
            try {
                // Loop through items in steps of BATCH_SIZE
                for (j in 0 until totalItems step batchSize) {
                    action(i, j)
                }
            } finally {
                latch.countDown()
            }
        }
    }
    latch.await()
    executor.shutdown()
}