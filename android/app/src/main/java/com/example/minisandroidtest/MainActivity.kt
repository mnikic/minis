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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

// --- CONFIGURATION ---
const val THREAD_COUNT = 10
const val ITEMS_PER_THREAD = 300_000 // 300k items per thread

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()

        setContent {
            MinisAndroidTestTheme {
                Scaffold(modifier = Modifier.fillMaxSize()) { innerPadding ->
                    BenchmarkScreen(
                        modifier = Modifier.padding(innerPadding)
                    )
                }
            }
        }
    }
}

@Composable
fun BenchmarkScreen(modifier: Modifier = Modifier) {
    var outputText by remember { mutableStateOf("Initializing Benchmark...") }
    val scope = rememberCoroutineScope()

    LaunchedEffect(Unit) {
        scope.launch(Dispatchers.Default) {
            // Run the Single Key Benchmark (SET / GET / DEL)
            runSingleKeyBenchmark { newLog ->
                // Simple log appending
                val current = outputText
                // Avoid duplicating the whole log on every update for cleaner UI updates
                if (newLog.startsWith("==")) {
                    outputText = newLog
                } else {
                    outputText = outputText.substringBeforeLast("\n") + "\n" + newLog
                }
            }
        }
    }

    Box(modifier = modifier
        .fillMaxSize()
        .verticalScroll(rememberScrollState())) {
        Text(text = outputText)
    }
}

/**
 * The Main Benchmark Function
 * Compares Java ConcurrentHashMap vs Minis (C++) for single-key throughput.
 */
suspend fun runSingleKeyBenchmark(onUpdate: (String) -> Unit) {
    val sb = StringBuilder()
    fun log(msg: String) {
        sb.append(msg).append("\n")
        onUpdate(sb.toString())
    }

    // Metric: Total Operations = (Sets + Gets + Dels)
    val totalOps = THREAD_COUNT.toLong() * ITEMS_PER_THREAD.toLong() * 3L

    log("== Minis Single-Key Showdown ==")
    log("Threads: $THREAD_COUNT")
    log("Items/Thread: $ITEMS_PER_THREAD")
    log("Total Atomic Ops: %,d".format(totalOps))

    // --- 1. JAVA BENCHMARK (Baseline) ---
    log("\n[Java ConcurrentHashMap] Running...")

    // Using ConcurrentHashMap to match Minis's new thread-safety guarantees
    val javaMap = ConcurrentHashMap<String, String>()

    val javaStart = System.nanoTime()

    runConcurrentWorkload(THREAD_COUNT, ITEMS_PER_THREAD) { threadIdx, itemIdx ->
        val key = "key_${threadIdx}_${itemIdx}"
        val value = "val_${threadIdx}"

        // 1. SET
        javaMap[key] = value

        // 2. GET
        javaMap[key]

        // 3. DEL
        javaMap.remove(key)
    }

    val javaDuration = System.nanoTime() - javaStart
    val javaSec = javaDuration / 1_000_000_000.0
    val javaOps = (totalOps / javaSec).toLong()

    log("  Time: %.3fs".format(javaSec))
    log("  Throughput: %,d ops/sec".format(javaOps))


    // --- 2. MINIS BENCHMARK ---
    log("\n[Minis JNI Single-Key] Running...")

    try {
        Minis().use { minis ->
            val minisStart = System.nanoTime()

            runConcurrentWorkload(THREAD_COUNT, ITEMS_PER_THREAD) { threadIdx, itemIdx ->
                val key = "key_${threadIdx}_${itemIdx}"
                val value = "val_${threadIdx}"

                // 1. SET (Native Lock: Shard X)
                minis.set(key, value)

                // 2. GET (Native Lock: Shard X)
                minis.get(key)

                // 3. DEL (Native Lock: Shard X)
                minis.del(key)
            }

            val minisDuration = System.nanoTime() - minisStart
            val minisSec = minisDuration / 1_000_000_000.0
            val minisOps = (totalOps / minisSec).toLong()

            log("  Time: %.3fs".format(minisSec))
            log("  Throughput: %,d ops/sec".format(minisOps))

            log("\n--------------------------------")
            if (minisOps > 0) {
                if (minisOps > javaOps) {
                    log("WINNER: Minis is %.2fx FASTER".format(minisOps.toDouble() / javaOps.toDouble()))
                } else {
                    log("Java is %.2fx faster".format(javaOps.toDouble() / minisOps.toDouble()))
                }
            }
        }
    } catch (e: Exception) {
        log("\nFATAL ERROR: ${e.message}")
        e.printStackTrace()
    }
}

/**
 * Helper: Runs the action N times on T threads concurrently.
 * @param threads Number of threads
 * @param itemsPerThread Iterations per thread
 * @param action Callback receiving (threadIndex, itemIndex)
 */
fun runConcurrentWorkload(threads: Int, itemsPerThread: Int, action: (Int, Int) -> Unit) {
    val executor = Executors.newFixedThreadPool(threads)
    val latch = CountDownLatch(threads)

    for (i in 0 until threads) {
        executor.submit {
            try {
                for (j in 0 until itemsPerThread) {
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
