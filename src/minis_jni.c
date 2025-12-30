#include <jni.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h> // Required for clock_gettime

#include "cache/minis.h" // Your Core Public API
#include "cache/t_string.h"
#include "common/macros.h"

#ifdef __ANDROID__
#include <android/log.h>
#define LOG_TAG "MinisJNI"
#define LOGI(...) __android_log_print(ANDROID_LOG_INFO, LOG_TAG, __VA_ARGS__)
#define LOGE(...) __android_log_print(ANDROID_LOG_ERROR, LOG_TAG, __VA_ARGS__)
#else
#define LOGI(...) do { printf("INFO: "); printf(__VA_ARGS__); printf("\n"); } while(0)
    #define LOGE(...) do { fprintf(stderr, "ERROR: "); fprintf(stderr, __VA_ARGS__); fprintf(stderr, "\n"); } while(0)
#endif

// --- 1. The Context Wrapper ---
// This lets us control the thread without modifying the core Minis struct.
typedef struct {
    Minis* minis;            // The actual DB engine
    pthread_t bg_thread;     // The maintenance thread handle
    volatile int is_running; // The "Stop" flag
} MinisContext;

static ALWAYS_INLINE uint64_t
get_us (void) {
    struct timespec tvs = { 0, 0 };
    clock_gettime (CLOCK_MONOTONIC, &tvs);
    return (uint64_t) ((tvs.tv_sec * 1000000) + (tvs.tv_nsec / 1000));
}

// --- 2. The Safe Background Thread ---
static void* maintenance_thread(void* arg) {
    MinisContext *ctx = (MinisContext*)arg; // We receive the Context, not just Minis
    int ticks = 0;

    // Check the flag on every loop iteration
    while (ctx->is_running) {

        // We sleep in small chunks so we can check 'is_running' frequently
        // (Sleeping 100ms is fine for this benchmark)
        usleep(100000);

        // Double check flag after waking up before doing work
        if (!ctx->is_running) break;

        uint64_t now = get_us ();

        if (ticks % 10 == 0) {
            LOGI("Minis Tick: Running Eviction at %lu", now);
        }

        // Safe because nativeFree waits for us to finish before destroying the lock
        minis_evict(ctx->minis, now);
        ticks++;
    }
    LOGI("Minis Background Thread Stopped Cleanly.");
    return NULL;
}

// --- JNI Implementation ---

JNIEXPORT jlong JNICALL
Java_com_minis_Minis_nativeInit(JNIEnv *env, jobject thiz) {
    // 1. Allocate the Context Wrapper
    MinisContext *ctx = malloc(sizeof(MinisContext));

    // 2. Initialize Core
    ctx->minis = minis_init();
    ctx->is_running = 1;

    // 3. Spawn Background Thread (Pass 'ctx' as argument)
    // We DO NOT detach it, because we need to join (wait) for it later.
    if (pthread_create(&ctx->bg_thread, NULL, maintenance_thread, ctx) != 0) {
        LOGE("Failed to create background thread");
        // Handle error cleanup if needed
    }

    // 4. Return pointer to the CONTEXT (not the engine directly)
    return (jlong) ctx;
}

JNIEXPORT void JNICALL
Java_com_minis_Minis_nativeFree(JNIEnv *env, jobject thiz, jlong ptr) {
    MinisContext *ctx = (MinisContext *) ptr;
    if (ctx) {
        LOGI("Stopping Minis...");

        // 1. Tell thread to stop
        ctx->is_running = 0;

        // 2. WAIT for thread to actually finish (Critical Fix)
        // This ensures minis_evict() is never called after we destroy the lock below.
        pthread_join(ctx->bg_thread, NULL);

        // 3. Now it is safe to destroy the core engine
        minis_free(ctx->minis);

        // 4. Free the wrapper
        free(ctx);
        LOGI("Minis Stopped.");
    }
}

// --- Helper Macro to Unwrap Context ---
#define GET_MINIS(ptr) (((MinisContext*)(ptr))->minis)

JNIEXPORT void JNICALL
Java_com_minis_Minis_nativeSet(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey, jstring jVal) {
    Minis *minis = GET_MINIS(ptr); // Unwrap

    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);
    const char *val = (*env)->GetStringUTFChars(env, jVal, 0);

    minis_set(minis, key, val, get_us());

    (*env)->ReleaseStringUTFChars(env, jKey, key);
    (*env)->ReleaseStringUTFChars(env, jVal, val);
}

JNIEXPORT jstring JNICALL
Java_com_minis_Minis_nativeGet(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey) {
    Minis *minis = GET_MINIS(ptr); // Unwrap
    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);
    const char *val = NULL;

    MinisError err = minis_get(minis, key, &val, get_us());

    (*env)->ReleaseStringUTFChars(env, jKey, key);

    if (err == MINIS_OK && val != NULL) {
        return (*env)->NewStringUTF(env, val);
    }
    return NULL;
}

JNIEXPORT jint JNICALL
Java_com_minis_Minis_nativeDel(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey) {
    Minis *minis = GET_MINIS(ptr); // Unwrap
    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);

    int deleted = 0;
    minis_del(minis, key, &deleted, get_us());

    (*env)->ReleaseStringUTFChars(env, jKey, key);
    return (jint) deleted;
}

JNIEXPORT jint JNICALL
Java_com_minis_Minis_nativePexpire(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey, jlong ttlMs) {
    Minis *minis = GET_MINIS(ptr); // Unwrap
    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);

    int set = 0;
    minis_expire(minis, key, (int64_t)ttlMs, &set, get_us());

    (*env)->ReleaseStringUTFChars(env, jKey, key);
    return (jint) set;
}

JNIEXPORT jlong JNICALL
Java_com_minis_Minis_nativePttl(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey) {
    Minis *minis = GET_MINIS(ptr); // Unwrap
    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);

    int64_t ttl = 0;
    minis_ttl(minis, key, &ttl, get_us());

    (*env)->ReleaseStringUTFChars(env, jKey, key);
    return (jlong) ttl;
}

JNIEXPORT jboolean JNICALL
Java_com_minis_Minis_nativeSave(JNIEnv *env, jobject thiz, jlong ptr, jstring jPath) {
    Minis *minis = GET_MINIS(ptr); // Unwrap
    const char *path = (*env)->GetStringUTFChars(env, jPath, 0);

    MinisError err = minis_save(minis, path, get_us());

    (*env)->ReleaseStringUTFChars(env, jPath, path);
    return (err == MINIS_OK);
}

JNIEXPORT jboolean JNICALL
Java_com_minis_Minis_nativeLoad(JNIEnv *env, jobject thiz, jlong ptr, jstring jPath) {
    Minis *minis = GET_MINIS(ptr); // Unwrap
    const char *path = (*env)->GetStringUTFChars(env, jPath, 0);

    MinisError err = minis_load(minis, path, get_us());

    (*env)->ReleaseStringUTFChars(env, jPath, path);
    return (err == MINIS_OK);
}

// --- JNI Visitor Adapter ---

typedef struct {
    JNIEnv *env;
    jobjectArray jArray;
    int index;
} JniMGetContext;

// The Visitor that writes to a Java Array
static bool
jni_mget_visitor(const char *val, void *ctx) {
    JniMGetContext *jctx = (JniMGetContext*) ctx;

    if (val) {
        // Create a Java String from the C pointer
        jstring jVal = (*jctx->env)->NewStringUTF(jctx->env, val);
        // Put it directly into the array
        (*jctx->env)->SetObjectArrayElement(jctx->env, jctx->jArray, jctx->index, jVal);
        // Clean up local ref
        (*jctx->env)->DeleteLocalRef(jctx->env, jVal);
    }
    // If val is NULL, we do nothing. The array index is already null by default.

    jctx->index++;
    return true; // Always continue in JNI (unless OOM, but hard to detect here)
}

// --- JNI MGET Implementation ---

JNIEXPORT jobjectArray JNICALL
Java_com_minis_Minis_nativeMGet(JNIEnv *env, jobject thiz, jlong ptr, jobjectArray jKeys) {
    Minis *minis = GET_MINIS(ptr); // (Using the context wrapper from before)
    jsize count = (*env)->GetArrayLength(env, jKeys);

    // 1. Prepare C Keys (same as before)
    const char **cKeys = malloc(sizeof(char*) * count);
    for (int i = 0; i < count; i++) {
        jstring js = (jstring) (*env)->GetObjectArrayElement(env, jKeys, i);
        cKeys[i] = (*env)->GetStringUTFChars(env, js, 0);
    }

    // 2. Prepare Result Array
    jclass stringClass = (*env)->FindClass(env, "java/lang/String");
    jobjectArray result = (*env)->NewObjectArray(env, count, stringClass, NULL);

    // 3. Prepare Visitor Context
    JniMGetContext visitorCtx = {
            .env = env,
            .jArray = result,
            .index = 0
    };

    // 4. Call Core (Uses your new Visitor API!)
    minis_mget(minis, cKeys, count, jni_mget_visitor, &visitorCtx, get_us());

    // 5. Cleanup Keys
    for (int i = 0; i < count; i++) {
        jstring js = (jstring) (*env)->GetObjectArrayElement(env, jKeys, i);
        (*env)->ReleaseStringUTFChars(env, js, cKeys[i]);
        (*env)->DeleteLocalRef(env, js);
    }
    free(cKeys);

    return result;
}

JNIEXPORT jlong JNICALL
Java_com_minis_Minis_nativeMDel(JNIEnv *env, jobject thiz, jlong ptr, jobjectArray jKeys) {
    Minis *minis = GET_MINIS(ptr);
    jsize count = (*env)->GetArrayLength(env, jKeys);

    // 1. Allocate for Keys AND Refs
    const char **cKeys = malloc(sizeof(char*) * count);
    jstring *jStrings = malloc(sizeof(jstring) * count);

    if (!cKeys || !jStrings) {
        if(cKeys) free(cKeys);
        if(jStrings) free(jStrings);
        return 0;
    }

    bool failed = false;
    int successCount = 0;

    for (int i = 0; i < count; i++) {
        jstring js = (jstring) (*env)->GetObjectArrayElement(env, jKeys, i);
        jStrings[i] = js; // Save ref

        cKeys[i] = (*env)->GetStringUTFChars(env, js, 0);

        if (cKeys[i] == NULL) {
            failed = true;
            break;
        }
        successCount++;
    }

    // 2. Call Core
    uint64_t deleted = 0;
    if (!failed) {
        minis_mdel(minis, cKeys, count, &deleted, get_us());
    }

    // 3. Cleanup
    for (int i = 0; i < successCount; i++) {
        (*env)->ReleaseStringUTFChars(env, jStrings[i], cKeys[i]);
        (*env)->DeleteLocalRef(env, jStrings[i]);
    }

    // Clean up the partial failure item if it exists
    if (failed && jStrings[successCount] != NULL) {
        (*env)->DeleteLocalRef(env, jStrings[successCount]);
    }

    free(cKeys);
    free(jStrings);

    return (jlong)deleted;
}

JNIEXPORT void JNICALL
Java_com_minis_Minis_nativeMSet(JNIEnv *env, jobject thiz, jlong ptr, jobjectArray jKeyVals) {
    Minis *minis = GET_MINIS(ptr);
    jsize count = (*env)->GetArrayLength(env, jKeyVals);

    if (count % 2 != 0) return;

    // 1. Allocate arrays for C strings AND Java string references
    // We MUST save jStrings to release them correctly later.
    const char **cKeyVals = malloc(sizeof(char*) * count);
    jstring *jStrings = malloc(sizeof(jstring) * count);

    // Safety: OOM check
    if (!cKeyVals || !jStrings) {
        if(cKeyVals) free(cKeyVals);
        if(jStrings) free(jStrings);
        return;
    }

    bool failed = false;
    int successCount = 0;

    for (int i = 0; i < count; i++) {
        // Get and SAVE the reference
        jstring js = (jstring) (*env)->GetObjectArrayElement(env, jKeyVals, i);
        jStrings[i] = js;

        // Convert
        cKeyVals[i] = (*env)->GetStringUTFChars(env, js, 0);

        // Safety: Check for NULL (JNI OOM)
        if (cKeyVals[i] == NULL) {
            failed = true;
            break;
        }
        successCount++;
    }

    // 2. Call Core (Only if conversion succeeded)
    if (!failed) {
        minis_mset(minis, cKeyVals, (size_t)count, get_us());
    }

    // 3. Cleanup using the SAVED references
    // We only clean up what we successfully allocated
    for (int i = 0; i < successCount; i++) {
        (*env)->ReleaseStringUTFChars(env, jStrings[i], cKeyVals[i]);
        (*env)->DeleteLocalRef(env, jStrings[i]); // Now we delete the saved ref
    }

    // If we failed in the middle, we still need to release the jstring causing the failure
    if (failed && jStrings[successCount] != NULL) {
        (*env)->DeleteLocalRef(env, jStrings[successCount]);
    }

    free(cKeyVals);
    free(jStrings);
}
