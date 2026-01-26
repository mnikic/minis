#include <jni.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>

#include "cache/minis.h"
#include "cache/t_string.h"
#include "cache/hash.h"
#include "common/macros.h"
#include "cache/t_hash.h"

#ifdef __ANDROID__
#include <android/log.h>
#define LOG_TAG "MinisJNI"
#define LOGI(...) __android_log_print(ANDROID_LOG_INFO, LOG_TAG, __VA_ARGS__)
#define LOGE(...) __android_log_print(ANDROID_LOG_ERROR, LOG_TAG, __VA_ARGS__)
#else
#define LOGI(...) do { printf("INFO: "); printf(__VA_ARGS__); printf("\n"); } while(0)
    #define LOGE(...) do { fprintf(stderr, "ERROR: "); fprintf(stderr, __VA_ARGS__); fprintf(stderr, "\n"); } while(0)
#endif

typedef struct {
    Minis* minis;            // The actual DB engine
    pthread_t bg_thread;     // The maintenance thread handle
    volatile int is_running; // The "Stop" flag
    char *base_path;         // NEW: Directory for background saving
} MinisContext;

static ALWAYS_INLINE void
throw_exception(JNIEnv *env, const char *className, const char *msg) {
    jclass exClass = (*env)->FindClass(env, className);
    if (exClass) (*env)->ThrowNew(env, exClass, msg);
}

static ALWAYS_INLINE uint64_t
get_us (void) {
    struct timespec tvs = { 0, 0 };
    clock_gettime (CLOCK_MONOTONIC, &tvs);
    return ((tvs.tv_sec * 1000000) + (tvs.tv_nsec / 1000));
}

static void* maintenance_thread(void* arg) {
    MinisContext *ctx = (MinisContext*)arg;
    
    int ticks = 0;

    while (ctx->is_running) {
        // 10Hz Heartbeat (100ms)
        usleep(100000);

        if (!ctx->is_running) break;

        uint64_t now = get_us ();

        // Task A: Eviction (Run every 100ms)
        // This is fast CPU work.
        minis_evict(ctx->minis, now);

        // Task B: Incremental Persistence (Run every 1 second)
        // We don't want to hammer the disk check every 100ms.
        if (++ticks % 10 == 0) {
            if (ctx->base_path) {
                // This function is "smart" - it sorts by dirtiness 
                // and releases locks between shards. Safe to run here.
                minis_incremental_save (ctx->minis, ctx->base_path, now);
            }
        }
    }
    LOGI("Minis Background Thread Stopped Cleanly.");
    return NULL;
}

JNIEXPORT jlong JNICALL
Java_com_minis_Minis_nativeInit(JNIEnv *env, jobject thiz, jstring jBasePath) {
    (void)env;
    (void)thiz;
    MinisContext *ctx = malloc(sizeof(MinisContext));

    ctx->minis = minis_init();
    ctx->is_running = 1;

    // Save the path for the background thread
    const char *path = (*env)->GetStringUTFChars(env, jBasePath, 0);
    ctx->base_path = strdup(path);
    (*env)->ReleaseStringUTFChars(env, jBasePath, path);

    if (pthread_create(&ctx->bg_thread, NULL, maintenance_thread, ctx) != 0) {
        LOGE("Failed to create background thread");
        free(ctx->base_path);
        minis_free(ctx->minis);
        free(ctx);
        return 0;
    }

    return (jlong) ctx;
}

JNIEXPORT void JNICALL
Java_com_minis_Minis_nativeFree(JNIEnv *env, jobject thiz, jlong ptr) {
    (void) env;
    (void) thiz;
    MinisContext *ctx = (MinisContext *) ptr;
    if (ctx) {
        LOGI("Stopping Minis...");

        ctx->is_running = 0;
        pthread_join(ctx->bg_thread, NULL);
        
        if (ctx->base_path) free(ctx->base_path);
        minis_free(ctx->minis);

        free(ctx);
        LOGI("Minis Stopped.");
    }
}

JNIEXPORT void JNICALL
Java_com_minis_Minis_nativeSync(JNIEnv *env, jobject thiz, jlong ptr) {
    (void) env;
    (void) thiz;
    MinisContext *ctx = (MinisContext *) ptr;
    if (ctx && ctx->base_path) {
        // Trigger the logic immediately without waiting for the thread tick
        minis_incremental_save (ctx->minis, ctx->base_path, get_us());
    }
}

#define GET_MINIS(ptr) (((MinisContext*)(ptr))->minis)

JNIEXPORT void JNICALL
Java_com_minis_Minis_nativeSet(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey, jstring jVal) {
    (void) thiz;
    Minis *minis = GET_MINIS(ptr); // Unwrap

    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);
    const char *val = (*env)->GetStringUTFChars(env, jVal, 0);

    minis_set(minis, key, val, get_us());

    (*env)->ReleaseStringUTFChars(env, jKey, key);
    (*env)->ReleaseStringUTFChars(env, jVal, val);
}

typedef struct {
    char *value_copy; // We will malloc here inside lock
} JniGetCtx;

static bool
jni_get_visitor_fast(const Entry *ent, void *ctx)
{
    JniGetCtx *jctx = (JniGetCtx *)ctx;
    if (ent && ent->val) {
        jctx->value_copy = strdup(ent->val); 
    }
    return true;
}

JNIEXPORT jstring JNICALL
Java_com_minis_Minis_nativeGet(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey)
{
    (void) thiz;
    Minis *minis = GET_MINIS(ptr);
    const char *key = (*env)->GetStringUTFChars(env, jKey, NULL);
    
    JniGetCtx ctx = { .value_copy = NULL };
    
    // 1. LOCK -> COPY -> UNLOCK
    // This critical section is now extremely short (just a malloc/memcpy)
    MinisError err = minis_get(minis, key, jni_get_visitor_fast, &ctx, get_us());
    
    (*env)->ReleaseStringUTFChars(env, jKey, key);

    jstring result = NULL;

    if (err == MINIS_OK && ctx.value_copy) {
        // 2. Heavy JNI allocation happens OUTSIDE the lock
        // Other threads can now use the shard!
        result = (*env)->NewStringUTF(env, ctx.value_copy);
        free(ctx.value_copy); // Clean up temp buffer
    } else if (err == MINIS_ERR_TYPE) {
         // handle error...
    }

    return result;
}

JNIEXPORT jint JNICALL
Java_com_minis_Minis_nativeDel(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey) {
    (void) thiz;
    Minis *minis = GET_MINIS(ptr); // Unwrap
    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);

    int deleted = 0;
    minis_del(minis, key, &deleted, get_us());

    (*env)->ReleaseStringUTFChars(env, jKey, key);
    return (jint) deleted;
}

JNIEXPORT jint JNICALL
Java_com_minis_Minis_nativePexpire(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey, jlong ttlMs) {
    (void) thiz;
    Minis *minis = GET_MINIS(ptr); // Unwrap
    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);

    int set = 0;
    minis_expire(minis, key, (int64_t)ttlMs, &set, get_us());

    (*env)->ReleaseStringUTFChars(env, jKey, key);
    return (jint) set;
}

JNIEXPORT jlong JNICALL
Java_com_minis_Minis_nativePttl(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey) {
    (void) thiz;
    Minis *minis = GET_MINIS(ptr);
    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);

    int64_t ttl = 0;
    minis_ttl(minis, key, &ttl, get_us());

    (*env)->ReleaseStringUTFChars(env, jKey, key);
    return (jlong) ttl;
}

JNIEXPORT jboolean JNICALL
Java_com_minis_Minis_nativeSave(JNIEnv *env, jobject thiz, jlong ptr, jstring jPath) {
    (void) thiz;
    Minis *minis = GET_MINIS(ptr);
    const char *path = (*env)->GetStringUTFChars(env, jPath, 0);

    MinisError err = minis_save(minis, path, get_us());

    (*env)->ReleaseStringUTFChars(env, jPath, path);
    return (err == MINIS_OK);
}

JNIEXPORT jboolean JNICALL
Java_com_minis_Minis_nativeLoad(JNIEnv *env, jobject thiz, jlong ptr, jstring jPath) {
    (void) thiz;
    Minis *minis = GET_MINIS(ptr);
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
jni_mget_visitor(const Entry *ent, void *ctx) {
    JniMGetContext *jctx = (JniMGetContext*) ctx;

    const char *valStr = NULL;
    if (ent && ent->type == T_STR) {
        valStr = ent->val;
    }
    if (valStr) {
        jstring jVal = (*jctx->env)->NewStringUTF(jctx->env, valStr);
        (*jctx->env)->SetObjectArrayElement(jctx->env, jctx->jArray, jctx->index, jVal);
        (*jctx->env)->DeleteLocalRef(jctx->env, jVal);
    }
    // Else: We do nothing. The JNI Object Array is initialized with NULLs by default.

    jctx->index++;
    return true; 
}

JNIEXPORT jobjectArray JNICALL
Java_com_minis_Minis_nativeMGet(JNIEnv *env, jobject thiz, jlong ptr, jobjectArray jKeys) {
    (void) thiz;
    Minis *minis = GET_MINIS(ptr);
    jsize count = (*env)->GetArrayLength(env, jKeys);

    const char **cKeys = malloc(sizeof(char*) * count);
    for (int i = 0; i < count; i++) {
        jstring js = (jstring) (*env)->GetObjectArrayElement(env, jKeys, i);
        cKeys[i] = (*env)->GetStringUTFChars(env, js, 0);
    }

    jclass stringClass = (*env)->FindClass(env, "java/lang/String");
    jobjectArray result = (*env)->NewObjectArray(env, count, stringClass, NULL);

    JniMGetContext visitorCtx = {
            .env = env,
            .jArray = result,
            .index = 0
    };

    minis_mget(minis, cKeys, count, jni_mget_visitor, &visitorCtx, get_us());

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
    (void) thiz;
    Minis *minis = GET_MINIS(ptr);
    jsize count = (*env)->GetArrayLength(env, jKeys);

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

    uint64_t deleted = 0;
    if (!failed) {
        minis_mdel(minis, cKeys, count, &deleted, get_us());
    }

    for (int i = 0; i < successCount; i++) {
        (*env)->ReleaseStringUTFChars(env, jStrings[i], cKeys[i]);
        (*env)->DeleteLocalRef(env, jStrings[i]);
    }

    if (failed && jStrings[successCount] != NULL) {
        (*env)->DeleteLocalRef(env, jStrings[successCount]);
    }

    free(cKeys);
    free(jStrings);

    return (jlong)deleted;
}

JNIEXPORT void JNICALL
Java_com_minis_Minis_nativeMSet(JNIEnv *env, jobject thiz, jlong ptr, jobjectArray jKeyVals) {
    (void) thiz;
    Minis *minis = GET_MINIS(ptr);
    jsize count = (*env)->GetArrayLength(env, jKeyVals);

    if (count % 2 != 0) return;

    const char **cKeyVals = malloc(sizeof(char*) * count);
    jstring *jStrings = malloc(sizeof(jstring) * count);

    if (!cKeyVals || !jStrings) {
        if(cKeyVals) free(cKeyVals);
        if(jStrings) free(jStrings);
        return;
    }

    bool failed = false;
    int successCount = 0;

    for (int i = 0; i < count; i++) {
        jstring js = (jstring) (*env)->GetObjectArrayElement(env, jKeyVals, i);
        jStrings[i] = js;

        cKeyVals[i] = (*env)->GetStringUTFChars(env, js, 0);

        if (cKeyVals[i] == NULL) {
            failed = true;
            break;
        }
        successCount++;
    }

    if (!failed) {
        minis_mset(minis, cKeyVals, (size_t)count, get_us());
    }

    for (int i = 0; i < successCount; i++) {
        (*env)->ReleaseStringUTFChars(env, jStrings[i], cKeyVals[i]);
        (*env)->DeleteLocalRef(env, jStrings[i]); // Now we delete the saved ref
    }

    if (failed && jStrings[successCount] != NULL) {
        (*env)->DeleteLocalRef(env, jStrings[successCount]);
    }

    free(cKeyVals);
    free(jStrings);
}

JNIEXPORT jint JNICALL
Java_com_minis_Minis_nativeHSet(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey, jstring jField, jstring jValue) {
    (void) thiz;
    Minis *minis = GET_MINIS(ptr);
    
    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);
    const char *field = (*env)->GetStringUTFChars(env, jField, 0);
    const char *value = (*env)->GetStringUTFChars(env, jValue, 0);

    int added = 0;
    MinisError err = minis_hset(minis, key, field, value, &added, get_us());

    (*env)->ReleaseStringUTFChars(env, jKey, key);
    (*env)->ReleaseStringUTFChars(env, jField, field);
    (*env)->ReleaseStringUTFChars(env, jValue, value);

    if (err == MINIS_ERR_OOM) {
        throw_exception(env, "java/lang/OutOfMemoryError", "Minis OOM in HSET");
        return 0;
    }
    if (err == MINIS_ERR_TYPE) {
        throw_exception(env, "com/minis/MinisException", "WRONGTYPE Operation against a key holding the wrong kind of value");
        return 0;
    }

    return (jint)added;
}

typedef struct {
    JNIEnv *env;
    jstring result;
} JniHGetCtx;

static bool jni_hget_visitor(const HashEntry *entry, void *arg) {
    JniHGetCtx *ctx = (JniHGetCtx *)arg;
    if (entry && entry->value) {
        ctx->result = (*ctx->env)->NewStringUTF(ctx->env, entry->value);
    }
    return true; // Stop after one? actually hget only calls visitor once.
}

JNIEXPORT jstring JNICALL
Java_com_minis_Minis_nativeHGet(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey, jstring jField) {
    (void) thiz;
    Minis *minis = GET_MINIS(ptr);
    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);
    const char *field = (*env)->GetStringUTFChars(env, jField, 0);

    JniHGetCtx ctx = { .env = env, .result = NULL };
    
    // Using the Visitor version of hget you implemented earlier
    MinisError err = minis_hget(minis, key, field, jni_hget_visitor, &ctx, get_us());

    (*env)->ReleaseStringUTFChars(env, jKey, key);
    (*env)->ReleaseStringUTFChars(env, jField, field);

    if (err == MINIS_ERR_NIL) return NULL;
    if (err == MINIS_ERR_TYPE) {
        throw_exception(env, "com/minis/MinisException", "WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    
    return ctx.result;
}

JNIEXPORT jint JNICALL
Java_com_minis_Minis_nativeHDel(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey, jobjectArray jFields) {
    (void) thiz;
    Minis *minis = GET_MINIS(ptr);
    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);
    
    jsize count = (*env)->GetArrayLength(env, jFields);
    
    // Prepare C array of fields
    const char **cFields = malloc(sizeof(char*) * count);
    jstring *jStrRefs = malloc(sizeof(jstring) * count); // Keep refs to release later
    
    if (!cFields || !jStrRefs) {
         if (cFields) free(cFields);
         if (jStrRefs) free(jStrRefs);
         (*env)->ReleaseStringUTFChars(env, jKey, key);
         return 0; // OOM
    }

    for(int i=0; i<count; i++) {
        jstring js = (jstring)(*env)->GetObjectArrayElement(env, jFields, i);
        jStrRefs[i] = js;
        cFields[i] = (*env)->GetStringUTFChars(env, js, 0);
    }

    int deleted = 0;
    MinisError err = minis_hdel(minis, key, cFields, count, &deleted, get_us());

    // Cleanup
    for(int i=0; i<count; i++) {
        (*env)->ReleaseStringUTFChars(env, jStrRefs[i], cFields[i]);
        (*env)->DeleteLocalRef(env, jStrRefs[i]);
    }
    free(cFields);
    free(jStrRefs);
    (*env)->ReleaseStringUTFChars(env, jKey, key);

    if (err == MINIS_ERR_TYPE) {
        throw_exception(env, "com/minis/MinisException", "WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    return (jint)deleted;
}

JNIEXPORT jboolean JNICALL
Java_com_minis_Minis_nativeHExists(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey, jstring jField) {
    (void) thiz;
    Minis *minis = GET_MINIS(ptr);
    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);
    const char *field = (*env)->GetStringUTFChars(env, jField, 0);

    int exists = 0;
    MinisError err = minis_hexists(minis, key, field, &exists, get_us());

    (*env)->ReleaseStringUTFChars(env, jKey, key);
    (*env)->ReleaseStringUTFChars(env, jField, field);

    if (err == MINIS_ERR_TYPE) {
        throw_exception(env, "com/minis/MinisException", "WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    return (jboolean)(exists == 1);
}

JNIEXPORT jlong JNICALL
Java_com_minis_Minis_nativeHLen(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey) {
    (void) thiz;
    Minis *minis = GET_MINIS(ptr);
    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);

    size_t len = 0;
    MinisError err = minis_hlen(minis, key, &len, get_us());

    (*env)->ReleaseStringUTFChars(env, jKey, key);

    if (err == MINIS_ERR_NIL) return 0;
    if (err == MINIS_ERR_TYPE) {
        throw_exception(env, "com/minis/MinisException", "WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    return (jlong)len;
}

typedef struct {
    JNIEnv *env;
    jobject hashMap;
    jmethodID putMethod;
} JniHGetAllCtx;

static bool jni_hgetall_visitor(const HashEntry *entry, void *arg) {
    JniHGetAllCtx *ctx = (JniHGetAllCtx *)arg;
    
    jstring jField = (*ctx->env)->NewStringUTF(ctx->env, entry->field);
    jstring jValue = (*ctx->env)->NewStringUTF(ctx->env, entry->value);

    (*ctx->env)->CallObjectMethod(ctx->env, ctx->hashMap, ctx->putMethod, jField, jValue);

    (*ctx->env)->DeleteLocalRef(ctx->env, jField);
    (*ctx->env)->DeleteLocalRef(ctx->env, jValue);
    
    return true;
}

JNIEXPORT jobject JNICALL
Java_com_minis_Minis_nativeHGetall(JNIEnv *env, jobject thiz, jlong ptr, jstring jKey) {
    (void) thiz;
    Minis *minis = GET_MINIS(ptr);
    const char *key = (*env)->GetStringUTFChars(env, jKey, 0);

    // 1. Create a new java.util.HashMap
    jclass mapClass = (*env)->FindClass(env, "java/util/HashMap");
    jmethodID initMethod = (*env)->GetMethodID(env, mapClass, "<init>", "()V");
    jobject hashMap = (*env)->NewObject(env, mapClass, initMethod);
    
    // 2. Get the Map.put method ID
    jmethodID putMethod = (*env)->GetMethodID(env, mapClass, "put", 
        "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

    JniHGetAllCtx ctx = {
        .env = env,
        .hashMap = hashMap,
        .putMethod = putMethod
    };

    MinisError err = minis_hgetall(minis, key, jni_hgetall_visitor, &ctx, get_us());

    (*env)->ReleaseStringUTFChars(env, jKey, key);

    if (err == MINIS_ERR_TYPE) {
        throw_exception(env, "com/minis/MinisException", "WRONGTYPE Operation against a key holding the wrong kind of value");
        return NULL;
    }
    // Note: If key is missing (NIL), minis_hgetall usually returns NIL or just doesn't call visitor.
    // In both cases, we return an empty Map, which is standard Redis client behavior.

    return hashMap;
}
