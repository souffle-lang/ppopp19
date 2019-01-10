#ifndef _HTM_INTEL_
#define _HTM_INTEL_

#include <stdio.h>
#include <string.h>

#include <immintrin.h> /* Intel RTM */

static souffle::SpinLock fallback_lock;
static int fallback_unlocked_value;

#define MAX_RETRIES 15
#define NL 1
#define EL 0
#define IS_LOCKED(lock) \
    (__atomic_load_n((long int*)&fallback_lock, __ATOMIC_SEQ_CST) != fallback_unlocked_value)

#define TX_RETRIES(num) int retries = num;
#define TX_START(type)                                \
    while (1) {                                       \
        while (IS_LOCKED(fallback_lock))              \
            ;                                         \
        unsigned status = _xbegin();                  \
        if (status == _XBEGIN_STARTED) {              \
            if (IS_LOCKED(fallback_lock)) _xabort(1); \
            break;                                    \
        } else {                                      \
            if (!(status & _XABORT_RETRY))            \
                retries = 0;                          \
            else                                      \
                retries--;                            \
        }                                             \
        if (retries <= 0) {                           \
            fallback_lock.lock();                     \
            break;                                    \
        }                                             \
    }

#define TX_START_INST(type, data)                                                                          \
    while (1) {                                                                                            \
        while (IS_LOCKED(fallback_lock))                                                                   \
            ;                                                                                              \
        data->nb_transactions++;                                                                           \
        unsigned status = _xbegin();                                                                       \
        if (status == _XBEGIN_STARTED) {                                                                   \
            if (IS_LOCKED(fallback_lock)) _xabort(1);                                                      \
            break;                                                                                         \
        } else {                                                                                           \
            data->nb_aborts++;                                                                             \
            if (!(status & _XABORT_RETRY))                                                                 \
                retries = 0;                                                                               \
            else                                                                                           \
                retries--;                                                                                 \
            if (status & _XABORT_CONFLICT) data->nb_aborts_conflict++;                                     \
            if (status & _XABORT_CAPACITY) data->nb_aborts_capacity++;                                     \
            if (status & _XABORT_EXPLICIT && _XABORT_CODE(status) == 1) data->nb_aborts_fallback_locked++; \
            if (status == 0) data->nb_aborts_unknown++;                                                    \
        }                                                                                                  \
        if (retries <= 0) {                                                                                \
            data->nb_fallbacks++;                                                                          \
            fallback_lock.lock();                                                                          \
            break;                                                                                         \
        }                                                                                                  \
    }

#define TX_END                  \
    if (retries > 0) {          \
        _xend();                \
    } else {                    \
        fallback_lock.unlock(); \
    }

#define TX_LOAD(addr) (*(addr))
#define TX_STORE(addr, value) (*(addr) = (value))
#define MALLOC(size) malloc(size)
#define FREE(addr, size) free(addr)
#define TM_CALLABLE __attribute__((tm_callable))

#endif
