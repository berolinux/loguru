/*
 * Lock-free atomic operations for the shared-memory ring buffer.
 *
 * Compiled once at first import:
 *   gcc -shared -fPIC -O2 -o _atomic_ops.so _atomic_ops.c
 *
 * All functions operate on raw pointers into mmap'd memory so that
 * multiple processes sharing the same mapping get hardware-level
 * atomicity without any OS lock primitive.
 */

#include <stdint.h>

/* ---- 64-bit atomic helpers ---- */

uint64_t rb_atomic_load_u64(volatile void *ptr)
{
    return __atomic_load_n((volatile uint64_t *)ptr, __ATOMIC_SEQ_CST);
}

void rb_atomic_store_u64(volatile void *ptr, uint64_t val)
{
    __atomic_store_n((volatile uint64_t *)ptr, val, __ATOMIC_SEQ_CST);
}

uint64_t rb_atomic_fetch_add_u64(volatile void *ptr, uint64_t val)
{
    return __atomic_fetch_add((volatile uint64_t *)ptr, val, __ATOMIC_SEQ_CST);
}

/*
 * Compare-and-swap.  Returns the value that was at *ptr BEFORE the
 * operation.  If the return value == expected the swap succeeded.
 */
uint64_t rb_atomic_cas_u64(volatile void *ptr,
                           uint64_t expected,
                           uint64_t desired)
{
    uint64_t old = expected;
    __atomic_compare_exchange_n((volatile uint64_t *)ptr,
                                &old, desired,
                                /*weak=*/0,
                                __ATOMIC_SEQ_CST,
                                __ATOMIC_SEQ_CST);
    return old;
}

/* ---- 8-bit atomic helpers (for frame status bytes) ---- */

uint8_t rb_atomic_load_u8(volatile void *ptr)
{
    return __atomic_load_n((volatile uint8_t *)ptr, __ATOMIC_ACQUIRE);
}

void rb_atomic_store_u8(volatile void *ptr, uint8_t val)
{
    __atomic_store_n((volatile uint8_t *)ptr, val, __ATOMIC_RELEASE);
}

/* ---- memory fence ---- */

void rb_memory_fence(void)
{
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
}
