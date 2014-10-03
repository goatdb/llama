/*
 * ll_lock.h
 * LLAMA Graph Analytics
 *
 * Copyright 2014
 *      The President and Fellows of Harvard College.
 *
 * Copyright 2014
 *      Oracle Labs.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE UNIVERSITY OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */


#ifndef _LL_LOCK_H
#define _LL_LOCK_H

#include <stdint.h>

/**
 * Spinlock
 */
typedef volatile int32_t ll_spinlock_t;


/**
 * Try to acquire a spinlock
 *
 * @param ptr the spinlock
 * @return true if acquired
 */
inline bool ll_spinlock_try_acquire(ll_spinlock_t* ptr) {
    int ret = __sync_lock_test_and_set(ptr, 1);
    return ret == 0; // 0 means success
}


/**
 * Acquire a spinlock
 *
 * @param ptr the spinlock
 */
inline void ll_spinlock_acquire(ll_spinlock_t* ptr) {
    while (__sync_lock_test_and_set(ptr, 1)) {
        while (*ptr == 1) {
			asm volatile ("pause" ::: "memory");
        }
    }
}


/**
 * Release a spinlock
 *
 * @param ptr the spinlock
 */
inline void ll_spinlock_release(ll_spinlock_t* ptr) {
	__sync_synchronize();
    *ptr = 0;
}


#define LL_CACHELINE            8


/**
 * Spinlock table
 */
template <int size>					// Must be a power of 2
class ll_spinlock_table_ext {

	ll_spinlock_t ll_spinlock_tab[size * LL_CACHELINE];


public:
	
	/**
	 * Initialize
	 */
	ll_spinlock_table_ext() {
		for (int i = 0; i < size * LL_CACHELINE; i++)
			ll_spinlock_tab[i] = 0;
	}


	/**
	 * Acquire
	 *
	 * @param x the value
	 */
	template <typename T>
	void acquire_for(T x) {
		uint32_t entry_idx = (uint32_t)(x & (size - 1));
		uint32_t tab_idx = entry_idx * LL_CACHELINE;
		ll_spinlock_acquire(&ll_spinlock_tab[tab_idx]);
	}


	/**
	 * Release
	 *
	 * @param x the value
	 */
	template <typename T>
	void release_for(T x) {
		uint32_t entry_idx = (uint32_t)(x & (size - 1));
		uint32_t tab_idx = entry_idx * LL_CACHELINE;
		ll_spinlock_release(&ll_spinlock_tab[tab_idx]);
	}
};


/**
 * The default spinlock table
 */
typedef ll_spinlock_table_ext<1024> ll_spinlock_table;


#endif
