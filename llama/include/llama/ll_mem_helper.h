/*
 * ll_mem_helper.h
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


#ifndef _LL_MEM_HELPER_H
#define _LL_MEM_HELPER_H

#include <cassert>
#include <cstdlib>
#include <vector>

#include "llama/ll_lock.h"
#include "llama/ll_utils.h"

#define LL_MEM_POOL_ALIGN_BITS				3
#define	LL_MEM_POOL_ALIGN					(1 << (LL_MEM_POOL_ALIGN_BITS))


/**
 * Memory pool
 */
class ll_memory_pool {

	size_t _chunk_size;
	ssize_t _retain_max;

	std::vector<void*> _buffers;

	ll_spinlock_t _lock;

	size_t _last_used;
	size_t _chunk_index;


public:

	/**
	 * Initialize
	 *
	 * @param chunk_size the chunk size
	 * @param retain_max the maximum number of chunks to retain (-1 = all)
	 */
	ll_memory_pool(size_t chunk_size = 32 * 1048576ul,
			ssize_t retain_max = -1) {

		_chunk_size = chunk_size;
		_retain_max = retain_max;

		_chunk_index = 0;
		_last_used = 0;
		_lock = 0;
	}


	/**
	 * Destroy
	 */
	~ll_memory_pool() {

		for (int i = ((int) _buffers.size()) - 1; i >= 0; i--) {
			::free(_buffers[i]);
		}
	}


	/**
	 * Get the chunk size
	 *
	 * @return the chunk size in bytes
	 */
	inline size_t chunk_size() const {
		return _chunk_size;
	}


	/**
	 * Free the entire memory pool
	 *
	 * @param retain true to retain all allocated buffers (default)
	 */
	void free(bool retain=true) {

		ll_spinlock_acquire(&_lock);

		if (_retain_max >= 0 || !retain) {
			ssize_t m = retain ? _retain_max : 0;
			for (ssize_t i = ((ssize_t) _buffers.size()) - 1; i >= m; i--) {
				::free(_buffers[i]);
			}
			_buffers.resize(std::min((size_t) _retain_max, _buffers.size()));
		}

		_chunk_index = 0;
		_last_used = 0;

		ll_spinlock_release(&_lock);
	}


	/**
	 * Get a pointer to the given location within the buffer
	 *
	 * @param chunk the chunk number
	 * @param offset the given offset
	 * @return the pointer
	 */
	void* pointer(size_t chunk, size_t offset) {
		assert(chunk <= _chunk_index && offset < _chunk_size);
		return ((char*) _buffers[chunk]) + offset;
	}


	/**
	 * Allocate memory
	 *
	 * @param num the number of elements
	 * @param o_chunk the pointer to store the chunk number
	 * @param o_offset the pointer to store the offset within the chunk
	 * @return the allocated memory
	 */
	template<typename T> T* allocate(size_t num=1, size_t* o_chunk=NULL,
			size_t* o_offset=NULL) {
		
		size_t bytes = sizeof(T) * num;
		ll_spinlock_acquire(&_lock);

		if (bytes > _chunk_size) {
			LL_E_PRINT("The allocation is too large\n");
			abort();
		}

		if (_buffers.empty()) {
			void* b = malloc(_chunk_size);
			if (b == NULL) {
				LL_E_PRINT("** OUT OF MEMORY **\n");
				abort();
			}
			_buffers.push_back(b);
			_last_used = 0;
		}

		void* p = ((char*) _buffers[_chunk_index]) + _last_used;
		_last_used += bytes;

		size_t lu_remainder = _last_used & ((1ul << LL_MEM_POOL_ALIGN_BITS) - 1);
		if (lu_remainder != 0) _last_used += LL_MEM_POOL_ALIGN - lu_remainder;

		if (_last_used > _chunk_size) {
			_chunk_index++;
			if (_chunk_index < _buffers.size()) {
				p = _buffers[_chunk_index];
			}
			else {
				p = malloc(_chunk_size);
				if (p == NULL) {
					LL_E_PRINT("** OUT OF MEMORY **\n");
					abort();
				}
				_buffers.push_back(p);
			}
			_last_used = bytes;
		}

		if (o_chunk  != NULL) *o_chunk  = _chunk_index;
		if (o_offset != NULL) *o_offset = _last_used - bytes;
		
		ll_spinlock_release(&_lock);

		return (T*) p;
	}
};


/**
 * Memory pool for a small number of very large allocations of similar sizes
 */
class ll_memory_pool_for_large_allocations {

	typedef struct {
		size_t b_size;
		volatile bool b_in_use;
		void* b_buffer;
	} buffer_t;

	std::vector<buffer_t> _buffers;
	ll_spinlock_t _lock;
	double _overprovision;


public:

	/**
	 * Create an instance of type ll_memory_pool_for_large_allocations
	 */
	ll_memory_pool_for_large_allocations() {
		_lock = 0;
		_overprovision = 0.2;
	}


	/**
	 * Destroy the pool
	 */
	~ll_memory_pool_for_large_allocations() {
		for (size_t i = 0; i < _buffers.size(); i++) {
			if (_buffers[i].b_buffer != NULL) ::free(_buffers[i].b_buffer);
		}
	}


	/**
	 * Allocate a buffer
	 *
	 * @param size the size
	 * @return the buffer
	 */
	void* allocate(size_t size) {

		ll_spinlock_acquire(&_lock);


		// Find the smallest available buffer that would work

		size_t best = (size_t) -1;
		for (size_t i = 0; i < _buffers.size(); i++) {
			buffer_t& b = _buffers[i];
			if (b.b_in_use || b.b_buffer == NULL) continue;

			if (size <= b.b_size) {
				if (best == (size_t) -1) {
					best = i;
				}
				else {
					if (b.b_size < _buffers[best].b_size) {
						best = i;
					}
				}
			}
		}

		if (best != (size_t) -1) {
			buffer_t& b = _buffers[best];
			b.b_in_use = true;
			ll_spinlock_release(&_lock);
			return b.b_buffer;
		}


		// Find the largest buffer to realloc - or make a new allocation

		for (size_t i = 0; i < _buffers.size(); i++) {
			buffer_t& b = _buffers[i];
			if (b.b_in_use || b.b_buffer == NULL) continue;

			if (best == (size_t) -1) {
				best = i;
			}
			else {
				if (b.b_size > _buffers[best].b_size) {
					best = i;
				}
			}
		}

		size_t s = (size_t) ((1 + _overprovision) * size);
		void* p = NULL;

		if (best != (size_t) -1) {
			buffer_t& b = _buffers[best];

			::free(b.b_buffer);

			b.b_in_use = true;
			b.b_size = s;
			b.b_buffer = p = malloc(s);
		}
		else {
			buffer_t nb;
			memset(&nb, 0, sizeof(nb));
			_buffers.push_back(nb);
			buffer_t& b = _buffers[_buffers.size()-1];

			b.b_in_use = true;
			b.b_size = s;
			b.b_buffer = p = malloc(s);
		}

		ll_spinlock_release(&_lock);

		if (p == NULL) {
			LL_E_PRINT("** Out of memory **\n");
			abort();
		}

		return p;
	}


	/**
	 * Free a buffer
	 *
	 * @param buffer the buffer
	 */
	void free(void* buffer) {

		ll_spinlock_acquire(&_lock);

		for (size_t i = 0; i < _buffers.size(); i++) {
			buffer_t& b = _buffers[i];
			if (b.b_buffer == buffer) {
				assert(b.b_in_use);
				b.b_in_use = false;
				ll_spinlock_release(&_lock);
				return;
			}
		}

		ll_spinlock_release(&_lock);

		LL_E_PRINT("** Invalid free **\n");
		abort();
	}
};


/**
 * Memory helper
 */
class ll_memory_helper {

	std::vector<void*> _buffers;
	ll_spinlock_t _lock;


public:

	/**
	 * Initialize
	 */
	ll_memory_helper() {
		_lock = 0;
	}


	/**
	 * Destroy
	 */
	~ll_memory_helper() {
		for (int i = ((int) _buffers.size()) - 1; i >= 0; i--) {
			free(_buffers[i]);
		}
	}


	/**
	 * Allocate memory
	 *
	 * @param num the number of elements
	 * @return the allocated memory
	 */
	template<typename T> T* allocate(size_t num) {
		T* p = (T*) malloc(sizeof(T) * num);
		ll_spinlock_acquire(&_lock);
		_buffers.push_back(p);
		ll_spinlock_release(&_lock);
		return p;
	}
};

#endif
