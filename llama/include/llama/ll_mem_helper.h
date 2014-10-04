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

#include <cstdlib>
#include <vector>

#include "llama/ll_lock.h"
#include "llama/ll_utils.h"


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
	 * Allocate memory
	 *
	 * @param num the number of elements
	 * @return the allocated memory
	 */
	template<typename T> T* allocate(size_t num=1) {
		
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
		
		ll_spinlock_release(&_lock);

		return (T*) p;
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
