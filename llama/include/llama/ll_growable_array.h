/*
 * ll_growable_array.h
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


#ifndef LL_GROWABLE_ARRAY_H_
#define LL_GROWABLE_ARRAY_H_

#include "llama/ll_common.h"


/**
 * NOP Deallocator
 */
template <typename T> struct ll_nop_deallocator {
	inline void operator() (T) {}
};


/**
 * A growable block array
 */
template <typename T, int _block_size2, class deallocator>
class ll_growable_array {

private:

	int _blocks;
	int _size;
	T** _arrays;

	deallocator _deallocator;

	ll_spinlock_t _lock;


public:

	/**
	 * Create an empty growable array
	 *
	 * @param blocks the number of blocks to preallocate
	 */
	ll_growable_array(int blocks = 16) {
		_blocks = blocks;
		_size = 0;
		_lock = 0;
		_arrays = (T**) malloc(sizeof(T*) * _blocks);
		memset(_arrays, 0, sizeof(T*) * _blocks);
		_arrays[0] = (T*) malloc(sizeof(T) * (1 << _block_size2));
	}


	/**
	 * Destroy the array
	 */
	inline ~ll_growable_array() {

		for (int i = 0; i < _blocks; i++) {
			if (_arrays[i] == NULL) break;
			for (int j = 0; j < (1 << _block_size2); j++) {
				if (_size-- <= 0) break;
				_deallocator(_arrays[i][j]);
			}
			free(_arrays[i]);
		}
		free(_arrays);
	}


	/**
	 * Get the number of elements in the array
	 *
	 * @return the size of the array
	 */
	inline size_t size() const {
		return _size;
	}


	/**
	 * Clear
	 */
	void clear() {
		for (int i = 0; i < _blocks; i++) {
			if (_arrays[i] == NULL) break;
			for (int j = 0; j < (1 << _block_size2); j++) {
				if (_size-- <= 0) break;
				_deallocator(_arrays[i][j]);
			}
		}
		_size = 0;
	}


	/**
	 * Grow by one and return a pointer to the new unitialized cell
	 *
	 * @return a pointer to the new uninitialized cell
	 */
	T* append(void) {
		ll_spinlock_acquire(&_lock);

		T* p = &_arrays[_size >> _block_size2][_size & ((1 << _block_size2) - 1)];
		_size++;

		if ((_size & ((1 << _block_size2) - 1)) == 0) {
			int newBlock = _size >> _block_size2;
			if (newBlock == _blocks) {
				int n = _blocks * 2;
				T** a = (T**) malloc(sizeof(T*) * n);
				memcpy(a, _arrays, sizeof(T*) * _blocks);
				memset(&a[_blocks], 0, sizeof(T*) * (n - _blocks));
				free(_arrays);
				_arrays = a;
				_blocks = n;
			}
			if (_arrays[newBlock] == NULL) {
				_arrays[newBlock] = (T*) malloc(sizeof(T) * (1 << _block_size2));
			}
		}

		ll_spinlock_release(&_lock);
		return p;
	}


	/**
	 * Append a value
	 *
	 * @param value the value to append
	 * @return the appended value
	 */
	T append(T value) {
		*(append()) = value;
		return value;
	}


	/**
	 * Read from the array
	 *
	 * @return index the index to read
	 * @return the element at that position
	 */
	inline T& operator[] (size_t index) const {
		assert(index < (size_t) _size);
		return _arrays[index >> _block_size2][index & ((1 << _block_size2) - 1)];
	}
};

#endif
