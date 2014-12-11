/*
 * ll_writable_array.h
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


#ifndef LL_WRITABLE_ARRAY_H_
#define LL_WRITABLE_ARRAY_H_

#include "llama/ll_common.h"
#include "llama/ll_writable_elements.h"

#define LL_WRITABLE_SWCOW_FREE_LIST



//==========================================================================//
// Class: ll_w_vt_array                                                     //
//==========================================================================//

/**
 * An array representation of a mutable vertex table.
 *
 * @author Peter Macko <peter.macko@oracle.com>
 */
template <typename T, T nil, T block, typename allocator, typename deallocator>
class ll_w_vt_array {

	allocator _allocator;
	deallocator _deallocator;


public:

	/**
	 * Create an instance of ll_w_vt_array
	 *
	 * @param size the number of elements
	 */
	ll_w_vt_array(size_t size) {
		_size = size;
		_array = (std::atomic<T>*) malloc(sizeof(*_array) * size);
		
		if (nil == (T) 0) {
			memset(_array, 0, sizeof(*_array) * size);
		}
		else {
#			pragma omp parallel for schedule(dynamic,4096)
			for (size_t i = 0; i < _size; i++) {
				_array[i].store(nil);
			}
		}
	}


	/**
	 * Destroy the instance
	 */
	~ll_w_vt_array(void) {
		__COMPILER_FENCE;

#		pragma omp parallel for schedule(dynamic,4096)
		for (size_t i = 0; i < _size; i++) {
			register T r = *((T*) &_array[i]);
			if (r != nil && r != block) _deallocator(r);
		}

		free(_array);
	}


	/**
	 * Return the array size
	 *
	 * @return the array size
	 */
	inline size_t size() const {
		return _size;
	}


	/**
	 * Set the value
	 * 
	 * @param node the node id
	 * @param value the new value
	 * @return the associated value
	 */
	inline void set(node_t node, T value) {
		__COMPILER_FENCE;
		*((T*) &_array[node]) = value;
		__COMPILER_FENCE;
	}


	/**
	 * Return the value associated with the given vertex -- do a fast read
	 * without caring about concurrency
	 * 
	 * @param node the node id
	 * @return the associated value
	 */
	inline T fast_get(node_t node) const {
		__COMPILER_FENCE;
		register T r = *((T* volatile) &_array[node]);
		__COMPILER_FENCE;
		return r;
	}


	/**
	 * Return the value associated with the given vertex
	 * 
	 * @param node the node id
	 * @return the associated value
	 */
	inline T get(node_t node) const {
#ifdef WORD_ACCESS_IS_ALREADY_ATOMIC
		__COMPILER_FENCE;
		register T r = *((T* volatile) &_array[node]);
		if (r == block) r = nil;
		__COMPILER_FENCE;
		return r;
#else
		return _array[node].load();
#endif
	}


	/**
	 * Return the value associated with the given vertex (the same as get())
	 * 
	 * @param node the node id
	 * @return the associated value
	 */
	inline T operator[] (node_t node) const {
		return get(node);
	}


	/**
	 * Return the value associated with the given vertex if present, otherwise
	 * store the provided value
	 * 
	 * @param node the node id
	 * @param value the value to use if the element is nil
	 * @return the associated value
	 */
	inline T get_or_set(node_t node, T value) {
		T expected = nil;
		if (_array[node].compare_exchange_strong(expected, value))
			return value;
		else
			return expected;
	}


	/**
	 * Get the value associated with the given vertex if present, otherwise
	 * store the provided value
	 * 
	 * @param node the node id
	 * @param value the value to use if the element is nil
	 * @param result the result pointer to store the new value
	 * @return true if the original value was replaced by the provided value
	 */
	inline bool get_or_set_ext(node_t node, T value, T* result) {
		T expected = nil;
		if (_array[node].compare_exchange_strong(expected, value)) {
			*result = value;
			return true;
		}
		else {
			*result = expected;
			return false;
		}
	}


	/**
	 * Return the associated value, allocating it if not present
	 *
	 * @param node the node id
	 * @return the value
	 */
	inline T get_or_allocate(node_t node) {

		T v = nil;
		if (get_or_set_ext(node, block, &v)) {

			// We just claimed the value
			
			v = _allocator();
			set(node, v);
		}
		else {

			// There is a preexisting value

			if (v == block) {
				v = blocking_get(node);
			}
		}

		return v;
	}


	/**
	 * Return the value associated with the given vertex if present, blocking while
	 * the value is set to block
	 * 
	 * @param node the node id
	 * @return the associated value
	 */
	inline T blocking_get(node_t node) {
		T r = nil;
		//while ((r = _array[node].load()) == block);
		while ((r = fast_get(node)) == block);
		return r;
	}


	/**
	 * Return the value associated with the given vertex if present, otherwise
	 * store the provided value -- blocking while the value is set to block
	 * 
	 * @param node the node id
	 * @param value the value to use if the element is nil
	 * @return the associated value
	 */
	inline T blocking_get_or_set(node_t node, T value) {
		T r = nil;
		while ((r = get_or_set(node, value)) == block);
		return r;
	}


	/**
	 * Clear
	 */
	void clear() {
		__COMPILER_FENCE;

#ifdef PARALLEL_FREE_W_NODES
#		pragma omp parallel for schedule(dynamic,65536)
		for (size_t i = 0; i < _size; i++) {
			T r = *((T*) &_array[i]);
			if (r != nil && r != block) _deallocator(r);
			_array[i] = nil;
		}
#else
#		pragma omp critical 
		{
			for (size_t i = 0; i < _size; i++) {
				T r = *((T*) &_array[i]);
				if (r != nil && r != block) _deallocator(r);
			}
		}
		if (nil == (T) 0) {
			memset(_array, 0, sizeof(*_array) * _size);
		}
		else {
#			pragma omp parallel for schedule(dynamic,65536)
			for (size_t i = 0; i < _size; i++) {
				_array[i].store(nil);
			}
		}
#endif

		__COMPILER_FENCE;
	}


private:

	/// The array
	std::atomic<T>* _array;

	/// The array size
	size_t _size;
};



//==========================================================================//
// Class: ll_w_vt_swcow_array                                               //
//==========================================================================//


#ifdef LL_WRITABLE_SWCOW_FREE_LIST

// Note: If used, type T cannot be longer than long long.

#define FREE_W_VT_SWCOW_PAGES_LENGTH		4
static long long* __free_w_vt_swcow_pages[FREE_W_VT_SWCOW_PAGES_LENGTH] = { NULL, NULL, NULL, NULL };


/**
 * Allocate a new page; do not reuse an old page
 *
 * @return the page
 */
void* __w_vt_swcow_page_new(void) {
	void* p = malloc(sizeof(long long) * LL_ENTRIES_PER_PAGE);
	memset(p, 0, sizeof(long long) * LL_ENTRIES_PER_PAGE);
	return p;
}


/**
 * Allocate a new page
 *
 * @return the page
 */
void* __w_vt_swcow_page_allocate(void) {

	for (int i = 0; i < FREE_W_VT_SWCOW_PAGES_LENGTH; i++) {
		long long* x = __free_w_vt_swcow_pages[i];

		if (x != NULL) {
			if (__sync_bool_compare_and_swap(&__free_w_vt_swcow_pages[i], x, (long*) *x)) {
				return x;
			}
		}
	}

	return __w_vt_swcow_page_new();
}


/**
 * Deallocate a page
 *
 * @param page the page
 */
void __w_vt_swcow_page_deallocate(void* page) {

	int i = (int) ((((long) page) >> 6) % FREE_W_VT_SWCOW_PAGES_LENGTH);

	memset(page, 0, sizeof(long long) * LL_ENTRIES_PER_PAGE);
	long long* n = (long long*) page;

	long long* x;
	do {
		x = __free_w_vt_swcow_pages[i];
		*n = (long) x;
		__COMPILER_FENCE;
	}
	while (!__sync_bool_compare_and_swap(&__free_w_vt_swcow_pages[i], x, n));
}

#endif


/**
 * An SW-COW array representation of a mutable vertex table.
 *
 * @author Peter Macko <peter.macko@oracle.com>
 */
template <typename T, T nil, T block, typename allocator, typename deallocator>
class ll_w_vt_swcow_array {

	allocator _allocator;


public:

	/**
	 * Create an instance of ll_w_vt_swcow_array
	 *
	 * @param size the number of elements
	 */
	ll_w_vt_swcow_array(size_t size) : _array(size / LL_ENTRIES_PER_PAGE + 1) {
		_size = size;
	}


	/**
	 * Destroy the instance
	 */
	~ll_w_vt_swcow_array(void) {
	}


	/**
	 * Return the array size
	 *
	 * @return the array size
	 */
	inline size_t size() const {
		return _size;
	}


	/**
	 * Set the value
	 * 
	 * @param node the node id
	 * @param value the new value
	 * @return the associated value
	 */
	inline void set(node_t node, T value) {

		std::atomic<T>* a = (std::atomic<T>*) _array.get_or_allocate
									(node >> LL_ENTRIES_PER_PAGE_BITS);

		__COMPILER_FENCE;
		*((T*) &a[node & (LL_ENTRIES_PER_PAGE - 1)]) = value;
		__COMPILER_FENCE;
	}


	/**
	 * Return the value associated with the given vertex -- do a fast read
	 * without caring about concurrency
	 * 
	 * @param node the node id
	 * @return the associated value
	 */
	inline T fast_get(node_t node) const {

		std::atomic<T>* a
			= (std::atomic<T>*) _array[node >> LL_ENTRIES_PER_PAGE_BITS];

		if (a == NULL) return nil;
		if ((long) a == -1l) return block;

		__COMPILER_FENCE;
		register T r = *((T*) &a[node & (LL_ENTRIES_PER_PAGE - 1)]);
		__COMPILER_FENCE;

		return r;
	}


	/**
	 * Return the number of elements per page
	 *
	 * @return the number of elements per page
	 */
	inline size_t num_entries_per_page() const {
		return 1 << LL_ENTRIES_PER_PAGE_BITS;
	}


	/**
	 * Return the number of pages
	 *
	 * @return the number of pages
	 */
	inline size_t num_pages() const {
		size_t n = _size / LL_ENTRIES_PER_PAGE;
		if ((_size & (LL_ENTRIES_PER_PAGE - 1)) != 0) n++;
		return n;
	}


	/**
	 * Determine if there is anything in the given page
	 *
	 * @param p the page number
	 * @return true if there is anything
	 */
	inline bool page_with_contents(size_t p) const {
		return (std::atomic<T>*) _array[p] != NULL;
	}


	/**
	 * A fast page-based read
	 *
	 * @param p the page
	 * @param i the index within the page
	 * @return the value
	 */
	inline T page_fast_read(size_t p, size_t i) const {
		return ((T*) _array[p])[i];
	}


	/**
	 * Return the value associated with the given vertex
	 * 
	 * @param node the node id
	 * @return the associated value
	 */
	inline T get(node_t node) const {

		std::atomic<T>* a = (std::atomic<T>*) _array.fast_get
									(node >> LL_ENTRIES_PER_PAGE_BITS);
		if (a == NULL || (long) a == -1l) return nil;

		__COMPILER_FENCE;
		register T r = *((T*) &a[node & (LL_ENTRIES_PER_PAGE - 1)]);
		if (r == block) r = nil;
		__COMPILER_FENCE;

		return r;
	}


	/**
	 * Return the value associated with the given vertex (the same as get())
	 * 
	 * @param node the node id
	 * @return the associated value
	 */
	inline T operator[] (node_t node) const {
		return get(node);
	}


	/**
	 * Return the value associated with the given vertex if present, otherwise
	 * store the provided value
	 * 
	 * @param node the node id
	 * @param value the value to use if the element is nil
	 * @return the associated value
	 */
	inline T get_or_set(node_t node, T value) {
		T expected = nil;
		std::atomic<T>* a = (std::atomic<T>*) _array.get_or_allocate
										(node >> LL_ENTRIES_PER_PAGE_BITS);
		if (a[node & (LL_ENTRIES_PER_PAGE - 1)]
				.compare_exchange_strong(expected, value))
			return value;
		else
			return expected;
	}


	/**
	 * Get the value associated with the given vertex if present, otherwise
	 * store the provided value
	 * 
	 * @param node the node id
	 * @param value the value to use if the element is nil
	 * @param result the result pointer to store the new value
	 * @return true if the original value was replaced by the provided value
	 */
	inline bool get_or_set_ext(node_t node, T value, T* result) {
		T expected = nil;
		std::atomic<T>* a = (std::atomic<T>*) _array.get_or_allocate
										(node >> LL_ENTRIES_PER_PAGE_BITS);
		if (a[node & (LL_ENTRIES_PER_PAGE - 1)]
				.compare_exchange_strong(expected, value)) {
			*result = value;
			return true;
		}
		else {
			*result = expected;
			return false;
		}
	}


	/**
	 * Return the associated value, allocating it if not present
	 *
	 * @param node the node id
	 * @return the value
	 */
	inline T get_or_allocate(node_t node) {

		T v = nil;
		if (get_or_set_ext(node, block, &v)) {

			// We just claimed the value
			
			v = _allocator();
			set(node, v);
		}
		else {

			// There is a preexisting value

			if (v == block) {
				v = blocking_get(node);
			}
		}

		return v;
	}


	/**
	 * Return the value associated with the given vertex if present, blocking while
	 * the value is set to block
	 * 
	 * @param node the node id
	 * @return the associated value
	 */
	inline T blocking_get(node_t node) {
		T r = nil;
		while ((r = fast_get(node)) == block);
		return r;
	}


	/**
	 * Return the value associated with the given vertex if present, otherwise
	 * store the provided value -- blocking while the value is set to block
	 * 
	 * @param node the node id
	 * @param value the value to use if the element is nil
	 * @return the associated value
	 */
	inline T blocking_get_or_set(node_t node, T value) {
		T r = nil;
		while ((r = get_or_set(node, value)) == block);
		return r;
	}


	/**
	 * Clear
	 */
	void clear() {
		_array.clear();
	}


private:


	/// The allocator
	struct _inner_allocator {

		/**
		 * Allocate a new inner array
		 *
		 * @return the allocated array
		 */
		long operator() (void) {

#ifdef LL_WRITABLE_SWCOW_FREE_LIST
			std::atomic<T>* a = (std::atomic<T>*) __w_vt_swcow_page_allocate();
#else
			std::atomic<T>* a = (std::atomic<T>*) malloc
							(sizeof(std::atomic<T>) * LL_ENTRIES_PER_PAGE);
#endif
			
			if (nil == (T) 0) {
				memset(a, 0, sizeof(std::atomic<T>) * LL_ENTRIES_PER_PAGE);
			}
			else {
#			pragma omp parallel for schedule(dynamic,4096)
				for (size_t i = 0; i < LL_ENTRIES_PER_PAGE; i++) {
					a[i].store(nil);
				}
			}

			return (long) a;
		}
	};


	/// The deallocator
	struct _inner_deallocator {
		
		deallocator _deallocator;

		/**
		 * Deallocate the inner array
		 *
		 * @param array the array
		 */
		void operator() (long array) {

			for (size_t i = 0; i < LL_ENTRIES_PER_PAGE; i++) {
				T r = *((T*) &((std::atomic<T>*) array)[i]);
				if (r != nil && r != block) _deallocator(r);
			}

#ifdef LL_WRITABLE_SWCOW_FREE_LIST
			__w_vt_swcow_page_deallocate((std::atomic<T>*) array);
#else
			free((std::atomic<T>*) array);
#endif
		}
	};


	/// The indirection array
	ll_w_vt_array<long, 0l, -1l, _inner_allocator, _inner_deallocator> _array;

	/// The array size
	size_t _size;
};

#endif

