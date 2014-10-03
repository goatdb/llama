/*
 * ll_page_manager.h
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


#ifndef LL_PAGE_MANAGER_H_
#define LL_PAGE_MANAGER_H_

#include "llama/ll_common.h"

#if GCC_VERSION > 40500 // check for GCC > 4.5
#include <atomic>
#else
#include <cstdatomic>
#endif // if GCC_VERSION > 40500

#include <cassert>
#include <cstdio>

#include "llama/ll_growable_array.h"

#ifndef LL_PM_ALLOCATION_STEP_BITS
#define LL_PM_ALLOCATION_STEP_BITS			8
#endif
#define LL_PM_ALLOCATION_STEP				(1 << LL_PM_ALLOCATION_STEP_BITS)

//#define LL_PM_COUNTERS


/**
 * The page manager that supports allocation of fixed-size pages and reference
 * counting.
 */
template <typename T>
class ll_page_manager {

	size_t _page_length;
	size_t _page_bytes;
	bool _zero_pages;
	size_t _num_pages;

	ll_spinlock_t _lock;

	typedef struct {
		union {
			int refcounts[LL_PM_ALLOCATION_STEP];

			// Ensure 64-byte alignment of the rest of the data
			char bytes[64 + 64 * (LL_PM_ALLOCATION_STEP * sizeof(int) / 64)];
		};
	} _pages_t;

	struct _pages_deallocator {
		void operator() (_pages_t* p) {
			free(p);
		}
	};

	ll_growable_array<_pages_t*, 8, struct _pages_deallocator> _pages;

	ssize_t _zero_page;
	ssize_t* _free_list_next;


#ifdef LL_PM_COUNTERS
public:
	size_t _counter_free;
	size_t _counter_allocate_new;
	size_t _counter_allocate_reuse;
private:
#endif


public:

	/**
	 * Create a new instance of the class ll_page_manager
	 *
	 * @param page_length the number of elements per page
	 * @param zero_pages true to zero the pages
	 */
	ll_page_manager(size_t page_length, bool zero_pages) {
		
		_page_length = page_length;
		_page_bytes = sizeof(T) * page_length;
		_zero_pages = zero_pages;

		_num_pages = 0;
		_lock = 0;
		_zero_page = -1;

		_free_list_next = (ssize_t*) malloc(sizeof(ssize_t)
				* 8 * omp_get_max_threads());
		memset(_free_list_next, 0xff, sizeof(ssize_t)
				* 8 * omp_get_max_threads());
		
#ifdef LL_PM_COUNTERS
		_counter_free = 0;
		_counter_allocate_new = 0;
		_counter_allocate_reuse = 0;
#endif
	}


	/**
	 * Destroy the instance
	 */
	virtual ~ll_page_manager() {

		free(_free_list_next);
	}


	/**
	 * Get the number of elements per page
	 *
	 * @return the number of elements
	 */
	inline size_t page_length() const {
		return _page_length;
	}


	/**
	 * Get the page size in bytes
	 *
	 * @return the page size in bytes
	 */
	inline size_t page_bytes() const {
		return _page_bytes;
	}


	/**
	 * Determine if the page manager is configured to zero pages upon
	 * allocation
	 *
	 * @return true if it zeroes pages
	 */
	inline bool zeroes_pages() const {
		return _zero_pages;
	}


	/**
	 * Get a refcount of a page
	 *
	 * @param id the page ID
	 * @return the refcount
	 */
	size_t refcount(size_t id) {

		size_t index_outer = id >> LL_PM_ALLOCATION_STEP_BITS;
		size_t index_inner = id & (LL_PM_ALLOCATION_STEP - 1);

		_pages_t* p = _pages[index_outer];
		return p->refcounts[index_inner];
	}


	/**
	 * Allocate a page. Set the refcount to 1
	 *
	 * @param out the pointer to where to write the new address
	 * @return the page number
	 */
	size_t allocate(T** out) {
		return allocate(out, true);
	}


	/**
	 * Allocate a range of pages. Set the refcount to 1
	 *
	 * @param out_ptr the pointer to where to write the new addresses
	 * @param out_ref the pointer to write the new page IDs
	 * @param size the number of pages
	 */
	void allocate(T** out_ptr, size_t* out_ref, size_t size) {

		ll_spinlock_acquire(&_lock);

		for (size_t i = 0; i < size; i++) {
			out_ref[i] = allocate(&out_ptr[i], false);
		}

		ll_spinlock_release(&_lock);
	}


	/**
	 * Acquire a page by its ID and increase its refcount
	 *
	 * @param id the page ID
	 * @param count how many times to acquire the given page
	 * @return the page pointer
	 */
	T* acquire_page(size_t id, size_t count = 1) {

		size_t index_outer = id >> LL_PM_ALLOCATION_STEP_BITS;
		size_t index_inner = id & (LL_PM_ALLOCATION_STEP - 1);

		_pages_t* p = _pages[index_outer];
		__sync_fetch_and_add(&p->refcounts[index_inner], count);

		return page_pointer(p, index_inner);
	}


	/**
	 * Acquire multiple pages
	 *
	 * @param ids the page IDs
	 * @param size the number of pages
	 */
	void acquire_pages(size_t* ids, size_t pages) {

#		pragma omp parallel for schedule(static,131072)
		for (size_t i = 0; i < pages; i++) {

			size_t id = ids[i];
			size_t index_outer = id >> LL_PM_ALLOCATION_STEP_BITS;
			size_t index_inner = id & (LL_PM_ALLOCATION_STEP - 1);

			_pages_t* p = _pages[index_outer];
			__sync_fetch_and_add(&p->refcounts[index_inner], 1);
		}
	}


	/**
	 * COW a page
	 *
	 * @param out the pointer to where to write the new address
	 * @param src_id the page ID of the original page
	 * @param src_ptr the page pointer
	 * @return the new page number
	 */
	size_t cow(T** out, size_t src_id) {

		size_t index_outer = src_id >> LL_PM_ALLOCATION_STEP_BITS;
		size_t index_inner = src_id & (LL_PM_ALLOCATION_STEP - 1);

		T* ptr = page_pointer(_pages[index_outer], index_inner);
		return cow(out, src_id, ptr);
	}


	/**
	 * COW a page
	 *
	 * @param out the pointer to where to write the new address
	 * @param src_id the page ID of the original page
	 * @param src_ptr the source page pointer
	 * @return the new page number
	 */
	size_t cow(T** out, size_t src_id, T* src_ptr) {

		size_t p = allocate(out, true);
		memcpy(*out, src_ptr, _page_bytes);

		release_page(src_id);

		return p;
	}


	/**
	 * Release a page by its ID and decrease its refcount
	 *
	 * @param id the page ID
	 * @return the new reference count
	 */
	size_t release_page(size_t id) {

		size_t index_outer = id >> LL_PM_ALLOCATION_STEP_BITS;
		size_t index_inner = id & (LL_PM_ALLOCATION_STEP - 1);

		_pages_t* p = _pages[index_outer];
		ssize_t n = __sync_add_and_fetch(&p->refcounts[index_inner], -1);
		assert(n >= 0);

		if (n == 0 && n != _zero_page) {
			int i = omp_get_thread_num() << 3;

#ifdef LL_PM_COUNTERS
			__sync_add_and_fetch(&_counter_free, (size_t) 1);
#endif

			ssize_t x;
			ssize_t* ptr = (ssize_t*) (void*) page_pointer(p, index_inner);
			do {
				x = _free_list_next[i];
				*ptr = x;
				__COMPILER_FENCE;
			}
			while (!__sync_bool_compare_and_swap(&_free_list_next[i], x, id));
		}

		return (size_t) n;
	}


	/**
	 * Release multiple pages
	 *
	 * @param ids the page IDs
	 * @param size the number of pages
	 */
	void release_pages(size_t* ids, size_t pages) {

#		pragma omp parallel for schedule(static,131072)
		for (size_t i = 0; i < pages; i++) {

			size_t id = ids[i];
			if (id == (size_t) -1) continue;

			release_page(id);
		}
	}


	/**
	 * Get a zero page and increase its refcount
	 *
	 * @param out the pointer where to store the pointer
	 * @param count how many times to acquire the given page
	 * @return the page ID
	 */
	size_t zero_page(T** out, size_t count = 1) {

		if (_zero_page >= 0) {
			*out = acquire_page(_zero_page, count);
			return _zero_page;
		}
		else {

			// We can have a race condition here, but it is not a big deal: We
			// might end up with two zeroed pages, but the program behavior
			// will be still correct.

			size_t zp = allocate(out);
			if (!_zero_pages) memset(*out, 0, _page_bytes);
			if (count > 1) acquire_page(zp, count - 1);

			_zero_page = zp;
			return zp;
		}
	}


	/**
	 * Get the zero page ID, if created
	 *
	 * @return the zero page ID, or (size_t) -1 if not allocated
	 */
	size_t zero_page_id(void) {
		return (size_t) _zero_page;
	}


	/**
	 * Get the zero page pointer, if created
	 *
	 * @return the zero page pointer (not acquired), or NULL if not allocated
	 */
	T* zero_page_ptr(void) {

		if (_zero_page >= 0) {
			size_t index_outer = _zero_page >> LL_PM_ALLOCATION_STEP_BITS;
			size_t index_inner = _zero_page & (LL_PM_ALLOCATION_STEP - 1);
			return page_pointer(_pages[index_outer], index_inner);
		}
		else {
			return NULL;
		}
	}


protected:

	/**
	 * Get page pointer
	 * 
	 * @param p the page structure
	 * @param index the index within the page structure
	 * @return the pointer
	 */
	inline T* page_pointer(_pages_t* p, size_t index) {
		return (T*) (void*) ((char*) p + sizeof(_pages_t) + _page_bytes*index);
	}


	/**
	 * Allocate a page. Set the refcount to 1
	 *
	 * @param out the pointer to where to write the new address
	 * @param lock true to lock
	 * @return the page number
	 */
	size_t allocate(T** out, bool lock) {

		// First check the free-list
		
		int max = omp_get_max_threads();
		int t = omp_get_thread_num();

		for (int i = 0; i < max; i++) {
			int k = ((t + i) % max) << 3;
			ssize_t id = _free_list_next[k];
			if (id < 0) continue;

			size_t index_outer = id >> LL_PM_ALLOCATION_STEP_BITS;
			size_t index_inner = id & (LL_PM_ALLOCATION_STEP - 1);

			_pages_t* p = _pages[index_outer];
			T* ptr = page_pointer(p, index_inner);
			ssize_t* sptr = (ssize_t*) (void*) ptr;

			if (__sync_bool_compare_and_swap(&_free_list_next[k], id, *sptr)) {
				p->refcounts[index_inner] = 1;
				*out = ptr;
#ifdef LL_PM_COUNTERS
				__sync_add_and_fetch(&_counter_allocate_reuse, (size_t) 1);
#endif
				if (_zero_pages) memset(ptr, 0, _page_bytes);
				return (size_t) id;
			}
		}


		// Otherwise allocate a new page

		size_t page_no = __sync_fetch_and_add(&_num_pages, 1);
		size_t index_outer = page_no >> LL_PM_ALLOCATION_STEP_BITS;
		size_t index_inner = page_no & (LL_PM_ALLOCATION_STEP - 1);

#ifdef LL_PM_COUNTERS
		__sync_add_and_fetch(&_counter_allocate_new, (size_t) 1);
#endif

		if (index_outer < _pages.size()) {

			_pages_t* p = _pages[index_outer];
			p->refcounts[index_inner] = 1;
			*out = page_pointer(p, index_inner);
			return page_no;
		}
		else {

			if (lock) ll_spinlock_acquire(&_lock);

			if (index_outer < _pages.size()) {
				_pages_t* p = _pages[index_outer];
				p->refcounts[index_inner] = 1;
				*out = page_pointer(p, index_inner);
			}
			else {
				_pages_t* p;

				while (index_outer >= _pages.size()) {
					size_t size = sizeof(_pages_t)
						+ LL_PM_ALLOCATION_STEP * _page_bytes;
					p = (_pages_t*) malloc(size);
					if (p == NULL) {
						fprintf(stderr, "*** Out of memory ***\n");
						abort();
					}
					if (_zero_pages) memset(p, 0, size);
					_pages.append(p);
				}

				p->refcounts[index_inner] = 1;
				*out = page_pointer(p, index_inner);
			}

			if (lock) ll_spinlock_release(&_lock);
			return page_no;
		}
	}
};


#endif
