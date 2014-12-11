/*
 * ll_mem_array.h
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


#ifndef LL_MEM_ARRAY_H_
#define LL_MEM_ARRAY_H_

#include "llama/ll_common.h"

#if GCC_VERSION > 40500 // check for GCC > 4.5
#include <atomic>
#else
#include <cstdatomic>
#endif // if GCC_VERSION > 40500

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cassert>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

#include <unordered_map>
#include <vector>

#include "llama/ll_mlcsr_helpers.h"
#include "llama/ll_page_manager.h"
#include "llama/ll_writable_elements.h"


#define LL_VT_PAGESIZE(T) 	(sizeof(T) * LL_ENTRIES_PER_PAGE)



//==========================================================================//
// Struct: ll_vertex_iterator                                               //
//==========================================================================//

typedef struct {
	node_t vi_next_node;
	node_t vi_end;
	const void* vi_value;
} ll_vertex_iterator;



//==========================================================================//
// Class: ll_mem_array_collection                                           //
//==========================================================================//

/**
 * A collection of vertex tables
 */
template <class VT, typename T>
class ll_mem_array_collection {

	std::vector<VT*> _levels;
	ll_page_manager<T>* _page_manager;
	bool _own_page_manager;

	ll_mem_array_collection<VT, T>* _master;
	
#ifdef LL_MIN_LEVEL
	/// The minimum level to consider
	int _min_level;
#endif

	/// The maximum level to consider
	int _max_level;


public:

	/**
	 * Create an instance of class ll_mem_array_collection
	 *
	 * @param page_size the page size, or 0 to disable page manager
	 * @param zero_pages true to zero new pages
	 */
	ll_mem_array_collection(size_t page_size, bool zero_pages = true) {

		if (page_size > 0) {
			_page_manager = new ll_page_manager<T>(page_size, zero_pages);
		}
		else {
			_page_manager = NULL;
		}

		_own_page_manager = true;
		_max_level = -1;
		_master = NULL;

#ifdef LL_MIN_LEVEL
		_min_level = 0;
#endif
	}


	/**
	 * Create an instance of class ll_mem_array_collection
	 *
	 * @param zero_pages true to zero new pages
	 */
	ll_mem_array_collection(bool zero_pages = true) {

		size_t length = 1 << LL_ENTRIES_PER_PAGE_BITS;
		_page_manager = new ll_page_manager<T>(length, zero_pages);
		_own_page_manager = true;
		_max_level = -1;
		_master = NULL;

#ifdef LL_MIN_LEVEL
		_min_level = 0;
#endif
	}


	/**
	 * Create an instance of class ll_mem_array_collection
	 *
	 * @param page_manager the page manager (will not be destroyed with this object)
	 * @param own_page_manager true if this should own the page manager
	 */
	ll_mem_array_collection(ll_page_manager<T>* page_manager, bool own_page_manager=false) {

		_page_manager = page_manager;
		_own_page_manager = own_page_manager;
		_max_level = -1;
		_master = NULL;

#ifdef LL_MIN_LEVEL
		_min_level = 0;
#endif
	}


	/**
	 * Create a read-only clone of ll_mem_array_collection
	 *
	 * @param master the master array collection
	 * @param level the max level
	 */
	ll_mem_array_collection(ll_mem_array_collection<VT, T>* master, int level) {
		
		assert(master != NULL);

		_master = master;
		_own_page_manager = false;
		_page_manager = master->_page_manager;
		_max_level = master->_max_level;

		if (level >= (int) master->size()) level = (int) master->size() - 1;
		if (level < 0) level = 0;

		if (master->size() > 0) {
			for (int i = 0; i <= level; i++) {
				_levels.push_back(master->_levels[i]);
			}
		}

#ifdef LL_MIN_LEVEL
		_min_level = _master->_min_level;
#endif
	}


	/**
	 * Destroy the collection
	 */
	virtual ~ll_mem_array_collection() {

		if (_master != NULL) return;

		for (int l = _levels.size()-1; l >= 0; l--) {
			if (_levels[l] != NULL) {
				delete _levels[l];
				_levels[l] = NULL;
			}
		}

		if (_page_manager != NULL && _own_page_manager) {
			delete _page_manager;
		}
	}


	/**
	 * Get the page manager
	 *
	 * @return the page manager, or NULL if not initalized
	 */
	inline ll_page_manager<T>* page_manager() {
		return _page_manager;
	}


	/**
	 * Get the appropriate level
	 *
	 * @param index the index
	 * @return the level
	 */
	inline VT* operator[] (int index) {
		assert(index >= 0 && index < (int) _levels.size());
		return _levels[index];
	}


	/**
	 * Get the appropriate level
	 *
	 * @param index the index
	 * @return the level
	 */
	inline const VT* operator[] (int index) const {
		assert(index >= 0 && index < (int) _levels.size());
		return _levels[index];
	}


	/**
	 * Get the number of levels
	 *
	 * @return the number of levels - elements in the _level array
	 */
	inline size_t size() const {
		return _levels.size();
	}


	/**
	 * Determine if the collection is empty
	 *
	 * @return true if it is empty
	 */
	inline bool empty() const {
		return _levels.empty();
	}


	/**
	 * Get the capacity of the backing vector
	 *
	 * @return the capacity of the backing vector
	 */
	inline size_t capacity() const {
		return _levels.capacity();
	}
	

#ifdef LL_MIN_LEVEL

	/**
	 * Get the min level
	 *
	 * @return the minimum level to consider
	 */
	inline int min_level() const {
		return _min_level;
	}


	/**
	 * Set the minimum level to consider
	 *
	 * @param m the minimum level
	 */
	virtual void set_min_level(size_t m) {

		assert((int) _min_level <= (int) m);
		_min_level = m;
	}

#endif


	/**
	 * Get the max level
	 *
	 * @return the minimum level to consider
	 */
	inline int max_level() const {
		return _max_level;
	}


	/**
	 * Get the previous level
	 *
	 * @param level the current level ID
	 * @return the previous level
	 */
	inline VT* prev_level(int level) {

		int id = ((int) level) - 1;
#ifdef LL_MLCSR_LEVEL_ID_WRAP
		if (id < 0) id = LL_MAX_LEVEL;
#else
		assert(id >= 0);
#endif

		return (*this)[id];
	}


	/**
	 * Get the previous level
	 *
	 * @param level the current level ID
	 * @return the previous level
	 */
	inline const VT* prev_level(int level) const {

		int id = ((int) level) - 1;
#ifdef LL_MLCSR_LEVEL_ID_WRAP
		if (id < 0) id = LL_MAX_LEVEL;
#else
		assert(id >= 0);
#endif

		return (*this)[id];
	}


	/**
	 * Determine if there is a previous level
	 *
	 * @param level the current level ID
	 * @param true if there is a previous level
	 */
	inline bool has_prev_level(int level) const {

#ifdef LL_MLCSR_LEVEL_ID_WRAP
		int id = ((int) level) - 1;
		if (id < 0) id = LL_MAX_LEVEL;
		return id < (int) _levels.size() && (*this)[id] != NULL;
#else
		return level > 0 && (*this)[level-1] != NULL;
#endif
	}


	/**
	 * Get the latest level
	 *
	 * @return the latest level if available, or NULL otherwise
	 */
	VT* latest_level() {
		if (_max_level < 0) return NULL;
		return (*this)[_max_level];
	}


	/**
	 * Get the latest level
	 *
	 * @return the latest level if available, or NULL otherwise
	 */
	const VT* latest_level() const {
		if (_max_level < 0) return NULL;
		return (*this)[_max_level];
	}


	/**
	 * Get the next level ID, or fail if there is not enough space
	 *
	 * @return the next level ID
	 */
	int next_level_id() const {
		
#ifdef LL_MLCSR_LEVEL_ID_WRAP
		
		int new_level_id = _max_level + 1;
		if (new_level_id > (int) LL_MAX_LEVEL) {
			new_level_id = 0;
			if (_min_level == 0) {
				LL_E_PRINT("Maximum number of levels reached (%ld)\n",
						(ssize_t) LL_MAX_LEVEL + 1);
				LL_E_PRINT("Min level = %ld, max level = %ld\n",
						(ssize_t) _min_level, (ssize_t) _max_level);
				abort();
			}
		}

		if (_max_level >= 0 && _max_level < _min_level && new_level_id >= _min_level) {
			LL_E_PRINT("Maximum number of levels reached (%ld)\n",
						(ssize_t) LL_MAX_LEVEL + 1);
			LL_E_PRINT("Min level = %ld, max level = %ld\n",
					(ssize_t) _min_level, (ssize_t) _max_level);
			abort();
		}

#else

		int new_level_id = _levels.size();
		if (new_level_id > (int) LL_MAX_LEVEL) {
			LL_E_PRINT("Maximum number of levels reached (%ld)\n",
						(ssize_t) LL_MAX_LEVEL + 1);
			abort();
		}

#endif

		return new_level_id;
	}


	/**
	 * Add a new level
	 *
	 * @param size the size
	 * @return the new vertex table
	 */
	VT* new_level(size_t size) {

		assert(_master == NULL);
		
		int new_level_id = next_level_id();
		assert(new_level_id >= 0);

		_max_level = new_level_id;

		VT* v = new VT(this, new_level_id, size);

#ifdef LL_MLCSR_LEVEL_ID_WRAP
		while ((ssize_t) _levels.size() <= new_level_id) _levels.push_back(NULL);

		assert(_levels[new_level_id] == NULL);
		_levels[new_level_id] = v;
#else
		_levels.push_back(v);
#endif

		return v;
	}


	/**
	 * Add an existing level
	 *
	 * @param vt the vertex table
	 */
	void push_back(VT* vt) {
		assert(_master == NULL);
		_levels.push_back(vt);
	}


	/**
	 * Delete a level
	 *
	 * @param level the level number
	 */
	void delete_level(size_t level) {

		assert(level >= 0 && level < _levels.size());
		assert(_levels[level] != NULL);
		assert(_master == NULL);

		delete _levels[level];
		_levels[level] = NULL;
	}


	/**
	 * Delete all levels except the specified number of most recent levels
	 *
	 * @param keep the number of levels to keep
	 */
	void keep_only_recent_levels(size_t keep) {

#ifdef LL_MLCSR_LEVEL_ID_WRAP
		LL_NOT_IMPLEMENTED;
#endif

		for (int l = 0; l <= ((int) _max_level) - (int) keep; l++) {
			if (level_exists(l)) delete_level(l);
		}
	}


	/**
	 * Determine if the given level exists
	 *
	 * @param level the level number
	 * @return true if it exists
	 */
	bool level_exists(size_t level) {
		return level < _levels.size() && _levels[level] != NULL;
	}


	/**
	 * Count the number of existing levels
	 *
	 * @return the number of existing levels
	 */
	size_t count_existing_levels() {
		size_t x = 0;
		for (size_t i = 0; i < _levels.size(); i++) {
			if (_levels[i] != NULL) x++;
		}
		return x;
	}
};



//==========================================================================//
// Class: ll_mem_array_flat                                                 //
//==========================================================================//

/**
 * An array representation of the vertex table. To create, instantiate the
 * class and use the appropriate methods to build up the data structure.
 *
 * @author Peter Macko <peter.macko@oracle.com>
 */
template <typename T> class ll_mem_array_flat {

public:

	/**
	 * Create an instance of ll_mem_array_flat
	 *
	 * @param levels the vector of levels
	 * @param level the level number
	 * @param size the number of elements
	 */
	ll_mem_array_flat(ll_mem_array_collection<ll_mem_array_flat<T>, T>* levels,
			int level, size_t size) {

		_levels = levels;
		_level = level;
		_size = size;
		
#ifdef LL_ONE_VT

		if (level == 0) {
			_array = (T*) malloc(sizeof(T) * (size + LL_ENTRIES_PER_PAGE));
			memset(_array, 0xff, sizeof(T) * (size + LL_ENTRIES_PER_PAGE));
		}
		else {
			ll_mem_array_flat<T>* p = (*_levels)[level-1];
			if (size < p->_size) {
				LL_NOT_IMPLEMENTED;
			}
			else if (size == p->_size) {
				_array = p->_array;
			}
			else {
				_array = (T*) realloc(p->_array, (size + 4) * sizeof(T));
				memset(&_array[p->_size + 4], 0xff,
						sizeof(T) * (size - p->_size));
				if (_array != p->_array) {
					for (int l = 0; l < _level; l++) {
						(*_levels)[l]->_array = _array;
					}
				}
			}
		}

#else

		_array = (T*) malloc(sizeof(T) * (size + LL_ENTRIES_PER_PAGE));

		// TODO How to configure this? This is assumed by ll_slcsr
		memset(_array, 0xff, sizeof(T) * (size + LL_ENTRIES_PER_PAGE));

#endif
		
		_write = 0;
	}


	/**
	 * Destroy the instance
	 */
	~ll_mem_array_flat(void) {

#ifdef LL_ONE_VT
		if (_level == 0) free(_array);
#else
		free(_array);
#endif
	}


	/**
	 * Get the level number
	 *
	 * @return the level
	 */
	inline int level() const { return _level; }


	/**
	 * Init the vertex table as dense, which allows direct writes
	 */
	void dense_init() {
		// Nothing to do
	}


	/**
	 * Init the vertex table as COW
	 */
	void cow_init() {

		// Copy the previous vertex table

		if (_levels->has_prev_level(_level)) {
			ll_mem_array_flat<T>* p = _levels->prev_level(_level);
			size_t l = std::min(_size, p->_size) + 4;
			if (_array != p->_array) memcpy(_array, p->_array, l * sizeof(T));
		}
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
	 * Return the in-memory size
	 *
	 * @return the number of bytes occupied by this instance
	 */
	size_t in_memory_size() const {
		return sizeof(*this)
			+ /* data array size  */	sizeof(T) * _size;
	}


	/**
	 * Return the value associated with the given vertex
	 * 
	 * @param node the node id
	 * @return the associated value
	 */
	inline const T& operator[] (node_t node) const {
		return _array[node];
	}


	/**
	 * Append a node
	 *
	 * @param node the node
	 * @param value the value
	 */
	void append_node(node_t node, const T& value) {
		assert(_write == node);
		_array[_write++] = value;
	}


	/**
	 * Finish writing
	 */
	void finish(void) {
		// Nothing to do
	}


	/**
	 * Update a node -- do this only if you know what you are doing!
	 *
	 * This function is necessary to support the linked CSR design with
	 * a shared vertex table.
	 *
	 * @param node the node
	 * @param value the new value
	 */
	void update_node(node_t node, const T& value) {
		assert(node < _size);
		_array[node] = value;
	}


	/**
	 * Begin an iterator for nodes contained in this level of the vertex table
	 *
	 * @param iter the iterator variable
	 * @param start the start node ID
	 * @param end the last node ID (exclusive)
	 */
	void modified_node_iter_begin(ll_vertex_iterator& iter,
			node_t start = 0, node_t end = (node_t) -1) {

#ifdef LL_ONE_VT
		LL_NOT_IMPLEMENTED;
#endif

		memset(&iter, 0, sizeof(iter));
		iter.vi_next_node = start;
		iter.vi_end = end == -1 ? _size : std::min<node_t>(end, _size);

		if (_levels->has_prev_level(_level)) {
			auto prev = _levels->prev_level(_level);
			while (iter.vi_next_node < (node_t) prev->_size
					&& iter.vi_next_node < iter.vi_end) {
				if (_array[iter.vi_next_node] == prev->_array[iter.vi_next_node]) {
					iter.vi_next_node++;
				}
				else {
					break;
				}
			}
		}
	}


	/**
	 * Get the next (maybe) modified node
	 *
	 * @param iter the iterator variable
	 * @return the next node, or NIL_NODE if not available
	 */
	node_t modified_node_iter_next(ll_vertex_iterator& iter) {

		node_t r = iter.vi_next_node++;

		if (r >= iter.vi_end) {
			return LL_NIL_NODE;
		}
		else {
			iter.vi_value = &_array[r];

			if (_levels->has_prev_level(_level)) {
				auto prev = _levels->prev_level(_level);
				while (iter.vi_next_node < (node_t) prev->_size
						&& iter.vi_next_node < iter.vi_end) {
					if (_array[iter.vi_next_node] == prev->_array[iter.vi_next_node]) {
						iter.vi_next_node++;
					}
					else {
						break;
					}
				}
			}
		}

		return r;
	}


	/**
	 * Begin an iterator for nodes contained in this level of the vertex table
	 * and get the first node
	 *
	 * @param iter the iterator variable
	 * @param start the start node ID
	 * @param end the last node ID (exclusive)
	 * @return the next node, or NIL_NODE if not available
	 */
	inline node_t modified_node_iter_begin_next(ll_vertex_iterator& iter,
			node_t start = 0, node_t end = (node_t) -1) {
		modified_node_iter_begin(iter, start, end);
		return modified_node_iter_next(iter);
	}


	/**
	 * Direct write into a dense table
	 *
	 * @param node the node
	 * @param value the value
	 */
	void dense_direct_write(node_t node, const T& value) {
		_array[node] = value;
	}


	/**
	 * Finish writing
	 */
	void dense_finish(void) {
		// Nothing to do
	}


	/**
	 * Direct write into a COW table
	 *
	 * @param node the node
	 * @param value the value
	 */
	void cow_write(node_t node, const T& value) {
		_array[node] = value;
	}


	/**
	 * Finish COW
	 */
	void cow_finish(void) {
		// Nothing to do
	}


	/**
	 * Finish the edges part of the level
	 */
	void finish_level_edges(void) {
		// Nothing to do
	}


	/**
	 * Shrink the data
	 * 
	 * @param size the new size
	 */
	void shrink(size_t size) {

		if (_size < size) return;

		_size = size;
		_array = (T*) realloc(_array, sizeof(T) * (_size + LL_ENTRIES_PER_PAGE));
	}


	/**
	 * Get the number of pages
	 *
	 * @return the number of pages
	 */
	inline size_t pages() const {
		size_t n = _size >> LL_ENTRIES_PER_PAGE_BITS;
		if ((n << LL_ENTRIES_PER_PAGE_BITS) < _size) n++;
		return n;
	}


	/**
	 * Return the page
	 *
	 * @param index the page index
	 */
	inline T* page(size_t index) {
		return &_array[index << LL_ENTRIES_PER_PAGE_BITS];
	}



private:

	/// The array
	T* _array;

	/// The array size
	size_t _size;

	/// The write pointer
	size_t _write;

	/// The levels
	ll_mem_array_collection<ll_mem_array_flat<T>, T>* _levels;

	/// Level number
	int _level;
};



//==========================================================================//
// Class: ll_mem_array_swcow                                                //
//==========================================================================//

/**
 * A copy-on-write array representation of the vertex table -- a purely
 * software implementation using an extra indirection table.
 *
 * @author Peter Macko <peter.macko@oracle.com>
 */
template <typename T> class ll_mem_array_swcow {

public:

	/**
	 * Create an instance of ll_mem_array_swcow
	 *
	 * @param levels the vector of levels
	 * @param level the level number
	 * @param size the number of elements
	 */
	ll_mem_array_swcow(ll_mem_array_collection<ll_mem_array_swcow<T>, T>* levels,
			int level, size_t size) {

		assert(levels->page_manager()->page_bytes()
				== sizeof(T) * LL_ENTRIES_PER_PAGE);

		_levels = levels;
		_level = level;
		_size = size;
		_indirection = NULL;
		_modified_pages = 0;

		_cow_spinlock = 0;

		size_t entries_per_page = 1 << LL_ENTRIES_PER_PAGE_BITS;
		_pages = (size + 4) / entries_per_page;
		if ((size + 4) % entries_per_page > 0) _pages++;

		_indirection = NULL;
		_page_ids = NULL;

		memset(&_nil, 0, sizeof(_nil));
	}


	/**
	 * Destroy the instance
	 */
	virtual ~ll_mem_array_swcow(void) {

		_levels->page_manager()->release_pages(_page_ids, _pages + 1);

		bool free_indirection = _indirection != NULL;
		bool free_page_ids = _page_ids != NULL;

		if (_levels->has_prev_level(_level)) {
			auto vt = _levels->prev_level(_level);
			if (_indirection == vt->_indirection) free_indirection = false;
			if (_page_ids    == vt->_page_ids   ) free_page_ids    = false;
		}

		if (_level + 1 < (int) _levels->size() && (*_levels)[_level+1] != NULL) {
			auto vt = (*_levels)[_level+1];
			if (_indirection == vt->_indirection) free_indirection = false;
			if (_page_ids    == vt->_page_ids   ) free_page_ids    = false;
		}

		if (free_indirection) { free(_indirection); _indirection = NULL; }
		if (free_page_ids   ) { free(_page_ids   ); _page_ids    = NULL; }
	}


	/**
	 * Get the level number
	 *
	 * @return the level
	 */
	inline int level() const { return _level; }


	/**
	 * Return the array size
	 *
	 * @return the array size
	 */
	inline size_t size() const {
		return _size;
	}


	/**
	 * Return the in-memory size
	 *
	 * @return the number of bytes occupied by this instance
	 */
	size_t in_memory_size() const {
		return sizeof(*this)
			+ /* indirection size */	sizeof(T*) * _pages
			+ /* page IDs size    */	sizeof(size_t) * _pages
			+ /* data array size  */	_modified_pages
				* sizeof(T) * LL_ENTRIES_PER_PAGE;
	}


	/**
	 * Return the value associated with the given vertex
	 * 
	 * @param node the node id
	 * @return the associated value
	 */
	inline const T& operator[] (node_t node) const {
#ifdef FORCE_L0
		// TODO The data should fit in one chunk, and the indirection
		// should be hard-coded, such as:
		//   return _data[0][node];
		return _indirection[node >> LL_ENTRIES_PER_PAGE_BITS]
			[node & (LL_ENTRIES_PER_PAGE - 1)];
#else
		assert(node >= 0);
		assert(node <= (node_t) _size);		/* allow one extra value past the end */
		return _indirection[node >> LL_ENTRIES_PER_PAGE_BITS]
			[node & (LL_ENTRIES_PER_PAGE - 1)];
#endif
	}


	/**
	 * Init the vertex table as dense, which allows direct writes
	 */
	void dense_init() {

		_modified_pages = _pages + 1;

		assert(_indirection == NULL && _page_ids == NULL);
		_indirection = (T**) malloc(sizeof(T*) * (_pages + 1));
		memset(_indirection, 0, sizeof(T*) * (_pages + 1));
		_page_ids = (size_t*) malloc(sizeof(size_t) * (_pages + 1));
		memset(_page_ids, 0xff, sizeof(size_t) * (_pages + 1));

		// TODO Make sure this allocates contiguous memory for FORCE_L0
		_levels->page_manager()->allocate(_indirection, _page_ids, _pages + 1);

#ifdef _DEBUG
		// Assert that the pages are actually zeroed if configured to do so
		if (_levels->page_manager()->zeroes_pages()) {
			for (size_t i = 0; i < _size; i++)
				assert(*((int*) (void*) &(*this)[i]) == 0);
		}
#endif
	}


	/**
	 * Direct write into a dense table
	 *
	 * @param node the node
	 * @param value the value
	 */
	void dense_direct_write(node_t node, const T& value) {
		assert(node < (int) _modified_pages * LL_ENTRIES_PER_PAGE);

		size_t wp = node >> LL_ENTRIES_PER_PAGE_BITS;
		size_t wi = node & (LL_ENTRIES_PER_PAGE - 1);
		_indirection[wp][wi] = value;
	}


	/**
	 * Finish writing
	 */
	void dense_finish(void) {
	}


	/**
	 * Init the vertex table as a copy of the previous level, which can be
	 * further modified using copy-on-write until finalized
	 *
	 * @param zero true to zero the contents
	 */
	void cow_init(bool zero=false) {
		
		assert(_indirection == NULL && _page_ids == NULL);

		_modified_pages = 0;

		auto vt = _levels->prev_level(_level);
		assert(vt != NULL);

		if (_pages <= vt->_pages) {
			_indirection = vt->_indirection;
			_page_ids = vt->_page_ids;
			_levels->page_manager()->acquire_pages(_page_ids, _pages+1);
		}
		else {
			_indirection = (T**) malloc(sizeof(T*) * (_pages + 1));
			memset(_indirection, 0, sizeof(T*) * (_pages + 1));
			_page_ids = (size_t*) malloc(sizeof(size_t) * (_pages + 1));
			memset(_page_ids, 0xff, sizeof(size_t) * (_pages + 1));
			copy_indirection_range(0, _pages + 1);
		}
	}


	/**
	 * Write using copy-on-write
	 *
	 * @param node the node
	 * @param value the value
	 */
	void cow_write(node_t node, const T& value) {

		assert(node >= 0);
		assert(node <= (node_t) _size);		/* allow one extra value past the end */
		
		if (_modified_pages == 0) {
			ll_spinlock_acquire(&_cow_spinlock);
			if (_modified_pages == 0) {
				auto vt = _levels->prev_level(_level);
				if (_indirection == vt->_indirection) {
					_indirection = (T**) malloc(sizeof(T*) * (_pages + 1));
					_page_ids = (size_t*) malloc(sizeof(size_t) * (_pages + 1));
					memcpy(_indirection, vt->_indirection, sizeof(T*) * (_pages + 1));
					memcpy(_page_ids, vt->_page_ids, sizeof(size_t) * (_pages + 1));
				}
			}
			ll_spinlock_release(&_cow_spinlock);
		}

		size_t wp = node >> LL_ENTRIES_PER_PAGE_BITS;
		size_t wi = node & (LL_ENTRIES_PER_PAGE - 1);

		T* page = _indirection[wp];
		size_t page_id = _page_ids[wp];

		if (_levels->page_manager()->refcount(page_id) == 1) {
			page[wi] = value;
		}
		else {

			ll_spinlock_acquire(&_cow_spinlock);

			page = _indirection[wp];
			page_id = _page_ids[wp];

			if (_levels->page_manager()->refcount(page_id) == 1) {
				page[wi] = value;
				ll_spinlock_release(&_cow_spinlock);
				return;
			}


			// Copy on write

			_page_ids[wp] = _levels->page_manager()
				->cow(&_indirection[wp], page_id, page);
			_modified_pages++;

			_indirection[wp][wi] = value;

			ll_spinlock_release(&_cow_spinlock);
		}
	}


	/**
	 * Finish writing
	 */
	void cow_finish(void) {
		// Nothing to do
	}


	/**
	 * Finish the edges part of the level
	 */
	void finish_level_edges(void) {
		// Nothing to do
	}


	/**
	 * Shrink the data
	 * 
	 * @param size the new size
	 */
	void shrink(size_t size) {

		if (_size < size) return;

		_size = size;
		
		// Do we need to do anything else? Probably not, since if we have already
		// shrank the data structures in finish() or cow_finish().
	}


	/**
	 * Begin an iterator for nodes contained in this level of the vertex table
	 *
	 * @param iter the iterator variable
	 * @param start the start node ID
	 * @param end the last node ID (exclusive)
	 */
	void modified_node_iter_begin(ll_vertex_iterator& iter,
			node_t start = 0, node_t end = (node_t) -1) {

		memset(&iter, 0, sizeof(iter));
		iter.vi_next_node = start;
		iter.vi_end = end == -1 ? _size : std::min<node_t>(end, _size);
		
		if (_levels->has_prev_level(_level)) {
			auto prev = _levels->prev_level(_level);
			size_t page = iter.vi_next_node >> LL_ENTRIES_PER_PAGE_BITS;
			T* d = _indirection[page];

			if (page < prev->_pages) {
				T* p = prev->_indirection[page];

				size_t i = iter.vi_next_node
					& (LL_ENTRIES_PER_PAGE - 1);

				if (d == p || d[i] == p[i]) modified_node_iter_next(iter);
			}
		}
	}


	/**
	 * Get the next (maybe) modified node
	 *
	 * @param iter the iterator variable
	 * @return the next node, or NIL_NODE if not available
	 */
	node_t modified_node_iter_next(ll_vertex_iterator& iter) {

		node_t r = iter.vi_next_node++;

		if (r >= iter.vi_end) {
			return LL_NIL_NODE;
		}

		iter.vi_value = pointer(r);
			
		if (_levels->has_prev_level(_level)) {
			auto prev = _levels->prev_level(_level);
			T* zp = _levels->page_manager()->zero_page_ptr();

			while (iter.vi_next_node < (node_t) _size
					&& iter.vi_next_node < iter.vi_end) {
				size_t page = iter.vi_next_node >> LL_ENTRIES_PER_PAGE_BITS;
				assert(page < _pages);
				T* d = _indirection[page];


				// If there is a corresponding page in prev

				if (d != zp && page < prev->_pages) {
					T* p = prev->_indirection[page];
					
					// If the page in prev is identical

					if (d == p) {
						iter.vi_next_node = (iter.vi_next_node + LL_ENTRIES_PER_PAGE)
							& ~(LL_ENTRIES_PER_PAGE - 1);
						continue;
					}
					else {

						// The page in prev is different

						size_t i = iter.vi_next_node
							& (LL_ENTRIES_PER_PAGE - 1);

						while (i < LL_ENTRIES_PER_PAGE && iter.vi_next_node < iter.vi_end) {

							if (d[i] != p[i]) return r;

							iter.vi_next_node++;
							i++;
						}
					}
				}
				else {

					// This page is not in prev at all

					break;
				}
			}
		}

		return r;
	}


	/**
	 * Begin an iterator for nodes contained in this level of the vertex table
	 * and get the first node
	 *
	 * @param iter the iterator variable
	 * @param start the start node ID
	 * @param end the last node ID (exclusive)
	 * @return the next node, or NIL_NODE if not available
	 */
	inline node_t modified_node_iter_begin_next(ll_vertex_iterator& iter,
			node_t start = 0, node_t end = (node_t) -1) {
		modified_node_iter_begin(iter, start, end);
		return modified_node_iter_next(iter);
	}


	/**
	 * Get the number of pages
	 *
	 * @return the number of pages
	 */
	inline size_t pages() const {
		return _pages;
	}


	/**
	 * Return the page
	 *
	 * @param index the page index
	 */
	inline T* page(size_t index) {
		return _indirection[index];
	}


	/**
	 * Get the page ID of the given logical page index
	 *
	 * @param n the page index
	 * @return the page ID
	 */
	inline size_t page_id(size_t n) const {
		return _page_ids[n];
	}


	/**
	 * Find the lowest level ID for the given logical page index
	 *
	 * @param n the page index
	 * @return the lowest level ID in which the page is in this position
	 */
	size_t page_level_low(size_t n) const {

		size_t id = _page_ids[n];
		for (ssize_t l = _levels - 1; l >= 0; l--) {
			ll_mem_array_swcow<T>* level = (*_levels)[l];
			if (n  >= level->pages()) return l+1;
			if (id != level->page_id(n)) return l+1;
		}
		return 0;
	}


	/**
	 * Find the highest level ID for the given logical page index
	 *
	 * @param n the page index
	 * @return the highest level ID in which the page is in this position
	 */
	size_t page_level_high(size_t n) const {

		size_t id = _page_ids[n];
		for (ssize_t l = _levels + 1; l < _levels->size(); l++) {
			ll_mem_array_swcow<T>* level = (*_levels)[l];
			if (n  >= level->pages()) return l-1;
			if (id != level->page_id(n)) return l-1;
		}
		return _levels->size() - 1;
	}


private:

	/// The indirection table
	T** _indirection;

	/// The data arrays
	std::vector<T*> _data;

	/// The page IDs
	size_t* _page_ids;

	/// The zero page
	T* _zero_page;

	/// The array size
	size_t _size;

	/// The number of pages
	size_t _pages;

	/// The number of modified pages
	size_t _modified_pages;

	/// The block buffer
	T* _buffer;

	/// The write pointer
	size_t _write;

	/// The levels
	ll_mem_array_collection<ll_mem_array_swcow<T>, T>* _levels;

	/// Level number
	int _level;

	/// COW spinlock
	ll_spinlock_t _cow_spinlock;

	/// Nil
	T _nil;


	/**
	 * Return the pointer to the place in the data array associated with the given vertex
	 * 
	 * @param node the node id
	 * @return the pointer to the associated value, or NULL if it is out of bounds
	 */
	inline const T* pointer(node_t node) const {
		size_t page = node >> LL_ENTRIES_PER_PAGE_BITS;
		if (page >= _pages) return NULL;
		return &_indirection[page][node & (LL_ENTRIES_PER_PAGE - 1)];
	}


	/**
	 * Copy the indirection table
	 * 
	 * @param start where to start (page number)
	 * @param end where to end (exclusive)
	 */
	void copy_indirection_range(size_t start, size_t end) {

		if (start >= end) return;

		auto prev = _levels->prev_level(_level);
		size_t mp = prev->_pages;
		if (end <= mp) {
			memcpy(&_indirection[start],
					&prev->_indirection[start],
					sizeof(T*) * (end - start));
			memcpy(&_page_ids[start],
					&prev->_page_ids[start],
					sizeof(size_t) * (end - start));
			_levels->page_manager()->acquire_pages(&_page_ids[start], end-start);
		}
		else if (start < mp) {
			memcpy(&_indirection[start],
					&prev->_indirection[start],
					sizeof(T*) * (mp - start));
			memcpy(&_page_ids[start],
					&prev->_page_ids[start],
					sizeof(size_t) * (mp - start));
			_levels->page_manager()->acquire_pages(&_page_ids[start], mp-start);

			T* zp_ptr;
			size_t zp = _levels->page_manager()->zero_page(&zp_ptr, end-mp);
			for (size_t i = mp; i < end; i++) _indirection[i] = zp_ptr;
			for (size_t i = mp; i < end; i++) _page_ids[i] = zp;
		}
		else /* if (start >= mp) */ {
			T* zp_ptr;
			size_t zp = _levels->page_manager()->zero_page(&zp_ptr, end-start);
			for (size_t i = start; i < end; i++) _indirection[i] = zp_ptr;
			for (size_t i = start; i < end; i++) _page_ids[i] = zp;
		}
	}
};


#endif	/* LL_MEM_ARRAY_H_ */

