/*
 * ll_edge_table.h
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


#ifndef LL_EDGE_TABLE_H_
#define LL_EDGE_TABLE_H_

#include "llama/ll_common.h"
#include "llama/ll_mlcsr_helpers.h"



//==========================================================================//
// Class: ll_et_array                                                       //
//==========================================================================//

/**
 * An array representation of the edge table
 *
 * @author Peter Macko <peter.macko@oracle.com>
 */
template <typename T> struct ll_et_array {

	/// The values
	T _values[1];		/* must be the ONLY member */


	/**
	 * Access an element based on the index
	 *
	 * @param index the element
	 * @return the reference to the element
	 */
	inline T& operator[] (edge_t index) {
		return _values[index];
	}


	/**
	 * Access an element based on the index
	 *
	 * @param index the element
	 * @return the reference to the element
	 */
	inline const T& operator[] (edge_t index) const {
		return _values[index];
	}


	/**
	 * Access an element based on the node and the edge index
	 *
	 * @param node the node
	 * @param index the edge index
	 * @return the reference to the element
	 */
	inline T& edge_value(node_t node, edge_t edge) {
		return _values[edge];
	}


	/**
	 * Access an element based on the node and the edge index
	 *
	 * @param node the node
	 * @param index the edge index
	 * @return the reference to the element
	 */
	inline const T& edge_value(node_t node, edge_t edge) const {
		return _values[edge];
	}


	/**
	 * Access an element based on the node and the edge index
	 *
	 * @param node the node
	 * @param index the edge index
	 * @return the pointer to the element
	 */
	inline T* edge_ptr(node_t node, edge_t edge) {
		return &_values[edge];
	}


	/**
	 * Access an element based on the node and the edge index
	 *
	 * @param node the node
	 * @param index the edge index
	 * @return the pointer to the element
	 */
	inline const T* edge_ptr(node_t node, edge_t edge) const {
		return &_values[edge];
	}


	/**
	 * Memset
	 *
	 * @param start the start
	 * @param finish the finish (exclusive)
	 * @param byte the byte
	 */
	void memset(edge_t start, edge_t finish, int byte) {
		::memset(&_values[start], byte, (finish - start) * sizeof(T));
	}


	/**
	 * Copy a range of values
	 *
	 * @param to the destination
	 * @param source the source
	 * @param start the start
	 * @param length the length
	 */
	void copy(edge_t to, const ll_et_array<T>* source, edge_t start, size_t length) {
		memcpy(&_values[to], &source->_values[start], sizeof(T) * length);
	}


	/**
	 * Advise
	 *
	 * @param from the start index (inclusive)
	 * @param to the end index (exclusive)
	 * @param advice the advice (LL_ADV_*)
	 */
	void advise(edge_t from, edge_t to, int advice = LL_ADV_WILLNEED) {

		assert(from <= to);

#ifdef LL_PERSISTENCE

		// Assume that the start of the mapping is page-aligned

		size_t fi = from - from % (4096 / sizeof(T));
		size_t ti = to   + (4096 / sizeof(T) - to % (4096 / sizeof(T)));

		madvise(&_values[fi], sizeof(T) * (ti - fi), advice);
#else

		if (advice == LL_ADV_WILLNEED) {
			__builtin_prefetch(&_values[from]);
			__builtin_prefetch(&_values[from+1]);
		}
#endif
	}
};


/**
 * Create a new instance of ll_et_array
 *
 * @param capacity the capacity
 * @param max_nodes the number of nodes
 * @return the new instance
 */
template <typename T>
ll_et_array<T>* new_ll_et_array(size_t capacity, size_t max_nodes) {
	ll_et_array<T>* et = (ll_et_array<T>*) malloc(sizeof(ll_et_array<T>)
			+ capacity * sizeof(T));
	return et;
}


/**
 * Destroy an instance of ll_et_array
 *
 * @param et the edge table
 */
template <typename T>
void delete_ll_et_array(ll_et_array<T>* et) {
	free(et);
}



//==========================================================================//
// Class: ll_et_mmaped_array                                                //
//==========================================================================//

/**
 * An array representation of the edge table that can be mmap-ed
 */
template <typename T> struct ll_et_mmaped_array : ll_et_array<T> {};


/**
 * Create a new instance of ll_et_mmaped_array
 *
 * @param capacity the capacity
 * @param max_nodes the number of nodes
 * @return the new instance
 */
template <typename T>
ll_et_mmaped_array<T>* new_ll_et_mmaped_array(size_t capacity, size_t max_nodes) {
	// Nothing to do - this will be taken care of by the persistence manager
	return NULL;
}


/**
 * Destroy an instance of ll_et_mmaped_array
 *
 * @param et the edge table
 */
template <typename T>
void delete_ll_et_mmaped_array(ll_et_mmaped_array<T>* et) {
	// Nothing to do - this will be unmapped during the persistence destroy
}

#endif
