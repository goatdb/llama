/*
 * ll_slcsr.h
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


#ifndef LL_SLCSR_H_
#define LL_SLCSR_H_

#include "llama/ll_mlcsr_sp.h"



//==========================================================================//
// The basic desgin                                                         //
//==========================================================================//


/**
 * An element in the vertex table
 */
typedef struct {
	edge_t adj_list_start;
} ll_slcsr__begin_t;


/**
 * Compare two vertex table elements
 *
 * @param a the first element
 * @param b the second element
 * @return true if they are equal
 */
inline bool operator== (const ll_slcsr__begin_t& a,
		const ll_slcsr__begin_t& b) {

	return a.adj_list_start == b.adj_list_start;
}


/**
 * Compare two vertex table elements
 *
 * @param a the first element
 * @param b the second element
 * @return true if they are equal
 */
inline bool operator!= (const ll_slcsr__begin_t& a,
		const ll_slcsr__begin_t& b) {

	return a.adj_list_start != b.adj_list_start;
}


/**
 * The basic singlelevel CSR
 */
class ll_slcsr
	: public ll_csr_base<LL_VT, ll_slcsr__begin_t, node_t> {

	typedef node_t T;


public:

	/**
	 * Create a new instance of class ll_slcsr
	 *
	 * @param storage the persistence context
	 * @param name the name of this data component (must be a valid filename prefix)
	 */
	ll_slcsr(IF_LL_PERSISTENCE(ll_persistent_storage* storage,) const char* name)
		: ll_csr_base<LL_VT, ll_slcsr__begin_t, node_t>
		  (IF_LL_PERSISTENCE(storage,) name) {
	}


	/**
	 * Create a read-only clone of ll_slcsr
	 *
	 * @param master the master object
	 * @param level the max level
	 */
	ll_slcsr(ll_slcsr* master, int level)
		: ll_csr_base<LL_VT, ll_slcsr__begin_t, node_t>(master, level)
	{
	}


	/**
	 * Destroy the graph
	 */
	virtual ~ll_slcsr() {
	}


	/**
	 * Initialize a node
	 *
	 * @param node the node
	 * @param new_edges the number of new edges
	 * @param deleted_edges the number of deleted edges
	 * @return the corresponding edge table index
	 */
	size_t init_node(node_t node, size_t new_edges, size_t deleted_edges) {

		ll_slcsr__begin_t e;
		memset(&e, 0, sizeof(e));

		e.adj_list_start = this->_et_write_index;
		this->_latest_begin->dense_direct_write(node, e);

		this->_et_write_index += new_edges;
		return e.adj_list_start;
	}


	/**
	 * Initialize a level from an array of node degrees
	 *
	 * @param max_nodes the total number of nodes, cummulative up until this level
	 * @param new_edge_counts the array of numbers of new edges for each node
	 * @param deleted_edge_counts the array of numbers of deleted edges for each node
	 * @param copy_edge_callback the callback to copy edges
	 * @param copy_edge_callback_data the data for the callback
	 */
	virtual void init_level_from_degrees(size_t max_nodes,
			degree_t* new_edge_counts,
			degree_t* deleted_edge_counts,
			ll_copy_edge_callback_t copy_edge_callback = NULL,
			void* copy_edge_callback_data = NULL) {

		size_t max_edges = 0;
		size_t max_adj_lists = 0;
		for (size_t i = 0; i < max_nodes; i++) {
			max_edges += new_edge_counts[i];
			if (new_edge_counts[i] > 0) max_adj_lists++;
		}

		this->init_level(max_nodes, max_adj_lists, max_edges,
				copy_edge_callback, copy_edge_callback_data);
		auto* b = this->_begin[this->_begin.size() - 1];

		ll_slcsr__begin_t e;
		memset(&e, 0, sizeof(e));

		size_t x = 0;
		size_t source = 0;

		for (source = 0; source < max_nodes; source++) {
			e.adj_list_start = x;
			b->dense_direct_write(source, e);
			x += new_edge_counts[source];
		}

		e.adj_list_start = x;
		b->dense_direct_write(source, e);

		this->_max_nodes = max_nodes;
		this->_max_edges = max_edges;
	}


	/**
	 * Add a vertex with one edge
	 *
	 * @param node the node
	 * @param index the index within the node's adjacency list
	 * @param value the value
	 * @return the edge that was just written
	 */
	virtual edge_t write_value(node_t node, size_t index, const T& value) {

		int level = this->_begin.size() - 1;
		edge_t start = (*this->_begin[level])[node].adj_list_start;
		edge_t e = start + index;

		(*this->_values[level])[e] = value;

		return e;
	}


	/**
	 * Finish the vertices part of the level (use only for levels created
	 * directly using init_level, not init_level_from_degrees).
	 */
	virtual void finish_level_vertices() {

		// Finish the edge table by writing the last value

		ll_slcsr__begin_t e;
		e.adj_list_start = this->_et_write_index;
		this->_latest_begin->dense_direct_write(this->_max_nodes, e);


		// Fill in the holes

		// This depends on the thing being originally memset to 0xff
		// TODO This should be made explicit when initializing the new level

		for (node_t n = this->_max_nodes; n > 0; n--) {
			if ((*this->_latest_begin)[n-1].adj_list_start == (node_t) -1) {
				this->_latest_begin->dense_direct_write(n-1,
						(*this->_latest_begin)[n]);
			}
		}

		ll_csr_base<LL_VT, ll_slcsr__begin_t, node_t>::finish_level_vertices();
	}


	/**
	 * Finish the level
	 *
	 * @param index the index to the next available position in the _values array
	 */
	virtual void finish_level(size_t& index) {
		
		ll_slcsr__begin_t b;
		b.adj_list_start = index; //this->_max_edges;

		this->_latest_begin->dense_direct_write(this->_max_nodes, b);
		this->_perLevelEdges[this->_perLevelEdges.size()-1] = index;
	}


	/**
	 * Get the value associated with the given edge
	 *
	 * @param e the edge
	 * @return the value
	 */
	T value(edge_t e) const {
        return (*this->_values[0])[e];
    }


	/**
	 * Get the node degree
	 *
	 * @param n the node
	 * @return the degree
	 */
	size_t degree(node_t n) const {
		edge_t e = (*this->_latest_begin)[n].adj_list_start;
		return (*this->_latest_begin)[n+1].adj_list_start - e;
	}


	/**
	 * Get the node degree at the given level
	 *
	 * @param n the node
	 * @param level the level
	 * @return the degree
	 */
	size_t degree(node_t n, int level) const {
		return degree(n);
	}


	/**
	 * Start the iterator for the given node
	 *
	 * @param iter the iterator
	 * @param n the node
	 * @return the iterator
	 */
	void iter_begin(ll_edge_iterator& iter, node_t n, size_t a=0, size_t b=0)
		const {
	
		// TODO Is this still faster than the original Green-Marl?
		// I really hope so!

		iter.owner = LL_I_OWNER_RO_CSR;
		iter.node = n;
		iter.edge = (*this->_latest_begin)[n].adj_list_start;
		iter.left = (*this->_latest_begin)[n+1].adj_list_start - iter.edge;
		iter.ptr = &(*this->_latest_values)[iter.edge];

		LL_D_NODE_PRINT(n, "[left=%ld, edge=%lx, n_edge=%lx]\n",
				(long) iter.left, (long) iter.edge,
				(long) (*this->_latest_begin)[n+1].adj_list_start);
	}


	/**
	 * Determine if there are any more items left
	 *
	 * @param iter the iterator
	 * @return true if there are more items
	 */
	ITERATOR_DECL bool iter_has_next(ll_edge_iterator& iter) const {
		return iter.left > 0;
	}


	/**
	 * Get the next item
	 *
	 * @param iter the iterator
	 * @return the next edge or LL_NIL_EDGE if none
	 */
	ITERATOR_DECL edge_t iter_next(ll_edge_iterator& iter) const {

		// Note: Does not support deletions by design -- this is a single-level CSR

		if (iter.left > 0) {
			iter.left--;
        	iter.last_node = (*this->_latest_values)[iter.edge];
			return iter.edge++;
		}
		else {
        	iter.last_node = LL_NIL_NODE;
			return LL_NIL_EDGE;
		}
	}


	/**
	 * Start an iterator and get the next value immediately
	 *
	 * @param iter the iterator
	 * @param n the node
	 * @param level the level
	 * @param max_level the max level for deletions
	 * @return the next item, or LL_NIL_EDGE if none
	 */
	inline edge_t iter_begin_next(ll_edge_iterator& iter, node_t n,
			int level=-1, int max_level=-1) const {
		iter_begin(iter, n, level, max_level);
		return iter_next(iter);
	}


	/**
	 * Start the iterator for the given node, but only within this level
	 *
	 * @param iter the iterator
	 * @param n the node
	 * @param level the level (must be 0)
	 * @param max_level the maximum visible level for deletions (must be <= 0)
	 * @param vte the vertex table element (ignored)
	 * @return the iterator
	 */
	void iter_begin_within_level(ll_edge_iterator& iter, node_t n,
			int level, int max_level=-1, void* vte=NULL) const {
		assert(level <= 0 && max_level <= 0);
		iter_begin(iter, n);
	}


	/**
	 * Determine if there are any more items left, but only within this level
	 *
	 * @param iter the iterator
	 * @return true if there are more items
	 */
	ITERATOR_DECL bool iter_has_next_within_level(ll_edge_iterator& iter) const {
		return iter_has_next(iter);
	}


	/**
	 * Get the next item, but only within this level
	 *
	 * @param iter the iterator
	 * @return the next edge or LL_NIL_EDGE if none
	 */
	ITERATOR_DECL edge_t iter_next_within_level(ll_edge_iterator& iter) const {
		return iter_next(iter);
	}


	/**
	 * Find the given node and value combination
	 *
	 * @param node the node
	 * @param value the value
	 * @return the edge, or NIL_EDGE if it does not exist
	 */
	edge_t find(node_t node, T value) const {

		ll_edge_iterator iter;
		this->iter_begin(iter, node);
		FOREACH_ITER(e, *this, iter) {
			if (iter.last_node == value) return e;
		}

		return LL_NIL_EDGE;
	}


	/**
	 * Write a vertex with all of its edges
	 *
	 * @param node the node
	 * @param adj_list the adjacency list
	 */
	virtual void write_values(node_t node, const ll_w_out_edges_t& adj_list) {
		size_t start = LL_EDGE_INDEX((*this->_latest_begin)[node].adj_list_start);
		size_t level = LL_EDGE_LEVEL((*this->_latest_begin)[node].adj_list_start);
		for (size_t i = 0; i < adj_list.size(); i++) {
			w_edge* e = adj_list[i];
			if (e->exists()) {
				this->_latest_values->edge_value(node, start)
					= LL_VALUE_CREATE(e->we_target);
				e->we_numerical_id = LL_EDGE_CREATE(level, start);
				start++;
			}
		}
	}


	/**
	 * Write a vertex with all of its edges
	 *
	 * @param node the node
	 * @param adj_list the adjacency list
	 */
	virtual void write_values(node_t node, const ll_w_in_edges_t& adj_list) {
		size_t start = LL_EDGE_INDEX((*this->_latest_begin)[node].adj_list_start);
		size_t level = LL_EDGE_LEVEL((*this->_latest_begin)[node].adj_list_start);
		for (size_t i = 0; i < adj_list.size(); i++) {
			w_edge* e = adj_list[i];
			if (e->exists()) {
				this->_latest_values->edge_value(node, start)
					= LL_VALUE_CREATE(e->we_source);
				e->we_reverse_numerical_id = LL_EDGE_CREATE(level, start);
				start++;
			}
		}
	}


	/**
	 * C++ style iterator
	 */
	class iterator : public std::iterator<std::input_iterator_tag, node_t> {

		/// The owner
		const ll_slcsr* _owner;

		/// The nested iterator
		ll_edge_iterator _iter;

		/// Last edge
		edge_t _edge;

	public:

		/**
		 * Create an instace of the iterator
		 */
		inline iterator() {}

		/**
		 * Create an instace of the iterator
		 *
		 * @param iter the other iterator
		 */
		inline iterator(const iterator& iter) { *this = iter; }

		/**
		 * Create an instace of the iterator
		 *
		 * @param owner the owner
		 */
		inline iterator(const ll_slcsr& owner) : _owner(&owner) {}

		/**
		 * Destroy the iterator
		 */
		inline ~iterator() {}

		/**
		 * The nested iterator
		 *
		 * @return the nested iterator
		 */
		inline ll_edge_iterator& nested_iterator() { return _iter; }

		/**
		 * Set the owner
		 *
		 * @param owner the owner
		 */
		void set_owner(const ll_slcsr* owner) { _owner = owner; }

		/**
		 * Set to the end
		 */
		void move_to_end() {
			_edge = LL_NIL_EDGE;
			_iter.edge = LL_NIL_EDGE;
			_iter.last_node = LL_NIL_NODE;
			_iter.left = 0;
		}

		/**
		 * Assign the iterator
		 *
		 * @param iter the other iterator
		 */
		iterator& operator= (const iterator& iter) {
			_owner = iter._owner;
			_iter  = iter._iter;
			_edge  = iter._edge;
			return *this;
		}

		/**
		 * Advance the iterator
		 *
		 * @return the iterator
		 */
		inline iterator& operator++ () {
			_edge = _owner->iter_next(_iter);
			return *this;
		}

		/**
		 * Advance the iterator
		 *
		 * @return a copy of the previous state of the iterator
		 */
		inline iterator operator++ (int) {
			iterator tmp(*this);
			_edge = _owner->iter_next(_iter);
			return tmp;
		}

		/**
		 * Compare two iterators
		 *
		 * @param iter the other iterators
		 * @return true if they are equal
		 */
		inline bool operator== (const iterator& iter) const {
			return _edge == iter._edge;
		}

		/**
		 * Compare two iterators
		 *
		 * @param iter the other iterators
		 * @return true if they are not equal
		 */
		inline bool operator!= (const iterator& iter) const {
			return _edge != iter._edge;
		}

		/**
		 * Determine if the iterator is at the end
		 *
		 * @return true if it is at the end
		 */
		inline bool at_end() const { return _edge == LL_NIL_EDGE; }

		/**
		 * Dereference the iterator
		 *
		 * @return the node
		 */
		inline const node_t& operator*() const { return _iter.last_node; }

		/**
		 * Dereference the iterator
		 *
		 * @return the node
		 */
		inline node_t& operator*() { return _iter.last_node; }

		/**
		 * Dereference the iterator
		 *
		 * @return the node
		 */
		inline const node_t* operator->() const { return &_iter.last_node; }

		/**
		 * Dereference the iterator
		 *
		 * @return the node
		 */
		inline node_t* operator->() { return &_iter.last_node; }
	};


	/// Const iterator is the same
	typedef iterator const_iterator;


	/**
	 * Create a C++ style iterator
	 *
	 * @param node the node
	 * @return the iterator
	 */
	iterator begin(node_t node) {
		iterator iter(*this);
		iter_begin(iter.nested_iterator(), node);
		return ++iter;
	}


	/**
	 * Create a C++ style iterator
	 *
	 * @param iter the iterator
	 * @param node the node
	 * @return the iterator
	 */
	iterator& begin(iterator& iter, node_t node) {
		iter.set_owner(this);
		iter_begin(iter.nested_iterator(), node);
		return ++iter;
	}


	/**
	 * Create a C++ style iterator
	 *
	 * @param node the node
	 * @return the iterator
	 */
	const_iterator begin(node_t node) const {
		iterator iter(*this);
		iter_begin(iter.nested_iterator(), node);
		return ++iter;
	}


	/**
	 * Create the end iterator
	 *
	 * @param node the node (optional)
	 * @return the iterator
	 */
	iterator end(node_t node=0) {
		iterator iter(*this);
		iter.move_to_end();
		return iter;
	}


	/**
	 * Create the end iterator
	 *
	 * @param node the node (optional)
	 * @return the iterator
	 */
	const_iterator end(node_t node=0) const {
		iterator iter(*this);
		iter.move_to_end();
		return iter;
	}


protected:

	/**
	 * Calculate the max number of elements in the edge table array
	 *
	 * @param level the level number
	 * @param max_nodes the total number of nodes, cummulative up until this level
	 * @param max_adj_lists the max number of adj. lists in the level
	 * @param max_edges the number of edges within the level
	 * @return the number of elements
	 */
	virtual size_t values_length(int level, size_t max_nodes,
			size_t max_adj_lists, size_t max_edges) const {
#if defined(LL_SLCSR_ALE_ET)
		return max_edges + max_adj_lists;
#else
		return max_edges;
#endif
	}
};

#endif
