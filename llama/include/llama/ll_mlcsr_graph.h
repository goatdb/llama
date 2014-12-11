/*
 * ll_mlcsr_graph.h
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


#ifndef LL_MLCSR_GRAPH_H_
#define LL_MLCSR_GRAPH_H_

#include "llama/ll_mem_array.h"
#include "llama/ll_writable_elements.h"

#include "llama/ll_mlcsr_helpers.h"
#include "llama/ll_mlcsr_sp.h"
#include "llama/ll_mlcsr_properties.h"

class ll_database;



//==========================================================================//
// Interface: ll_mlcsr_checkpoint_source                                    //
//==========================================================================//

/**
 * The checkpoint data source
 */
class ll_mlcsr_checkpoint_source {

public:

	ll_mlcsr_checkpoint_source() {}
	virtual ~ll_mlcsr_checkpoint_source() {}

	virtual size_t num_new_nodes() = 0;
	virtual size_t num_new_edges() = 0;
	virtual size_t max_node_id() = 0;

	virtual ll_w_vt_vertices_t* vertex_table() = 0;

	virtual void get_out_edges(node_t node, std::vector<node_t>& new_edges) = 0;
};



//==========================================================================//
// The read-only graph                                                      //
//==========================================================================//

/**
 * The multilevel CSR graph
 */
class ll_mlcsr_ro_graph {

	/// The database
	ll_database* _database;

	/// The master copy (if this is a read-only clone)
	ll_mlcsr_ro_graph* _master;


	/*-----------------------------------------------------------------------*
	 * Graph structure                                                       *
	 *-----------------------------------------------------------------------*/

	/// The out-edges
	LL_CSR _out;
	
	/// The in-edges
	LL_CSR _in;

	/// All graph CSRs (user tables + out, in)
	std::unordered_map<std::string, LL_CSR*> _csrs;

	/// The _csrs map update lock
	ll_spinlock_t _csrs_update_lock;

	/// The memory pool for sparse node IDs
	ll_memory_pool_for_large_allocations* _pool_for_sparse_node_ids;

	/// The memory pool for sparse node data
	ll_memory_pool_for_large_allocations* _pool_for_sparse_node_data;



	/*-----------------------------------------------------------------------*
	 * Properties                                                            *
	 *-----------------------------------------------------------------------*/

	/// The node 32-bit properties
	std::unordered_map<std::string, ll_mlcsr_node_property<uint32_t>*>
		_node_properties_32;

	/// The node 64-bit properties
	std::unordered_map<std::string, ll_mlcsr_node_property<uint64_t>*>
		_node_properties_64;

	/// The edge 32-bit properties
	std::unordered_map<std::string, ll_mlcsr_edge_property<uint32_t>*>
		_edge_properties_32;

	/// The edge 64-bit properties
	std::unordered_map<std::string, ll_mlcsr_edge_property<uint64_t>*>
		_edge_properties_64;

	/// The edge 32-bit properties - get by ID
	ll_mlcsr_edge_property<uint32_t>*
		_edge_properties_by_id_32[LL_MAX_EDGE_PROPERTY_ID];

	/// The edge 64-bit properties - get by ID
	ll_mlcsr_edge_property<uint64_t>*
		_edge_properties_by_id_64[LL_MAX_EDGE_PROPERTY_ID];

#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES

	/// The edge integer weights for streaming applications
	ll_mlcsr_edge_property<uint32_t>* _edge_stream_weights;

	/// The edge forward pointers for updating weights in streaming applications
	ll_mlcsr_edge_property<edge_t>* _edge_stream_forward;

#endif

	/// The next node property ID
	std::atomic<int> _next_node_property_id;

	/// The next edge property ID
	std::atomic<int> _next_edge_property_id;


	/*-----------------------------------------------------------------------*
	 * Common                                                                *
	 *-----------------------------------------------------------------------*/

	/// The update lock
	ll_spinlock_t _update_lock;

	/// The persistent storage
	IF_LL_PERSISTENCE(ll_persistent_storage* _storage);


public:

	/**
	 * Create an instance of class ll_mlcsr_ro_graph
	 *
	 * @param database the database context
	 * @param storage the persistence context
	 */
	ll_mlcsr_ro_graph(ll_database* database
			IF_LL_PERSISTENCE(, ll_persistent_storage* storage))
		: _out(IF_LL_PERSISTENCE(storage,) "out"),
		_in(IF_LL_PERSISTENCE(storage,) "in")
	{

		_database = database;
		_csrs_update_lock = 0;
		_update_lock = 0;
		_master = NULL;

		_next_node_property_id = 0;
		_next_edge_property_id = 0;

		memset(&_edge_properties_by_id_32, 0, sizeof(_edge_properties_by_id_32));
		memset(&_edge_properties_by_id_64, 0, sizeof(_edge_properties_by_id_64));

		IF_LL_PERSISTENCE(_storage = storage);

		_pool_for_sparse_node_ids = new ll_memory_pool_for_large_allocations();
		_pool_for_sparse_node_data = new ll_memory_pool_for_large_allocations();

		_out.set_memory_pools_for_sparse_representaion(
				_pool_for_sparse_node_ids, _pool_for_sparse_node_data);
		_in.set_memory_pools_for_sparse_representaion(
				_pool_for_sparse_node_ids, _pool_for_sparse_node_data);

		_csrs[_out.name()] = & _out;
		_csrs[ _in.name()] = &  _in;

#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES
		_edge_stream_weights
			= this->create_uninitialized_edge_property_32("stream-weight", LL_T_INT32);

		// TODO This should be a *sparse* property, not dense -- use COW,
		// not DENSE_INIT when initializing
		_edge_stream_forward = reinterpret_cast<ll_mlcsr_edge_property<edge_t>*>(
				(sizeof(edge_t) == sizeof(uint32_t)
				 ? (void*) this->create_uninitialized_edge_property_32(
					 "stream-forward", LL_T_INT32)
				 : (void*) this->create_uninitialized_edge_property_64(
					 "stream-forward", LL_T_INT64)));
#endif

#ifdef LL_PERSISTENCE

		// Open all CSRs

		std::vector<std::string> csrs = storage->list_context_names("csr");
		for (size_t i = 0; i < csrs.size(); i++) {
			if (csrs[i] == "out" || csrs[i] == "in") continue;
			LL_CSR* csr = new LL_CSR(storage, csrs[i].c_str());
			csr->set_memory_pools_for_sparse_representaion(
					_pool_for_sparse_node_ids, _pool_for_sparse_node_data);
			_csrs[csrs[i]] = csr;
		}


		// Open all node properties

		std::vector<std::string> nps = storage->list_context_names("np");
		for (size_t i = 0; i < nps.size(); i++) {

			// Get the property metadata

			ll_length_and_data* ld = ll_persistence_context::read_header(
					storage, nps[i].c_str(), "np");
			if (ld == NULL) {
				LL_W_PRINT("No property metadata for %s\n", nps[i].c_str());
				continue;
			}
			if (ld->ld_length >
				sizeof(ll_mlcsr_node_property<node_t>::persistence_header)) {
				LL_W_PRINT("Bad property metadata for %s\n", nps[i].c_str());
				free(ld);
				continue;
			}

			ll_mlcsr_node_property<node_t>::persistence_header h;
			memcpy(&h, ld->ld_data, ld->ld_length);
			free(ld);


			// Open the integral node property

			if (ll_is_type_integral32(h.h_type)) {
				this->create_uninitialized_node_property_32(nps[i].c_str(),
						h.h_type);
				continue;
			}

			if (ll_is_type_integral64(h.h_type)) {
				this->create_uninitialized_node_property_64(nps[i].c_str(),
						h.h_type);
				continue;
			}


			// Otherwise it is not yet implemented

			LL_W_PRINT("Cannot open node property %s: Not implemented\n",
					nps[i].c_str());
		}


		// Open all edge properties

		std::vector<std::string> eps = storage->list_context_names("ep");
		for (size_t i = 0; i < eps.size(); i++) {

			if (eps[i].length() < 2) continue;
			if (strcmp(eps[i].c_str() + (eps[i].length() - 2), "-0") != 0)
				continue;

			char* px = strdup(eps[i].c_str());
			*(px + (eps[i].length() - 2)) = '\0';
			std::string pn = px;
			free(px);

			if (pn == "out-et" || pn == "in-et") continue;


			// TODO Open the edge property
			(void) pn;

			LL_W_PRINT("Cannot open edge property %s: Not implemented\n",
					pn.c_str());
		}
#endif
	}


	/**
	 * Create a read-only clone of ll_mlcsr_ro_graph
	 *
	 * @param master the master object
	 * @param level the max level (use -1 for latest)
	 */
	ll_mlcsr_ro_graph(ll_mlcsr_ro_graph* master, int level=-1)
		: _out(&master->_out, level < 0 ? master->max_level() : level),
		  _in (&master->_in , level < 0 ? master->max_level() : level)
	{
		if (level < 0) level = master->max_level();
		assert(level >= 0);

		_master = master;
		_database = master->_database;

		_csrs[_out.name()] = & _out;
		_csrs[ _in.name()] = &  _in;
		_csrs_update_lock = 0;

		_pool_for_sparse_node_ids = master->_pool_for_sparse_node_ids;
		_pool_for_sparse_node_data = master->_pool_for_sparse_node_data;

		for (auto it = master->_csrs.begin(); it != master->_csrs.end(); it++){
			if (it->second == NULL
					|| it->second == &master->_out
					|| it->second == &master->_in) continue;
			_csrs[it->first] = new LL_CSR(it->second, level);
		}

		_next_node_property_id = master->_next_node_property_id.load();
		_next_edge_property_id = master->_next_edge_property_id.load();

		memset(&_edge_properties_by_id_32, 0, sizeof(_edge_properties_by_id_32));
		memset(&_edge_properties_by_id_64, 0, sizeof(_edge_properties_by_id_64));

		for (auto it = master->_node_properties_32.begin();
				it != master->_node_properties_32.end(); it++) {
			if (it->second == NULL) continue;
			_node_properties_32[it->first]
				= new ll_mlcsr_node_property<uint32_t>(it->second, level);
		}

		for (auto it = master->_node_properties_64.begin();
				it != master->_node_properties_64.end(); it++) {
			if (it->second == NULL) continue;
			_node_properties_64[it->first]
				= new ll_mlcsr_node_property<uint64_t>(it->second, level);
		}

		for (auto it = master->_edge_properties_32.begin();
				it != master->_edge_properties_32.end(); it++) {
			if (it->second == NULL) continue;
			_edge_properties_32[it->first]
				= new ll_mlcsr_edge_property<uint32_t>(it->second, level);
			_edge_properties_by_id_32[_edge_properties_32[it->first]->id()]
				= _edge_properties_32[it->first];
		}

		for (auto it = master->_edge_properties_64.begin();
				it != master->_edge_properties_64.end(); it++) {
			if (it->second == NULL) continue;
			_edge_properties_64[it->first]
				= new ll_mlcsr_edge_property<uint64_t>(it->second, level);
			_edge_properties_by_id_64[_edge_properties_64[it->first]->id()]
				= _edge_properties_64[it->first];
		}

#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES

		_edge_stream_weights
			= reinterpret_cast<ll_mlcsr_edge_property<uint32_t>*>(
					get_edge_property_32("stream-weight"));

		_edge_stream_forward = reinterpret_cast<ll_mlcsr_edge_property<edge_t>*>(
				(sizeof(edge_t) == sizeof(uint32_t)
				 ? (void*) get_edge_property_32("stream-forward")
				 : (void*) get_edge_property_64("stream-forward")));
#endif

		_update_lock = 0;
		IF_LL_PERSISTENCE(_storage = master->_storage);
	}


	/**
	 * Destroy the instance of this class
	 */
	virtual ~ll_mlcsr_ro_graph() {

		for (auto it = _csrs.begin(); it != _csrs.end(); it++) {
			if (it->second != NULL
					&& it->second != &_out
					&& it->second != &_in) delete it->second;
		}

		for (auto it = _node_properties_32.begin();
				it != _node_properties_32.end(); it++) {
			if (it->second != NULL) delete it->second;
		}

		for (auto it = _node_properties_64.begin();
				it != _node_properties_64.end(); it++) {
			if (it->second != NULL) delete it->second;
		}

		for (auto it = _edge_properties_32.begin();
				it != _edge_properties_32.end(); it++) {
			if (it->second != NULL) delete it->second;
		}

		for (auto it = _edge_properties_64.begin();
				it != _edge_properties_64.end(); it++) {
			if (it->second != NULL) delete it->second;
		}

		if (_master == NULL) {
			_out.set_memory_pools_for_sparse_representaion(NULL, NULL);
			_in.set_memory_pools_for_sparse_representaion(NULL, NULL);
			delete _pool_for_sparse_node_ids;
			delete _pool_for_sparse_node_data;
		}
	}


	/**
	 * Get the database
	 *
	 * @return the database
	 */
	inline ll_database* database() {
		return _database;
	}


	/*-----------------------------------------------------------------------*
	 * Graph structure                                                       *
	 *-----------------------------------------------------------------------*/

	/**
	 * Get the out edges
	 *
	 * @return the out-edges table
	 */
	inline LL_CSR& out() {
		return _out;
	}


	/**
	 * Get the in edges
	 *
	 * @return the in-edges table
	 */
	inline LL_CSR& in() {
		return _in;
	}


	/**
	 * Get the out edges
	 *
	 * @return the out-edges table
	 */
	inline const LL_CSR& out() const {
		return _out;
	}


	/**
	 * Get the in edges
	 *
	 * @return the in-edges table
	 */
	inline const LL_CSR& in() const {
		return _in;
	}


	/**
	 * Get the graph CSRs
	 *
	 * @return the map of graph CSRs
	 */
	inline const std::unordered_map<std::string, LL_CSR*>& csrs() const {
		return _csrs;
	}


	/**
	 * Get the CSR by name
	 *
	 * @param name the name
	 * @return the CSR, or NULL if not found
	 */
	inline LL_CSR* csr(const char* name) {

		ll_spinlock_acquire(&_csrs_update_lock);
		auto it = _csrs.find(name);

		if (it == _csrs.end()) {
			ll_spinlock_release(&_csrs_update_lock);
			return NULL;
		}

		LL_CSR* x = it->second;
		ll_spinlock_release(&_csrs_update_lock);

		return x;
	}


	/**
	 * Create a new uninitialized CSR
	 *
	 * @param name the name
	 * @return the new, uninitialized CSR
	 */
	LL_CSR* create_uninitialized_csr(const char* name) {

		ll_spinlock_acquire(&_csrs_update_lock);

		if (_csrs.find(name) != _csrs.end()) {
			LL_E_PRINT("Already exists");
			abort();
		}

		LL_CSR* csr = new LL_CSR(IF_LL_PERSISTENCE(this->_storage,) name);
		csr->set_memory_pools_for_sparse_representaion(
				_pool_for_sparse_node_ids, _pool_for_sparse_node_data);
		_csrs[name] = csr;

		ll_spinlock_release(&_csrs_update_lock);

		return csr;
	}


	/*-----------------------------------------------------------------------*
	 * Other                                                                 *
	 *-----------------------------------------------------------------------*/

#ifdef LL_MIN_LEVEL
	/**
	 * Set the minimum level to consider
	 *
	 * @param m the minimum level
	 */
	void set_min_level(edge_t m) {

		ll_spinlock_acquire(&_csrs_update_lock);

#	ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES

		this->_out.set_min_level(m, this->_edge_stream_weights,
				this->_edge_stream_forward);
		for (auto it = _csrs.begin(); it != _csrs.end(); it++) {
			if (it->second != &this->_out) it->second->set_min_level(m);
		}

#	else

		for (auto it = _csrs.begin(); it != _csrs.end(); it++) {
			it->second->set_min_level(m);
		}

#	endif

		ll_spinlock_release(&_csrs_update_lock);

		ll_with(auto p = this->get_all_node_properties_32()) {
			for (auto it = p.begin(); it != p.end(); it++) {
				it->second->set_min_level(m);
			}
		}
		ll_with(auto p = this->get_all_node_properties_64()) {
			for (auto it = p.begin(); it != p.end(); it++) {
				it->second->set_min_level(m);
			}
		}

		ll_with(auto p = this->get_all_edge_properties_32()) {
			for (auto it = p.begin(); it != p.end(); it++) {
				it->second->set_min_level(m);
			}
		}
		ll_with(auto p = this->get_all_edge_properties_64()) {
			for (auto it = p.begin(); it != p.end(); it++) {
				it->second->set_min_level(m);
			}
		}
	}
#endif
	

	/**
	 * Set the deletion checkers
	 *
	 * @param deletions_out the deletions checker for the out-edges
	 * @param deletions_in the deletions checker for the in-edges
	 */
	void set_deletion_checkers(ll_mlcsr_external_deletions* deletions_out,
			ll_mlcsr_external_deletions* deletions_in) {

		_out.set_deletions_checker(deletions_out);
		_in.set_deletions_checker(deletions_in);
	}


	/**
	 * Return the maximum number of nodes (i.e. 1 + the maximum node ID)
	 *
	 * @return the maximum number of nodes
	 */
    inline node_t max_nodes() const {
        return _out.max_nodes();
    }


	/**
	 * Return the number of edges in the given level (i.e. 1 + the maximum node ID)
	 *
	 * @param level the level number
	 * @return the number of edges
	 */
    inline edge_t max_edges(int level) const {
        return _out.max_edges(level);
    }


	/**
	 * Return the max level
	 *
	 * @return the max level, or -1 if none
	 */
	inline int max_level() const {
        return ((int) num_levels()) - 1;
    }


	/**
	 * Return the number of levels
	 *
	 * @return the number of levels
	 */
	inline size_t num_levels() const {
        return _out.num_levels();
    }


	/**
	 * Get the read-only version of this graph -- which is in this case itself
	 *
	 * @return this read-only graph
	 */
	inline ll_mlcsr_ro_graph& ro_graph() {
		return *this;
	}


	/**
	 * Pick a random node
	 *
	 * @return a random node
	 */
    node_t pick_random_node() {
        while (true) {
			node_t n = ll_rand64_positive() % max_nodes();
			if (node_exists(n)) return n;
		}
    }


	/**
	 * Determine if the given node exists in the latest level
	 *
	 * @param node the node 
	 * @return true if it exists
	 */
	inline bool node_exists(node_t node) {
		if (_out.node_exists(node)) return true;
		if (_in.node_exists(node)) return true;
		return false;
	}


	/**
	 * Get the corresponding in-edge
	 *
	 * @param e the edge
	 * @return the corresponding in-edge
	 */
    edge_t out_to_in(edge_t e) {
		return _out.translate_edge(e);
    }


	/**
	 * Get the destination of the edge
	 *
	 * @param e the edge
	 * @return the destination node
	 */
    node_t edge_dst(edge_t e) {
		return _out.value(e);
    }


	/**
	 * Get the node out-degree
	 *
	 * @param n the node
	 * @return the out-degree
	 */
	size_t out_degree(node_t n) {
		return _out.degree(n);
	}


	/**
	 * Get the node out-degree
	 *
	 * @param n the node
	 * @param level the level
	 * @return the out-degree
	 */
	size_t out_degree(node_t n, int level) {
		return _out.degree(n, level);
	}


	/**
	 * Create iterator over all outgoing edges
	 *
	 * @param iter the iterator
	 * @param v the vertex
	 * @param level the level
	 * @param max_level the max level for deletions
	 */
	void out_iter_begin(ll_edge_iterator& iter, node_t v,
			int level=-1, int max_level=-1) {
		_out.iter_begin(iter, v, level, max_level);
	}


	/**
	 * Create iterator over all outgoing edges and get the first item
	 *
	 * @param iter the iterator
	 * @param v the vertex
	 * @param level the level
	 * @param max_level the max level for deletions
	 * @return the next item, or LL_NIL_EDGE if none
	 */
	inline edge_t out_iter_begin_next(ll_edge_iterator& iter, node_t v,
			int level=-1, int max_level=-1) {
		return _out.iter_begin_next(iter, v, level, max_level);
	}


	/**
	 * Determine if there are any more items left
	 *
	 * @param iter the iterator
	 * @return true if there are more items
	 */
	bool out_iter_has_next(ll_edge_iterator& iter) {
		return _out.iter_has_next(iter);
	}


	/**
	 * Get the next item
	 *
	 * @param iter the iterator
	 * @return the next item, or LL_NIL_EDGE if none
	 */
	edge_t out_iter_next(ll_edge_iterator& iter) {
		return _out.iter_next(iter);
	}


	/**
	 * Get the node in-degree
	 *
	 * @param n the node
	 * @return the in-degree
	 */
	size_t in_degree(node_t n) {
		return _in.degree(n);
	}


	/**
	 * Create iterator over all incoming edges
	 *
	 * @param iter the iterator
	 * @param v the vertex
	 * @param level the level
	 * @param max_level the max level for deletions
	 */
	void in_iter_begin(ll_edge_iterator& iter, node_t v,
			int level=-1, int max_level=-1) {
		_in.iter_begin(iter, v, level, max_level);
	}


	/**
	 * Determine if there are any more items left
	 *
	 * @param iter the iterator
	 * @return true if there are more items
	 */
	bool in_iter_has_next(ll_edge_iterator& iter) {
		return _in.iter_has_next(iter);
	}


	/**
	 * Get the next item
	 *
	 * @param iter the iterator
	 * @return the next edge, or LL_NIL_EDGE if none
	 */
	edge_t in_iter_next(ll_edge_iterator& iter) {
		edge_t e = _in.iter_next(iter);
		if (e == LL_NIL_EDGE) return LL_NIL_EDGE;
		return _in.translate_edge(e);
	}


	/**
	 * Create iterator over all incoming edges
	 *
	 * @param iter the iterator
	 * @param v the vertex
	 * @param level the level
	 * @param max_level the max level for deletions
	 */
	void in_iter_begin_fast(ll_edge_iterator& iter, node_t v,
			int level=-1, int max_level=-1) {
		_in.iter_begin(iter, v, level, max_level);
	}


	/**
	 * Determine if there are any more items left
	 *
	 * @param iter the iterator
	 * @return true if there are more items
	 */
	bool in_iter_has_next_fast(ll_edge_iterator& iter) {
		return _in.iter_has_next(iter);
	}


	/**
	 * Get the next item
	 *
	 * @param iter the iterator
	 * @return the next IN edge, or LL_NIL_EDGE if none
	 */
	edge_t in_iter_next_fast(ll_edge_iterator& iter) {
		return _in.iter_next(iter);
	}


	/**
	 * Create iterator over all incoming nodes
	 *
	 * @param iter the iterator
	 * @param v the vertex
	 * @param level the level
	 * @param max_level the max level for deletions
	 */
	void inm_iter_begin(ll_edge_iterator& iter, node_t v,
			int level=-1, int max_level=-1) {
		_in.iter_begin(iter, v, level, max_level);
	}


	/**
	 * Determine if there are any more items left
	 *
	 * @param iter the iterator
	 * @return true if there are more items
	 */
	bool inm_iter_has_next(ll_edge_iterator& iter) {
		return _in.iter_has_next(iter);
	}


	/**
	 * Get the next item
	 *
	 * @param iter the iterator
	 * @return the next node, or LL_NIL_EDGE if none
	 */
	node_t inm_iter_next(ll_edge_iterator& iter) {
		edge_t e = _in.iter_next(iter);
		if (e == LL_NIL_EDGE) return LL_NIL_NODE;

		return iter.last_node;
	}


	/**
	 * Find the given edge
	 *
	 * @param source the source node
	 * @param target the target node
	 * @return the edge, or NIL_EDGE if it does not exist
	 */
	edge_t find(node_t source, node_t target) {
		return _out.find(source, target);
	}


	/**
	 * Determine whether we have up-to-date reverse edges
	 *
	 * @return true if yes and up-to-date
	 */
	bool has_reverse_edges() {
		return _in.num_levels() == _out.num_levels();
	}


	static void in_copy_edge_callback(edge_t source_edge,
			edge_t target_edge, void* user) {

		ll_mlcsr_ro_graph* owner = (ll_mlcsr_ro_graph*) user;
		owner->_out.edge_translation().cow_write(
				owner->_in.translate_edge(source_edge),
				target_edge);
	}


	/**
	 * Make the reverse edges
	 *
	 * @param deletedInEdgeCounts the number of deleted in-edges per node (NULL for Level 0)
	 */
	void make_reverse_edges(degree_t* deletedInEdgeCounts=NULL) {

		for (size_t level = _in.num_levels();
				level < _out.num_levels(); level++) {

			node_t max_nodes = _out.max_nodes(level);


			// Compute the in-degrees

			degree_t* a = (degree_t*) malloc(sizeof(degree_t) * max_nodes);
			memset(a, 0, sizeof(degree_t) * max_nodes);

			// Add max_nodes just in case sometimes in the future
			int* loc = (int*) malloc(sizeof(int) * _out.edge_table_length(level));

#			pragma omp parallel for schedule(dynamic,4096)
			for (node_t source = 0; source < _out.max_nodes(); source++) {
				ll_edge_iterator iter;
				_out.iter_begin_within_level(iter, source, level);
				FOREACH_ITER_WITHIN_LEVEL(e, _out, iter) {
					node_t target = LL_ITER_OUT_NEXT_NODE(_out, iter, e);
					loc[LL_EDGE_INDEX(e)]
						= __sync_fetch_and_add(&a[target], 1);
				}
			}

			/*int zeros = 0;
			for (node_t source = 0; source < _out.max_nodes(); source++) {
				printf("%lu: %lu\t", source, a[source]);
				if ((source & 0xf) == 0xe) printf("\n");
				if (a[source] == 0) zeros++;
			}
			printf("\n\nZeros: %d\n\n", zeros);*/


			// Initialize the level and the vertex table

			assert(_in.has_edge_translation()
					== _out.has_edge_translation());
			bool has_edge_translation = _in.has_edge_translation()
				&& _out.has_edge_translation();

			if (has_edge_translation) {
				_out.edge_translation().cow_init_level(_out.max_edges(level));
			}

			_in.init_level_from_degrees(_out.max_nodes(), a,
					deletedInEdgeCounts,
					has_edge_translation ? in_copy_edge_callback : NULL,
					this);

			if (has_edge_translation) {
				_in.edge_translation().cow_init_level(_in.max_edges(level));
			}


			// Copy the data

#ifdef _DEBUG
			degree_t* degrees = a; a = (degree_t*) malloc(sizeof(degree_t)
					* _out.max_nodes());
			/*fprintf(stderr, "\n");
			for (size_t i = 0; i < _out.max_nodes(); i++) {
				if ((i+0) % 25 == 0) fprintf(stderr, "%4lu:", i);
				fprintf(stderr, " %3lu", degrees[i]);
				if ((i+1) % 25 == 0) fprintf(stderr, "\n");
			}*/
#endif
			memset(a, 0, sizeof(degree_t) * _out.max_nodes());

#			pragma omp parallel for schedule(dynamic,4096)
			for (node_t source = 0; source < _out.max_nodes(); source++) {
				ll_edge_iterator iter;
				_out.iter_begin_within_level(iter, source, level);
				FOREACH_ITER_WITHIN_LEVEL(e, _out, iter) {
					node_t target = LL_ITER_OUT_NEXT_NODE(_out, iter, e);
					size_t index = loc[LL_EDGE_INDEX(e)];
						//__sync_fetch_and_add(&a[target], 1);
					edge_t in_edge = _in.write_value(target, index, source);

					if (has_edge_translation) {
						_in.edge_translation().cow_write(in_edge, e);
						_out.edge_translation().cow_write(e, in_edge);
					}
				}
			}

			_in.finish_level_edges();

			if (has_edge_translation) {
				_out.edge_translation().cow_finish_level();
				_in.edge_translation().cow_finish_level();
			}


			// Finish

			free(a);
#ifdef _DEBUG
			free(degrees);
#endif
			if (loc != NULL) free(loc);
		}
	}


	/**
	 * Set the maximum visibility level for an edge
	 *
	 * @param edge the edge
	 * @param mlevel the maximum visibility level
	 */
	void update_max_visible_level(edge_t edge, int mlevel) {

		assert(mlevel > (int) LL_MAX_LEVEL);
		ll_spinlock_acquire(&_update_lock);

		_out.update_max_visible_level(edge, mlevel);

		if (has_reverse_edges()) {
			edge_t in_edge = out_to_in(edge);
			_in.update_max_visible_level(in_edge, mlevel);
		}

		ll_spinlock_release(&_update_lock);
	}


	/**
	 * Set the maximum visibility level for an edge, lower only
	 *
	 * @param edge the edge
	 * @param mlevel the maximum visibility level
	 * @return true if the value was lowered
	 */
	bool update_max_visible_level_lower_only(edge_t edge, int mlevel) {

		// I do not think this needs to be locked...
		//ll_spinlock_acquire(&_update_lock);

		bool r = _out.update_max_visible_level_lower_only(edge, mlevel);

		if (r) {
			if (has_reverse_edges()) {
				edge_t in_edge = out_to_in(edge);
				_in.update_max_visible_level_lower_only(in_edge, mlevel);
			}
		}

		//ll_spinlock_release(&_update_lock);

		return r;
	}


	/**
	 * Get all 32-bit node properties
	 *
	 * @return the map of all such properties
	 */
	const std::unordered_map<std::string, ll_mlcsr_node_property<uint32_t>*>&
	get_all_node_properties_32() {
		return _node_properties_32;
	}


	/**
	 * Get all 64-bit node properties
	 *
	 * @return the map of all such properties
	 */
	const std::unordered_map<std::string, ll_mlcsr_node_property<uint64_t>*>&
	get_all_node_properties_64() {
		return _node_properties_64;
	}


	/**
	 * Get all 32-bit edge properties
	 *
	 * @return the map of all such properties
	 */
	const std::unordered_map<std::string, ll_mlcsr_edge_property<uint32_t>*>&
	get_all_edge_properties_32() {
		return _edge_properties_32;
	}


	/**
	 * Get all 64-bit edge properties
	 *
	 * @return the map of all such properties
	 */
	const std::unordered_map<std::string, ll_mlcsr_edge_property<uint64_t>*>&
	get_all_edge_properties_64() {
		return _edge_properties_64;
	}


	/**
	 * Get a 32-bit node property
	 *
	 * @param name the property name
	 * @return the property if it exists, or NULL if not
	 */
	ll_mlcsr_node_property<uint32_t>* get_node_property_32(const char* name) {

		std::string s = name;
		auto it = _node_properties_32.find(s);
		if (it == _node_properties_32.end()) return NULL;

		return it->second;
	}


	/**
	 * Create a 32-bit node property, but do not initialize it
	 *
	 * @param name the property name
	 * @param type the property type
	 * @param destructor the value destructor to use if creating the property
	 * @return the property, or NULL on error
	 */
	ll_mlcsr_node_property<uint32_t>*
		create_uninitialized_node_property_32(const char* name,
			short type, void (*destructor)(const uint32_t&) = NULL) {

		std::string s = name;
		auto it = _node_properties_32.find(s);
		if (it != _node_properties_32.end()) return NULL;

		ll_mlcsr_node_property<uint32_t>* p
			= new ll_mlcsr_node_property<uint32_t>(
					IF_LL_PERSISTENCE(_storage,)
					_next_node_property_id++,
					name, type, destructor);
		_node_properties_32[s] = p;

		return p;
	}


	/**
	 * Get a 64-bit node property
	 *
	 * @param name the property name
	 * @return the property if it exists, or NULL if not
	 */
	ll_mlcsr_node_property<uint64_t>* get_node_property_64(const char* name) {

		std::string s = name;
		auto it = _node_properties_64.find(s);
		if (it == _node_properties_64.end()) return NULL;

		return it->second;
	}


	/**
	 * Create a 64-bit node property, but do not initialize it
	 *
	 * @param name the property name
	 * @param type the property type
	 * @param destructor the value destructor to use if creating the property
	 * @return the property, or NULL on error
	 */
	ll_mlcsr_node_property<uint64_t>*
		create_uninitialized_node_property_64(const char* name,
			short type, void (*destructor)(const uint64_t&) = NULL) {

		std::string s = name;
		auto it = _node_properties_64.find(s);
		if (it != _node_properties_64.end()) return NULL;

		ll_mlcsr_node_property<uint64_t>* p
			= new ll_mlcsr_node_property<uint64_t>(
					IF_LL_PERSISTENCE(_storage,)
					_next_node_property_id++,
					name, type, destructor);
		_node_properties_64[s] = p;

		return p;
	}


	/**
	 * Get a 32-bit edge property
	 *
	 * @param name the property name
	 * @return the property if it exists, or NULL if not
	 */
	ll_mlcsr_edge_property<uint32_t>* get_edge_property_32(const char* name) {

		std::string s = name;
		auto it = _edge_properties_32.find(s);
		if (it == _edge_properties_32.end()) return NULL;

		return it->second;
	}


	/**
	 * Get a 32-bit edge property by ID
	 *
	 * @param id the property ID
	 * @return the property if it exists, or NULL if not
	 */
	ll_mlcsr_edge_property<uint32_t>* get_edge_property_32(int id) {

		if (id < 0 || id >= LL_MAX_EDGE_PROPERTY_ID) return NULL;
		return _edge_properties_by_id_32[id];
	}


#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES

	/**
	 * Get the edge weight property for streaming
	 *
	 * @return the property
	 */
	inline ll_mlcsr_edge_property<uint32_t>* get_edge_weights_streaming() {
		return _edge_stream_weights;
	}


	/**
	 * Get the edge forward pointer property for streaming
	 *
	 * @return the property
	 */
	inline ll_mlcsr_edge_property<edge_t>* get_edge_forward_streaming() {
		return _edge_stream_forward;
	}

#endif


	/**
	 * Create a 32-bit edge property, but do not initialize it
	 *
	 * @param name the property name
	 * @param type the property type
	 * @return the property, or NULL on error
	 */
	ll_mlcsr_edge_property<uint32_t>*
		create_uninitialized_edge_property_32(const char* name,
			short type) {

		std::string s = name;
		auto it = _edge_properties_32.find(s);
		if (it != _edge_properties_32.end()) return NULL;

		int id = _next_edge_property_id++;
		if (id >= LL_MAX_EDGE_PROPERTY_ID) {
			_next_edge_property_id--;	// XXX Race condition?
			LL_E_PRINT("Too many edge properties");
			return NULL;
		}

		ll_mlcsr_edge_property<uint32_t>* p
			= new ll_mlcsr_edge_property<uint32_t>(
					IF_LL_PERSISTENCE(_storage,) id,
					name, type, NULL,
					_out.edge_property_level_creation_callback_32());
		_edge_properties_32[s] = p;
		_edge_properties_by_id_32[p->id()] = p;

		return p;
	}


	/**
	 * Get a 64-bit edge property
	 *
	 * @param name the property name
	 * @return the property if it exists, or NULL if not
	 */
	ll_mlcsr_edge_property<uint64_t>* get_edge_property_64(const char* name) {

		std::string s = name;
		auto it = _edge_properties_64.find(s);
		if (it == _edge_properties_64.end()) return NULL;

		return it->second;
	}


	/**
	 * Get a 64-bit edge property by ID
	 *
	 * @param id the property ID
	 * @return the property if it exists, or NULL if not
	 */
	ll_mlcsr_edge_property<uint64_t>* get_edge_property_64(int id) {

		if (id < 0 || id >= LL_MAX_EDGE_PROPERTY_ID) return NULL;
		return _edge_properties_by_id_64[id];
	}


	/**
	 * Create a 64-bit edge property, but do not initialize it
	 *
	 * @param name the property name
	 * @param type the property type
	 * @param destructor the value destructor to use if creating the property
	 * @return the property, or NULL on error
	 */
	ll_mlcsr_edge_property<uint64_t>*
		create_uninitialized_edge_property_64(const char* name,
			short type, void (*destructor)(const uint64_t&) = NULL) {

		std::string s = name;
		auto it = _edge_properties_64.find(s);
		if (it != _edge_properties_64.end()) return NULL;

		int id = _next_edge_property_id++;
		if (id >= LL_MAX_EDGE_PROPERTY_ID) {
			_next_edge_property_id--;	// XXX Race condition?
			LL_E_PRINT("Too many edge properties");
			return NULL;
		}

		ll_mlcsr_edge_property<uint64_t>* p
			= new ll_mlcsr_edge_property<uint64_t>(
					IF_LL_PERSISTENCE(_storage,) id,
					name, type, destructor,
					_out.edge_property_level_creation_callback_64());
		_edge_properties_64[s] = p;
		_edge_properties_by_id_64[p->id()] = p;

		return p;
	}


	/**
	 * Set node property -- a placeholder
	 *
	 * @param props the property
	 * @param n the node
	 * @param val the value
	 */
	template<typename U>
	void set_node_prop(U* props, node_t n, U val) {
		props[n] = val;
	}


	static void out_copy_edge_callback(edge_t source_edge, edge_t target_edge, void* user) {

#ifdef LL_REVERSE_EDGES
		ll_mlcsr_ro_graph* owner = (ll_mlcsr_ro_graph*) user;
		owner->_in.edge_translation().cow_write(
				owner->_out.translate_edge(source_edge),
				target_edge);
#endif
	}


	/**
	 * Initialize a level from an array of node degrees
	 *
	 * @param max_nodes the total number of nodes
	 * @param new_edge_counts the array of numbers of new edges for each node
	 * @param deleted_edge_counts the array of numbers of deleted edges
	 */
	void init_level_from_degrees(degree_t max_nodes, degree_t* new_edge_counts,
			degree_t* deleted_edge_counts) {

		assert(_in.has_edge_translation() == _out.has_edge_translation());

		if (has_reverse_edges() && _in.has_edge_translation()) {
			_in.edge_translation().cow_init_level_partial();
		}

		_out.init_level_from_degrees(max_nodes,
				new_edge_counts, deleted_edge_counts,
				_in.has_edge_translation()
					&& _out.has_edge_translation()
						? out_copy_edge_callback : NULL,
				this);
	}


	/**
	 * Partially initialize a level from node and edge counts
	 *
	 * @param max_nodes the total number of nodes, cummulative up until this level
	 * @param max_adj_lists the max number of adj. lists in the level
	 * @param max_edges the max number of edges within the level
	 * @param deleted_edge_counts the array of numbers of deleted edges for each node
	 */
	void partial_init_level(size_t max_nodes, size_t max_adj_lists,
			size_t max_edges) {

		assert(_in.has_edge_translation() == _out.has_edge_translation());

		if (has_reverse_edges() && _in.has_edge_translation()) {
			_in.edge_translation().cow_init_level_partial();
		}

		_out.init_level(max_nodes, max_adj_lists, max_edges,
				_in.has_edge_translation() && _out.has_edge_translation()
						? out_copy_edge_callback : NULL, this);
	}


	/**
	 * Partially initialize the in-edges of a level from node and edge counts
	 *
	 * @param max_nodes the total number of nodes, cummulative up until this level
	 * @param max_adj_lists the max number of adj. lists in the level
	 * @param max_edges the max number of edges within the level
	 * @param deleted_edge_counts the array of numbers of deleted edges for each node
	 */
	void partial_init_level_in(size_t max_nodes, size_t max_adj_lists,
			size_t max_edges) {

		assert(_in.has_edge_translation() == _out.has_edge_translation());

		if (has_reverse_edges() && _out.has_edge_translation()) {
			_out.edge_translation().cow_init_level_partial();
		}

		_in.init_level(max_nodes, max_adj_lists, max_edges,
				_in.has_edge_translation() && _out.has_edge_translation()
						? in_copy_edge_callback : NULL, this);
	}


	/**
	 * Finish the edges part of the level
	 */
	void finish_level_edges() {
		_out.finish_level_edges();
	}


	/**
	 * Initialize a set new level for a checkpoint
	 *
	 * @param source the checkpoint source
	 * @param config the loader config
	 */
	void checkpoint(ll_mlcsr_checkpoint_source* source,
			const ll_loader_config* config) {


		// Check features

		feature_vector_t features;
		features << LL_L_FEATURE(lc_reverse_edges);
		features << LL_L_FEATURE(lc_reverse_maps);

		config->assert_features(false /*direct*/, true /*error*/, features);

		// TODO Apply deletions for LL_TIMESTAMPS


		// Initialize

		size_t level = _out.num_levels();
		//size_t num_total_nodes = _out.max_nodes() + source->num_new_nodes();
		size_t num_total_nodes = source->max_node_id() + 1;
		assert((node_t) num_total_nodes >= _out.max_nodes());

		ll_w_vt_vertices_t* vt = source->vertex_table();


		// There is an issue pertaining to LL_COPY_ADJ_LIST__LARGE (and probably also
		// __SMALL) in which a node's adj. list switches from being copied to differences
		// esp. due to deletions.
		//
		// This now works with the STOP_HERE flag and either ADJ_LIST_LENGTH
		// or COPY_ADJ_LIST_ON_DELETION.


		// Construct the new_degrees and nodes_with_new_edges arrays

		degree_t* new_degrees
			= (degree_t*) malloc(sizeof(degree_t) * num_total_nodes);
		memset(new_degrees, 0, sizeof(degree_t) * num_total_nodes);

		degree_t* deleted_frozen_out_edges
			= (degree_t*) malloc(sizeof(degree_t) * num_total_nodes);
		memset(deleted_frozen_out_edges, 0, sizeof(degree_t)*num_total_nodes);

		degree_t* deleted_frozen_in_edges
			= (degree_t*) malloc(sizeof(degree_t) * num_total_nodes);
		memset(deleted_frozen_in_edges, 0, sizeof(degree_t)*num_total_nodes);

#		pragma omp parallel
		{
#			pragma omp for schedule(static,4096)
			for (size_t p = 0; p < vt->num_pages(); p++) {
				if (!vt->page_with_contents(p)) continue;
				node_t n = p * vt->num_entries_per_page();

				for (size_t i = 0; i < vt->num_entries_per_page(); i++, n++) {
					w_node* w = (w_node*) vt->page_fast_read(p, i);
					if (w == NULL) continue;

					new_degrees[n] = w->wn_out_edges_delta;
					deleted_frozen_out_edges[n] = w->wn_num_deleted_out_edges;
					deleted_frozen_in_edges[n] = w->wn_num_deleted_in_edges;

					LL_D_NODE_PRINT(n, "nd=%u, dfo=%u, dfi=%u\n",
							new_degrees[n], deleted_frozen_out_edges[n],
							deleted_frozen_in_edges[n]);
				}
			}
		}


		// Initialize the new level

		_out.init_level_from_degrees(num_total_nodes, new_degrees,
				deleted_frozen_out_edges);


		// Begin checkpointing edge properties - create a new level for each property

		// (Note that this should work even if we already called writable_init().)

		ll_with(auto p = this->get_all_edge_properties_32()) {
			for (auto it = p.begin(); it != p.end(); it++) {
				it->second->cow_init_level(_out.max_edges(level));
			}
		}
		ll_with(auto p = this->get_all_edge_properties_64()) {
			for (auto it = p.begin(); it != p.end(); it++) {
				it->second->cow_init_level(_out.max_edges(level));
			}
		}


		// Add all the new edges

		int max_edge_property_id = _next_edge_property_id;
		(void) max_edge_property_id;

#ifndef LL_PERSISTENCE
#		pragma omp parallel
#endif
		{
#	ifdef LL_SORT_EDGES
			std::vector<node_t> v;
#	endif

#ifndef LL_PERSISTENCE
#			pragma omp for schedule(dynamic,4096)
#endif
			for (size_t p = 0; p < vt->num_pages(); p++) {
				if (!vt->page_with_contents(p)) continue;
				node_t n = p * vt->num_entries_per_page();

				for (size_t i = 0; i < vt->num_entries_per_page(); i++, n++) {
					w_node* w = (w_node*) vt->page_fast_read(p, i);
					if (w == NULL) continue;

#	if defined(LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES)
#		ifdef LL_SORT_EDGES
#			error "Not implemented"
#		endif
					_out.write_values(n, w->wn_out_edges,
							this->_edge_stream_forward);
#	elif defined(LL_SORT_EDGES)
					v.clear();
					source->get_out_edges(n, v);
					std::sort(v.begin(), v.end());
					_out.write_values(n, v);
#	else
					_out.write_values(n, w->wn_out_edges);
#	endif


					// Write the new edge property values

					for (size_t ei = 0; ei < w->wn_out_edges.size(); ei++) {
						w_edge* e = w->wn_out_edges[ei];
						if (e->exists()) {
#if LL_MAX_EDGE_PROPERTY_ID > 0
							for (int j = 0; j < max_edge_property_id; j++) {
								if (e->we_properties_32[j] != 0) {
									get_edge_property_32(j)
										->cow_write(e->we_numerical_id,
												e->we_properties_32[j]);
								}
								if (e->we_properties_64[j].second != 0) {
									get_edge_property_64(j)
										->cow_write(e->we_numerical_id,
												e->we_properties_64[j].second);
								}
							}
#endif
						}
					}
				}
			}
		}

		_out.finish_level_edges();


		// Cleanup and finalize

#ifdef LL_REVERSE_EDGES
		if (config->lc_reverse_edges
				&& _in.num_levels() + 1 == _out.num_levels()) {

			// Compute the reverse edges

#	ifdef LL_SORT_EDGES
			abort();
#	endif

			memset(new_degrees, 0, sizeof(degree_t) * num_total_nodes);

#			pragma omp parallel
			{
#				pragma omp for schedule(dynamic,4096)
				for (size_t p = 0; p < vt->num_pages(); p++) {
					if (!vt->page_with_contents(p)) continue;
					node_t n = p * vt->num_entries_per_page();

					for (size_t i = 0; i < vt->num_entries_per_page();
							i++, n++) {
						w_node* w = (w_node*) vt->page_fast_read(p, i);
						if (w == NULL) continue;

						new_degrees[n] = w->wn_in_edges_delta;
					}
				}
			}

			_in.init_level_from_degrees(num_total_nodes, new_degrees,
					deleted_frozen_in_edges);

			assert(_in.has_edge_translation()
					== _out.has_edge_translation());
			bool has_edge_translation = _in.has_edge_translation()
				&& _out.has_edge_translation()
				&& config->lc_reverse_maps;

			if (has_edge_translation) {
				_out.edge_translation().cow_init_level(_out.max_edges(level));
				_in.edge_translation().cow_init_level(_in.max_edges(level));
			}


#			pragma omp parallel
			{
#	ifndef LL_PERSISTENCE
#				pragma omp for schedule(dynamic,4096)
#	endif
				for (size_t p = 0; p < vt->num_pages(); p++) {
					if (!vt->page_with_contents(p)) continue;
					node_t n = p * vt->num_entries_per_page();

					for (size_t i = 0; i < vt->num_entries_per_page(); i++, n++) {
						w_node* w = (w_node*) vt->page_fast_read(p, i);
						if (w == NULL) continue;

						_in.write_values(n, w->wn_in_edges);
					}
				}

#				pragma omp for schedule(dynamic,4096)
				for (size_t p = 0; p < vt->num_pages(); p++) {
					if (!vt->page_with_contents(p)) continue;
					node_t n = p * vt->num_entries_per_page();

					for (size_t i = 0; i < vt->num_entries_per_page(); i++, n++) {
						w_node* w = (w_node*) vt->page_fast_read(p, i);
						if (w == NULL) continue;

						for (size_t j = 0; j < w->wn_out_edges.size(); j++) {
							w_edge* e = w->wn_out_edges[j];
							if (e->exists() && has_edge_translation) {
								_out.edge_translation().cow_write(e->we_numerical_id,
										e->we_reverse_numerical_id);
							}
						}

						for (size_t j = 0; j < w->wn_in_edges.size(); j++) {
							w_edge* e = w->wn_in_edges[j];
							if (e->exists() && has_edge_translation) {
								_in.edge_translation().cow_write(
										e->we_reverse_numerical_id,
										e->we_numerical_id);
							}
						}
					}
				}
			}

			_in.finish_level_edges();

			if (has_edge_translation) {
				_out.edge_translation().cow_finish_level();
				_in.edge_translation().cow_finish_level();
			}
		}
#endif


		// Cleanup

		free(deleted_frozen_in_edges);

		free(new_degrees);
		free(deleted_frozen_out_edges);


		// Checkpoint node properties

		{
			auto p = this->get_all_node_properties_32();
			for (auto it = p.begin(); it != p.end(); it++) {
				if (!it->second->writable())
					it->second->writable_init(num_total_nodes);
				it->second->freeze(num_total_nodes);
				if (it->second->max_level() != _out.max_level()) {
					fflush(stdout);
					fprintf(stderr, "\nASSERT FAILED: Node property checkpoint "
							"for '%s': %d level(s), %d expected\n",
							it->first.c_str(), it->second->max_level(),
							_out.max_level());
					exit(1);
				}
				assert(it->second->max_level() == _out.max_level());
			}
		}
		{
			auto p = this->get_all_node_properties_64();
			for (auto it = p.begin(); it != p.end(); it++) {
				if (!it->second->writable())
					it->second->writable_init(num_total_nodes);
				it->second->freeze(num_total_nodes);
				assert(it->second->max_level() == _out.max_level());
			}
		}


		// Checkpoint edge properties - finish the levels

		{
			auto p = this->get_all_edge_properties_32();
			for (auto it = p.begin(); it != p.end(); it++) {
				if (it->second->writable())
					it->second->freeze();
				else
					it->second->cow_finish_level();
				if (it->second->max_level() != _out.max_level()) {
					fflush(stdout);
					fprintf(stderr, "\nASSERT FAILED: Edge property checkpoint "
							"for '%s': %d level(s), %d expected\n",
							it->first.c_str(), it->second->max_level(),
							_out.max_level());
					exit(1);
				}
				assert(it->second->max_level() == _out.max_level());
			}
		}
		{
			auto p = this->get_all_edge_properties_64();
			for (auto it = p.begin(); it != p.end(); it++) {
				if (it->second->writable())
					it->second->freeze();
				else
					it->second->cow_finish_level();
				assert(it->second->max_level() == _out.max_level());
			}
		}
	}


	/**
	 * Delete a level
	 *
	 * @param level the level number
	 */
	void delete_level(size_t level) {

		// Delete structure levels

		for (auto it = _csrs.begin(); it != _csrs.end(); it++) {
			if (it->second->num_levels() >= level) {
				it->second->delete_level(level);
			}
		}


		// Delete node property levels

		ll_with(auto p = this->get_all_node_properties_32()) {
			for (auto it = p.begin(); it != p.end(); it++) {
				it->second->delete_level(level);
			}
		}
		ll_with(auto p = this->get_all_node_properties_64()) {
			for (auto it = p.begin(); it != p.end(); it++) {
				it->second->delete_level(level);
			}
		}


		// Delete edge property levels

		ll_with(auto p = this->get_all_edge_properties_32()) {
			for (auto it = p.begin(); it != p.end(); it++) {
				it->second->delete_level(level);
			}
		}
		ll_with(auto p = this->get_all_edge_properties_64()) {
			for (auto it = p.begin(); it != p.end(); it++) {
				it->second->delete_level(level);
			}
		}
	}


	/**
	 * Delete all old versions except the specified number of most recent levels
	 *
	 * @param keep the number of levels to keep
	 */
	void keep_only_recent_versions(size_t keep) {

		// Delete structure levels

		for (auto it = _csrs.begin(); it != _csrs.end(); it++) {
			if (it->second->num_levels() >= keep) {
				it->second->keep_only_recent_versions(keep);
			}
		}


		// Delete node property levels

		ll_with(auto p = this->get_all_node_properties_32()) {
			for (auto it = p.begin(); it != p.end(); it++) {
				it->second->keep_only_recent_levels(keep);
			}
		}
		ll_with(auto p = this->get_all_node_properties_64()) {
			for (auto it = p.begin(); it != p.end(); it++) {
				it->second->keep_only_recent_levels(keep);
			}
		}


		// Delete edge property levels

		ll_with(auto p = this->get_all_edge_properties_32()) {
			for (auto it = p.begin(); it != p.end(); it++) {
				it->second->keep_only_recent_levels(keep);
			}
		}
		ll_with(auto p = this->get_all_edge_properties_64()) {
			for (auto it = p.begin(); it != p.end(); it++) {
				it->second->keep_only_recent_levels(keep);
			}
		}
	}


public:

	typedef LL_CSR::iterator iterator;
	typedef LL_CSR::const_iterator const_iterator;


	/**
	 * Create a C++ style iterator
	 *
	 * @param node the node
	 * @return the iterator
	 */
	iterator out_begin(node_t node) { return _out.begin(node); }


	/**
	 * Create a C++ style iterator
	 *
	 * @param iter the iterator
	 * @param node the node
	 * @return the iterator
	 */
	iterator& out_begin(iterator& iter, node_t node) { return _out.begin(iter, node); }


	/**
	 * Create a C++ style iterator
	 *
	 * @param node the node
	 * @return the iterator
	 */
	const_iterator out_begin(node_t node) const { return _out.begin(node); }


	/**
	 * Create a C++ style iterator
	 *
	 * @param node the node
	 * @return the iterator
	 */
	iterator in_begin(node_t node) { return _in.begin(node); }


	/**
	 * Create a C++ style iterator
	 *
	 * @param iter the iterator
	 * @param node the node
	 * @return the iterator
	 */
	iterator& in_begin(iterator& iter, node_t node) { return _in.begin(iter, node); }


	/**
	 * Create a C++ style iterator
	 *
	 * @param node the node
	 * @return the iterator
	 */
	const_iterator in_begin(node_t node) const { return _in.begin(node); }


	/**
	 * Create the end iterator
	 *
	 * @param node the node (optional)
	 * @return the iterator
	 */
	iterator end(node_t node=0) { return _out.end(node); }


	/**
	 * Create the end iterator
	 *
	 * @param node the node (optional)
	 * @return the iterator
	 */
	const_iterator end(node_t node=0) const { return _out.end(node); }

};


#endif	/* LL_MULTILEVEL_CSR_H_ */

