/*
 * ll_writable_graph.h
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


#ifndef LL_WRITABLE_GRAPH_H_
#define LL_WRITABLE_GRAPH_H_

#include <sys/mman.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <strings.h>
#include <unistd.h>

#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "llama/ll_common.h"
#include "llama/ll_mlcsr_graph.h"
#include "llama/ll_writable_array.h"
#include "llama/ll_writable_elements.h"



//==========================================================================//
// Class: ll_writable_graph                                                 //
//==========================================================================//

#ifdef LL_TX

/// The global thread-local transaction timestamp (is this a good idea?)
__thread long g_tx_timestamp;

/// Did this thread do any writes?
__thread bool g_tx_write;

/// The global transaction timestamp counter
std::atomic<long> g_last_timestamp(0);

/// The total number of active transactions
std::atomic<int> g_active_transactions(0);

#ifdef LL_TIMESTAMPS
#define LL_TX_TIMESTAMP		g_tx_timestamp
#else
#define LL_TX_TIMESTAMP		0
#endif

#else
#define LL_TX_TIMESTAMP		0
#endif



/**
 * The writable graph
 *
 * @author Peter Macko
 */
class ll_writable_graph {

	typedef struct {
		std::vector<edge_t> an_deleted_edges;
	} affected_node_by_edge_deletion_t;


public:

	/**
	 * Create an instance of ll_writable_graph
	 *
	 * @param database the database context
	 * @param storage the persistence context
	 * @param max_nodes maximum number of nodes
	 */
	ll_writable_graph(ll_database* database,
			IF_LL_PERSISTENCE(ll_persistent_storage* storage,)
			size_t max_nodes)

		: _ro_graph(database IF_LL_PERSISTENCE(, storage)),
		  _vertices(max_nodes),
		  _deletions_adapter_out(*this),
		  _deletions_adapter_in(*this)
	{
		_newNodes.store(0);
		_delNodes.store(0);
		_newEdges.store(0);
		_delNewEdges.store(0);
		_delFrozenEdges.store(0);

		_new_node_lock = 0;
		_deletions_out_lock = 0;
		_deletions_in_lock = 0;
		_property_lock = 0;

		_ro_graph.set_deletion_checkers(&_deletions_adapter_out,
				&_deletions_adapter_in);

		_next_new_node_id = _ro_graph.max_nodes();

#ifdef LL_WRITABLE_USE_MEMORY_POOL
		if (__w_pool.chunk_size() > ((1ul << LL_MEM_POOL_ALIGN_BITS) << LL_W_MEM_POOL_MAX_OFFSET_BITS)) {
			LL_E_PRINT("__w_pool.chunk_size() is too large\n");
			abort();
		}
#endif
	}


	/**
	 * Destroy the instance of this class
	 */
	virtual ~ll_writable_graph(void) {

#ifdef LL_WRITABLE_USE_MEMORY_POOL
		ll_free_w_pool();
#else
		delete_free_w_nodes();
		delete_free_w_edges();
#endif
	}
	

#ifdef LL_MIN_LEVEL
	/**
	 * Set the minimum level to consider
	 *
	 * @param m the minimum level
	 */
	void set_min_level(edge_t m) {

		_ro_graph.set_min_level(m);
	}
#endif


	/**
	 * Begin a transaction
	 *
	 * @return the new transaction ID
	 */
	long tx_begin() {
#ifdef LL_TX
		g_active_transactions.fetch_add(1);
		g_tx_timestamp = g_last_timestamp.fetch_add(1) + 1;
		g_tx_write = false;
		return g_tx_timestamp;
#else
		return 0;
#endif
	}


	/**
	 * Commit the transaction
	 */
	void tx_commit() {
#ifdef LL_TX
		g_tx_timestamp = 0;
		int n = g_active_transactions.fetch_add(-1);
		assert(n > 0); (void) n;
#endif
	}


	/**
	 * Abort the transaction
	 */
	void tx_abort() {
#ifdef LL_TX
		g_tx_timestamp = 0;
		int n = g_active_transactions.fetch_add(-1);
		assert(n > 0); (void) n;
#endif

		// TODO What to do now?

		abort();
	}


	/**
	 * Return the number of nodes
	 *
	 * @return the number of nodes
	 */
    node_t max_nodes() {
		return _next_new_node_id;
    }


	/**
	 * Return the number of edges within the given level
	 *
	 * @param level the level number
	 * @return the number of edges
	 */
    edge_t max_edges(int level) {
		if (level >= (int) _ro_graph.num_levels())
			return _newEdges;
		else 
			return _ro_graph.max_edges(level);
    }


	/**
	 * Return the number of levels
	 *
	 * @return the number of levels
	 */
    inline size_t num_levels() {
        return _ro_graph.num_levels() + 1;
    }


	/**
	 * Determine whether we have up-to-date reverse edges
	 *
	 * @return true if yes and up-to-date
	 */
	bool has_reverse_edges() {
		return _ro_graph.has_reverse_edges();
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
	 * Get the read-only graph for the analytics
	 *
	 * @return the read-only graph
	 */
	ll_mlcsr_ro_graph& ro_graph(void) {
		return _ro_graph;
	}


	/**
	 * A hook for when a new level was added to the read-only graph
	 */
	void callback_ro_changed(void) {

		ll_spinlock_acquire(&_new_node_lock);
		_next_new_node_id = _ro_graph.max_nodes();
		ll_spinlock_release(&_new_node_lock);
	}


	/**
	 * Determine if the given node exists in the latest level
	 *
	 * @param node the node 
	 * @return true if it exists
	 */
	inline bool node_exists(node_t node) {

		w_node* r = (w_node*) _vertices.get(node);
		if (r != NULL) return true;

		return _ro_graph.node_exists(node);
	}


	/**
	 * Add a node
	 *
	 * @return the node ID, or NIL_NODE if there is no more space in the vertex map
	 */
	node_t add_node() {

		ll_spinlock_acquire(&_new_node_lock);

		if (_next_new_node_id + 1 >= (node_t) _vertices.size()) {
			ll_spinlock_release(&_new_node_lock);
			return LL_NIL_NODE;
		}

#ifdef LL_TX
		g_tx_write = true;
#endif

		node_t n = _next_new_node_id++;
		assert(!node_exists(n));

		// TODO We just want to allocate - or do we need to
		// do that spinlock thing? Maybe it's not necessary.
		w_node* r = (w_node*) _vertices.get_or_allocate(n);
		(void) r;

#ifdef LL_TIMESTAMPS
		r->wn_timestamp_creation = LL_TX_TIMESTAMP;
#endif

		_newNodes++;
		ll_spinlock_release(&_new_node_lock);

		return n;
	}


	/**
	 * Add a node with the given ID
	 *
	 * @param id the new node ID
	 * @return true if it went okay; false if we failed
	 */
	bool add_node(node_t id) {

		ll_spinlock_acquire(&_new_node_lock);

		if (id >= (node_t) _vertices.size()) {
			ll_spinlock_release(&_new_node_lock);
			return false;
		}

#ifdef LL_TX
		g_tx_write = true;
#endif

		if (id > _next_new_node_id) _next_new_node_id = id;
		if (node_exists(id)) {
			ll_spinlock_release(&_new_node_lock);
			return false;
		}

		// TODO We just want to allocate - or do we need to
		// do that spinlock thing? Maybe it's not necessary.
		w_node* r = (w_node*) _vertices.get_or_allocate(id);
		(void) r;

#ifdef LL_TIMESTAMPS
		r->wn_timestamp_creation = LL_TX_TIMESTAMP;
#endif

		_newNodes++;	// TODO Make checkpointing to work
		ll_spinlock_release(&_new_node_lock);

		return true;
	}


	/**
	 * Delete a node
	 *
	 * @return node the node
	 */
	void delete_node(node_t node) {

#ifdef LL_DELETIONS
		w_node* p_node = lock_node(node);


		// Check if the node is already deleted

#ifdef LL_TIMESTAMPS
		long t = LL_TX_TIMESTAMP;
		if (t < p_node->wn_timestamp_creation
				|| t >= p_node->wn_timestamp_deletion) {
			release_node(p_node);
			return;
		}
#else
		if (!p_node->exists()) {
			release_node(p_node);
			return;
		}
#endif

#ifdef D_DEBUG_NODE
		if (node == D_DEBUG_NODE) {
			fprintf(stderr, "%p: delete_node %ld: %ld new out-edges, "
					"%ld new in-edges, %s RO graph\n", this, node,
					p_node->wn_out_edges.size(),
					p_node->wn_in_edges.size(),
					_ro_graph.node_exists(node) ? "in" : "not in");
		}
#endif


		// Mark the transaction as write

#ifdef LL_TX
		g_tx_write = true;
#endif


		// Update the counter

		if (p_node->exists()) {
			_delNodes++;
		}


		// Mark the node as deleted

#ifdef LL_TIMESTAMPS
		p_node->wn_timestamp_deletion = LL_TX_TIMESTAMP;
#else
		p_node->wn_deleted = true;
#endif


		if (_ro_graph.node_exists(node)) {

			// Delete the frozen out-edges

#ifdef D_DEBUG_NODE
			if (node == D_DEBUG_NODE)
				fprintf(stderr, "%p: delete_node %ld: frozen out-edges\n", this, node);
#endif
			ll_edge_iterator iter;
			_ro_graph.out_iter_begin(iter, node);
			FOREACH_OUTEDGE_ITER(edge, _ro_graph, iter) {
				delete_edge(node, edge);
			}


			// Delete the frozen in-edges

#ifdef D_DEBUG_NODE
			if (node == D_DEBUG_NODE)
				fprintf(stderr, "%p: delete_node %ld: frozen in-edges\n", this, node);
#endif
			_ro_graph.in_iter_begin(iter, node);
			FOREACH_INEDGE_ITER(edge, _ro_graph, iter) {
				delete_edge(iter.last_node, edge);
			}
		}


		// Delete the new out-edges

		for (size_t i = 0; i < p_node->wn_out_edges.size(); i++) {
			w_edge* e = p_node->wn_out_edges[i];
			if (e->exists()) _delNewEdges++;

			w_node* p_target = e->we_target == node
				? p_node : try_lock_node(e->we_target);
			if (p_target == NULL) {
				release_node(p_node);
				lock_nodes(node, e->we_target, p_node, p_target);
			}

#ifdef D_DEBUG_NODE
			if (node == D_DEBUG_NODE || e->we_target == D_DEBUG_NODE)
				fprintf(stderr, "%p: delete_node %ld: new out-edge (%p) %ld --> %ld\n",
						this, node, e, node, e->we_target);
#endif

#ifdef LL_TIMESTAMPS
			if (t < e->we_timestamp_deletion) {
				e->we_timestamp_deletion = t;
				if (t > p_target->wn_timestamp_update) p_target->wn_timestamp_update = t;
				p_target->wn_in_edges_delta--;	// TODO What kind of condition do I need for this?
			}
#else
			if (e->exists()) {
				e->we_deleted = true;
				assert(p_target->wn_in_edges_delta > 0);
				p_target->wn_in_edges_delta--;
			}
#endif

			if (p_node != p_target) release_node(p_target);
		}


		// Delete the new in-edges

		for (size_t i = 0; i < p_node->wn_in_edges.size(); i++) {
			w_edge* e = p_node->wn_in_edges[i];
			if (e->we_source == node) continue;
			if (e->exists()) _delNewEdges++;

			w_node* p_source = try_lock_node(e->we_source);
			if (p_source == NULL) {
				release_node(p_node);
				lock_nodes(node, e->we_source, p_node, p_source);
			}
			
#ifdef D_DEBUG_NODE
			if (node == D_DEBUG_NODE || e->we_source == D_DEBUG_NODE)
				fprintf(stderr, "%p: delete_node %ld: new in-edge (%p) %ld --> %ld\n",
						this, node, e, e->we_source, node);
#endif


#ifdef LL_TIMESTAMPS
			if (t < e->we_timestamp_deletion) {
				e->we_timestamp_deletion = t;
				if (t > p_source->wn_timestamp_update) p_source->wn_timestamp_update = t;
				p_source->wn_out_edges_delta--;
			}
#else
			if (e->exists()) {
				e->we_deleted = true;
				assert(p_source->wn_out_edges_delta > 0);
				p_source->wn_out_edges_delta--;
			}
#endif

			release_node(p_source);
		}
		

		// Update the counters

		p_node->wn_out_edges_delta = 0;
		p_node->wn_in_edges_delta = 0;


		// Cleanup

		release_node(p_node);
#endif
	}


protected:

	/**
	 * Add an edge
	 *
	 * @param source the source node
	 * @param target the destination node
	 * @param p_source the locked source node
	 * @param p_target the locked destination node
	 * @return the edge ID
	 */
	edge_t add_edge(node_t source, node_t target, w_node* p_source, w_node* p_target) {

		LL_D_NODE2_PRINT(source, target, "Add %ld --> %ld\n", source, target);

		w_edge_allocator _allocator;
#ifdef LL_WRITABLE_USE_MEMORY_POOL
		size_t we_chunk, we_offset;
		w_edge* we = _allocator(&we_chunk, &we_offset);
		we->we_public_id = LL_EDGE_CREATE(LL_WRITABLE_LEVEL,
				(we_chunk << LL_W_MEM_POOL_MAX_OFFSET_BITS)
				| (we_offset >> LL_MEM_POOL_ALIGN_BITS));
#else
		w_edge* we = _allocator();
		we->we_public_id = LL_EDGE_CREATE(LL_WRITABLE_LEVEL, (edge_t) (long) we);
#endif
		w_edge* p_edge = p_source->wn_out_edges.append(we);
		p_target->wn_in_edges.append(p_edge);

#ifndef LL_WRITABLE_USE_MEMORY_POOL
#ifdef LL_NODE32
#warning "LL_NODE32 is not good; please use LL_NODE64 instead!"
		if (((unsigned long) p_edge) >= (1ul << 28)) {
			fprintf(stderr, "\n\nFATAL: Edge pointer out of range %p "
					"-- use LL_NODE64\n", p_edge);
			abort();
		}
#endif
		// Huh, it would be great if we did not need this!
		if (((unsigned long) p_edge) >= (1ull << LL_BITS_INDEX)) {
			fprintf(stderr, "\n\nFATAL: Edge pointer out of range %p\n", p_edge);
			abort();
		}
#endif

#ifdef LL_TX
		g_tx_write = true;
#endif

		edge_t out_edge = we->we_public_id;
		assert(LL_EDGE_GET_WRITABLE(out_edge) == p_edge);

		p_edge->we_target = target;
		p_edge->we_source = source;

#ifdef LL_TIMESTAMPS
		long t = LL_TX_TIMESTAMP;

		p_edge->we_timestamp_creation = t;

		if (t > p_source->wn_timestamp_update) p_source->wn_timestamp_update = t;
		if (t > p_target->wn_timestamp_update) p_target->wn_timestamp_update = t;

		p_edge->we_timestamp_deletion = LONG_MAX;
#else
		p_edge->we_deleted = false;
#endif

		p_source->wn_out_edges_delta++;
		p_target->wn_in_edges_delta++;

		uint32_t id = _newEdges.fetch_add(1);
		p_edge->we_numerical_id = id;

		return out_edge;
	}


public:


	/**
	 * Add an edge
	 *
	 * @param source the source node
	 * @param target the destination node
	 * @return the edge ID
	 */
	edge_t add_edge(node_t source, node_t target) {

		edge_t out_edge;
		w_node* p_source;
		w_node* p_target;

		lock_nodes(source, target, p_source, p_target);
		out_edge = add_edge(source, target, p_source, p_target);
		release_nodes(p_source, p_target);

		return out_edge;
	}


	/**
	 * Add edge if it does not already exists. If the edge already exists,
	 * return its ID in place of the new edge ID.
	 *
	 * It requires that there is at most one existing edge that matches
	 *
	 * @param source the source node
	 * @param target the destination node
	 * @param out the output for the edge ID
	 * @return true if the edge was added; false if it it already exists
	 */
	bool add_edge_if_not_exists(node_t source, node_t target, edge_t* out) {

		edge_t e;
		
		if (_ro_graph.node_exists(source) && _ro_graph.node_exists(target)) {
			e = _ro_graph.find(source, target);
			if (e != LL_NIL_EDGE) {
				*out = e;
				return false;
			}
		}

		w_node* p_source;
		w_node* p_target;

		lock_nodes(source, target, p_source, p_target);

		ll_edge_iterator iter;
		this->out_iter_begin_within_level(iter, source);
		FOREACH_OUTEDGE_ITER_WITHIN_LEVEL(e, *this, iter) {
			if (iter.last_node == target) {
				*out = e;
				release_nodes(p_source, p_target);
				return false;
			}
		}

		e = add_edge(source, target, p_source, p_target);
		release_nodes(p_source, p_target);

		*out = e;
		return true;
	}


#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES

	/**
	 * Add an edge for streaming applications with weights. If an edge already
	 * exists in the read-only graph, delete it, and recreate it with the
	 * incremented weight. If the edge already exists in the writable level,
	 * increment the weight.
	 *
	 * @param source the source node
	 * @param target the destination node
	 * @param out the output for the edge ID
	 * @return true if the edge was added; false if it it already exists
	 */
	bool add_edge_for_streaming_with_weights(node_t source, node_t target, edge_t* out) {

		LL_D_NODE2_PRINT(source, target, "Add for streaming: %ld --> %ld\n",
				source, target);

		ll_mlcsr_edge_property<uint32_t>* w = get_edge_weights_streaming();
		uint32_t ro_weight = 0;
		edge_t ro_edge = LL_NIL_EDGE;

		w_node* p_source;
		w_node* p_target;

		lock_nodes(source, target, p_source, p_target);

		ll_edge_iterator iter;
		this->out_iter_begin_within_level(iter, source);
		FOREACH_OUTEDGE_ITER_WITHIN_LEVEL(e, *this, iter) {
			if (iter.last_node == target) {

				w->add(e, 1);
				release_nodes(p_source, p_target);

				*out = e;

				LL_D_NODE2_PRINT(source, target,
						"Found duplicate of R/W edge %ld --> %ld\n",
						source, target);

				return false;
			}
		}

		if (_ro_graph.node_exists(source) && _ro_graph.node_exists(target)) {
			ro_edge = _ro_graph.find(source, target);
			if (ro_edge != LL_NIL_EDGE) {
				ro_weight = (*w)[ro_edge];
				LL_D_NODE2_PRINT(source, target,
						"Found duplicate of %lx, old weight = %u, "
						"old forward = %lx\n",
						ro_edge, ro_weight,
						_ro_graph.get_edge_forward_streaming()->get(ro_edge));
				assert(_ro_graph.get_edge_forward_streaming()->get(ro_edge) == 0);
				delete_edge(source, ro_edge);
			}
		}

		edge_t new_edge = add_edge(source, target, p_source, p_target);
		w->set(new_edge, ro_weight + 1);

		LL_EDGE_GET_WRITABLE(new_edge)->we_supersedes = ro_edge;

		release_nodes(p_source, p_target);
		*out = new_edge;
		return true;
	}

#endif



	/**
	 * Delete an edge
	 *
	 * @param source the source node
	 * @return edge the edge
	 */
	void delete_edge(node_t source, edge_t edge) {

#ifdef LL_DELETIONS

		// Needs edge sources

		w_node* p_source;
		w_node* p_target;

		long t = LL_TX_TIMESTAMP;
		(void) t;

		if (LL_EDGE_IS_WRITABLE(edge)) {

			w_edge* w = LL_EDGE_GET_WRITABLE(edge);

			node_t target = w->we_target;
			assert(source == w->we_source);
			//node_t source = w->we_source;

			lock_nodes(source, target, p_source, p_target);

			LL_D_NODE2_PRINT(source, target,
					"delete writable edge=%08lx %ld --> %ld",
					edge, source, target);

#ifdef LL_TX
			g_tx_write = true;
#endif


			// Delete the edge and update the timestamps

#ifdef LL_TIMESTAMPS
			if (t < w->we_timestamp_deletion) {
				w->we_timestamp_deletion = t;
				if (t > p_source->wn_timestamp_update) p_source->wn_timestamp_update = t;
				if (t > p_target->wn_timestamp_update) p_target->wn_timestamp_update = t;

				p_source->wn_out_edges_delta--;
				p_target->wn_in_edges_delta--;

				if (w->we_timestamp_deletion != LONG_MAX) {
					_delNewEdges++;
				}
			}
#else
			if (w->exists()) {
				p_source->wn_out_edges_delta--;
				p_target->wn_in_edges_delta--;
			}

			w->we_deleted = true;

			_delNewEdges++;
#endif

			// Finish

			release_nodes(p_source, p_target);
		}
		else {

			//node_t source = _ro_graph.edge_src(edge);
			assert(source == _ro_graph.in().value(_ro_graph.out_to_in(edge)));
			node_t target = _ro_graph.edge_dst(edge);

			LL_D_NODE2_PRINT(source, target,
					"delete frozen edge=%08lx, in_edge=%08lx %ld --> %ld",
					edge, _ro_graph.out_to_in(edge), source, target);

#ifdef LL_TIMESTAMPS

			// Lock / latch the data structures

			// TODO A finer granularity lock? The following are just intended to be latches
			// for the deletion maps data structures

			ll_spinlock_acquire(&_deletions_out_lock);
			ll_spinlock_acquire(&_deletions_in_lock);


			// Update the external deletion maps

#ifdef LL_TX
			g_tx_write = true;
#endif

			auto it = _deletions_out_map.find(edge);
			bool alreadyDeleted = false;
			if (it == _deletions_out_map.end()) {
				_deletions_out_map[edge] = t;
			}
			else {
				it->second = std::min(t, it->second);
				alreadyDeleted = true;
			}

			if (!alreadyDeleted) {
				_deletions_nodes_out[LL_D_STRIPE(source)][source]
					.an_deleted_edges.push_back(edge);
			}

			if (_ro_graph.has_reverse_edges()) {
				edge_t in_edge = _ro_graph.out_to_in(edge);
				it = _deletions_in_map.find(in_edge);
				if (it == _deletions_in_map.end()) {
					_deletions_in_map[in_edge] = t;
					alreadyDeleted = false;
				}
				else {
					it->second = std::min(t, it->second);
					alreadyDeleted = true;
				}

				if (!alreadyDeleted) {
					_deletions_nodes_in[LL_D_STRIPE(target)][target]
						.an_deleted_edges.push_back(in_edge);
				}
			}


			// TODO Update the timestamps at the nodes?


			// Update the max. level in the CSR

			if (_ro_graph.update_max_visible_level_lower_only(edge, LL_CHECK_EXT_DELETION)) {
#	ifdef D_DEBUG_NODE
				if (source == D_DEBUG_NODE || target == D_DEBUG_NODE) fprintf(stderr, " [ok]");
#	endif
				__sync_fetch_and_add(&writable_node(source)->wn_num_deleted_out_edges, 1);
				__sync_fetch_and_add(&writable_node(target)->wn_num_deleted_in_edges , 1);
				_delFrozenEdges++;
			}


			// Release locks

			ll_spinlock_release(&_deletions_in_lock);
			ll_spinlock_release(&_deletions_out_lock);

#else /* !LL_TIMESTAMPS */


			// Update the max. level in the CSR

			if (_ro_graph.update_max_visible_level_lower_only(edge, _ro_graph.num_levels())) {
#	ifdef D_DEBUG_NODE
				if (source == D_DEBUG_NODE || target == D_DEBUG_NODE) fprintf(stderr, " [ok]");
#	endif
				__sync_fetch_and_add(&writable_node(source)->wn_num_deleted_out_edges, 1);
				__sync_fetch_and_add(&writable_node(target)->wn_num_deleted_in_edges , 1);
				_delFrozenEdges++;		// already atomic
			}
#endif

#ifdef D_DEBUG_NODE
			if (source == D_DEBUG_NODE || target == D_DEBUG_NODE) fprintf(stderr, "\n");
#endif
		}

#endif
	}


	/**
	 * Get the destination of the edge
	 *
	 * @param e the edge
	 * @return the node
	 */
    node_t edge_dst(edge_t e) {
		if (LL_EDGE_IS_WRITABLE(e))
			return LL_EDGE_GET_WRITABLE(e)->we_target;
		else
			return _ro_graph.edge_dst(e);
    }


	/**
	 * Get the node out-degree
	 *
	 * @param node the node
	 * @return the out-degree
	 */
	size_t out_degree(node_t node) {

		w_node* r = (w_node*) _vertices.get(node);
		size_t d = 0;

		if (r != NULL) {

#ifdef LL_DELETIONS
#	ifdef LL_TIMESTAMPS
			long t = LL_TX_TIMESTAMP;
			if (t < r->wn_timestamp_creation
					|| t >= r->wn_timestamp_deletion) return 0;

			size_t n = r->wn_out_edges.size();
			for (size_t i = 0; i < n; i++) {
				const w_edge& e = r->wn_out_edges[i];
				if (t >= e.we_timestamp_creation
						&& t < e.we_timestamp_deletion) d++;
			}
#	else
			if (!r->exists()) return 0;
			d = r->wn_out_edges_delta;
#	endif
#else
			d = r->wn_out_edges.size();
#endif
#ifndef LL_TIMESTAMPS
			d -= r->wn_num_deleted_out_edges;
#endif
		}

#ifdef LL_CHECK_NODE_EXISTS_IN_RO
		d += _ro_graph.out_degree(node);
#else
		d += _ro_graph.node_exists(node) ? _ro_graph.out_degree(node) : 0;
#endif

#ifdef LL_DELETIONS
#ifdef LL_TIMESTAMPS
		
		// Having to take this spinlock is extremely slow - so let's try something else

retry:
		auto& h = _deletions_nodes_out[LL_D_STRIPE(node)];
		size_t size = h.size();
		__COMPILER_FENCE;
		auto it = h.find(node);
		if (it != h.end()) {
			auto& x = it->second;
			__COMPILER_FENCE;
			size_t size2 = h.size();
			if (size != size2) goto retry;

			auto& v = it->second.an_deleted_edges;
			long t = LL_TX_TIMESTAMP;
			for (size_t i = 0; i < v.size(); i++) {
				if (_deletions_out_map[v[i]] <= t) d--;
			}
		}

#endif
#endif

		return d;
	}


	/**
	 * Get the node in-degree
	 *
	 * @param node the node
	 * @return the in-degree
	 */
	size_t in_degree(node_t node) {

		w_node* r = (w_node*) _vertices.get(node);
		size_t d = 0;

		if (r != NULL) {

#ifdef LL_DELETIONS
#	ifdef LL_TIMESTAMPS
			long t = LL_TX_TIMESTAMP;
			if (t < r->wn_timestamp_creation
					|| t >= r->wn_timestamp_deletion) return 0;

			size_t n = r->wn_in_edges.size();
			for (size_t i = 0; i < n; i++) {
				const w_edge& e = r->wn_in_edges[i];
				if (t >= e.we_timestamp_creation
						&& t < e.we_timestamp_deletion) d++;
			}
#	else
			if (!r->exists()) return 0;
			d = r->wn_in_edges_delta;
#	endif
#else
			d = r->wn_in_edges.size();
#endif
#ifndef LL_TIMESTAMPS
			d -= r->wn_num_deleted_in_edges;
#endif
		}

#ifdef LL_CHECK_NODE_EXISTS_IN_RO
		d += _ro_graph.in_degree(node);
#else
		d += _ro_graph.node_exists(node) ? _ro_graph.in_degree(node) : 0;
#endif

#ifdef LL_DELETIONS
#ifdef LL_TIMESTAMPS
		
		// Having to take this spinlock is extremely slow - so let's try something else

retry:
		auto& h = _deletions_nodes_in[LL_D_STRIPE(node)];
		size_t size = h.size();
		__COMPILER_FENCE;
		auto it = h.find(node);
		if (it != h.end()) {
			auto& x = it->second;
			__COMPILER_FENCE;
			size_t size2 = h.size();
			if (size != size2) goto retry;

			auto& v = it->second.an_deleted_edges;
			long t = LL_TX_TIMESTAMP;
			for (size_t i = 0; i < v.size(); i++) {
				if (_deletions_in_map[v[i]] <= t) d--;
			}
		}
#endif
#endif

		return d;
	}


	/**
	 * Create iterator over all outgoing edges
	 *
	 * @param iter the iterator
	 * @param v the vertex
	 * @return the iterator
	 */
	void out_iter_begin(ll_edge_iterator& iter, node_t node) {

		w_node* r = (w_node*) _vertices.get(node);
		if (r == NULL) {
#ifndef LL_CHECK_NODE_EXISTS_IN_RO
			if (!_ro_graph.node_exists(node)) {
				_ro_graph.out().iter_set_to_end(iter);
				LL_D_NODE_PRINT(node,
						"The node does not exist or have any edges\n");
				return;
			}
#endif
			_ro_graph.out_iter_begin(iter, node, _ro_graph.num_levels()-1,
					_ro_graph.num_levels());
			LL_D_NODE_PRINT(node, "[owner=%d, left=%ld]\n",
					(int) iter.owner, (long) iter.left);
			return;
		}


		// Check if the node is deleted

#ifdef LL_DELETIONS
#ifdef LL_TIMESTAMPS
		long t = LL_TX_TIMESTAMP;
		if (t < r->wn_timestamp_creation
				|| t >= r->wn_timestamp_deletion) {
			_ro_graph.out().iter_set_to_end(iter);
			return;
		}
#else
		if (!r->exists()) {
			_ro_graph.out().iter_set_to_end(iter);
			return;
		}
#endif
#endif


		// Create the iterator struct

		iter.owner = LL_I_OWNER_WRITABLE;
		iter.ptr = r;
		iter.node = node;
		iter.left = r->wn_out_edges.size();

		if (iter.left == 0) {
#ifndef LL_CHECK_NODE_EXISTS_IN_RO
			if (!_ro_graph.node_exists(node)) {
				_ro_graph.out().iter_set_to_end(iter);
				LL_D_NODE_PRINT(node,
						"The node does not exist or have any edges\n");
				return;
			}
#endif
			_ro_graph.out_iter_begin(iter, node, _ro_graph.num_levels()-1,
					_ro_graph.num_levels());
		}
		else {
			w_edge* e = ((w_node*) iter.ptr)->wn_out_edges[--iter.left];
			iter.edge = (long) e;
#ifdef LL_DELETIONS
#ifdef LL_TIMESTAMPS
			if (g_tx_timestamp < e->we_timestamp_creation
					|| g_tx_timestamp >= e->we_timestamp_deletion) {
				out_iter_next(iter);
			}
#else
			if (!e->exists()) {
				out_iter_next(iter);
			}
#endif
#endif
		}

		LL_D_NODE_PRINT(node, "[owner=%d, left=%ld]\n",
				(int) iter.owner, (long) iter.left);
	}


	/**
	 * Determine if there are any more items left
	 *
	 * @param iter the iterator
	 * @return true if there are more items
	 */
	ITERATOR_DECL bool out_iter_has_next(ll_edge_iterator& iter) {

		if (iter.owner != LL_I_OWNER_WRITABLE) {
			return _ro_graph.out_iter_has_next(iter);
		}

		return iter.edge != LL_NIL_EDGE;
	}


	/**
	 * Get the next item
	 *
	 * @param iter the iterator
	 * @return the next item, or LL_NIL_EDGE if none
	 */
	ITERATOR_DECL edge_t out_iter_next(ll_edge_iterator& iter) {

		if (iter.owner != LL_I_OWNER_WRITABLE) {
			return _ro_graph.out_iter_next(iter);
		}

		edge_t r = iter.edge;
		if (r == LL_NIL_EDGE) return LL_NIL_EDGE;

		iter.last_node = ((w_edge*) r)->we_target;

		w_edge* e;
		do {
			if (iter.left == 0)	{
#ifndef LL_CHECK_NODE_EXISTS_IN_RO
				if (!_ro_graph.node_exists(iter.node)) {
					_ro_graph.out().iter_set_to_end(iter);
					break;
				}
#endif
				_ro_graph.out_iter_begin(iter, iter.node, _ro_graph.num_levels()-1, _ro_graph.num_levels());
				break;
			}

			assert(iter.left > 0);
			e = ((w_node*) iter.ptr)->wn_out_edges[--iter.left];
			iter.edge = (long) e;

#ifdef LL_DELETIONS
		}
#ifdef LL_TIMESTAMPS
		while (g_tx_timestamp < e->we_timestamp_creation
				|| g_tx_timestamp >= e->we_timestamp_deletion);
#else
		while (!e->exists());
#endif
#elif 0
		{
#else
			break;
		}
		while (true);
#endif

		return LL_W_EDGE_CREATE(r);
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
	inline edge_t out_iter_begin_next(ll_edge_iterator& iter, node_t n,
			int level=-1, int max_level=-1) {
		this->out_iter_begin(iter, n);
		return this->out_iter_next(iter);
	}


	/**
	 * Finish the iterator
	 *
	 * @param iter the iterator
	 */
	inline void out_iter_end(ll_edge_iterator& iter) {
	}


	/**
	 * Create iterator over all outgoing edges within this level
	 *
	 * @param iter the iterator
	 * @param v the vertex
	 * @return the iterator
	 */
	void out_iter_begin_within_level(ll_edge_iterator& iter, node_t node) {

		w_node* r = (w_node*) _vertices.get(node);

		if (r == NULL) {
			iter.left = 0;
			iter.edge = LL_NIL_EDGE;
		}
		else {

			// Check if the node is deleted

#ifdef LL_DELETIONS
#ifdef LL_TIMESTAMPS
			long t = LL_TX_TIMESTAMP;
			if (t < r->wn_timestamp_creation
					|| t >= r->wn_timestamp_deletion) {
				_ro_graph.out().iter_set_to_end(iter);
				return;
			}
#else
			if (!r->exists()) {
				_ro_graph.out().iter_set_to_end(iter);
				return;
			}
#endif
#endif


			// Create the iterator struct

			iter.owner = LL_I_OWNER_WRITABLE;
			iter.ptr = r;
			iter.node = node;
			iter.left = r->wn_out_edges.size();
			
			if (iter.left == 0) {
				iter.edge = LL_NIL_EDGE;
			}
			else {
				w_edge* e = ((w_node*) iter.ptr)->wn_out_edges[--iter.left];
				iter.edge = (long) e;

#ifdef LL_DELETIONS
#ifdef LL_TIMESTAMPS
				if (g_tx_timestamp < e->we_timestamp_creation
						|| g_tx_timestamp >= e->we_timestamp_deletion) {
					out_iter_next_within_level(iter);
				}
#else
				if (!e->exists()) {
					out_iter_next_within_level(iter);
				}
#endif
#endif
			}
		}
	}


	/**
	 * Determine if there are any more items left within this level
	 *
	 * @param iter the iterator
	 * @return true if there are more items
	 */
	ITERATOR_DECL bool out_iter_has_next_within_level(ll_edge_iterator& iter) {
		return iter.edge != LL_NIL_EDGE;
	}


	/**
	 * Get the next item, but only within this level
	 *
	 * @param iter the iterator
	 * @return the next item, or LL_NIL_EDGE if none
	 */
	ITERATOR_DECL edge_t out_iter_next_within_level(ll_edge_iterator& iter) {

		edge_t r = iter.edge;
		if (r == LL_NIL_EDGE) return LL_NIL_EDGE;

		iter.last_node = ((w_edge*) r)->we_target;
		
		w_edge* e;
		do {
			if (iter.left == 0)	{
				iter.edge = LL_NIL_EDGE;
				break;
			}

			assert(iter.left > 0);
			e = ((w_node*) iter.ptr)->wn_out_edges[--iter.left];
			iter.edge = (long) e;

#ifdef LL_DELETIONS
		}
#ifdef LL_TIMESTAMPS
		while (g_tx_timestamp < e->we_timestamp_creation
				|| g_tx_timestamp >= e->we_timestamp_deletion);
#else
		while (!e->exists());
#endif
#elif 0
		{
#else
			break;
		}
		while (true);
#endif

		return LL_W_EDGE_CREATE(r);
	}


	/**
	 * Create iterator over all incoming edges
	 *
	 * @param iter the iterator
	 * @param v the vertex
	 * @return the iterator
	 */
	void in_iter_begin_fast(ll_edge_iterator& iter, node_t node) {

		w_node* r = (w_node*) _vertices.get(node);
		if (r == NULL) {
#ifndef LL_CHECK_NODE_EXISTS_IN_RO
			if (!_ro_graph.node_exists(node)) {
				_ro_graph.in().iter_set_to_end(iter);
				return;
			}
#endif
			_ro_graph.in_iter_begin_fast(iter, node);
			return;
		}


		// Check if the node is deleted

#ifdef LL_DELETIONS
#ifdef LL_TIMESTAMPS
		long t = LL_TX_TIMESTAMP;
		if (t < r->wn_timestamp_creation
				|| t >= r->wn_timestamp_deletion) {
			_ro_graph.in().iter_set_to_end(iter);
			return;
		}
#else
		if (!r->exists()) {
			_ro_graph.in().iter_set_to_end(iter);
			return;
		}
#endif
#endif


		// Create the iterator struct

		iter.owner = LL_I_OWNER_WRITABLE;
		iter.ptr = r;
		iter.node = node;
		iter.left = r->wn_in_edges.size();

		if (iter.left == 0) {
#ifndef LL_CHECK_NODE_EXISTS_IN_RO
			if (!_ro_graph.node_exists(node)) {
				_ro_graph.in().iter_set_to_end(iter);
				return;
			}
#endif
			_ro_graph.in_iter_begin_fast(iter, node);
		}
		else {
			w_edge* e = ((w_node*) iter.ptr)->wn_in_edges[--iter.left];
			iter.edge = (long) e;
#ifdef LL_DELETIONS
#ifdef LL_TIMESTAMPS
			if (g_tx_timestamp < e->we_timestamp_creation
					|| g_tx_timestamp >= e->we_timestamp_deletion) {
				in_iter_next_fast(iter);
			}
#else
			if (!e->exists()) {
				in_iter_next_fast(iter);
			}
#endif
#endif
		}
	}


	/**
	 * Determine if there are any more items left
	 *
	 * @param iter the iterator
	 * @return true if there are more items
	 */
	ITERATOR_DECL bool in_iter_has_next_fast(ll_edge_iterator& iter) {

		if (iter.owner != LL_I_OWNER_WRITABLE) {
			return _ro_graph.in_iter_has_next_fast(iter);
		}

		return iter.edge != LL_NIL_EDGE;
	}


	/**
	 * Get the next item
	 *
	 * @param iter the iterator
	 * @return the next item, or LL_NIL_EDGE if none
	 */
	ITERATOR_DECL edge_t in_iter_next_fast(ll_edge_iterator& iter) {

		if (iter.owner != LL_I_OWNER_WRITABLE) {
			return _ro_graph.in_iter_next_fast(iter);
		}

		edge_t r = iter.edge;
		if (r == LL_NIL_EDGE) return LL_NIL_EDGE;

		iter.last_node = ((w_edge*) r)->we_source;

		w_edge* e;
		do {
			if (iter.left == 0)	{
#ifndef LL_CHECK_NODE_EXISTS_IN_RO
				if (!_ro_graph.node_exists(iter.node)) {
					_ro_graph.in().iter_set_to_end(iter);
					break;
				}
#endif
				_ro_graph.in_iter_begin_fast(iter, iter.node);
				break;
			}

			assert(iter.left > 0);
			e = ((w_node*) iter.ptr)->wn_in_edges[--iter.left];
			iter.edge = (long) e;

#ifdef LL_DELETIONS
		}
#ifdef LL_TIMESTAMPS
		while (g_tx_timestamp < e->we_timestamp_creation
				|| g_tx_timestamp >= e->we_timestamp_deletion);
#else
		while (!e->exists());
#endif
#elif 0
		{
#else
			break;
		}
		while (true);
#endif

		return LL_W_EDGE_CREATE(r);
	}


	/**
	 * Create iterator over all incoming nodes
	 *
	 * @param iter the iterator
	 * @param node the node
	 * @return the iterator
	 */
	void inm_iter_begin(ll_edge_iterator& iter, node_t node) {

		w_node* r = (w_node*) _vertices.get(node);
		if (r == NULL) {
#ifndef LL_CHECK_NODE_EXISTS_IN_RO
			if (!_ro_graph.node_exists(node)) {
				_ro_graph.in().iter_set_to_end(iter);
				LL_D_NODE_PRINT(node, "Node does not exist\n");
				return;
			}
#endif
			LL_D_NODE_PRINT(node, "Not in ll_writable_graph, descending\n");
			_ro_graph.inm_iter_begin(iter, node, _ro_graph.num_levels()-1,
					_ro_graph.num_levels());
			return;
		}

		// Check if the node is deleted

#ifdef LL_DELETIONS
#ifdef LL_TIMESTAMPS
		long t = LL_TX_TIMESTAMP;
		if (t < r->wn_timestamp_creation
				|| t >= r->wn_timestamp_deletion) {
			_ro_graph.in().iter_set_to_end(iter);
			LL_D_NODE_PRINT(node, "Deleted at timestamp %ld\n",
					r->wn_timestamp_deletion);
			return;
		}
#else
		if (!r->exists()) {
			_ro_graph.in().iter_set_to_end(iter);
			LL_D_NODE_PRINT(node, "Deleted\n");
			return;
		}
#endif
#endif


		// Create the iterator struct

		iter.owner = LL_I_OWNER_WRITABLE;
		iter.ptr = r;
		iter.node = node;
		iter.left = r->wn_in_edges.size();

		if (iter.left == 0) {
#ifndef LL_CHECK_NODE_EXISTS_IN_RO
			if (!_ro_graph.node_exists(node)) {
				_ro_graph.in().iter_set_to_end(iter);
				return;
			}
#endif
			_ro_graph.inm_iter_begin(iter, node, _ro_graph.num_levels()-1,
					_ro_graph.num_levels());
			LL_D_NODE_PRINT(node, "No writable edges, descending\n");
		}
		else {
			w_edge* e = ((w_node*) iter.ptr)->wn_in_edges[--iter.left];
			iter.edge = (long) e;

			LL_D_NODE_PRINT(node, "left=%lu\n", (size_t) iter.left);

#ifdef LL_DELETIONS
#ifdef LL_TIMESTAMPS
			if (g_tx_timestamp < e->we_timestamp_creation
					|| g_tx_timestamp >= e->we_timestamp_deletion) {
				inm_iter_next(iter);
			}
#else
			if (!e->exists()) {
				inm_iter_next(iter);
			}
#endif
#endif
		}
	}


	/**
	 * Determine if there are any more items left
	 *
	 * @param iter the iterator
	 * @return true if there are more items
	 */
	bool inm_iter_has_next(ll_edge_iterator& iter) {

		if (iter.owner != LL_I_OWNER_WRITABLE) {
			return _ro_graph.inm_iter_has_next(iter);
		}

		return iter.edge != LL_NIL_EDGE;
	}


	/**
	 * Get the next item
	 *
	 * @param iter the iterator
	 * @return the next node, or LL_NIL_EDGE if none
	 */
	node_t inm_iter_next(ll_edge_iterator& iter) {

		if (iter.owner != LL_I_OWNER_WRITABLE) {
			return _ro_graph.inm_iter_next(iter);
		}

		edge_t r = iter.edge;
		if (r == LL_NIL_EDGE) return LL_NIL_NODE;
		
		w_edge* e;
		do {
			if (iter.left == 0)	{
#ifndef LL_CHECK_NODE_EXISTS_IN_RO
				if (!_ro_graph.node_exists(iter.node)) {
					_ro_graph.in().iter_set_to_end(iter);
					break;
				}
#endif
				_ro_graph.inm_iter_begin(iter, iter.node, _ro_graph.num_levels()-1, _ro_graph.num_levels());
				break;
			}

			assert(iter.left > 0);
			e = ((w_node*) iter.ptr)->wn_in_edges[--iter.left];
			iter.edge = (long) e;

#ifdef LL_DELETIONS
		}
#ifdef LL_TIMESTAMPS
		while (g_tx_timestamp < e->we_timestamp_creation
				|| g_tx_timestamp >= e->we_timestamp_deletion);
#else
		while (!e->exists());
#endif
#elif 0
		{
#else
			break;
		}
		while (true);
#endif

		return ((w_edge*) r)->we_source;
	}


	/**
	 * Find the given edge
	 *
	 * @param source the source node
	 * @param target the target node
	 * @return the edge, or NIL_EDGE if it does not exist
	 */
	edge_t find(node_t source, node_t target) {

		ll_edge_iterator iter;
		this->out_iter_begin(iter, source);
		FOREACH_OUTEDGE_ITER(e, *this, iter) {
			if (iter.last_node == target) return e;
		}

		return LL_NIL_EDGE;
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


	/**
	 * Get a 32-bit node property
	 *
	 * @param name the property name
	 * @return the property if it exists, or NULL if not
	 */
	ll_mlcsr_node_property<uint32_t>* get_node_property_32(const char* name) {

		auto p = _ro_graph.get_node_property_32(name);
		if (p == NULL) return NULL;

		ll_spinlock_acquire(&_property_lock);
		if (!p->writable()) p->writable_init(_vertices.size());
		ll_spinlock_release(&_property_lock);

		return p;
	}


	/**
	 * Get a 64-bit node property
	 *
	 * @param name the property name
	 * @return the property if it exists, or NULL if not
	 */
	ll_mlcsr_node_property<uint64_t>* get_node_property_64(const char* name) {

		auto p = _ro_graph.get_node_property_64(name);
		if (p == NULL) return NULL;

		ll_spinlock_acquire(&_property_lock);
		if (!p->writable()) p->writable_init(_vertices.size());
		ll_spinlock_release(&_property_lock);

		return p;
	}


	/**
	 * Get a 32-bit edge property
	 *
	 * @param name the property name
	 * @return the property if it exists, or NULL if not
	 */
	ll_mlcsr_edge_property<uint32_t>* get_edge_property_32(const char* name) {

		auto p = _ro_graph.get_edge_property_32(name);
		if (p == NULL) return NULL;

		ll_spinlock_acquire(&_property_lock);
		if (!p->writable()) p->writable_init();
		ll_spinlock_release(&_property_lock);

		return p;
	}


#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES

	/**
	 * Get the edge weight property for streaming
	 *
	 * @return the property
	 */
	inline ll_mlcsr_edge_property<uint32_t>* get_edge_weights_streaming() {

		auto p = _ro_graph.get_edge_weights_streaming();
		if (p == NULL) return NULL;

		if (!p->writable()) {
			ll_spinlock_acquire(&_property_lock);
			if (!p->writable()) p->writable_init();
			ll_spinlock_release(&_property_lock);
		}

		return p;
	}

#endif


	/**
	 * Get a 64-bit edge property
	 *
	 * @param name the property name
	 * @return the property if it exists, or NULL if not
	 */
	ll_mlcsr_edge_property<uint64_t>* get_edge_property_64(const char* name) {

		auto p = _ro_graph.get_edge_property_64(name);
		if (p == NULL) return NULL;

		ll_spinlock_acquire(&_property_lock);
		if (!p->writable()) p->writable_init();
		ll_spinlock_release(&_property_lock);

		return p;
	}


protected:

	/**
	 * The checkpoint data source adapter
	 */
	class checkpoint_adapter : public ll_mlcsr_checkpoint_source {

		ll_writable_graph& _owner;


	public:

		checkpoint_adapter(ll_writable_graph& owner) : _owner(owner) {}
		virtual ~checkpoint_adapter() {}

		virtual size_t num_new_nodes() { return _owner._newNodes.load(); }
		virtual size_t num_new_edges() { return _owner._newEdges.load(); }
		virtual size_t max_node_id() { return _owner._next_new_node_id - 1; }
		virtual ll_w_vt_vertices_t* vertex_table() { return &_owner._vertices; }

		virtual void get_out_edges(node_t node, std::vector<node_t>& new_edges) {
			w_node* w = (w_node*) _owner._vertices.fast_get(node);
			size_t num = w->wn_out_edges.size();
			for (size_t i = 0; i < num; i++) {
				if (w->wn_out_edges[i]->exists()) {
					new_edges.push_back(w->wn_out_edges[i]->we_target);
				}
			}
		}
	};


	/**
	 * The external deletions adapter for the out-edges
	 */
	class deletions_adapter_out : public ll_mlcsr_external_deletions {

		ll_writable_graph& _owner;


	public:

		deletions_adapter_out(ll_writable_graph& owner) : _owner(owner) {}
		
		virtual bool is_edge_deleted(edge_t edge) {
#ifdef LL_DELETIONS
			ll_spinlock_acquire(&_owner._deletions_out_lock);
			auto it = _owner._deletions_out_map.find(edge);
			if (it != _owner._deletions_out_map.end()) {
#ifdef LL_TIMESTAMPS
				bool r = g_tx_timestamp >= it->second;
				ll_spinlock_release(&_owner._deletions_out_lock);
				return r;
#else
				ll_spinlock_release(&_owner._deletions_out_lock);
				return true;
#endif
			}
			ll_spinlock_release(&_owner._deletions_out_lock);
#endif
			return false;
		}
	};


	/**
	 * The external deletions adapter for the in-edges
	 */
	class deletions_adapter_in : public ll_mlcsr_external_deletions {

		ll_writable_graph& _owner;


	public:

		deletions_adapter_in(ll_writable_graph& owner) : _owner(owner) {}
		
		virtual bool is_edge_deleted(edge_t edge) {
#ifdef LL_DELETIONS
			ll_spinlock_acquire(&_owner._deletions_in_lock);
			auto it = _owner._deletions_in_map.find(edge);
			if (it != _owner._deletions_in_map.end()) {
#ifdef LL_TIMESTAMPS
				bool r = g_tx_timestamp >= it->second;
				ll_spinlock_release(&_owner._deletions_in_lock);
				return r;
#else
				ll_spinlock_release(&_owner._deletions_in_lock);
				return true;
#endif
			}
			ll_spinlock_release(&_owner._deletions_in_lock);
#endif
			return false;
		}
	};


public:

	/**
	 * Checkpoint
	 *
	 * @param config the loader config
	 */
	void checkpoint(const ll_loader_config* config = NULL) {

		if (_newNodes.load() == 0
				&& _delNodes.load() == 0
				&& _newEdges.load() == 0
				&& _delNewEdges.load() == 0
				&& _delFrozenEdges.load() == 0) return;

		ll_loader_config default_config;
		default_config.lc_reverse_edges = has_reverse_edges();
		default_config.lc_reverse_maps = has_reverse_edges()
			&& _ro_graph.out().has_edge_translation();
		const ll_loader_config* c = config == NULL ? &default_config : config;


		// Check whether we ran out of the level ID space, and if so, fail

		/*if (_ro_graph.num_levels() + 1 >= LL_MAX_LEVEL) {
			fprintf(stderr, "Fatal: Too many RO levels. Cannot recover!");
			abort();
		}*/
		
		
		// Create the new level

		checkpoint_adapter adapter(*this);

		__COMPILER_FENCE;
		_ro_graph.checkpoint(&adapter, c);
		__COMPILER_FENCE;


		// Clear the writable representation

		_newNodes.store(0);
		_delNodes.store(0);
		_newEdges.store(0);
		_delNewEdges.store(0);
		_delFrozenEdges.store(0);

		_vertices.clear();

#ifdef LL_WRITABLE_USE_MEMORY_POOL
		ll_free_w_pool();
#endif

		_deletions_out_map.clear();
		_deletions_in_map.clear();

		for (int i = 0; i < LL_D_STRIPES; i++) {
			_deletions_nodes_out[i].clear();
			_deletions_nodes_in[i].clear();
		}

		callback_ro_changed();


		// Check whether we ran out of the level ID space

		/*if (_ro_graph.num_levels() >= LL_MAX_LEVEL) {
			fprintf(stderr, "Error: Too many levels\n");
			abort();
		}*/
	}


	/**
	 * Delete a level
	 *
	 * @param level the level number
	 */
	void delete_level(size_t level) {

		_ro_graph.delete_level(level);
		callback_ro_changed();
	}


private:

	/*
	 * The read-only graph
	 */

	/// The read-only graph
	ll_mlcsr_ro_graph _ro_graph;


	/*
	 * The writable representation
	 */

	/// The adjacency lists
	ll_w_vt_vertices_t _vertices;

	/// Lock for creating new nodes
	ll_spinlock_t _new_node_lock;
	volatile node_t _next_new_node_id;

	/// The number of new and deleted nodes
	std::atomic<long> _newNodes;
	std::atomic<long> _delNodes;
	std::atomic<long> _newEdges;
	std::atomic<long> _delNewEdges;
	std::atomic<long> _delFrozenEdges;


	/*
	 * The deletions in the read-only graph
	 */

	/// The deletions adapters
	deletions_adapter_out _deletions_adapter_out;
	deletions_adapter_in _deletions_adapter_in;

	/// The deletions from the RO graph - map and a spinlock for out-edges
	std::unordered_map<edge_t, long> _deletions_out_map;
	ll_spinlock_t _deletions_out_lock;

	/// The deletions from the RO graph - map and a spinlock for in-edges
	std::unordered_map<edge_t, long> _deletions_in_map;
	ll_spinlock_t _deletions_in_lock;

	/// Affected nodes by the deletion of out-edges
	std::unordered_map<node_t, affected_node_by_edge_deletion_t> _deletions_nodes_out[LL_D_STRIPES];

	/// Affected nodes by the deletion of in-edges
	std::unordered_map<node_t, affected_node_by_edge_deletion_t> _deletions_nodes_in[LL_D_STRIPES];


	/*
	 * Properties
	 */

	/// The global property data-structures lock (don't care about performance)
	ll_spinlock_t _property_lock;


	/**
	 * Get a writable node, creating it if necessary, but not locking it
	 * 
	 * @param node the node
	 * @return the node structure
	 */
	inline w_node* writable_node(node_t node) {
		w_node* r = (w_node*) _vertices.get_or_allocate(node);
		return r;
	}


	/**
	 * Get and lock a node, creating it if necessary
	 * 
	 * @param node the node
	 * @return the node structure
	 */
	w_node* lock_node(node_t node) {
		w_node* r = (w_node*) _vertices.get_or_allocate(node);
		ll_spinlock_acquire(&r->wn_lock);

		// XXX Does this belong here? Certainly not if we have timestamps,
		// or if there is any way we need to initialize the node
		if (node >= _next_new_node_id) {
			ll_spinlock_acquire(&_new_node_lock);
			if (node >= _next_new_node_id) {
				_next_new_node_id = node + 1;
				_newNodes++;
			}
			ll_spinlock_release(&_new_node_lock);
		}

		return r;
	}


	/**
	 * Get and lock a node, creating it if necessary - but do not wait
	 * 
	 * @param node the node
	 * @return the node structure, or NULL if already locked
	 */
	w_node* try_lock_node(node_t node) {
		w_node* r = (w_node*) _vertices.get_or_allocate(node);
		return ll_spinlock_try_acquire(&r->wn_lock) ? r : NULL;
	}


	/**
	 * Get and lock two nodes, creating them if necessary, and locking the
	 * node with the lower ID first, thus avoiding deadlocks through enforcing
	 * proper lock ordering
	 * 
	 * @param node1 the node
	 * @param node2 the node
	 * @param p_node1 the pointer for the node structure
	 * @param p_node2 the pointer for the node structure
	 */
	void lock_nodes(node_t node1, node_t node2, w_node*& p_node1, w_node*& p_node2) {
		if (node1 == node2) {
			p_node1 = p_node2 = lock_node(node1);
		}
		else if (node1 < node2) {
			p_node1 = lock_node(node1);
			p_node2 = lock_node(node2);
		}
		else {
			p_node2 = lock_node(node2);
			p_node1 = lock_node(node1);
		}
	}


	/**
	 * Unlock a node
	 *
	 * @param node the node
	 */
	void release_node(w_node* node) {
		ll_spinlock_release(&node->wn_lock);
	}


	/**
	 * Unlock two nodes
	 *
	 * @param node1 node 1
	 * @param node2 node 2
	 */
	void release_nodes(w_node* node1, w_node* node2) {
		if (node1 == node2) {
			ll_spinlock_release(&node1->wn_lock);
		}
		else {
			ll_spinlock_release(&node1->wn_lock);
			ll_spinlock_release(&node2->wn_lock);
		}
	}
};


#endif /* LL_WRITABLE_GRAPH_H_ */
