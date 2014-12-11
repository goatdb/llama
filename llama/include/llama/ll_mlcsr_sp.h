/*
 * ll_mlcsr_sp.h
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


#ifndef LL_MLCSR_SP_H_
#define LL_MLCSR_SP_H_

#include "llama/ll_mem_array.h"
#include "llama/ll_edge_table.h"

#ifdef LL_PERSISTENCE
#include "llama/ll_persistent_storage.h"
#endif

#include "llama/ll_mlcsr_iterator.h"
#include "llama/ll_mlcsr_properties.h"



//==========================================================================//
// Counters                                                                 //
//==========================================================================//

#ifdef LL_COUNTERS
std::atomic<size_t> g_iter_begin;
std::atomic<size_t> g_iter_descend;
std::atomic<size_t> g_iter_next;

/**
 * Clear the counters
 */
void ll_clear_counters() {
	g_iter_begin = 0;
	g_iter_descend = 0;
	g_iter_next = 0;
}

/**
 * Print the counters
 *
 * @param f the output file
 * @param sep the separator
 */
void ll_print_counters(FILE* f = stderr, const char* sep = ":\t") {
	fprintf(f, "iter_begin%s%lu\n", sep, g_iter_begin.load());
	fprintf(f, "iter_descend%s%lu\n", sep, g_iter_descend.load());
	fprintf(f, "iter_next%s%lu\n", sep, g_iter_next.load());
}
#endif



//==========================================================================//
// The base class: ll_csr_base                                              //
//==========================================================================//

typedef void (*ll_copy_edge_callback_t)(edge_t source_edge, edge_t target_edge,
		void* user);


/**
 * The base class of the multi-level CSR variants: a few basic things that are not
 * performance critical.
 */
template <template <typename> class VT_TABLE, class VT_ELEMENT, typename T>
class ll_csr_base {

protected:

	/// The name
	std::string _name;

	/// The master copy (if this is a read-only clone)
	ll_csr_base<VT_TABLE, VT_ELEMENT, T>* _master;

	/// The maximum number of nodes (i.e. 1 + the maximum node ID)
	node_t _max_nodes;

	/// The maximum number of edges
	edge_t _max_edges;
	
#ifdef LL_MIN_LEVEL
	/// The minimum level to consider
	int _minLevel;
#endif

	/// The maximum level to consider
	int _maxLevel;

	/// The per-level number of nodes
	std::vector<node_t> _perLevelNodes;

	/// The per-level number of adjacency lists
	std::vector<node_t> _perLevelAdjLists;

	/// The per-level number of edges
	std::vector<edge_t> _perLevelEdges;

	/// The external deletions
	ll_mlcsr_external_deletions* _deletions;

	/// The vertex table for each level: node ID --> adjacency list begins
	LL_VT_COLLECTION<VT_TABLE<VT_ELEMENT>, VT_ELEMENT> _begin;
	VT_TABLE<VT_ELEMENT>* _latest_begin;

	/// The edge table for each level: edge ID --> the associated value
	std::vector<LL_ET<T>*> _values;
	LL_ET<T>* _latest_values;

	/// The edge translation property
	ll_mlcsr_edge_property<edge_t> _edge_translation;

	/// Is the edge translation feature on?
	bool _has_edge_translation;
	
#ifdef LL_DELETIONS
	/// The lock table
	ll_spinlock_table _lt;
#endif

	/// For use during initialization: The edge table write index
	size_t _et_write_index;

	/// For use during initialization: The callback to copy edges
	ll_copy_edge_callback_t _copy_edge_callback;

	/// For use during initialization: Caller-provided data for the callback
	void* _copy_edge_callback_data;

	/// The vertex IDs for the sparse column-store like representation
	std::vector<node_t*> _sparse_node_ids;

	/// The vertex data for the sparse column-store like representation
	std::vector<VT_ELEMENT*> _sparse_node_data;

	/// The length of the arrays for the sparse column-store like representation
	std::vector<size_t> _sparse_length;

	/// The memory pool for sparse node IDs
	ll_memory_pool_for_large_allocations* _pool_for_sparse_node_ids;

	/// The memory pool for sparse node data
	ll_memory_pool_for_large_allocations* _pool_for_sparse_node_data;


public:

	/**
	 * Create a new instance of class ll_csr_base
	 *
	 * @param storage the persistence context
	 * @param name the name of this data component (must be a valid filename prefix)
	 * @param edge_translation true to enable the edge translation map
	 */
	ll_csr_base(IF_LL_PERSISTENCE(ll_persistent_storage* storage,)
		const char* name, bool edge_translation = true) :
#ifdef LL_PERSISTENCE
		_begin(storage, name, "csr"),
#endif
		_edge_translation(IF_LL_PERSISTENCE(storage,) -1,
				(std::string(name) + std::string("-et")).c_str(), LL_T_INT64,
				NULL,
				reinterpret_cast
					<ll_mlcsr_edge_property_level_creation_callback<edge_t>*>
						(sizeof(edge_t) == sizeof(uint32_t)
						 ? (void*) edge_property_level_creation_callback_32()
						 : (void*) edge_property_level_creation_callback_64()))
	{
		_max_nodes = 0;
		_max_edges = 0;
		_name = name;
		_master = NULL;

		_deletions = NULL;

		_latest_begin = NULL;
		_latest_values = NULL;

		_pool_for_sparse_node_ids = NULL;
		_pool_for_sparse_node_data = NULL;

		_has_edge_translation = edge_translation;

#ifdef LL_MIN_LEVEL
		_minLevel = 0;
#endif

		_maxLevel = -1;

#ifdef LL_PERSISTENCE
		for (size_t l = 0; l < _begin.size(); l++) {
			VT_TABLE<VT_ELEMENT>& b = *_begin[l];

			_latest_begin = _begin[l];
			_perLevelNodes.push_back(b.size());
			_max_nodes = b.size();

			// TODO Update _perLevelAdjLists properly
			_perLevelAdjLists.push_back(_max_nodes);

			_values.push_back(NULL);

			_maxLevel = _values.size()-1;
			b.set_edge_table_ptr(&_values[_maxLevel]);
			_latest_values = _values[_maxLevel];

			size_t edges = b.edges();
			_perLevelEdges.push_back(edges);
			_max_edges = edges;

			_sparse_node_ids.push_back(NULL);
			_sparse_node_data.push_back(NULL);
			_sparse_length.push_back(0);
		}

		_has_edge_translation = _begin.size() == 0
			? edge_translation : _edge_translation.max_level_id() >= 0;
#endif
	}


	/**
	 * Create a read-only clone of ll_csr_base
	 *
	 * @param master the master object
	 * @param level the max level
	 */
	ll_csr_base(ll_csr_base<VT_TABLE, VT_ELEMENT, T>* master, int level)
		: _begin(&master->_begin, level),
		_edge_translation(&master->_edge_translation, level)
	{

		_master = master;

		_name = master->_name;
		_deletions = master->_deletions;
		_has_edge_translation = master->_has_edge_translation;

#ifdef LL_MIN_LEVEL
		_minLevel = master->_minLevel;
#endif

		_maxLevel = master->_maxLevel;

		_et_write_index = 0;
		_copy_edge_callback = NULL;
		_copy_edge_callback_data = NULL;

		_pool_for_sparse_node_ids = master->_pool_for_sparse_node_ids;
		_pool_for_sparse_node_data = master->_pool_for_sparse_node_data;

		if (master->num_levels() > 0) {

			// XXX Wrap
#ifdef LL_MLCSR_LEVEL_ID_WRAP
#	error "Not implemented"
#endif

			for (int i = 0; i <= level; i++) {
				_perLevelNodes.push_back(master->_perLevelNodes[i]);
				_perLevelAdjLists.push_back(master->_perLevelAdjLists[i]);
				_perLevelEdges.push_back(master->_perLevelEdges[i]);
				_values.push_back(master->_values[i]);

				_sparse_node_ids.push_back(master->_sparse_node_ids[i]);
				_sparse_node_data.push_back(master->_sparse_node_data[i]);
				_sparse_length.push_back(master->_sparse_length[i]);
			}

			_latest_begin = _begin.latest_level();
			_latest_values = _values[_values.size() - 1];

			_max_nodes = _perLevelNodes[level];
			_max_edges = _perLevelEdges[level];
		}
		else {

			_latest_begin = NULL;
			_latest_values = NULL;

			_max_nodes = 0;
			_max_edges = 0;
		}
	}


	/**
	 * Destroy the graph
	 */
	virtual ~ll_csr_base(void) {

		if (_master != NULL) return;
		
		for (size_t l = 0; l < _values.size(); l++) {
			if (_values[l] != NULL) DELETE_LL_ET<T>(_values[l]);
		}
		
		if (_pool_for_sparse_node_ids != NULL) {
			for (size_t l = 0; l < _sparse_node_ids.size(); l++) {
				if (_sparse_node_ids[l] != NULL)
					_pool_for_sparse_node_ids->free(_sparse_node_ids[l]);
			}
		}
		
		if (_pool_for_sparse_node_data != NULL) {
			for (size_t l = 0; l < _sparse_node_data.size(); l++) {
				if (_sparse_node_data[l] != NULL)
					_pool_for_sparse_node_data->free(_sparse_node_data[l]);
			}
		}
	}


	/**
	 * Get the name
	 *
	 * @return the name
	 */
	inline const char* name(void) const {
		return _name.c_str();
	}


	/**
	 * Get the appropriate _begin data structure corresponing to the given level
	 *
	 * @param level the level
	 * @return the data structure
	 */
	inline VT_TABLE<VT_ELEMENT>* vertex_table(size_t level) {
#ifdef FORCE_L0
		return _latest_begin;
#else
		return _begin[level];
#endif
	}


	/**
	 * Get the appropriate _begin data structure corresponing to the given level
	 *
	 * @param level the level
	 * @return the data structure
	 */
	inline const VT_TABLE<VT_ELEMENT>* vertex_table(size_t level) const {
#ifdef FORCE_L0
		return _latest_begin;
#else
		return _begin[level];
#endif
	}


	/**
	 * Get the appropriate _begin data structure prior to the given level
	 *
	 * @param level the level
	 * @return the data structure
	 */
	inline VT_TABLE<VT_ELEMENT>* prev_vertex_table(size_t level) {
		return _begin.prev_level(level);
	}


	/**
	 * Get the appropriate _begin data structure prior to the given level
	 *
	 * @param level the level
	 * @return the data structure
	 */
	inline const VT_TABLE<VT_ELEMENT>* prev_vertex_table(size_t level) const {
		return _begin.prev_level(level);
	}


	/**
	 * Get the appropriate _begin data structure corresponing to the given level
	 *
	 * @return the data structure
	 */
	inline VT_TABLE<VT_ELEMENT>* vertex_table() {
		return _latest_begin;
	}


	/**
	 * Get the appropriate _begin data structure corresponing to the given level
	 *
	 * @return the data structure
	 */
	inline const VT_TABLE<VT_ELEMENT>* vertex_table() const {
		return _latest_begin;
	}


	/**
	 * Get the appropriate _values data structure corresponing to the given level
	 *
	 * @param level the level
	 * @return the data structure
	 */
	inline LL_ET<T>* edge_table(size_t level) {
#ifdef FORCE_L0
		return _latest_values;
#else
		return _values[level];
#endif
	}


	/**
	 * Get the appropriate _values data structure corresponing to the given level
	 *
	 * @param level the level
	 * @return the data structure
	 */
	inline const LL_ET<T>* edge_table(size_t level) const {
#ifdef FORCE_L0
		return _latest_values;
#else
		return _values[level];
#endif
	}


	/**
	 * Get the appropriate _values data structure corresponing to the given level
	 *
	 * @return the data structure
	 */
	inline LL_ET<T>* edge_table() {
		return _latest_values;
	}


	/**
	 * Get the appropriate _values data structure corresponing to the given level
	 *
	 * @param level the level
	 * @return the data structure
	 */
	inline const LL_ET<T>* edge_table() const {
		return _latest_values;
	}


	/**
	 * Calculate the max number of elements in the edge table array
	 *
	 * @return the number of elements
	 */
	size_t edge_table_length(size_t level) const {
		return values_length(level, max_nodes(level), _perLevelAdjLists[level],
				max_edges(level));
	}
	

#ifdef LL_MIN_LEVEL

	/**
	 * Get the min level
	 *
	 * @return the minimum level to consider
	 */
	inline int min_level() const {
		return _minLevel;
	}


	/**
	 * Set the minimum level to consider
	 *
	 * @param m the minimum level
	 * @param streaming_weights the streaming weights
	 * @param forward_pointers the forward pointers for streaming
	 */
	virtual void set_min_level(size_t m,
			ll_mlcsr_edge_property<uint32_t>* streaming_weights = NULL,
			ll_mlcsr_edge_property<edge_t>* forward_pointers = NULL) {

		assert((int) _minLevel <= (int) m);
		_minLevel = m;

		_begin.set_min_level(_minLevel);
	}

#endif


	/**
	 * Get the max level
	 *
	 * @return the minimum level to consider
	 */
	inline int max_level() const {
		return _maxLevel;
	}
	

	/**
	 * Set the deletions checker
	 *
	 * @param deletions the deletions checker
	 */
	void set_deletions_checker(ll_mlcsr_external_deletions* deletions) {
		_deletions = deletions;
	}


	/**
	 * Return the in-memory size
	 *
	 * @return the number of bytes occupied by this instance
	 */
	virtual size_t in_memory_size() {

		size_t vt_size = 0;
		for (size_t i = 0; i < _begin.size(); i++) {
			if (_begin[i] != NULL) {
				vt_size += _begin[i]->in_memory_size();
			}
		}
		vt_size += _begin.capacity() * sizeof(VT_TABLE<VT_ELEMENT>*);

		size_t values_size = 0;
		for (size_t i = 0; i < _values.size(); i++) {
			values_size += sizeof(T) * edge_table_length(i);
		}
		values_size += _values.capacity() * sizeof(T*);

		return sizeof(*this)
			+ /* the vertex table */	vt_size
			+ /* the values array */	values_size
			+ /* vector           */	_perLevelNodes.capacity() * sizeof(node_t)
			+ /* vector           */	_perLevelAdjLists.capacity() * sizeof(node_t)
			+ /* vector           */	_perLevelEdges.capacity() * sizeof(edge_t);
	}


	/**
	 * Return the number of levels
	 *
	 * @return the number of levels
	 */
    inline size_t num_levels() const {
        return _begin.size();
    }


	/**
	 * Return the maximum number of nodes (i.e. 1 + the maximum node ID)
	 *
	 * @return the maximum number of nodes
	 */
    inline node_t max_nodes() const {
        return _max_nodes;
    }


	/**
	 * Return the number of nodes in the given level
	 *
	 * @param level the level number
	 * @return the number of nodes
	 */
    inline node_t max_nodes(int level) const {
        return _perLevelNodes[level];
    }


	/**
	 * Return the number of edges in the given level (i.e. 1 + the maximum node ID)
	 *
	 * @param level the level number
	 * @return the number of edges
	 */
    inline edge_t max_edges(int level) const {
        return _perLevelEdges[level];
    }


	/**
	 * Initialize a level
	 *
	 * @param max_nodes the max number of nodes, cummulative up until this level
	 * @param max_adj_lists the max number of adj. lists in the level
	 * @param max_edges the max number of edges within the level
	 * @param copy_edge_callback the callback to copy edges
	 * @param copy_edge_callback_data the data for the callback
	 * @param level the new level number (optional; use -1 to specify the latest level)
	 */
	virtual void init_level(size_t max_nodes, size_t max_adj_lists,
			size_t max_edges,
			ll_copy_edge_callback_t copy_edge_callback = NULL,
			void* copy_edge_callback_data = NULL,
			int level=-1) {


		// Initialize

		this->_copy_edge_callback = copy_edge_callback;
		this->_copy_edge_callback_data = copy_edge_callback_data;
		this->_et_write_index = 0;

#ifdef _DEBUG
		if (level != -1) {
			LL_W_PRINT("Creating a new level with a prespecified ID %d\n",
					level);
		}
#endif


		// Get the new level ID and create space for it in the vectors

		if (level == -1) level = this->_begin.next_level_id();

		while ((ssize_t) this->_values.size() <= level)
			this->_values.push_back(NULL);
		while ((ssize_t) this->_perLevelNodes.size() <= level)
			this->_perLevelNodes.push_back(0);
		while ((ssize_t) this->_perLevelAdjLists.size() <= level)
			this->_perLevelAdjLists.push_back(0);
		while ((ssize_t) this->_perLevelEdges.size() <= level)
			this->_perLevelEdges.push_back(0);
		while ((ssize_t) this->_sparse_node_ids.size() <= level)
			this->_sparse_node_ids.push_back(NULL);
		while ((ssize_t) this->_sparse_node_data.size() <= level)
			this->_sparse_node_data.push_back(NULL);
		while ((ssize_t) this->_sparse_length.size() <= level)
			this->_sparse_length.push_back(0);

		assert(this->_values[level] == NULL);
		assert(this->_begin.max_level() == this->_maxLevel);

		this->_maxLevel = level;


		// Create the edge table

		size_t et_capacity = values_length(level, max_nodes + 4,
				max_adj_lists + 4, max_edges);
		auto et = NEW_LL_ET<T>(et_capacity, max_nodes);
#ifndef LL_PERSISTENCE
		if (et == NULL) {
			LL_E_PRINT("** out of memory ** cannot allocate the edge table\n");
			abort();
		}
#endif

		this->_values[level] = et;


		// Create and begin initialization of the vertex table. Note that this
		// would modify this->_values[level] under LL_PERSISTENCE

		auto* b = this->_begin.new_level(max_nodes);
#ifdef LL_PERSISTENCE
		if (this->_begin.count_existing_levels() == 1)
			b->dense_init(&this->_values[level], et_capacity);
		else
			b->cow_init(&this->_values[level], et_capacity);
#else
		if (this->_begin.count_existing_levels() == 1)
			b->dense_init();
		else
			b->cow_init();
#endif

		this->_latest_begin = this->_begin.latest_level();
		this->_latest_values = et = this->_values[level];
		

		// Remember the important parameters

		this->_perLevelNodes[level] = max_nodes;
		this->_perLevelAdjLists[level] = max_adj_lists;
		this->_perLevelEdges[level] = max_edges;

		this->_max_nodes = max_nodes;
		this->_max_edges = max_edges;
	}


	/**
	 * Restart an initialized but unfinished level. This does not undo any
	 * writes, but it just resets the internal initialization state.
	 *
	 * Use this only if you know what you are doing.
	 */
	virtual void restart_init_level() {
		this->_et_write_index = 0;
	}


	/**
	 * Get the write index into the edge table.
	 *
	 * Use this only if you know what you are doing.
	 */
	size_t et_write_index() {
		return this->_et_write_index;
	}


	/**
	 * Initialize a level from an array of node degrees. This creates a fully
	 * initialized vertex table and a partially initialized edge table.
	 *
	 * This method should be a simple wrapper around init_level(), init_node(),
	 * and finish_level_vertices(). After using this method, make sure to write
	 * all edges and then call finish_level_edges() when finished.
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
			void* copy_edge_callback_data = NULL) = 0;


	/**
	 * Write a vertex with one edge
	 *
	 * @param node the node
	 * @param index the index within the node's adjacency list
	 * @param value the value
	 * @return the edge that was just written
	 */
	virtual edge_t write_value(node_t node, size_t index, const T& value) = 0;


	/**
	 * Write a vertex with all of its edges
	 *
	 * @param node the node
	 * @param adj_list the adjacency list
	 */
	virtual void write_values(node_t node, const std::vector<T>& adj_list) {
		for (size_t i = 0; i < adj_list.size(); i++) {
			write_value(node, i, adj_list[i]);
		}
	}


	/**
	 * Finish the vertices part of the level (use only for levels created
	 * directly using init_level, not init_level_from_degrees).
	 */
	virtual void finish_level_vertices() {

		int level = this->_begin.size() - 1;
		auto* vt = this->_begin[level];

		assert(this->_latest_values == this->_values[this->_maxLevel]);

		VT_ELEMENT e;
		memset(&e, 0, sizeof(e));
		e.adj_list_start = LL_EDGE_CREATE(level, this->_et_write_index);

		if (level >= 1) {
			vt->cow_write(this->_max_nodes, e);
			vt->cow_finish();
		}
		else {
			vt->dense_direct_write(this->_max_nodes, e);
			vt->dense_finish();
		}

		this->_perLevelEdges[this->_maxLevel] = this->_et_write_index;
		this->_max_edges = this->_et_write_index;
	}


	/**
	 * Finish the edges part of the level
	 */
	virtual void finish_level_edges() {
		
		assert(this->_latest_begin == this->_begin.latest_level());

#ifdef LL_PERSISTENCE
		this->_latest_begin->finish_level_edges();
		this->_latest_values = this->_values[this->_values.size() - 1];
#endif
	}


	/**
	 * Update the max-level part of the payload associated with the given edge
	 *
	 * @param edge the edge
	 * @param mlevel the new value of max-level
	 */
	void update_max_visible_level(edge_t edge, int mlevel) {
#ifdef LL_DELETIONS
		T& v = (*this->edge_table(LL_EDGE_LEVEL(edge)))[LL_EDGE_INDEX(edge)];
		v = LL_VALUE_CREATE_EXT(
				LL_VALUE_PAYLOAD(v),
				mlevel);
#endif
	}


	/**
	 * Update the max-level part of the payload associated with the given edge,
	 * only lowering it, but never raising it
	 *
	 * @param edge the edge
	 * @param mlevel the new value of max-level
	 * @return true if the value was lowered
	 */
	bool update_max_visible_level_lower_only(edge_t edge, int mlevel) {
		bool r = false;
#ifdef LL_DELETIONS
		_lt.acquire_for(edge);
		T& v = (*this->edge_table(LL_EDGE_LEVEL(edge)))[LL_EDGE_INDEX(edge)];
		if (mlevel < (int) LL_VALUE_MAX_LEVEL(v)) {
			r = true;
			v = LL_VALUE_CREATE_EXT(
					LL_VALUE_PAYLOAD(v),
					mlevel);
		}
		_lt.release_for(edge);
#endif
		return r;
	}


	/**
	 * Determine if the given node exists in the latest level
	 *
	 * @param node the node 
	 * @return true if it exists
	 */
	inline bool node_exists(node_t node) const {

#ifdef LL_CHECK_NODE_FASTER
		if (node >= max_nodes()) return false;
#else
		if (node < 0 || node >= max_nodes()) return false;
#endif

		// TODO Implement a saner check for the deleted nodes

#ifdef LL_SLCSR
		size_t degree = (*this->_latest_begin)[node+1].adj_list_start
			- (*this->_latest_begin)[node].adj_list_start;
		if (degree == 0) return false;
#else
		const VT_ELEMENT& b = (*this->_latest_begin)[node];
		if (b.degree == 0) {
			LL_D_NODE_PRINT(node, "Does not exist; level=%d, degree=%lu, "
					"level_length=%lu, max_nodes=%lu, begin=%p\n",
					(int) this->_latest_begin->level(),
					(size_t) b.degree, (size_t) b.level_length,
					(size_t) max_nodes(), this->_latest_begin);
			return false;
		}
#endif

		return true;
	}


	/**
	 * Determine if the given edge exists in the latest level
	 *
	 * @param edge the edge 
	 * @param level the effective level
	 * @return true if it exists
	 */
	inline bool edge_exists(edge_t edge, int level) const {

		// Note that this has a bit different semantics than is_edge_deleted, especially
		// when it comes to invalid edge IDs.

		if (edge < 0) return false;
		if (LL_EDGE_INDEX(edge)
				>= max_edges(LL_EDGE_LEVEL(edge))) return false;

#ifndef LL_DELETIONS
		return true;
#else
		T& value = this->values(LL_EDGE_LEVEL(edge))[LL_EDGE_INDEX(edge)];
		if (LL_VALUE_IS_DELETED(value, level)) {
#	ifdef LL_TIMESTAMPS
			if (LL_VALUE_MAX_LEVEL(value) == num_levels() && _deletions != NULL) {
				// We might get here even if the edge was deleted BEFORE the writable
				// level, but we don't care - the result will be correct nonetheless
				return _deletions->is_edge_deleted(iter.edge);
			}
#	else
			return false;
#	endif
		}
		return true;
#endif
	}


	/**
	 * Create an iterator right at the end
	 *
	 * @param iter the iterator
	 * @param n the node
	 * @return the iterator
	 */
	void iter_set_to_end(ll_edge_iterator& iter) const {

		iter.owner = LL_I_OWNER_RO_CSR;
		iter.node = LL_NIL_NODE;
		iter.edge = LL_NIL_EDGE;
		iter.left = 0;
	}


	/**
	 * Delete a level. The ID of the level to be deleted cannot be more than
	 * the minimum effective level (as set by set_min_level()) minus TWO, not
	 * minus one due to the fact of how iter_descend() works. This is arguably
	 * a bug and should be fixed.
	 *
	 * @param level the level number
	 */
	void delete_level(size_t level) {

		LL_D_PRINT("[%s] level=%lu\n", this->name(), level);

		assert(level >= 0);
#ifdef LL_MIN_LEVEL
		assert(level + 1 < (size_t) _minLevel);
#endif

		if (this->_begin.level_exists(level)) {		// Do we need this?
			this->_begin.delete_level(level);
		}

		if (level < this->_values.size() && this->_values[level] != NULL) {
			DELETE_LL_ET<T>(this->_values[level]);
			this->_values[level] = NULL;
		}

		if (level < this->_perLevelNodes.size()) {
			this->_perLevelNodes[level] = 0;
			this->_perLevelAdjLists[level] = 0;
			this->_perLevelEdges[level] = 0;
		}

		if (level < this->_sparse_node_ids.size()) {

			if (this->_sparse_node_ids[level] != NULL)
				_pool_for_sparse_node_ids->free(_sparse_node_ids[level]);
				//free(this->_sparse_node_ids[level]);

			if (this->_sparse_node_data[level] != NULL)
				_pool_for_sparse_node_data->free(_sparse_node_data[level]);
				//free(this->_sparse_node_data[level]);

			this->_sparse_node_ids[level] = NULL;
			this->_sparse_node_data[level] = NULL;
			this->_sparse_length[level] = 0;
		}

		if (_edge_translation.level_exists(level)) {
			_edge_translation.delete_level(level);
		}
	}


	/**
	 * Delete all old versions except the specified number of most recent levels
	 *
	 * @param keep the number of levels to keep
	 */
	void keep_only_recent_versions(size_t keep) {

		// We can drop old vertex tables only if we know for sure they would
		// not be needed to access the most recent versions of the data

#if defined(LL_MLCSR_CONTINUATIONS)

#ifdef LL_MIN_LEVEL
		// We need to keep the sparse representations for set_min_level() to
		// work properly
		for (int l = 0; l <= ((int) max_level()) - (int) keep; l++) {
			this->create_sparse_representation(l);
		}
#endif

		this->_begin.keep_only_recent_levels(keep);
#endif

		this->_edge_translation.keep_only_recent_levels(keep);
	}


	/**
	 * Set memory pools for sparse data
	 *
	 * @param pool_ids the pool for the IDs
	 * @param pool_data the pool for the data
	 */
	void set_memory_pools_for_sparse_representaion(
			ll_memory_pool_for_large_allocations* pool_ids,
			ll_memory_pool_for_large_allocations* pool_data) {

		_pool_for_sparse_node_ids = pool_ids;
		_pool_for_sparse_node_data = pool_data;
	}


	/**
	 * Generate the sparse vertex table representation for the given level
	 *
	 * @param level the level
	 */
	void create_sparse_representation(int level) {

		if (!this->_begin.level_exists(level)) return;
		if (this->_sparse_node_ids[level] != NULL) return;

		auto vt = vertex_table(level);

		size_t length = 0;
		ssize_t size = vt->size();
		ssize_t pages = vt->pages();

		size_t nt = omp_get_max_threads();
		size_t lengths[nt];

#		pragma omp parallel
		{
			size_t l = 0;
			ssize_t t = omp_get_thread_num();
			ssize_t ts = ((pages-1) * t) / nt;
			ssize_t te = std::min<ssize_t>((ssize_t) pages-1,
					(ssize_t) ((pages-1) * (t+1)) / nt);

			for (ssize_t p = ts; p < te; p++) {
				VT_ELEMENT* page = vt->page(p);
				for (ssize_t i = 0; i < LL_ENTRIES_PER_PAGE; i++) {
					if ((int) LL_EDGE_LEVEL(page[i].adj_list_start) == level) {
						l++;
					}
				}
			}

			lengths[t] = l;
			ATOMIC_ADD<size_t>(&length, l);
		}

		size_t tail_index = length;
		if (pages > 0) {
			VT_ELEMENT* page = vt->page(pages-1);
			for (ssize_t i = 0; i < size - pages * LL_ENTRIES_PER_PAGE; i++) {
				if ((int) LL_EDGE_LEVEL(page[i].adj_list_start) == level) {
					length++;
				}
			}
		}

		/*node_t* ids = (node_t*) malloc(sizeof(node_t) * length);
		VT_ELEMENT* data = (VT_ELEMENT*) malloc(sizeof(VT_ELEMENT) * length);
		if (ids == NULL || data == NULL) {
			LL_E_PRINT("** out of memory ** need space for %lu vertives\n", length);
			abort();
		}*/

		node_t* ids = (node_t*) _pool_for_sparse_node_ids->allocate(
				sizeof(node_t) * length);
		VT_ELEMENT* data = (VT_ELEMENT*) _pool_for_sparse_node_data->allocate(
				sizeof(VT_ELEMENT) * length);

#		pragma omp parallel
		{
			ssize_t t = omp_get_thread_num();
			ssize_t ts = ((pages-1) * t) / nt;
			ssize_t te = std::min<ssize_t>((ssize_t) pages-1,
					(ssize_t) ((pages-1) * (t+1)) / nt);

			size_t index = 0;
			for (ssize_t i = 0; i < t; i++) index += lengths[i];

			for (ssize_t p = ts; p < te; p++) {
				VT_ELEMENT* page = vt->page(p);
				node_t n = p << LL_ENTRIES_PER_PAGE_BITS;
				for (ssize_t i = 0; i < LL_ENTRIES_PER_PAGE; i++, n++) {
					if ((int) LL_EDGE_LEVEL(page[i].adj_list_start) == level) {
						ids[index] = n;
						data[index] = page[i];
						index++;
					}
				}
			}
		}
		if (pages > 0) {
			VT_ELEMENT* page = vt->page(pages-1);
			node_t n = (pages-1) << LL_ENTRIES_PER_PAGE_BITS;
			for (ssize_t i = 0; i < size - pages * LL_ENTRIES_PER_PAGE; i++, n++) {
				if ((int) LL_EDGE_LEVEL(page[i].adj_list_start) == level) {
					ids[tail_index] = n;
					data[tail_index] = page[i];
					tail_index++;
				}
			}
		}

		this->_sparse_length[level] = length;
		this->_sparse_node_ids[level] = ids;
		this->_sparse_node_data[level] = data;
	}


	/**
	 * Determine if the given level has a sparse representation
	 *
	 * @param level the level
	 * @return true if it has the sparse representation
	 */
	bool has_sparse_representation(int level) const {
		if (level < 0 || level >= (int) this->_sparse_node_ids.size()) return false;
		return this->_sparse_node_ids[level] != NULL;
	}


	/**
	 * Get the length associated with the given sparse representation
	 *
	 * @param level the level
	 * @return the length
	 */
	size_t sparse_length(int level) const {
		return this->_sparse_length[level];
	}


	/**
	 * Get the node IDs associated with the given sparse representation
	 *
	 * @param level the level
	 * @return the corresponding column array
	 */
	const node_t* sparse_node_ids(int level) const {
		return this->_sparse_node_ids[level];
	}


	/**
	 * Get the node data associated with the given sparse representation
	 *
	 * @param level the level
	 * @return the corresponding column array
	 */
	const VT_ELEMENT* sparse_node_data(int level) const {
		return this->_sparse_node_data[level];
	}


	/**
	 * Get the corresponding edge property level creation callback for 32-bit
	 * properties
	 *
	 * @return the callback if there is one; otherwise return NULL
	 */
	virtual ll_mlcsr_edge_property_level_creation_callback<uint32_t>*
		edge_property_level_creation_callback_32() { return NULL; }


	/**
	 * Get the corresponding edge property level creation callback for 64-bit
	 * properties
	 *
	 * @return the callback if there is one; otherwise return NULL
	 */
	virtual ll_mlcsr_edge_property_level_creation_callback<uint64_t>*
		edge_property_level_creation_callback_64() { return NULL; }


	/**
	 * Do we have the edge translation feature? Return true if it is enabled,
	 * even if it might not be necessarily quite up to date, such as if more
	 * levels have been added to the out-edges of the graph without calling
	 * ll_mlcsr_ro_graph::make_reverse_edges().
	 *
	 * @return true if it is enabled
	 */
	inline bool has_edge_translation() const {
		return _has_edge_translation;
	}


	/**
	 * Enable or disable the edge translation feature, but do not initialize it
	 * or clean it up -- just set the internal flags accordingly.
	 *
	 * @param v true to enable, false to disable
	 */
	inline void set_edge_translation(bool v) {
		_has_edge_translation = v;
	}


	/**
	 * Translate the edge ID using the stored edge translation map.
	 *
	 * The behavior is undefined if the edge translation map is not present.
	 *
	 * @param edge the edge ID
	 * @return the translated edge ID
	 */
	edge_t translate_edge(edge_t edge) const {
		return _edge_translation[edge];
	}


	/**
	 * Get the edge translation property
	 *
	 * The behavior is undefined if the edge translation map is not present.
	 *
	 * @param edge the edge ID
	 * @param translation the edge ID translation
	 */
	inline ll_mlcsr_edge_property<node_t>& edge_translation() {
		return _edge_translation;
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
		return max_edges;
	}


	/**
	 * Determine if the given edge pointed to by the iterator has already been
	 * deleted.
	 *
	 * @param iter the iterator
	 * @return true if it has already been deleted
	 */
	inline bool is_edge_deleted(ll_edge_iterator& iter) const {
#ifndef LL_DELETIONS
		return false;
#else
		if (iter.edge == LL_NIL_EDGE) return false;
		const T& value = this->edge_table(LL_EDGE_LEVEL(iter.edge))
			->edge_value(iter.node, LL_EDGE_INDEX(iter.edge));
		if (LL_VALUE_IS_DELETED(value, iter.max_level)) {
#	ifdef LL_TIMESTAMPS
			if (LL_VALUE_MAX_LEVEL(value) == num_levels() && _deletions != NULL) {
				// We might get here even if the edge was deleted BEFORE the writable
				// level, but we don't care - the result will be correct nonetheless
				return _deletions->is_edge_deleted(iter.edge);
			}
#	else
			return true;
#	endif
		}
		return false;
#endif
	}
};



//==========================================================================//
// The base class: ll_mlcsr_core                                            //
//==========================================================================//

/**
 * The basic multilevel CSR design: length in the vertex table
 */
class ll_mlcsr_core
	: public ll_csr_base<LL_VT, ll_mlcsr_core__begin_t, node_t> {

	typedef node_t T;


public:

	/**
	 * Create a new instance of class ll_mlcsr_core
	 *
	 * @param storage the persistence context
	 * @param name the name of this data component (must be a valid filename prefix)
	 */
	ll_mlcsr_core(IF_LL_PERSISTENCE(ll_persistent_storage* storage,)
			const char* name)
		: ll_csr_base<LL_VT, ll_mlcsr_core__begin_t, node_t>
		  	(IF_LL_PERSISTENCE(storage,) name),
		_edge_property_level_initializer_32(*this),
		_edge_property_level_initializer_64(*this)
	{
	}


	/**
	 * Create a read-only clone of ll_mlcsr_core
	 *
	 * @param master the master object
	 * @param level the max level
	 */
	ll_mlcsr_core(ll_mlcsr_core* master, int level)
		: ll_csr_base<LL_VT, ll_mlcsr_core__begin_t, node_t>(master, level),
		_edge_property_level_initializer_32(*this),
		_edge_property_level_initializer_64(*this)
	{
	}


	/**
	 * Destroy the graph
	 */
	virtual ~ll_mlcsr_core() {
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

		// XXX level comparisons do not work with LL_MLCSR_LEVEL_ID_WRAP

		int level = this->_begin.size() - 1;
		auto* vt = this->_begin[level];

		ll_mlcsr_core__begin_t e;
		memset(&e, 0, sizeof(e));

		LL_D_NODE_PRINT(node, "[%s] Init %ld: level=%d new_edges=%lu "
				"deleted_edges=%lu\n", this->name(), node, level, new_edges,
				deleted_edges);


		// Create a NULL node if all of the following conditions are met:
		//   * The level is not 0
		//   * There are no new edges
		//   * There are no deleted edges
		//   * This node does not exist in the previous level

		if (!(level == 0 || new_edges > 0 || deleted_edges > 0)) {
			// Do not need if the pages are zeroed
			if (node >= (node_t) this->vertex_table(level-1)->size()) {
				memset(&e, 0, sizeof(e));
				e.adj_list_start = LL_NIL_EDGE;
				vt->cow_write(node, e);
			}
			return this->_et_write_index;
		}


		// Initialize the node struct:
		//   * If there are new edges or the level is 0, initialize the node as
		//     normal: Set the adjacency list length to new_edges
		//   * If there are no new edges for levels > 0, it either means that
		//     there are some deleted edges or that node does not exist in the
		//     previous level. Fetch the node from the previous level if it
		//     exists, or initialize the struct as for a NULL node if it does not

		if (level == 0 || new_edges > 0) {
			memset(&e, 0, sizeof(e));
			e.adj_list_start = new_edges == 0
				? LL_NIL_EDGE : LL_EDGE_CREATE(level, this->_et_write_index);
			e.level_length = new_edges;
		}
		else /* if (level > 0 && new_edges == 0) */ {
			if (node >= (node_t) this->vertex_table(level-1)->size()) {
				// Does this actually happen?
				memset(&e, 0, sizeof(e));
				e.adj_list_start = LL_NIL_EDGE;
			}
			else {
				const ll_mlcsr_core__begin_t& bprev
					= (*this->vertex_table(level-1))[node];
				e = bprev;
			}
		}


		// Update the precomputed node degree:
		//   * Degree = previous degree + new_edges - deleted_edges
		//   * If the degree drops to 0, set the node struct to the NULL node

#ifdef LL_PRECOMPUTED_DEGREE
		e.degree = new_edges - deleted_edges;

		if (level > 0 && node < (node_t) this->vertex_table(level-1)->size()) {
			const ll_mlcsr_core__begin_t& bprev
				= (*this->vertex_table(level-1))[node];

			LL_D_NODE_PRINT(node, "[%s] Prev %ld: "
					"[Start=%lx Length=%d Degree=%d]\n",
					this->name(), node, bprev.adj_list_start,
					bprev.level_length, (signed) bprev.degree);

			if (bprev.adj_list_start != LL_NIL_EDGE) {
				e.degree = e.degree + bprev.degree;
			}
		}

		assert((signed) e.degree >= 0);
		if (e.degree == 0) {
			e.adj_list_start = LL_NIL_EDGE;
			e.level_length = 0;
		}
#endif


		// Write the continuation record marking where the adjacency list
		// continues for levels > 0 if the following conditions are met:
		//   * LL_MLCSR_CONTINUATIONS is enabled
		//   * If this is not the very first level (Level > 0 in most cases)
		//   * The adjacency list resides in this level
		//   * The adjacency list is not copied
		// If the adjacency list is copied, write the NULL record.

		size_t delta_edges = new_edges;

#ifdef LL_MLCSR_CONTINUATIONS
		if (IFE_LL_MLCSR_LEVEL_ID_WRAP(this->_begin.has_prev_level(level), level > 0)
				&& e.adj_list_start != LL_NIL_EDGE) {
			size_t t = this->_et_write_index + delta_edges;
			T* ptr = this->_latest_values->edge_ptr(node, t);
			LL_XD_PRINT("%4ld) e=%lu wp=%lu, no copy\n", node,
					(size_t) new_edges, t);
			auto prev = this->_begin.prev_level(level);
			if (node < (node_t) prev->size()) {
				const ll_mlcsr_core__begin_t& bprev = (*prev)[node];
				*((ll_mlcsr_core__begin_t*) (void*) ptr) = bprev;
			}
			else {
				ll_mlcsr_core__begin_t x;
				memset(&x, 0, sizeof(x));
				x.adj_list_start = LL_NIL_EDGE;
				*((ll_mlcsr_core__begin_t*) (void*) ptr) = x;
			}
			LL_D_NODE_PRINT(node, "[%s] Init %ld: Write continuation to p=%p, "
					"et_index=%ld\n",
					this->name(), node, ptr, t);
			delta_edges += sizeof(ll_mlcsr_core__begin_t) / sizeof(T);
			if (sizeof(ll_mlcsr_core__begin_t) % sizeof(T) != 0) delta_edges++;
		}
#endif


		// Print some debugging information (if enabled):
		//   * Print the information about the node init if successful
		//   * Print a special note for the case of initing a NULL node

#ifdef D_DEBUG_NODE
		if ((signed long) e.adj_list_start != -1) { 
			LL_D_NODE_PRINT(node, "[%s] Init %ld: "
					"[L=%d I=%llx Len=%d] level=%d "
					"delta_edges=%d deleted_edges=%d p=%p\n",
					this->name(), node,
					(int) LL_EDGE_LEVEL(e.adj_list_start),
					LL_EDGE_INDEX(e.adj_list_start), (int) e.level_length,
					(int) level, (int) delta_edges, (int) deleted_edges,
					&(*this->_latest_values)[LL_EDGE_INDEX(e.adj_list_start)]);
			/*if (node == D_DEBUG_NODE) {
				T* ptr = this->_latest_values->edge_ptr(node, this->_et_write_index);
				for (size_t i = 0; i < 24; i += 4) {
					fprintf(stderr, "%p: %016lx %016lx %016lx %016lx\n",
							(((T*) ptr) + i + 0),
							*(((T*) ptr) + i + 0),
							*(((T*) ptr) + i + 1),
							*(((T*) ptr) + i + 2),
							*(((T*) ptr) + i + 3));
				}
			}*/
		}
		else {
			LL_D_NODE_PRINT(node, "[%s] Init %ld: [L=- I=%lx Len=%d]\n",
					this->name(), node,
					e.adj_list_start, e.level_length);
		}
#endif


		// Finish writing the new node:
		//   * Write the node struct to the vertex table
		//   * Advance the edge table write index

		if (level >= 1) {
			vt->cow_write(node, e);
		}
		else {
			vt->dense_direct_write(node, e);
		}

		size_t start_et_write_index = this->_et_write_index;
		this->_et_write_index += delta_edges;

		return start_et_write_index;
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


		// Calculate max_edges from the provided degree counts for new and
		// deleted edges

		size_t max_edges = 0;
		size_t max_adj_lists = 0;

		for (size_t i = 0; i < max_nodes; i++) {
			max_edges += new_edge_counts[i];
			if (new_edge_counts[i] > 0) max_adj_lists++;
		}


		// Print some debugging info and initialize the level

		LL_D_PRINT("[%s] level=%d max_nodes=%lu max_edges=%lu "
				"max_adj_lists=%lu deleted_edge_counts=%s\n",
				this->name(), (int) this->_begin.size(), max_nodes,
				max_edges, max_adj_lists,
				deleted_edge_counts != NULL ? "available" : "N/A");

		this->init_level(max_nodes, max_adj_lists, max_edges,
				copy_edge_callback, copy_edge_callback_data);

		int level = this->_begin.size() - 1;


		// Initialize each node

		for (size_t node = 0; node < max_nodes; node++) {
			size_t deleted = level == 0 || deleted_edge_counts == NULL
				? 0 : deleted_edge_counts[node];
			init_node(node, new_edge_counts[node], deleted);
		}


		// Finish the vertices

		this->finish_level_vertices();
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
		edge_t edge = (*this->_latest_begin)[node].adj_list_start;
		assert(edge != LL_NIL_EDGE);

		size_t start = LL_EDGE_INDEX(edge);
		this->_latest_values->edge_value(node, start + index)
			= LL_VALUE_CREATE(value);
		return edge + index;
	}


	/**
	 * Write a vertex with all of its edges
	 *
	 * @param node the node
	 * @param adj_list the adjacency list
	 */
	virtual void write_values(node_t node, const std::vector<T>& adj_list) {
		size_t start = LL_EDGE_INDEX((*this->_latest_begin)[node].adj_list_start);
		T* p = &this->_latest_values->edge_value(node, start);
		for (size_t i = 0; i < adj_list.size(); i++) {
			*(p++) = LL_VALUE_CREATE(adj_list[i]);
		}
	}


	/**
	 * Write a vertex with all of its edges
	 *
	 * @param node the node
	 * @param adj_list the adjacency list
	 * @param forward_pointers the forward pointers for streaming
	 */
	virtual void write_values(node_t node, const ll_w_out_edges_t& adj_list,
			ll_mlcsr_edge_property<edge_t>* forward_pointers = NULL) {

		size_t start = LL_EDGE_INDEX((*this->_latest_begin)[node].adj_list_start);
		size_t level = LL_EDGE_LEVEL((*this->_latest_begin)[node].adj_list_start);

		//for (size_t i = 0; i < adj_list.size(); i++) {
		//	w_edge* e = adj_list[i];

		size_t n = adj_list.block_count();
		for (size_t b = 0; b < n; b++) {
			w_edge* const* l = adj_list.block(b);
			size_t m = adj_list.block_size(b);
			for (size_t i = 0; i < m; i++, l++) {
				w_edge* e = *l;

				if (e->exists()) {
					this->_latest_values->edge_value(node, start)
						= LL_VALUE_CREATE(e->we_target);
					e->we_numerical_id = LL_EDGE_CREATE(level, start);
					start++;

#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES
					if (e->we_supersedes != LL_NIL_EDGE) {
						assert(LL_EDGE_LEVEL(e->we_supersedes) < level);

					forward_pointers->cow_write(e->we_supersedes,
							e->we_numerical_id);

						//LL_D_PRINT("%lx --> %lx\n", e->we_supersedes,
								//e->we_numerical_id);
					}
#endif
				}
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

		size_t n = adj_list.block_count();
		for (size_t b = 0; b < n; b++) {
			w_edge* const* l = adj_list.block(b);
			size_t m = adj_list.block_size(b);
			for (size_t i = 0; i < m; i++, l++) {
				w_edge* e = *l;

				if (e->exists()) {
					this->_latest_values->edge_value(node, start)
						= LL_VALUE_CREATE(e->we_source);
					e->we_reverse_numerical_id = LL_EDGE_CREATE(level, start);
					start++;
				}
			}
		}
	}


#ifdef LL_MIN_LEVEL

	/**
	 * Get the minimum level to consider
	 *
	 * @return the minimum level to consider
	 */
	inline int min_level() const {
		return this->_minLevel;
	}


private:

	/**
	 * Set the minimum level -- update procedure for the given node
	 *
	 * @param n the node
	 * @param vt the vertex table element
	 * @param l the level
	 * @param streaming_weights the streaming weights
	 * @param forward_pointers the forward pointers for streaming
	 */
	void set_min_level_helper(node_t n, const ll_mlcsr_core__begin_t* vte,
			size_t l,
			ll_mlcsr_edge_property<uint32_t>* streaming_weights = NULL,
			ll_mlcsr_edge_property<edge_t>* forward_pointers = NULL) {

		// Note: Maybe we can combine the two for loops

#		ifdef LL_S_UPDATE_PRECOMPUTED_DEGREES
		size_t e_remove = 0;  // The number of existing edges to remove
		ll_foreach_edge_within_level(e, t, *this, n, l,
				this->num_levels() - 1, vte) {

			(void) t;
			e_remove++;
		}

		if (e_remove > 0) {
			auto b = (*this->_latest_begin)[n];
			b.degree -= e_remove;

			this->_latest_begin->cow_write(n, b);

			LL_D_NODE_PRINT(n, "Removing %lu old edges, level=%lu, "
					"old_degree=%d, new_degree=%d\n", e_remove, l,
					(int) (b.degree + e_remove), (int) b.degree);
			assert((int) b.degree >= 0);
		}
#		endif

#		ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES
		if (forward_pointers != NULL) {
			assert(streaming_weights != NULL);

			ll_foreach_edge_within_level(e, t, *this, n, l, l, vte) {

				(void) t;
				e_remove++;

				auto weight = streaming_weights->get(e);
				if (weight != 0) {
					for (edge_t forward = forward_pointers->get(e);
							forward != LL_NIL_EDGE && forward != 0;
							forward = forward_pointers->get(forward)) {
						//LL_D_PRINT("Weight age-off: "
						LL_D_NODE2_PRINT(n, t, "Weight age-off: "
								"edge=%lx %ld --> %ld: forward=%lx "
								"w=%d\n",
								e, n, t, forward, (int) weight);
						assert(LL_EDGE_LEVEL(e)
								< LL_EDGE_LEVEL(forward));
						streaming_weights->cow_write_add(forward,
								-weight);
					}
				}
			}
		}
#		endif
	}


public:

	/**
	 * Set the minimum level to consider
	 *
	 * @param m the minimum level
	 * @param streaming_weights the streaming weights
	 * @param forward_pointers the forward pointers for streaming
	 */
	virtual void set_min_level(size_t m,
			ll_mlcsr_edge_property<uint32_t>* streaming_weights = NULL,
			ll_mlcsr_edge_property<edge_t>* forward_pointers = NULL) {

		assert((ssize_t) this->_minLevel <= (ssize_t) m);

		_begin.set_min_level(m);

#	if defined(LL_S_UPDATE_PRECOMPUTED_DEGREES) \
		|| defined(LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES)

#	  ifdef LL_PERSISTENCE
		// We cannot write to a finalized level, so we are stuck
#		error "LL_S_* and LL_PERSISTENCE are not compatible"
#	  endif


		// Update the precomputed degrees and weights

		for (size_t l = this->_minLevel; l < m; l++) {
			if (l >= _begin.size()) break;

			if (this->has_sparse_representation(l)) {
				size_t length = this->sparse_length(l);
				const node_t* ids = this->sparse_node_ids(l);
				const ll_mlcsr_core__begin_t* data = this->sparse_node_data(l);

#				pragma omp parallel for schedule(static,65536)
				for (size_t i = 0; i < length; i++) {
					set_min_level_helper(ids[i], &data[i], l,
							streaming_weights, forward_pointers);
				}
			}
			else {
				auto vt = this->vertex_table(l);

#ifdef D_DEBUG_NODE
				ll_foreach_node_within_level(n, *this, l) {
#elif 0
				/* to deal with indentation */ }
#else
				ll_foreach_node_within_level_omp(n, *this, l, 4096) {
#endif
					const ll_mlcsr_core__begin_t* vte = &(*vt)[n];
					set_min_level_helper(n, vte, l, streaming_weights,
							forward_pointers);
				}
			}
		}

#	endif 

		this->_minLevel = m;
	}
#endif


	/**
	 * Get the maximum level to consider
	 *
	 * @return the maximum level to consider
	 */
	inline int max_level() const {
		return this->_maxLevel;
	}


	/**
	 * Get the value associated with the given edge
	 *
	 * @param e the edge
	 * @return the value
	 */
	T value(edge_t e) const {
#ifdef LL_DELETIONS
        return LL_VALUE_PAYLOAD((*this->edge_table(LL_EDGE_LEVEL(e)))
				[LL_EDGE_INDEX(e)]);
#else
        return (*this->edge_table(LL_EDGE_LEVEL(e)))[LL_EDGE_INDEX(e)];
#endif
    }



	/**
	 * Get the node degree
	 *
	 * @param n the node
	 * @return the degree
	 */
	size_t degree(node_t n) {

#ifdef LL_CHECK_NODE_EXISTS_IN_RO
#ifndef FORCE_L0
		if (!this->node_exists(n)) {
			return 0;
		}
#endif
#endif

#ifdef LL_PRECOMPUTED_DEGREE

		const ll_mlcsr_core__begin_t& b = (*this->_latest_begin)[n];

#	if defined(LL_MIN_LEVEL) && !defined(LL_S_UPDATE_PRECOMPUTED_DEGREES)

		size_t d = b.degree;

		if (this->_minLevel == 0 || this->_begin[this->_minLevel-1]->size()<=n)
			return d;

		const ll_mlcsr_core__begin_t& bp = (*this->_begin[this->_minLevel-1])[n];
#	  if defined(LL_DELETIONS)
		LL_NOT_IMPLEMENTED;
#	  else
		return b.degree - bp.degree;
#	  endif

#	else
		return b.degree;
#	endif

#else
#	ifdef LL_DELETIONS
		ll_edge_iterator iter;
		iter_begin(iter, n);
		size_t r = 0;
		FOREACH_ITER(e, *this, iter) r++;
		return r;
#	else

		const ll_mlcsr_core__begin_t& b = (*this->_latest_begin)[n];

#	ifdef FORCE_L0
		return b.level_length;
#	else

		edge_t e = b.adj_list_start;
		if (e == LL_NIL_EDGE) return 0;
		size_t r = b.level_length;

		while (LL_EDGE_LEVEL(e) > 0) {
			if (this->_begin[LL_EDGE_LEVEL(e)-1]->size() >= n) return r;
			const ll_mlcsr_core__begin_t& x = (*this->_begin[LL_EDGE_LEVEL(e)-1])[n];
			e = x.adj_list_start;
			if (e == LL_NIL_EDGE) return r;
			r += x.level_length;
		}

		return r;
#	endif
#	endif
#endif
	}


	/**
	 * Get the node degree at the given level
	 *
	 * @param n the node
	 * @param level the level
	 * @return the degree
	 */
	size_t degree(node_t n, int level) {

#ifdef LL_PRECOMPUTED_DEGREE

		const ll_mlcsr_core__begin_t& b = (*this->_begin[level])[n];
		return b.degree;

#else
		LL_NOT_IMPLEMENTED;
#endif
	}


	/**
	 * Start the iterator for the given node
	 *
	 * @param iter the iterator
	 * @param n the node
	 * @param level the level
	 * @param max_level the max level for deletions
	 */
	void iter_begin(ll_edge_iterator& iter, node_t n,
			int level=-1, int max_level=-1) const {

#ifdef LL_COUNTERS
		g_iter_begin++;
#endif

#ifdef LL_CHECK_NODE_EXISTS_IN_RO
#ifndef FORCE_L0
		if (!this->node_exists(n)) {
			iter.edge = LL_NIL_EDGE;
			return;
		}
#endif
#endif

		iter.owner = LL_I_OWNER_RO_CSR;
		iter.node = n;
#ifdef LL_DELETIONS
		int l = (level == -1) ? this->max_level() : level;
		iter.max_level = max_level < 0 ? l : max_level;
#endif

		const ll_mlcsr_core__begin_t& b = level == -1
			? (*this->_latest_begin)[n] : (*this->vertex_table(level))[n];
		iter.edge = b.adj_list_start;

#ifdef LL_MIN_LEVEL
		if (LL_EDGE_LEVEL(iter.edge) < (size_t) this->_minLevel) {
			iter.left = 0;
			iter.edge = LL_NIL_EDGE;
			return;
		}
#endif

		iter.left = b.level_length;

		if (iter.left == 0)
			iter.edge = LL_NIL_EDGE;
		else {
			iter.ptr = this->edge_table(LL_EDGE_LEVEL(iter.edge))
				->edge_ptr(iter.node, LL_EDGE_INDEX(iter.edge));
			__builtin_prefetch(iter.ptr);
		}

#ifdef LL_DELETIONS
		if (this->is_edge_deleted(iter)) {
        	node_t n = iter.last_node;
			iter_next(iter);
        	iter.last_node = n;
		}
#endif

		LL_D_NODE_PRINT(n, "[left=%ld"
				IF_LL_PRECOMPUTED_DEGREE(", degree=%ld")
				IF_LL_DELETIONS(", max_level=%d")
				"]\n", (long) iter.left
				IF_LL_PRECOMPUTED_DEGREE(, (long) b.degree)
				IF_LL_DELETIONS(, (int) iter.max_level));
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
	 * Determine if there are any more items left
	 *
	 * @param iter the iterator
	 * @return true if there are more items
	 */
	ITERATOR_DECL bool iter_has_next(ll_edge_iterator& iter) const {
		return iter.edge != LL_NIL_EDGE;
	}


private:

	/**
	 * Descend to the next level
	 *
	 * @param iter the iterator
	 */
	void iter_descend(ll_edge_iterator& iter) const {

		LL_D_NODE_PRINT(iter.node, "Descend\n");

		int level = LL_EDGE_LEVEL(iter.edge);
		if (level == 0 
				|| iter.node >= (node_t) this->_begin[level-1]->size()) {
			iter.edge = LL_NIL_EDGE;
		}
		else {
#ifdef LL_MLCSR_CONTINUATIONS
			const ll_mlcsr_core__begin_t& b = *((ll_mlcsr_core__begin_t*)
					(void*) (((T*) iter.ptr) + 1));
#else
			const ll_mlcsr_core__begin_t& b = (*this->_begin[level-1])[iter.node];
#endif
			iter.edge = b.adj_list_start;
#ifdef LL_MIN_LEVEL
			if (LL_EDGE_LEVEL(iter.edge) < (size_t) this->_minLevel) {
				iter.left = 0;
				iter.edge = LL_NIL_EDGE;
			}
			else {
#endif

#ifdef LL_COUNTERS
				g_iter_descend++;
#endif

				iter.left = b.level_length;
				//
				// The previous level contains zeros instead of -1 for some nodes
				// contained in level-1 but not in level-2
				if (iter.left == 0)
					iter.edge = LL_NIL_EDGE;		// HACK!
				else {
					iter.ptr = this->edge_table(LL_EDGE_LEVEL(iter.edge))
						->edge_ptr(iter.node, LL_EDGE_INDEX(iter.edge));
					__builtin_prefetch(iter.ptr);
				}
#ifdef LL_MIN_LEVEL
			}
#endif
		}
	}


public:

	/**
	 * Get the next item
	 *
	 * @param iter the iterator
	 * @return the next item, or LL_NIL_EDGE if none
	 */
	ITERATOR_DECL edge_t iter_next(ll_edge_iterator& iter) const {

#ifdef LL_COUNTERS
		g_iter_next++;
#endif

#ifdef D_DEBUG_NODE
		T _v = 0;
		if (iter.edge != LL_NIL_EDGE)
			_v = this->edge_table(LL_EDGE_LEVEL(iter.edge))
				->edge_value(iter.node, LL_EDGE_INDEX(iter.edge));
		LL_D_NODE2_PRINT(iter.node, LL_VALUE_PAYLOAD(_v),
				"[%s] edge=%08lx %08ld --> %08ld, max_level=%d, left=%d %s\n",
				this->name(), iter.edge, iter.node,
				iter.edge == -1l ? -1l : (long) LL_VALUE_PAYLOAD(_v),
				(int) IFE_LL_DELETIONS(LL_VALUE_MAX_LEVEL(_v), -1),
				(int) iter.left, this->is_edge_deleted(iter) ? " DELETED" : "");
#endif

		edge_t r = iter.edge;
		
		if (r != LL_NIL_EDGE) {
#ifdef LL_DELETIONS
        	iter.last_node = LL_VALUE_PAYLOAD(*((T*) iter.ptr));
#else
        	iter.last_node = *((T*) iter.ptr); //value(r);
#endif
#ifdef LL_DELETIONS
			do {
#endif
				if (iter.left > 1) {
					iter.left--;
					iter.edge = LL_EDGE_NEXT_INDEX(iter.edge);
					iter.ptr = ((T*) iter.ptr) + 1;
					__builtin_prefetch(((T*) iter.ptr) + 32);
				}
				else {
#if defined(FORCE_L0)
					iter.edge = LL_NIL_EDGE;
#else
					iter_descend(iter);
#endif
				}

#ifdef LL_DELETIONS
			}
			while (this->is_edge_deleted(iter));
#endif
		}

		return r;
	}


	/**
	 * Start the iterator for the given node, but only within this level
	 *
	 * @param iter the iterator
	 * @param n the node
	 * @param level the level
	 * @param max_level the maximum visible level for deletions (-1 = level)
	 * @param vte the corresponding vertex table element (if known)
	 */
	void iter_begin_within_level(ll_edge_iterator& iter, node_t n,
			int level, int max_level=-1, const ll_mlcsr_core__begin_t* vte=NULL) const {

		iter.owner = LL_I_OWNER_RO_CSR;
		iter.node = n;

		IF_LL_DELETIONS(iter.max_level = max_level < 0 ? level : max_level);

#ifdef LL_CHECK_NODE_EXISTS_IN_RO
#ifndef FORCE_L0
		if (vte == NULL && !this->node_exists(n)) {
			iter.edge = LL_NIL_EDGE;
			return;
		}
#endif
#endif

		// TODO Should this be here? Is this too slow?
		if (vte == NULL && (node_t) this->_begin[level]->size() <= n) {
			iter.edge = LL_NIL_EDGE;
			return;
		}

		const ll_mlcsr_core__begin_t* b = vte == NULL ? &(*this->_begin[level])[n] : vte;
		iter.edge = b->adj_list_start;
		iter.left = b->level_length;

		if (iter.left == 0) iter.edge = LL_NIL_EDGE;

		if (LL_EDGE_LEVEL(iter.edge) != (size_t) level) {
			iter.edge = LL_NIL_EDGE;
		}

		if (iter.edge != LL_NIL_EDGE) {
			//iter.ptr = &(*this->edge_table(LL_EDGE_LEVEL(iter.edge)))
			//[LL_EDGE_INDEX(iter.edge)];
			iter.ptr = this->edge_table(LL_EDGE_LEVEL(iter.edge))
				->edge_ptr(iter.node, LL_EDGE_INDEX(iter.edge));
			__builtin_prefetch(iter.ptr);
		}

#ifdef LL_DELETIONS
		if (this->is_edge_deleted(iter)) {
			iter_next_within_level(iter);
		}
#endif

		LL_D_NODE_PRINT(n, "level=%d [left=%ld"
				IF_LL_PRECOMPUTED_DEGREE(", degree=%ld")
				IF_LL_DELETIONS(", max_level=%d")
				", this=%p, begin=%p]\n", (int) level, (long) iter.left
				IF_LL_PRECOMPUTED_DEGREE(, (long) b.degree)
				IF_LL_DELETIONS(, (int) iter.max_level)
				this, this->_begin[level]);
	}


	/**
	 * Determine if there are any more items left
	 *
	 * @param iter the iterator
	 * @return true if there are more items
	 */
	ITERATOR_DECL bool iter_has_next_within_level(ll_edge_iterator& iter) const {
		return iter.edge != LL_NIL_EDGE;
	}


	/**
	 * Get the next item, but only within this level
	 *
	 * @param iter the iterator
	 * @return the next item, or LL_NIL_EDGE if none
	 */
	ITERATOR_DECL edge_t iter_next_within_level(ll_edge_iterator& iter) const {

#ifdef D_DEBUG_NODE
		T _v = 0;
		if (iter.edge != LL_NIL_EDGE)
			_v = (*this->edge_table(LL_EDGE_LEVEL(iter.edge)))
				[LL_EDGE_INDEX(iter.edge)];
		LL_D_NODE2_PRINT(iter.node, LL_VALUE_PAYLOAD(_v),
				"edge=%08lx %08ld --> %08ld, max_level=%d, left=%d %s\n",
				iter.edge, iter.node,
				iter.edge == -1l ? -1l : (long) LL_VALUE_PAYLOAD(_v),
				(int) IFE_LL_DELETIONS(LL_VALUE_MAX_LEVEL(_v), -1),
				(int) iter.left, this->is_edge_deleted(iter) ? " DELETED" : "");
#endif

		edge_t r = iter.edge;
		
		if (r != LL_NIL_EDGE) {
#ifdef LL_DELETIONS
        	iter.last_node = LL_VALUE_PAYLOAD(*((T*) iter.ptr));
#else
        	iter.last_node = *((T*) iter.ptr); //value(r);
#endif
#ifdef LL_DELETIONS
			do {
#endif
				if (iter.left > 1) {
					iter.left--;
					iter.edge = LL_EDGE_NEXT_INDEX(r);
					iter.ptr = ((T*) iter.ptr) + 1;
				}
				else {
					iter.edge = LL_NIL_EDGE;
					IF_LL_DELETIONS(break);
				}
#ifdef LL_DELETIONS
			}
			while (this->is_edge_deleted(iter));
#endif
		}

		return r;
	}


	/**
	 * Find the given node and value combination
	 *
	 * @param node the node
	 * @param value the value
	 * @return the edge, or NIL_EDGE if it does not exist
	 */
	edge_t find(node_t node, T value) const {

		auto vt = vertex_table();
		if (node >= (node_t) vt->size()) return LL_NIL_EDGE;

		ll_edge_iterator iter;
		this->iter_begin(iter, node);
		FOREACH_ITER(e, *this, iter) {
			if (iter.last_node == value) return e;
		}

		return LL_NIL_EDGE;
	}


	/**
	 * Find the given node and value combination for the given level
	 *
	 * @param node the node
	 * @param value the value
	 * @param level the level
	 * @param max_level the max level for deletions
	 * @return the edge, or NIL_EDGE if it does not exist
	 */
	edge_t find(node_t node, T value, int level, int max_level) const {

		auto vt = vertex_table(level);
		if (node >= (node_t) vt->size()) return LL_NIL_EDGE;

		ll_edge_iterator iter;
		this->iter_begin(iter, node, level, max_level);
		FOREACH_ITER(e, *this, iter) {
			if (iter.last_node == value) return e;
		}

		return LL_NIL_EDGE;
	}


	/**
	 * Print the entire level, and also optionally search the target arrays
	 * (useful for debugging merging)
	 *
	 * @param file the output file
	 * @param level the level number
	 * @param targets1 the target array 1
	 * @param targets1_length the number of elements in the target array
	 * @param targets2 the target array 2
	 * @param targets2_length the number of elements in the target array
	 */
	virtual void print_level(FILE* file, int level,
			long* targets1 = NULL, size_t targets1_length = 0,
			long* targets2 = NULL, size_t targets2_length = 0) {

		size_t max_nodes = this->_perLevelNodes[level];
		size_t valuesIndex = 0;

		fprintf(file, "\n========== Level %d ==========\n", level);
		
		for (node_t n = 0; n < (node_t) max_nodes; n++) {
			auto& b = (*this->_begin[level])[n];
			if (b.adj_list_start == LL_NIL_EDGE) {
				fprintf(file, "[%4lld] NIL\n", (long long) n);
				continue;
			}
			if (b.level_length == 0) {
				fprintf(file, "[%4lld] NIL [len=0]\n", (long long) n);
				continue;
			}

			int bl = LL_EDGE_LEVEL(b.adj_list_start);
			size_t bi = LL_EDGE_INDEX(b.adj_list_start);
			size_t blen = b.level_length;

			if (bl == level && valuesIndex < bi) {
				fprintf(file, "[----] start=%d:%ld len=%ld:", bl, valuesIndex,
						bi - valuesIndex);
				while (valuesIndex < bi) {
					fprintf(file, " %4ld",
							(long) LL_VALUE_PAYLOAD(
								(*this->_values[level])[valuesIndex++]));
				}
				fprintf(file, "\n");
			}

			fprintf(file, "[%4lld] start=%d:%ld len=%ld:",
					(long long) n, bl, bi, blen);
			if (bl == level && blen > 0) {
				for (size_t i = 0; i < blen; i++) {
					fprintf(file, " %4ld",
							(long) LL_VALUE_PAYLOAD((*this->_values[level])[bi + i]));
					for (size_t x = 0; x < targets1_length; x++) {
						if (bi+i == (size_t) targets1[x]) fprintf(stderr, "(a%ld)", x);
					}
					for (size_t x = 0; x < targets2_length; x++) {
						if (bi+i == (size_t) targets2[x]) fprintf(stderr, "(b%ld)", x);
					}
				}
				valuesIndex = bi + blen;
			}
			fprintf(file, "\n");
		}
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

#ifdef LL_MLCSR_CONTINUATIONS

		// XXX Fix this for LL_STREAMING
		size_t continuation_size = sizeof(ll_mlcsr_core__begin_t) / sizeof(T);
		if (sizeof(ll_mlcsr_core__begin_t) % sizeof(T) != 0) continuation_size++;
		size_t with_continuations = max_edges + max_adj_lists * continuation_size;

#	ifdef LL_MLCSR_LEVEL_ID_WRAP
		return with_continuations;
#	else
		return level == 0 ? max_edges : with_continuations;
#	endif

#else
		return max_edges;
#endif
	}


public:

	/**
	 * C++ style iterator
	 */
	class iterator : public std::iterator<std::input_iterator_tag, node_t> {

		/// The owner
		const ll_mlcsr_core* _owner;

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
		inline iterator(const ll_mlcsr_core& owner) : _owner(&owner) {}

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
		void set_owner(ll_mlcsr_core* owner) { _owner = owner; }

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
	 * The adapter for edge property level initialization
	 */
	template <typename T>
	class edge_property_level_creation_callback
		: public ll_mlcsr_edge_property_level_creation_callback<T> {

		/// The owner
		ll_mlcsr_core& _owner;

		/// True for out, false for in
		bool _out_property;


	public:

		/**
		 * Initialize the object
		 *
		 * @param owner the owner
		 * @param out_property true for out, false for in
		 */
		edge_property_level_creation_callback(ll_mlcsr_core& owner)
			: _owner(owner) {}


		/**
		 * Dense-initialize the given level
		 *
		 * @param edge_property the edge property this is a part of
		 * @param level_property the property object to initialize
		 * @param level the level number
		 * @param max_edges the number of new edges
		 */
		virtual void dense_init_edge_level(
				ll_mlcsr_edge_property<T>* edge_property,
				ll_mlcsr_node_property<T>* level_property,
				int level, size_t max_edges) {

			level_property->dense_init_level(_owner.edge_table_length(level));
		}
	};

	/// The edge 32-bit property level initializer
	edge_property_level_creation_callback<uint32_t>
		_edge_property_level_initializer_32;

	/// The edge 64-bit property level initializer
	edge_property_level_creation_callback<uint64_t>
		_edge_property_level_initializer_64;


public:

	/**
	 * Get the corresponding edge property level creation callback for 32-bit
	 * properties
	 *
	 * @return the callback if there is one; otherwise return NULL
	 */
	virtual ll_mlcsr_edge_property_level_creation_callback<uint32_t>*
		edge_property_level_creation_callback_32() {
		return &_edge_property_level_initializer_32;
	}


	/**
	 * Get the corresponding edge property level creation callback for 64-bit
	 * properties
	 *
	 * @return the callback if there is one; otherwise return NULL
	 */
	virtual ll_mlcsr_edge_property_level_creation_callback<uint64_t>*
		edge_property_level_creation_callback_64() {
		return &_edge_property_level_initializer_64;
	}
};

#endif
