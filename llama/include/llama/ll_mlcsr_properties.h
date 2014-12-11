/*
 * ll_mlcsr_properties.h
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


#ifndef LL_MLCSR_PROPERTY_H_
#define LL_MLCSR_PROPERTY_H_

#include "llama/ll_mem_array.h"
#include "llama/ll_writable_elements.h"

#include "llama/ll_mlcsr_helpers.h"



//==========================================================================//
// A multi-version property array                                           //
//==========================================================================//

/**
 * A multi-version property array
 *
 * @author Peter Macko <peter.macko@oracle.com>
 */
template <typename T>
class ll_multiversion_property_array {

	/// The ID
	int _id;

	/// The name
	std::string _name;

	/// The master copy (if this is a read-only clone)
	ll_multiversion_property_array<T>* _master;

	/// The property data structure
	LL_PT_COLLECTION<LL_PT<T>, T> _properties;

	/// The last level properties
	LL_PT<T>* _latest_properties;

	/// The page manager
	ll_page_manager<T>* _page_manager;

	/// True if this object owns the page manager
	bool _own_page_manager;

	/// The type code
	short _type;

	/// The number of elements in the last level
	size_t _max_nodes;

	/// Last writer position
	node_t _last_write;

	/// The value destructor
	void (*_destructor)(const T&);

	/// Whether the last level is writable
	bool _latest_writable;

	/// The lock table for atomic property updates
	// TODO Move it to some shared lock object?
	ll_spinlock_table_ext<64> _lt;


	/**
	 * Get the appropriate _properties data structure corresponing to the given
	 * level
	 *
	 * @param level the level
	 * @return the data structure
	 */
	inline LL_PT<T>* properties(size_t level) {
#ifdef FORCE_L0
		return _latest_properties;
#else
		return _properties[level];
#endif
	}


public:

	/**
	 * The node property persistence header
	 */
	struct persistence_header {
		short h_type;
	};
	

public:

	/**
	 * Create an instance of type ll_multiversion_property_array
	 *
	 * @param storage the persistence context
	 * @param id the ID
	 * @param name the property name
	 * @param type the type code
	 * @param destructor the value destructor
	 * @param pageManager the page manager
	 * @param ns the namespace for the persistent storage
	 */
	ll_multiversion_property_array(IF_LL_PERSISTENCE(ll_persistent_storage* storage,)
			int id, const char* name, short type,
			void (*destructor)(const T&) = NULL,
			ll_page_manager<T>* pageManager = NULL,
			const char* ns="np")
#ifdef LL_PERSISTENCE
		: _properties(storage, name, ns)
#else
		: _properties(pageManager != NULL
				? pageManager
				: new ll_page_manager<T>(1 << LL_ENTRIES_PER_PAGE_BITS,
					true /* zero pages */),
				pageManager == NULL /* own page manager */)
#endif
	{
		_id = id;
		_name = name;
		_type = type;
		_master = NULL;

		_latest_properties = NULL;

		_max_nodes = 0;
		_last_write = 0;
		_destructor = destructor;
		_latest_writable = false;

		if (_properties.size() > 0) {
			_latest_properties = _properties.latest_level();
			_max_nodes = _latest_properties->size();
		}

#ifdef LL_PERSISTENCE
		_page_manager = NULL;
		_own_page_manager = false;
#else
		_page_manager = _properties.page_manager();
		_own_page_manager = _page_manager != pageManager;
#endif

#ifdef LL_PERSISTENCE
		ll_length_and_data* ld = _properties.persistence().read_header();
		if (ld == NULL) {
			persistence_header h;
			h.h_type = type;
			_properties.persistence().write_header(&h, sizeof(h));
		}
		else if (ld->ld_length > sizeof(persistence_header)) {
			LL_E_PRINT("Invalid header");
			free(ld);
			abort();
		}
		else {
			persistence_header h;
			memcpy(&h, (persistence_header*) ld->ld_data, ld->ld_length);
			free(ld);
			if (h.h_type != type) {
				LL_E_PRINT("The type does not match");
				abort();
			}
		}
#endif
	}


	/**
	 * Create a read-only clone of ll_multiversion_property_array
	 *
	 * @param master the master object
	 * @param level the max level
	 */
	ll_multiversion_property_array(ll_multiversion_property_array<T>* master,
			int level) 
		: _properties(&master->_properties, level)
	{
		_master = master;
		
		_id = master->_id;
		_name = master->_name;
		_latest_properties = _properties[_properties.size() - 1];
		_page_manager = master->_page_manager;
		_own_page_manager = false;
		_type = master->_type;
		_max_nodes = _latest_properties->size();

		_last_write = 0;
		_destructor = NULL;
		_latest_writable = false;
	}


	/**
	 * Destroy the property
	 */
	virtual ~ll_multiversion_property_array() {

		// Destroy the values, if applicable

		if (_destructor != NULL) {
			for (int l = _properties.size()-1; l >= 0; l--) {
				if (_properties[l] != NULL) {
					auto vt = _properties[l];

					// Assumes that levels were deleted in age order

					if (l == 0 || _properties[l-1] == NULL) {
#						pragma omp parallel for schedule(dynamic,1024*128)
						for (node_t n = 0; n < (node_t) (*vt).size(); n++) {
							const T& value = (*vt)[n];
							if (value != (T) 0) _destructor(value);
						}
					}
					else {
#						pragma omp parallel for schedule(dynamic,1)
						for (node_t start = 0; start < (node_t) vt->size();
								start += 4096) {
							ll_vertex_iterator iter;
							vt->modified_node_iter_begin(iter, start,
									start + 4096);
							for (node_t n = vt->modified_node_iter_next(iter);
									n != LL_NIL_NODE;
									n = vt->modified_node_iter_next(iter)) {
								T& value = *((T*) iter.vi_value);
								if (value != (T) 0) _destructor(value);
							}
						}
					}
				}
			}
		}
	}


	/**
	 * Get the ID
	 *
	 * @return the ID
	 */
	inline int id() const {
		return _id;
	}


	/**
	 * Get the type code
	 *
	 * @return the type
	 */
	inline short type() const {
		return _type;
	}
	

#ifdef LL_MIN_LEVEL

	/**
	 * Get the min level
	 *
	 * @return the minimum level to consider
	 */
	inline int min_level() const {
		return _properties.min_level();
	}


	/**
	 * Set the minimum level to consider
	 *
	 * @param m the minimum level
	 */
	virtual void set_min_level(size_t m) {
		_properties.set_min_level(m);
	}

#endif


	/**
	 * Get the max level
	 *
	 * @return the minimum level to consider
	 */
	inline int max_level() const {
		return _properties.max_level();
	}


	/**
	 * Return the max level ID that is currently in use (might still refer
	 * to NULL, but its space is allocated in the array of levels)
	 *
	 * @return the max level ID
	 */
	inline int max_level_id() const {
		return ((int) _properties.size()) - 1;
	}


	/**
	 * Get the property value
	 *
	 * @param index the index
	 * @return the property value
	 */
	inline const T get(size_t index) {
		return (*_latest_properties)[index];
	}


	/**
	 * Get the property value (alias)
	 *
	 * @param index the index
	 * @return the property value
	 */
	inline const T operator[](size_t index) {
		return get(index);
	}


	/**
	 * Get the number of elements in the last level
	 *
	 * @return the number of elements
	 */
	inline size_t size() const {
		return _max_nodes;
	}


	/**
	 * Init level
	 *
	 * @param max_nodes the number of nodes
	 */
	void init_level(size_t max_nodes) {

		auto* b = this->_properties.new_level(max_nodes);

		if (this->_properties.count_existing_levels() <= 1)
			b->dense_init();
		else
			b->cow_init();

		this->_latest_properties = b;
		_last_write = -1;
		_max_nodes = max_nodes;
	}


	/**
	 * Append node
	 *
	 * @param node the node
	 * @param value the value
	 */
	void append_node(node_t node, const T& value) {

		if (_last_write + 1 < node) {
			if (this->_properties.size() == 1) {
				for (node_t i = _last_write + 1; i < node; i++) {
					this->_latest_properties->dense_direct_write(i, (T) 0);
				}
			}
		}

		if (this->_properties.size() == 1)
			this->_latest_properties->dense_direct_write(node, value);
		else
			this->_latest_properties->cow_write(node, value);

		_last_write = node;
	}


	/**
	 * Finish level
	 */
	void finish_level() {

		if (this->_properties.size() == 1) {
			for (size_t i = _last_write + 1;
					i < this->_latest_properties->size(); i++) {
				this->_latest_properties->dense_direct_write(i, (T) 0);
			}
			this->_latest_properties->dense_finish();
			this->_latest_properties->finish_level_edges();
		}
		else {
			this->_latest_properties->cow_finish();
			this->_latest_properties->finish_level_edges();
		}
	}


	/**
	 * Init the property as dense, which allows direct writes
	 *
	 * @param max_nodes the number of nodes
	 */
	void dense_init_level(size_t max_nodes) {
		init_level(max_nodes);
	}


	/**
	 * Direct write into a dense property
	 *
	 * @param node the node
	 * @param value the value
	 */
	void dense_direct_write(node_t node, const T& value) {
		this->_latest_properties->dense_direct_write(node, value);
	}


	/**
	 * Finish a dense level
	 */
	void dense_finish_level() {
		this->_latest_properties->dense_finish();
		this->_latest_properties->finish_level_edges();
	}


	/**
	 * Init the property using copy-on-write, which allows direct writes
	 *
	 * @param max_nodes the number of nodes
	 */
	void cow_init_level(size_t max_nodes) {
		init_level(max_nodes);
	}


	/**
	 * Direct write into a COW property
	 *
	 * @param node the node
	 * @param value the value
	 */
	void cow_write(node_t node, const T& value) {
		assert(_master == NULL);
		this->_latest_properties->cow_write(node, value);
	}


	/**
	 * Add a value to the COW property
	 *
	 * @param node the node
	 * @param value the value to add
	 */
	void cow_write_add(node_t node, const T& value) {
		this->_lt.acquire_for(node);
		this->_latest_properties->cow_write(node, get(node) + value);
		this->_lt.release_for(node);
	}


	/**
	 * Finish a COW level
	 */
	void cow_finish_level() {
		this->_latest_properties->cow_finish();
		this->_latest_properties->finish_level_edges();
	}


	/**
	 * Determine if the given level exists
	 *
	 * @param level the level number
	 * @return true if it exists
	 */
	bool level_exists(size_t level) {
		return this->_properties.level_exists(level);
	}


	/**
	 * Count the number of existing levels
	 *
	 * @return the number of existing levels
	 */
	size_t count_existing_levels() {
		return this->_properties.count_existing_levels();
	}


	/********** Writable API **********/

	/**
	 * Determine if the latest level is writable
	 *
	 * @return true if it is writable
	 */
	inline bool writable() {
		return _latest_writable;
	}


	/**
	 * Create a new writable level
	 *
	 * @param maxNodes the maximum number of nodes
	 */
	void writable_init(size_t maxNodes) {
		assert(!_latest_writable);
		_latest_writable = true;
		if (_properties.empty())
			dense_init_level(maxNodes);
		else
			cow_init_level(maxNodes);
	}


	/**
	 * Write into the writable level
	 *
	 * @param node the node
	 * @param value the value
	 */
	inline void set(node_t node, const T& value) {
		assert(_latest_writable);
		cow_write(node, value);
	}


	/**
	 * Freeze the writable level
	 *
	 * @param max_nodes the updated number of nodes
	 */
	void freeze(size_t max_nodes) {
		assert(_latest_writable);

		this->_latest_properties->shrink(max_nodes);

		if (_properties.size() == 1)
			dense_finish_level();
		else
			cow_finish_level();

		_latest_writable = false;
	}


	/********** Maintenance and Support API **********/

	/**
	 * Ensure that we have at least the given number of levels
	 *
	 * @param minLevels the minimum number of levels
	 * @param max_nodes the number of nodes for which to initialize any new levels
	 */
	void ensure_min_levels(size_t minLevels, size_t max_nodes) {

		while (this->_properties.size() < minLevels) {
			cow_init_level(max_nodes);
			cow_finish_level();
		}
	}


	/**
	 * Delete a level
	 *
	 * @param level the level number
	 */
	void delete_level(int level) {

		if (_destructor != NULL) {

			// For now, enable this only if we are deleting the levels in age
			// order

			assert(level == 0 || this->_properties[level-1] == NULL);
			assert(level >= max_level_id() || this->_properties[level+1] != NULL);


			if (level >= max_level_id()) {

				// All levels are deleted!

				auto& p = *this->_properties[level];

#				pragma omp parallel for schedule(dynamic,1024*128)
				for (size_t n = 0; n < p.size(); n++) {
					const T& value = p[n];
					if (value != (T) 0) _destructor(value);
				}
			}
			else {

				// Delete everything that is not needed for the next level

				auto& p = *this->_properties[level];
				auto& pn = *this->_properties[level+1];

				// Does this take everything?

#				pragma omp parallel
				{
					ll_vertex_iterator iter;
					node_t m = std::min<node_t>(p.size(), pn.size());

#					pragma omp for schedule(dynamic,1)
					for (node_t start = 0; start < m; start += 4096) {
						pn.modified_node_iter_begin(iter, start, start + 4096);
						for (node_t n = pn.modified_node_iter_next(iter);
								n != LL_NIL_NODE;
								n = pn.modified_node_iter_next(iter)) {
							if (n >= m) break;
							const T& value = p[n];
							if (value != (T) 0) _destructor(value);
						}
					}
				}
			}

		}

		this->_properties.delete_level(level);
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

		for (int l = 0; l <= ((int) max_level()) - (int) keep; l++) {
			if (level_exists(l)) delete_level(l);
		}
	}
};



//==========================================================================//
// A node property                                                          //
//==========================================================================//

#define ll_mlcsr_node_property	ll_multiversion_property_array



//==========================================================================//
// An edge property                                                         //
//==========================================================================//


template <typename T> class ll_mlcsr_edge_property;


/**
 * A callback for edge property level initialization
 */
template <typename T>
class ll_mlcsr_edge_property_level_creation_callback {

public:

	/**
	 * Dense-initialize the given level
	 *
	 * @param edge_property the edge property this is a part of
	 * @param level_property the property object to initialize
	 * @param level the level number
	 * @param max_edges the number of new edges
	 */
	virtual void dense_init_edge_level(ll_mlcsr_edge_property<T>* edge_property,
			ll_multiversion_property_array<T>* level_property,
			int level, size_t max_edges) = 0;
};


/**
 * An edge property
 *
 * @author Peter Macko <peter.macko@oracle.com>
 */
template <typename T>
class ll_mlcsr_edge_property {

	/// The ID
	int _id;

	/// The name
	std::string _name;

	/// The master copy (if this is a read-only clone)
	ll_mlcsr_edge_property<T>* _master;

	/// The properties per edge level
	std::vector<ll_multiversion_property_array<T>*> _properties;

	/// The page manager
	ll_page_manager<T>* _page_manager;

	/// The persistent storage
	IF_LL_PERSISTENCE(ll_persistent_storage* _storage);

	/// The type code
	short _type;

	/// The value destructor
	void (*_destructor)(const T&);

	/// Whether the last level is writable
	bool _latest_writable;

	/// Partial init
	bool _partial_init;

	/// The level creation callback
	ll_mlcsr_edge_property_level_creation_callback<T>* _edge_level_creation;
	
#ifdef LL_MIN_LEVEL
	/// The minimum level to consider
	int _min_level;
#endif

	/// The maximum level to consider
	int _max_level;


public:

	/**
	 * Create an instance of class ll_mlcsr_edge_property
	 *
	 * @param storage the persistence context
	 * @param id the ID
	 * @param name the property name
	 * @param type the type code
	 * @param destructor the value destructor (for 64-bit data types only)
	 * @param edge_level_creation the callback for new edge level initialization
	 */
	ll_mlcsr_edge_property(IF_LL_PERSISTENCE(ll_persistent_storage* storage,)
			int id, const char* name, short type,
			void (*destructor)(const T&) = NULL,
			ll_mlcsr_edge_property_level_creation_callback<T>*
				edge_level_creation = NULL) {

		_id = id;
		_name = name;
		_type = type;
		_master = NULL;

		_destructor = destructor;
		_latest_writable = false;
		_partial_init = false;
		_edge_level_creation = edge_level_creation;

		IF_LL_PERSISTENCE(_storage = storage);
		_page_manager = new ll_page_manager<T>(1 << LL_ENTRIES_PER_PAGE_BITS,
				true /* zero pages */);

#ifdef LL_MIN_LEVEL
		_min_level = 0;
#endif
		_max_level = -1;

		// TODO Automatically init from the persistent storage
	}


	/**
	 * Create a read-only clone of ll_mlcsr_edge_property
	 *
	 * @param master the master object
	 * @param level the max level
	 */
	ll_mlcsr_edge_property(ll_mlcsr_edge_property<T>* master,
			int level) 
	{
		_master = master;

		_id = master->_id;
		_name = master->_name;
		_page_manager = master->_page_manager;
		IF_LL_PERSISTENCE(_storage = master->_storage);
		_type = master->_type;

		_destructor = NULL;
		_latest_writable = false;
		_page_manager = NULL;
		_edge_level_creation = NULL;

#ifdef LL_MIN_LEVEL
		_min_level = master->_min_level;
#endif
		_max_level = master->_max_level;

		if (master->max_level_id() >= 0) {

			if (level < 0) level = 0;
			while ((ssize_t) _properties.size() <= master->max_level_id())
				_properties.push_back(NULL);

			int local_level = -1;
			for (int i = level; i >= 0;
					i = prev_level_id_nofail(i)) {
				local_level++;
				if (master->_properties[i] == NULL) continue;

				_properties[i] = (new ll_multiversion_property_array<T>
						(master->_properties[i], local_level));
			}
		}
	}

	
	/**
	 * Destroy the object
	 */
	virtual ~ll_mlcsr_edge_property() {

		for (size_t i = 0; i < _properties.size(); i++) {
			if (_properties[i] != NULL) delete _properties[i];
		}

		if (_master == NULL) {
			delete _page_manager;
		}
	}


	/**
	 * Get the ID
	 *
	 * @return the ID
	 */
	inline int id() const {
		return _id;
	}


	/**
	 * Get the type code
	 *
	 * @return the type
	 */
	inline short type() const {
		return _type;
	}


	/**
	 * Get the name
	 *
	 * @return the name
	 */
	inline const char* name() const {
		return _name.c_str();
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
	 * Return the max level ID that is currently in use (might still refer
	 * to NULL, but its space is allocated in the array of levels)
	 *
	 * @return the max level ID
	 */
	inline int max_level_id() const {
		return ((int) _properties.size()) - 1;
	}


	/**
	 * Determine if this level exists
	 *
	 * @param level the level
	 * @return if the level exists
	 */
	bool level_exists(int level) {
#ifdef LL_MIN_LEVEL
		if (!ll_level_within_bounds(level, _min_level, _max_level))
			return false;
#else
		if (level > _max_level) return false;
#endif
		return _properties[level] != NULL;
	}


	/**
	 * Return the value destructor
	 *
	 * @return the destructor
	 */
	void (*destructor())(const T&) {
		return _destructor;
	}


	/**
	 * Get the property value
	 *
	 * @param edge the edge
	 * @return the property value
	 */
	inline const T get(edge_t edge) const {

#ifdef FORCE_L0
		return _properties[0]->get(edge);
#else
		if (LL_EDGE_IS_WRITABLE(edge)) {
			if (sizeof(T) == 4) {
				return LL_EDGE_GET_WRITABLE(edge)->get_property_32<T>(_id);
			}
			else {
				return LL_EDGE_GET_WRITABLE(edge)->get_property_64<T>(_id);
			}
		}

		// TODO I hope the following check does not make it too slow...

		size_t level = LL_EDGE_LEVEL(edge);
		if (_properties[level] == NULL) return (T) 0;

		return _properties[level]->get(LL_EDGE_INDEX(edge));
#endif
	}


	/**
	 * Get the property value (alias)
	 *
	 * @param index the index
	 * @return the property value
	 */
	inline const T operator[](size_t index) const {
		return get(index);
	}
	

#ifdef LL_PERSISTENCE

	/**
	 * Init from the persistent storage
	 *
	 * @param levels the number of levels
	 */
	void init_from_persistent_storage(size_t levels) {

		for (size_t i = 0; i < levels; i++) {
			char s[32]; sprintf(s, "-%lu", i);
			std::string n = _name; n += s;
			ll_multiversion_property_array<T>* p
				= new ll_multiversion_property_array<T>(_id,
					n.c_str(), _type, _destructor, _page_manager, "ep");
			_properties.push_back(p);
		}

		_max_level = ((int) _properties.size()) - 1;
	}

#endif


	/**
	 * Partially init the property using copy-on-write, which allows direct writes
	 */
	void cow_init_level_partial() {

		if (!_partial_init) {
			for (size_t i = 0; i < _properties.size(); i++) {
				if (_properties[i] == NULL) continue;
				if (_properties[i]->count_existing_levels() > 0)
					_properties[i]->cow_init_level(_properties[i]->size());
			}
			_partial_init = true;
		}
	}


	/**
	 * Init the property using copy-on-write, which allows direct writes
	 *
	 * @param max_edges the number of edges
	 */
	void cow_init_level(size_t max_edges) {

		cow_init_level_partial();
		_partial_init = false;

#ifdef LL_MLCSR_LEVEL_ID_WRAP
#error "NOT IMPLEMENTED"
#else
		int level = _properties.size();
#endif

		char s[32]; sprintf(s, "-%d", level);
		std::string n = _name; n += s;

		// XXX Wrap

		while ((ssize_t) this->_properties.size() <= level)
			this->_properties.push_back(NULL);

		ll_multiversion_property_array<T>* p = new ll_multiversion_property_array<T>(
				IF_LL_PERSISTENCE(_storage,)
				_id, n.c_str(), _type, _destructor, _page_manager, "ep");

		_properties[level] = p;
		_max_level = level;

		if (_edge_level_creation == NULL) {
			p->dense_init_level(max_edges);
		}
		else {
			_edge_level_creation->dense_init_edge_level(this, p,
					level, max_edges);
		}
	}


	/**
	 * Direct write into a COW property
	 *
	 * @param edge the edge
	 * @param value the value
	 */
	void cow_write(edge_t edge, const T& value) {
		
		assert(_master == NULL);

#ifdef FORCE_L0
		_properties[0]->dense_direct_write(edge, value);
#else
		size_t level = LL_EDGE_LEVEL(edge);
		auto* p = _properties[level];
		if (p == NULL) {
			LL_E_PRINT("Attempting to write to NULL level %lu\n", level);
			abort();
		}
		if (p->max_level_id() <= 0)
			p->dense_direct_write(LL_EDGE_INDEX(edge), value);
		else
			p->cow_write(LL_EDGE_INDEX(edge), value);
#endif
	}


	/**
	 * Direct write into a COW property: Atomically add
	 *
	 * @param edge the edge
	 * @param value the value
	 */
	void cow_write_add(edge_t edge, const T& value) {
#ifdef FORCE_L0
		_properties[0]->cow_write_add(edge, value);
#else
		size_t level = LL_EDGE_LEVEL(edge);
		auto* p = _properties[level];
		if (p == NULL) {
			LL_E_PRINT("Attempting to write to NULL level %lu\n", level);
			abort();
		}
		p->cow_write_add(LL_EDGE_INDEX(edge), value);
#endif
	}


	/**
	 * Finish a COW level
	 */
	void cow_finish_level() {
		for (size_t i = 0; i < _properties.size(); i++) {
			if (_properties[i] == NULL || _max_level == (int) i) continue;
			_properties[i]->cow_finish_level();
		}
		_properties[_max_level]->dense_finish_level();
	}


	/**
	 * Finish level
	 */
	void finish_level() {
		cow_finish_level();
	}


	/**
	 * Copy properites from a previous level using dense-write to the current level
	 *
	 * @param to_index the target index
	 * @param from_level the source level
	 * @param from_index the source index
	 * @param length the number of properties to copy
	 */
	void dense_copy(size_t to_index, size_t from_level, size_t from_index,
			size_t length) {
		ll_multiversion_property_array<T>* from_property = _properties[from_level];
		ll_multiversion_property_array<T>* to_property
			= _properties[_properties.size() - 1];
		while (length --> 0) {
			to_property->dense_direct_write(to_index++,
					from_property->get(from_index++));
		}
	}


	/********** Writable API **********/

	/**
	 * Determine if the latest level is writable
	 *
	 * @return true if it is writable
	 */
	inline bool writable() {
		return _latest_writable;
	}


	/**
	 * Create a new writable level
	 */
	void writable_init() {
		assert(!_latest_writable);
		assert(!_partial_init);

		_latest_writable = true;
		_partial_init = true;

		for (size_t i = 0; i < _properties.size(); i++) {
			if (_properties[i] != NULL) {
				_properties[i]->writable_init(_properties[i]->size());
			}
		}
	}


	/**
	 * Write into the writable level
	 *
	 * @param edge the edge
	 * @param value the value
	 */
	inline void set(edge_t edge, const T& value) {
		assert(_latest_writable);

		if (LL_EDGE_IS_WRITABLE(edge)) {
			if (sizeof(T) == 4) {
				LL_EDGE_GET_WRITABLE(edge)->set_property_32<T>(_id, value);
			}
			else {
				LL_EDGE_GET_WRITABLE(edge)->set_property_64<T>(_id, value,
						(void (*)(const uint64_t&)) (void*) _destructor);
			}
		}
		else {
			cow_write(edge, value);
		}
	}


	/**
	 * Write into the writable level: Atomically add
	 *
	 * @param edge the edge
	 * @param value the value
	 */
	inline void add(edge_t edge, const T& value) {
		assert(_latest_writable);

		if (LL_EDGE_IS_WRITABLE(edge)) {
			if (sizeof(T) == 4) {
				LL_EDGE_GET_WRITABLE(edge)->add_property_32<T>(_id, value);
			}
			else {
				assert(_destructor == NULL);
				LL_EDGE_GET_WRITABLE(edge)->add_property_64<T>(_id, value);
			}
		}
		else {
			cow_write_add(edge, value);
		}
	}


	/**
	 * Freeze the writable level
	 */
	void freeze() {
		assert(_latest_writable);
		_latest_writable = false;

		for (size_t i = 0; i < _properties.size(); i++) {
			if (_properties[i] == NULL) continue;
			if (_properties[i]->writable()) {
				_properties[i]->freeze(_properties[i]->size());
			}
			else {
				if (i == _properties.size()-1)
					_properties[i]->dense_finish_level();
				else
					_properties[i]->cow_finish_level();
			}
		}
	}


	/********** Maintenance and Support API **********/

	/**
	 * Ensure that we have at least the given number of levels
	 *
	 * @param minLevels the minimum number of levels
	 * @param max_edges the number of edges for which to initialize any new levels
	 */
	void ensure_min_levels(size_t minLevels, size_t max_edges) {

		// TODO This is not particularly efficient due to dense_init_level

		while (this->_properties.size() < minLevels) {
			cow_init_level(max_edges);
			cow_finish_level();
		}
	}


	/**
	 * Delete a level
	 *
	 * @param level the level number
	 */
	void delete_level(int level) {

		// NOTE: This assumes that all previous levels are deleted

#ifdef LL_MIN_LEVEL
		assert(!ll_level_within_bounds(level, this->_min_level, this->_max_level));
#else
		LL_E_PRINT("Cannot delete a level without using the LL_MIN_LEVEL feature\n");
		abort();
#endif

		if (_properties[level] == NULL) return;

		for (int i = 0; i <= this->_properties[level]->max_level_id(); i++) {
			if (this->_properties[level]->level_exists(i))
				this->_properties[level]->delete_level(i);
		}

		assert(_properties[level]->count_existing_levels() == 0);
		delete _properties[level];
		_properties[level] = NULL;
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
			if (_properties[l] == NULL) continue;
			_properties[l]->keep_only_recent_levels(keep);
		}
	}


private:

	/**
	 * Get the previous level ID
	 *
	 * @param level the current level ID
	 * @return the previous level ID, or -1 if none
	 */
	inline const int prev_level_id_nofail(int level) const {

		int id = ((int) level) - 1;

#ifdef LL_MIN_LEVEL
		if (id == this->_min_level) return -1;
#endif
#ifdef LL_MLCSR_LEVEL_ID_WRAP
		if (id < 0) id = LL_MAX_LEVEL;
#endif

		return id;
	}
};

#endif

