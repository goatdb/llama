/*
 * ll_writable_elements.h
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


#ifndef LL_WRITABLE_ELEMENTS_H_
#define LL_WRITABLE_ELEMENTS_H_

#include "llama/ll_common.h"
#include "llama/ll_growable_array.h"
#include "llama/ll_writable_array.h"
#include "llama/ll_mlcsr_helpers.h"

#include <climits>
#include <unordered_map>

#define PARALLEL_FREE_W_NODES

#define FOUR_NULLS					NULL, NULL, NULL, NULL
#define EIGHT_NULLS					FOUR_NULLS, FOUR_NULLS
#define THIRTY_TWO_NULLS			EIGHT_NULLS, EIGHT_NULLS, EIGHT_NULLS, EIGHT_NULLS

#define LL_MAX_EDGE_PROPERTY_ID		16
#define LL_WRITABLE_USE_MEMORY_POOL

// HACK!!!
#ifdef LL_ONE_VT
#define LL_MAX_EDGE_PROPERTY_ID		0
#endif

#ifdef LL_WRITABLE_USE_MEMORY_POOL
#include "llama/ll_mem_helper.h"
#endif

#ifdef LL_WRITABLE_USE_MEMORY_POOL
#define LL_W_MEM_POOL_MAX_BUFFERS_BITS		15
#define LL_W_MEM_POOL_MAX_BUFFERS			(1ul << (LL_W_MEM_POOL_MAX_BUFFERS_BITS))
#define LL_W_MEM_POOL_MAX_OFFSET_BITS		(26 - LL_MEM_POOL_ALIGN_BITS)
#endif

#ifdef LL_WRITABLE_USE_MEMORY_POOL
#define LL_EDGE_GET_WRITABLE(x)				((w_edge*) (__w_pool.pointer( \
				(((x) >> LL_W_MEM_POOL_MAX_OFFSET_BITS) \
					 & (LL_W_MEM_POOL_MAX_BUFFERS - 1)), \
				((x) & ((1ul << LL_W_MEM_POOL_MAX_OFFSET_BITS)-1)) \
					<< LL_MEM_POOL_ALIGN_BITS)))
#else
#define LL_EDGE_GET_WRITABLE(x)				((w_edge*) (LL_EDGE_INDEX(x)))
#endif

#define LL_W_EDGE_CREATE(edge)				(((w_edge*) (long) (edge))->we_public_id)

#ifdef LL_WRITABLE_USE_MEMORY_POOL
#ifdef LL_NODE32
#	if (32 - LL_BITS_LEVEL) < (LL_W_MEM_POOL_MAX_BUFFERS_BITS + LL_W_MEM_POOL_MAX_OFFSET_BITS)
#		error "Not enough bits to encode the w_edge position in the memory pool"
#	endif
#else
#	if (64 - LL_BITS_LEVEL) < (LL_W_MEM_POOL_MAX_BUFFERS_BITS + LL_W_MEM_POOL_MAX_OFFSET_BITS)
#		error "Not enough bits to encode the w_edge position in the memory pool"
#	endif
#endif
#endif


//==========================================================================//
// Class: w_edge                                                            //
//==========================================================================//

/**
 * A writable edge
 */
class w_edge {

public:

	union {
		struct {

			/// The target endpoint
			node_t we_target;

			/// The source endpoint
			node_t we_source;
		};

		/// The next edge in the free list
		struct w_edge* we_next;
	};

	/// A numerical ID
	edge_t we_numerical_id;

	union {		// TODO Check if using union here is okay

		/// A numerical ID of the reverse edge
		edge_t we_reverse_numerical_id;

		/// The public node ID
		edge_t we_public_id;

	};

#ifdef LL_TIMESTAMPS

	/// The creation timestamp
	long we_timestamp_creation;

	/// The deletion timestamp
	long we_timestamp_deletion;

#else

	/// Deletion
	bool we_deleted;

#endif

	/// The property spinlock
	ll_spinlock_t we_properties_spinlock;

	/// The 32-bit properties
	uint32_t we_properties_32[LL_MAX_EDGE_PROPERTY_ID];

	/// The 64-bit properties: Property ID --> (destructor, value)
	std::pair<void (*)(const uint64_t&), uint64_t>
		we_properties_64[LL_MAX_EDGE_PROPERTY_ID];

#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES

	/// The frozen edge that this edge supersedes
	edge_t we_supersedes;

#endif


public:

	/**
	 * Create an instance of type w_edge
	 */
	w_edge() {

		we_properties_spinlock = 0;

		memset(we_properties_32, 0, sizeof(we_properties_32));
		memset(we_properties_64, 0, sizeof(we_properties_64));

#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES
		we_supersedes = LL_NIL_EDGE;
#endif
	}


	/**
	 * Destructor
	 */
	~w_edge() {
		clear();
	}


	/**
	 * Clear
	 */
	void clear(void) {

		for (int i = 0; i < LL_MAX_EDGE_PROPERTY_ID; i++) {
			std::pair<void (*)(const uint64_t&), uint64_t>& p = we_properties_64[i];
			if (p.first != NULL) p.first(p.second);
		}

		memset(we_properties_32, 0, sizeof(we_properties_32));
		memset(we_properties_64, 0, sizeof(we_properties_64));

		we_properties_spinlock = 0;

#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES
		we_supersedes = LL_NIL_EDGE;
#endif

#ifdef LL_TIMESTAMPS
		we_timestamp_deletion = LONG_MAX;
#else
		we_deleted = false;
#endif
	}


	/**
	 * Determine whether the edge still exists
	 *
	 * @return true if it still exists
	 */
	inline bool exists(void) {
#ifdef LL_TIMESTAMPS
		return we_timestamp_deletion == LONG_MAX;
#else
		return !we_deleted;
#endif
	}


	/**
	 * Get the value of a 32-bit property
	 *
	 * @param property_id the property ID
	 * @return the value, or 0 if not found
	 */
	template <typename T>
	inline T get_property_32(int property_id) {
		return we_properties_32[property_id];
	}


	/**
	 * Set the value of a 32-bit property
	 *
	 * @param property_id the property ID
	 * @param value the value
	 */
	template <typename T>
	void set_property_32(int property_id, T value) {
		we_properties_32[property_id] = value;
	}


	/**
	 * Atomicaly add the value to a 32-bit property
	 *
	 * @param property_id the property ID
	 * @param value the value
	 * @return the new value
	 */
	template <typename T>
	T add_property_32(int property_id, T value) {
		return __sync_add_and_fetch(&we_properties_32[property_id], value);
	}


	/**
	 * Get the value of a 64-bit property
	 *
	 * @param property_id the property ID
	 * @return the value, or 0 if not found
	 */
	template <typename T>
	inline T get_property_64(int property_id) {
		return we_properties_64[property_id].second;
	}


	/**
	 * Set the value of a 64-bit property
	 *
	 * @param property_id the property ID
	 * @param value the value
	 * @param destructor the destructor
	 */
	template <typename T>
	void set_property_64(int property_id, T value,
			void (*destructor)(const uint64_t&) = NULL) {
		
		ll_spinlock_acquire(&we_properties_spinlock);

		std::pair<void (*)(const uint64_t&), uint64_t>& p = we_properties_64[property_id];
		if (p.first != NULL) p.first(p.second);
		p.first = destructor;
		p.second = value;

		ll_spinlock_release(&we_properties_spinlock);
	}


	/**
	 * Atomicaly add the value to a 64-bit property. This ASSUMES that the value destructor
	 * is NULL, so please use with caution.
	 *
	 * @param property_id the property ID
	 * @param value the value
	 * @return the new value
	 */
	template <typename T>
	T add_property_64(int property_id, T value) {
		return __sync_add_and_fetch(&we_properties_64[property_id].second, value);
	}
};



//==========================================================================//
// Edge Allocator and Deallocator                                           //
//==========================================================================//

/**
 * The writable edge NOOP deallocator
 */
struct w_edge_noop {

	/**
	 * Do nothing with a writable edge
	 *
	 * @param edge the writable edge
	 */
	void operator() (w_edge* edge) {
	}
};


#ifdef LL_WRITABLE_USE_MEMORY_POOL

static ll_memory_pool __w_pool;


/**
 * Generic allocator
 */
struct w_generic_allocator {

	/**
	 * Allocate a new object
	 *
	 * @param size the number of bytes
	 * @return the new object
	 */
	void* operator() (size_t size) {
		return __w_pool.allocate<char>(size);
	}
};


/**
 * The writable edge allocator
 */
struct w_edge_allocator {

	/**
	 * Allocate a new writable edge
	 *
	 * @return the writable edge
	 */
	w_edge* operator() (void) {
		w_edge* w = __w_pool.allocate<w_edge>();
		new (w) w_edge();
		return w;
	}

	/**
	 * Allocate a new writable edge
	 *
	 * @param o_chunk the pointer to store the chunk number
	 * @param o_offset the pointer to store the offset within the chunk
	 * @return the writable edge
	 */
	w_edge* operator() (size_t* o_chunk, size_t* o_offset) {
		w_edge* w = __w_pool.allocate<w_edge>(1, o_chunk, o_offset);
		new (w) w_edge();
		return w;
	}
};


/**
 * The writable edge deallocator
 */
struct w_edge_deallocator {

	/**
	 * Deallocate a writable edge
	 *
	 * @param edge the writable edge
	 */
	void operator() (w_edge* edge) {
		// Nothing to do
		// XXX This does not call destructor on edge property values
	}
};


/**
 * Helper types
 */

/// The out-edges
typedef ll_growable_array<w_edge*, 4, w_edge_deallocator, false,
		w_generic_allocator, ll_nop_deallocator<void*>, false> ll_w_out_edges_t;

/// The in-edges
typedef ll_growable_array<w_edge*, 4, w_edge_noop, false,
		w_generic_allocator, ll_nop_deallocator<void*>, false> ll_w_in_edges_t;


#else /* LL_WRITABLE_USE_MEMORY_POOL */

#define FREE_W_EDGES_LENGTH		(4*8)
static w_edge* __free_w_edges[FREE_W_EDGES_LENGTH] = { THIRTY_TWO_NULLS };


/**
 * The writable edge allocator
 */
struct w_edge_allocator {

	/**
	 * Allocate a new writable edge
	 *
	 * @return the writable edge
	 */
	w_edge* operator() (void) {

		for (int i = 0; i < FREE_W_EDGES_LENGTH; i += 8) {
			w_edge* x = __free_w_edges[i];

			if (x != NULL) {
				if (__sync_bool_compare_and_swap(&__free_w_edges[i], x,
							x->we_next)) {
					x->we_next = (w_edge*) 0;
					return x;
				}
			}
		}

		return new w_edge();
	}
};


/**
 * The writable edge deallocator
 */
struct w_edge_deallocator {

	/**
	 * Deallocate a writable edge
	 *
	 * @param edge the writable edge
	 */
	void operator() (w_edge* edge) {

		int i = 8 * (int) (((((long) edge) / sizeof(w_edge)) >> 6)
				% (FREE_W_EDGES_LENGTH / 8));
		// or: int i = omp_get_thread_num() << 3;

		w_edge* n = edge;
		n->clear();

		w_edge* x;
		do {
			x = __free_w_edges[i];
			n->we_next = x;
			__COMPILER_FENCE;
		}
		while (!__sync_bool_compare_and_swap(&__free_w_edges[i], x, n));

		/* If we are guaranteed to be single-threaded, we can switch to:
			w_edge* x = __free_w_edges[i];
			n->we_next = x;
			__free_w_edges[i] = n;*/
	}
};


/**
 * Delete all w_edge's in the free list
 */
inline void delete_free_w_edges(void) {

#pragma omp critical 
	{
		for (int i = 0; i < FREE_W_EDGES_LENGTH; i++) {
			w_edge* x = __free_w_edges[i];
			if (x != NULL) {
				__free_w_edges[i] = NULL;
				while (x != NULL) {
					w_edge* next = x->we_next;
					delete x;
					x = next;
				}
			}
		}
	}
}


/**
 * Helper types
 */

/// The out-edges
typedef ll_growable_array<w_edge*, 4, w_edge_deallocator> ll_w_out_edges_t;

/// The in-edges
typedef ll_growable_array<w_edge*, 4, w_edge_noop, false> ll_w_in_edges_t;

#endif



//==========================================================================//
// Class: w_node                                                            //
//==========================================================================//

/**
 * A writable node
 */
class w_node {

public:

	/// Update lock
	ll_spinlock_t wn_lock;

	/// The out-edges
	ll_w_out_edges_t wn_out_edges;
	
	/// The in-edges
	ll_w_in_edges_t wn_in_edges;

	union {
		struct {

			// TODO short or unigned or int?

			/// The number of added and not yet deleted out-edges
			unsigned short wn_out_edges_delta;

			/// The number of added and not yet deleted in-edges
			unsigned short wn_in_edges_delta;

			/// The number of deleted out-edges
			unsigned short wn_num_deleted_out_edges;

			/// The number of deleted in-edges
			unsigned short wn_num_deleted_in_edges;
		};

		/// The "next" pointer in the free list
		w_node* wn_next;
	};

#ifdef LL_TIMESTAMPS

	/// The creation timestamp
	long wn_timestamp_creation;

	/// The update timestamp
	long wn_timestamp_update;

	/// The deletion timestamp
	long wn_timestamp_deletion;

#else

	/// The deletion timestamp
	bool wn_deleted;

#endif


	/**
	 * Create an instance of w_node
	 */
	w_node(void) {

		wn_lock = 0;
		wn_out_edges_delta = 0;
		wn_in_edges_delta = 0;
		wn_next = NULL;

#ifdef LL_TIMESTAMPS
		wn_timestamp_creation = 0;
		wn_timestamp_update   = 0;
		wn_timestamp_deletion = LONG_MAX;
#else
		wn_deleted = false;
#endif

		wn_num_deleted_out_edges = 0;
		wn_num_deleted_in_edges = 0;
	}


	/**
	 * Clear the data
	 */
	void clear(void) {

		wn_lock = 0;
		wn_out_edges_delta = 0;
		wn_in_edges_delta = 0;

		wn_out_edges.clear();
		wn_in_edges.clear();

#ifdef LL_TIMESTAMPS
		wn_timestamp_creation = 0;
		wn_timestamp_update   = 0;
		wn_timestamp_deletion = LONG_MAX;
#else
		wn_deleted = false;
#endif

		wn_num_deleted_out_edges = 0;
		wn_num_deleted_in_edges = 0;
	}


	/**
	 * Determine whether the node still exists
	 *
	 * @return true if it still exists
	 */
	inline bool exists(void) {
#ifdef LL_TIMESTAMPS
		return wn_timestamp_deletion == LONG_MAX;
#else
		return !wn_deleted;
#endif
	}
};



//==========================================================================//
// Node Allocator and Deallocator                                           //
//==========================================================================//

#ifdef LL_WRITABLE_USE_MEMORY_POOL

/**
 * The writable node allocator
 */
template <typename Output = w_node*>
struct w_node_allocator_ext {

	/**
	 * Allocate a new writable node
	 *
	 * @return the writable node
	 */
	Output operator() (void) {
		w_node* w = __w_pool.allocate<w_node>();
		new (w) w_node();
		return (Output) w;
	}
};


/**
 * The default writable node allocator
 */
typedef struct w_node_allocator_ext<> w_node_allocator;


/**
 * The writable node deallocator
 */
template <typename Input = w_node*>
struct w_node_deallocator_ext {

	/**
	 * Deallocate a writable node
	 *
	 * @param node the writable node
	 */
	void operator() (Input node) {
		// Nothing to do
		// XXX This does not call destructor on node property values
	}
};


/**
 * The default writable node deallocator
 */
typedef struct w_node_deallocator_ext<> w_node_deallocator;


/**
 * Delete all w_node's in the free list
 */
inline void ll_free_w_pool(void) {
	__w_pool.free();
}


#else /* LL_WRITABLE_USE_MEMORY_POOL */

#define FREE_W_NODES_LENGTH			4
static w_node* __free_w_nodes[FREE_W_NODES_LENGTH] = { FOUR_NULLS };


/**
 * The writable node allocator
 */
template <typename Output = w_node*>
struct w_node_allocator_ext {

	/**
	 * Allocate a new writable node
	 *
	 * @return the writable node
	 */
	Output operator() (void) {

		for (int i = 0; i < FREE_W_NODES_LENGTH; i++) {
			w_node* x = __free_w_nodes[i];

			if (x != NULL) {
				if (__sync_bool_compare_and_swap(&__free_w_nodes[i], x,
							x->wn_next)) {
					x->wn_next = (w_node*) 0;
					return (Output) x;
				}
			}
		}

		return (Output) (new w_node());
	}
};


/**
 * The default writable node allocator
 */
typedef struct w_node_allocator_ext<> w_node_allocator;


#ifdef PARALLEL_FREE_W_NODES

/**
 * The writable node deallocator
 */
template <typename Input = w_node*>
struct w_node_deallocator_ext {

	/**
	 * Deallocate a writable node
	 *
	 * @param node the writable node
	 */
	void operator() (Input node) {
		//delete (w_node*) node;

		int i = (int) (((((long) node) / sizeof(w_node)) >> 6) % FREE_W_NODES_LENGTH);
		w_node* n = (w_node*) node;
		n->clear();

		w_node* x;
		do {
			x = __free_w_nodes[i];
			n->wn_next = x;
			__COMPILER_FENCE;
		}
		while (!__sync_bool_compare_and_swap(&__free_w_nodes[i], x, n));
	}
};


/**
 * The default writable node allocator
 */
typedef struct w_node_deallocator_ext<> w_node_deallocator;


/**
 * Delete all w_node's in the free list
 */
inline void delete_free_w_nodes(void) {

	for (int i = 0; i < FREE_W_NODES_LENGTH; i++) {
		while (true) {
			w_node* x = __free_w_nodes[i];
			if (x == NULL) break;

			if (__sync_bool_compare_and_swap(&__free_w_nodes[i], x, NULL)) {
				while (x != NULL) {
					w_node* next = x->wn_next;
					delete x;
					x = next;
				}
				break;
			}
		}
	}
}


#else

/**
 * The writable node deallocator
 */
template <typename Input = w_node*>
struct w_node_deallocator_ext {

	/**
	 * Deallocate a writable node
	 *
	 * @param node the writable node
	 */
	void operator() (Input node) {
		
		// Must be run inside a critical section

		w_node* n = (w_node*) node;
		n->clear();

		int i = rand() % FREE_W_NODES_LENGTH;
			//or: (int) ((((long) node) >> 8) % FREE_W_NODES_LENGTH);
		/*n->wn_next = __free_w_nodes;
		__free_w_nodes = n;*/
		n->wn_next = *((w_node**) &__free_w_nodes[i]);
		*((w_node**) &__free_w_nodes[i]) = n;
	}
};


/**
 * The default writable node allocator
 */
typedef struct w_node_deallocator_ext<> w_node_deallocator;


/**
 * Delete all w_node's in the free list
 */
inline void delete_free_w_nodes(void) {

#pragma omp critical 
	{
		for (int i = 0; i < FREE_W_NODES_LENGTH; i++) {
			w_node* x = *((w_node**) &__free_w_nodes[i]);
			if (x != NULL) {
				//__free_w_nodes = NULL;
				*((w_node**) &__free_w_nodes[i]) = NULL;
				while (x != NULL) {
					w_node* next = x->wn_next;
					delete x;
					x = next;
				}
			}
		}
	}
}

#endif

#endif /* ! LL_WRITABLE_USE_MEMORY_POOL */

#endif /* LL_WRITABLE_ELEMENTS_H_ */

