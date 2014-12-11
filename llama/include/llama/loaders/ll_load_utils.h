/*
 * ll_load_utils.h
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


#ifndef LL_LOAD_UTILS_H_
#define LL_LOAD_UTILS_H_

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

#include <string>
#include <vector>

#include "llama/ll_config.h"
#include "llama/ll_streaming.h"
#include "llama/ll_external_sort.h"
#include "llama/loaders/ll_load_async_writable.h"


// High-level configuration

//#define LL_LOAD_CREATE_REV_EDGE_MAP



/**
 * A stateless file loader prototype
 */
class ll_file_loader {

public:

	/**
	 * Create a new instance of ll_file_loader
	 */
	ll_file_loader() {}


	/**
	 * Destroy the instance
	 */
	virtual ~ll_file_loader() {}


	/**
	 * Determine if this file can be opened by this loader
	 *
	 * @param file the file
	 * @return true if it can be opened
	 */
	virtual bool accepts(const char* file) = 0;


	/**
	 * Load directly into the read-only representation by creating a new
	 * level
	 *
	 * @param graph the graph
	 * @param file the file
	 * @param config the loader configuration
	 */
	virtual void load_direct(ll_mlcsr_ro_graph* graph, const char* file,
			const ll_loader_config* config) = 0;


	/**
	 * Load directly into the read-only representation by creating a new
	 * level
	 *
	 * @param graph the graph
	 * @param file the file
	 * @param config the loader configuration
	 */
	virtual void load_direct(ll_writable_graph* graph, const char* file,
			const ll_loader_config* config) {

		graph->checkpoint(config);
		load_direct(&graph->ro_graph(), file, config);
		graph->callback_ro_changed();
	}


	/**
	 * Load incrementally into the writable representation
	 *
	 * @param graph the graph
	 * @param file the file
	 * @param config the loader configuration
	 */
	virtual void load_incremental(ll_writable_graph* graph, const char* file,
			const ll_loader_config* config) = 0;


	/**
	 * Create a data source object for the given file
	 *
	 * @param file the file
	 * @return the data source
	 */
	virtual ll_data_source* create_data_source(const char* file) = 0;
};



/**
 * A generic edge-list loader
 */
template <typename NodeType, bool HasWeight=false,
		typename WeightType=float, int WeightTypeCode=LL_T_FLOAT>
class ll_edge_list_loader : public ll_data_source {

	/**
	 * Item format for external sort - for out-edges
	 */
	struct xs_edge {
		NodeType tail;
		NodeType head;
	};

	/**
	 * Comparator for xs_edge
	 */
	struct xs_edge_comparator {
		bool operator() (const xs_edge& a, const xs_edge& b) {
			if (a.tail != b.tail)
				return a.tail < b.tail;
			else
				return a.head < b.head;
		}
	};

	/**
	 * Item format for external sort - for out-edges
	 */
	struct xs_w_edge {
		NodeType tail;
		NodeType head;
		WeightType weight;
	};

	/**
	 * Comparator for xs_w_edge
	 */
	struct xs_w_edge_comparator {
		bool operator() (const xs_w_edge& a, const xs_w_edge& b) {
			if (a.tail != b.tail)
				return a.tail < b.tail;
			else
				return a.head < b.head;
		}
	};

	/**
	 * Item format for external sort - for in-edges
	 */
	struct xs_in_edge {
		NodeType tail;
		NodeType head;
#ifdef LL_LOAD_CREATE_REV_EDGE_MAP
		edge_t out_edge;
#endif
	};

	/**
	 * Comparator for xs_in_edge
	 */
	struct xs_in_edge_comparator {
		bool operator() (const xs_in_edge& a, const xs_in_edge& b) {
			if (a.head != b.head)
				return a.head < b.head;
			else
				return a.tail < b.tail;
		}
	};


private:

	/// True if the data file has still potentially more data left in it
	volatile bool _has_more;

	/// The last value of _has_more
	volatile bool _last_has_more;


public:

	/**
	 * Create an instance of class ll_edge_list_loader
	 */
	ll_edge_list_loader() {
		_has_more = true;
		_last_has_more = _has_more;
	}


	/**
	 * Destroy the loader
	 */
	virtual ~ll_edge_list_loader() {
	}


protected:

	/**
	 * Read the next edge
	 *
	 * @param o_tail the output for tail
	 * @param o_head the output for head
	 * @param o_weight the output for weight (ignore if HasWeight is false)
	 * @return true if the edge was loaded, false if EOF or error
	 */
	virtual bool next_edge(NodeType* o_tail, NodeType* o_head,
			WeightType* o_weight) = 0;


	/**
	 * Rewind the input file
	 */
	virtual void rewind() = 0;


	/**
	 * Get graph stats if they are available
	 *
	 * @param o_nodes the output for the number of nodes (1 + max node ID)
	 * @param o_edges the output for the number of edges
	 * @return true if succeeded, or false if not or the info is not available
	 */
	virtual bool stat(size_t* o_nodes, size_t* o_edges) {
		return false;
	}


public:

	/**
	 * Return true if the data file has potentially more data in it
	 *
	 * @return true if it has more data left
	 */
	virtual bool has_more() {
		return _has_more;
	}


	/**
	 * Load the graph directly into the read-only representation
	 *
	 * @param graph the graph
	 * @param config the loader configuration
	 * @return true on no error
	 */
	bool load_direct(ll_mlcsr_ro_graph* graph,
			const ll_loader_config* config) {

#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES
		LL_E_PRINT("LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES not supported");
		abort();
#endif


		// Check if we have stat and if we can load the data just using the
		// info stat gives us.
		
		// Specifically, we need to be able to get a reasonable estimate of the
		// max size of the edge table, which gets tricky on levels > 0 when
		// copying adjacency lists or deleting using continuations. Loading
		// levels > 0 with continuations should work, but it will result in
		// reserving space for all continuations, which will be a big waste of
		// space in many (most?) cases, so disable it for now (we should
		// reenable it when we implement variable-sized edge tables or shrinking
		// of edge tables).

		// TODO Avoid calling stat twice

		size_t new_level = graph->num_levels();
		size_t max_nodes = 0;
		size_t max_edges = 0;

		if (IF_LL_MLCSR_CONTINUATIONS(new_level == 0 &&)
				stat(&max_nodes, &max_edges)) {
			return load_direct_with_stat(graph, config);
		}

		LL_D_PRINT("Load without stat, level=%lu\n", new_level);


		// Check features

		feature_vector_t features;
		features << LL_L_FEATURE(lc_direction);
		features << LL_L_FEATURE(lc_reverse_edges);
		features << LL_L_FEATURE(lc_deduplicate);
		features << LL_L_FEATURE(lc_no_properties);

		config->assert_features(false /*direct*/, true /*error*/, features);


		// Initialize

		bool print_progress = config->lc_print_progress;
		bool reverse = config->lc_reverse_edges;
		bool load_weight = !config->lc_no_properties;

		xs_w_edge e;

		if (new_level > 0) {
			if (max_nodes < (size_t) graph->out().max_nodes()) {
				max_nodes = graph->out().max_nodes();
			}
		}


		// Initialize external sort

		ll_external_sort<xs_w_edge, xs_w_edge_comparator>* out_sort = NULL;


		// Get the degrees

		size_t degrees_capacity = 80 * 1000ul * 1000ul;
		degree_t* degrees_out = NULL;
		degree_t* degrees_in = NULL;
		
		degrees_out = (degree_t*) malloc(sizeof(*degrees_out)*degrees_capacity);
		memset(degrees_out, 0, sizeof(*degrees_out) * degrees_capacity);
		if (reverse) {
			degrees_in = (degree_t*) malloc(
					sizeof(*degrees_in) * degrees_capacity);
			memset(degrees_in, 0, sizeof(*degrees_in) * degrees_capacity);
		}


		/*
		 * PASS 1
		 *   - Determine the node degrees
		 *   - Feed the edges to the external sort (if not already sorted)
		 */

		NodeType last_tail = 0;
		NodeType last_head = 0;

		bool already_sorted = true;
		bool out_called_sort = false;
		if (config->lc_direction == LL_L_UNDIRECTED_DOUBLE) {
			already_sorted = false;
			out_sort = new ll_external_sort<xs_w_edge,
					 xs_w_edge_comparator>(config);
		}

		size_t step = 10 * 1000 * 1000ul;
		if (print_progress) {
			fprintf(stderr, "[<]");
		}


		// XXX This split should be done instead if
		// config->lc_direction == LL_L_UNDIRECTED_DOUBLE,
		// since that's guaranteed to mess up the sort order,
		// while config->lc_deduplicate does not necessarily
		// mess up things

		if (config->lc_deduplicate) {

			if (already_sorted) {
				already_sorted = false;
				out_sort = new ll_external_sort<xs_w_edge,
						 xs_w_edge_comparator>(config);
			}

			while (next_edge(&e.tail, &e.head, &e.weight)) {
				max_edges++;

				if (config->lc_direction == LL_L_UNDIRECTED_ORDERED) {
					if (e.tail > e.head) {
						unsigned x = e.tail; e.tail = e.head; e.head = x;
					}
				}
				if (last_head == e.head && last_tail == e.tail) {
					continue;
				}

				last_head = e.head;
				last_tail = e.tail;

				if (e.tail >= (NodeType) max_nodes) max_nodes = e.tail + 1;
				if (e.head >= (NodeType) max_nodes) max_nodes = e.head + 1;

				*out_sort << e;

				if (config->lc_direction == LL_L_UNDIRECTED_DOUBLE) {
					if (e.tail != e.head) {
						unsigned x = e.tail; e.tail = e.head; e.head = x;
						*out_sort << e;
						max_edges++;
					}
				}

				if (print_progress) {
					if (max_edges % step == 0) {
						fprintf(stderr, ".");
						if (max_edges % (step * 10) == 0) {
							fprintf(stderr, "%lu", max_edges / 1000000ul);
						}
					}
				}
			}

			if (max_nodes > degrees_capacity) {
				size_t d = max_nodes;

				degree_t* x = (degree_t*) realloc(degrees_out,
						sizeof(*degrees_out)*d);
				memset(&x[degrees_capacity], 0,
						sizeof(*x)*(d-degrees_capacity));
				degrees_out = x;

				if (reverse) {
					x = (degree_t*) realloc(degrees_in, sizeof(*degrees_in)*d);
					memset(&x[degrees_capacity], 0,
							sizeof(*x)*(d-degrees_capacity));
					degrees_in = x;
				}

				degrees_capacity = d;
			}

			out_sort->sort();
			out_called_sort = true;

			xs_w_edge* buffer;
			size_t length;
			size_t index = 0;

			last_tail = LL_NIL_NODE;
			last_head = LL_NIL_NODE;

			if (print_progress) {
				fprintf(stderr, "[+]");
			}

			while (out_sort->next_block(&buffer, &length)) {
				while (length --> 0) {
					if (last_head == buffer->head
							&& last_tail == buffer->tail) {
						buffer++;
						continue;
					}

					last_head = buffer->head;
					last_tail = buffer->tail;

					degrees_out[buffer->tail]++;
					if (reverse) degrees_in[buffer->head]++;
					buffer++;

					index++;

					if (print_progress) {
						if (index % step == 0) {
							fprintf(stderr, ".");
							if (index % (step * 10) == 0) {
								fprintf(stderr, "%lu", index / 1000000ul);
							}
						}
					}
				}
			}
		}
		else /* if (!config->lc_deduplicate) */ {

			size_t loaded_edges = 0;

			while (next_edge(&e.tail, &e.head, &e.weight)) {
				max_edges++;
				loaded_edges++;

				if (config->lc_direction == LL_L_UNDIRECTED_ORDERED) {
					if (e.tail > e.head) {
						unsigned x = e.tail; e.tail = e.head; e.head = x;
					}
				}

				if (already_sorted) {
					if (last_tail > e.tail
							|| (last_tail == e.tail && last_head > e.head)) {
						already_sorted = false;
						loaded_edges = 0;

						rewind();

						max_edges = 0;
						memset(degrees_out, 0, sizeof(*degrees_out)
								* degrees_capacity);
						if (reverse) {
							memset(degrees_in, 0, sizeof(*degrees_in)
									* degrees_capacity);
						}

						out_sort = new ll_external_sort<xs_w_edge,
								 xs_w_edge_comparator>(config);
						continue;
					}
				}

				last_head = e.head;
				last_tail = e.tail;

				if (e.tail >= (NodeType) max_nodes) max_nodes = e.tail + 1;
				if (e.head >= (NodeType) max_nodes) max_nodes = e.head + 1;

				if (max_nodes > degrees_capacity) {
					size_t d = degrees_capacity;
					while (d < max_nodes + 16) d *= 2;

					degree_t* x = (degree_t*) realloc(degrees_out,
							sizeof(*degrees_out)*d);
					memset(&x[degrees_capacity], 0,
							sizeof(*x)*(d-degrees_capacity));
					degrees_out = x;

					if (reverse) {
						x = (degree_t*) realloc(degrees_in,
								sizeof(*degrees_in)*d);
						memset(&x[degrees_capacity], 0,
								sizeof(*x)*(d-degrees_capacity));
						degrees_in = x;
					}

					degrees_capacity = d;
				}

				degrees_out[e.tail]++;
				if (reverse) degrees_in[e.head]++;

				if (!already_sorted) {
					*out_sort << e;
				}

				if (config->lc_direction == LL_L_UNDIRECTED_DOUBLE) {
					if (e.tail != e.head) {
						unsigned x = e.tail; e.tail = e.head; e.head = x;
						max_edges++;

						degrees_out[e.tail]++;
						if (reverse) degrees_in[e.head]++;

						if (!already_sorted) {
							*out_sort << e;
						}
					}
				}

				if (print_progress) {
					if (loaded_edges % step == 0) {
						fprintf(stderr, ".");
						if (loaded_edges % (step * 10) == 0) {
							fprintf(stderr, "%lu", loaded_edges / 1000000ul);
						}
					}
				}
			} /* Ends: while(...) { ... } */

		} /* Ends: if (config->lc_deduplicate) { ... } else { ... } */


		/*
		 * PASS 2
		 *   - Write out the level, either by re-reading the input file if it
		 *     is sorted or by pulling them out of external sort
		 *   - Feed the edges into the in-edges external sort
		 */

		// Create the out-edges level

		auto& out = graph->out();
		out.init_level_from_degrees(max_nodes, degrees_out, NULL); 

		LL_ET<node_t>* et = graph->out().edge_table(new_level);
		auto* vt = out.vertex_table(new_level); (void) vt;


		// If the out-to-in, in-to-out properties are not enabled, disable
		// that feature in the corresponding ll_csr_base

		if (!config->lc_reverse_edges || !config->lc_reverse_maps) {
			graph->out().set_edge_translation(false);
			graph->in().set_edge_translation(false);
		}


		// Initialize the weight property

		ll_mlcsr_edge_property<WeightType>* prop_weight = NULL;
		if (load_weight) prop_weight = init_prop_weight(graph);


		// Initialize the external sort for the in-edges

		ll_external_sort<xs_in_edge, xs_in_edge_comparator>* in_sort = NULL;
		if (reverse) {
			in_sort = new ll_external_sort<xs_in_edge,
					xs_in_edge_comparator>(config);
		}


		// Write the out-edges

		if (print_progress) {
			fprintf(stderr, "[O]");
		}

		if (already_sorted) {
			assert(config->lc_direction != LL_L_UNDIRECTED_DOUBLE);
			rewind();

			last_head = LL_NIL_NODE;
			last_tail = LL_NIL_NODE;

			size_t index = 0;
			while (next_edge(&e.tail, &e.head, &e.weight)) {

				if (config->lc_direction == LL_L_UNDIRECTED_ORDERED) {
					if (e.tail > e.head) {
						unsigned x = e.tail; e.tail = e.head; e.head = x;
					}
				}

				if (config->lc_deduplicate && last_head == e.head
						&& last_tail == e.tail) {
					continue;
				}

#ifdef LL_MLCSR_CONTINUATIONS
				if (last_tail != e.tail) {
					auto& vt_value = (*vt)[e.tail];
					assert(LL_EDGE_LEVEL(vt_value.adj_list_start)
							== new_level);
					index = LL_EDGE_INDEX(vt_value.adj_list_start);
				}
#endif

				last_head = e.head;
				last_tail = e.tail;

				(*et)[index] = LL_VALUE_CREATE((node_t) e.head);

				if (HasWeight && load_weight) {
					edge_t edge = LL_EDGE_CREATE(new_level, index);
					prop_weight->cow_write(edge, e.weight);
				}

				if (reverse) {
					xs_in_edge x;
					x.head = e.head;
					x.tail = e.tail;
#ifdef LL_LOAD_CREATE_REV_EDGE_MAP
					x.out_edge = edge;
#endif
					*in_sort << x;
				}

				index++;

				if (print_progress) {
					if (index % step == 0) {
						fprintf(stderr, ".");
						if (index % (step * 10) == 0) {
							fprintf(stderr, "%lu", index / 1000000ul);
						}
					}
				}
			}

#ifndef LL_MLCSR_CONTINUATIONS
			assert(index == max_edges);
#endif
		}
		else /* if (!already_sorted) */ {

			if (out_called_sort)
				out_sort->rewind_sorted();
			else
				out_sort->sort();

			xs_w_edge* buffer;
			size_t length;
			size_t index = 0;

			last_tail = LL_NIL_NODE;
			last_head = LL_NIL_NODE;

			while (out_sort->next_block(&buffer, &length)) {
				while (length --> 0) {

					if (config->lc_deduplicate && last_head == buffer->head
							&& last_tail == buffer->tail) {
						buffer++;
						continue;
					}

#ifdef LL_MLCSR_CONTINUATIONS
					if (last_tail != buffer->tail) {
						auto& vt_value = (*vt)[buffer->tail];
						assert(LL_EDGE_LEVEL(vt_value.adj_list_start)
								== new_level);
						index = LL_EDGE_INDEX(vt_value.adj_list_start);
					}
#endif

					last_head = buffer->head;
					last_tail = buffer->tail;

					(*et)[index] = LL_VALUE_CREATE((node_t) buffer->head);

					if (HasWeight && load_weight) {
						edge_t edge = LL_EDGE_CREATE(new_level, index);
						prop_weight->cow_write(edge, e.weight);
					}

					if (reverse) {
						xs_in_edge x;
						x.head = buffer->head;
						x.tail = buffer->tail;
#ifdef LL_LOAD_CREATE_REV_EDGE_MAP
						x.out_edge = edge;
#endif
						*in_sort << x;
					}

					index++;
					buffer++;

					if (print_progress) {
						if (index % step == 0) {
							fprintf(stderr, ".");
							if (index % (step * 10) == 0) {
								fprintf(stderr, "%lu", index / 1000000ul);
							}
						}
					}
				}
			}

			delete out_sort;
			out_sort = NULL;

		} /* Ends: if (already_sorted) { ... } else { ... } */


		graph->out().finish_level_edges();

		if (HasWeight && load_weight) {
			prop_weight->finish_level();
		}


		/*
		 * PASS 3
		 *   - Compute the in-edges, if applicable
		 */

		// Do the in-edges

		if (reverse) {

			if (print_progress) {
				fprintf(stderr, "[I]");
			}

			graph->in().init_level_from_degrees(max_nodes, degrees_in, NULL); 
			et = graph->in().edge_table(new_level);
			vt = graph->in().vertex_table(new_level); (void) vt;


			// If the out-to-in, in-to-out properties are not enabled, disable
			// that feature in the corresponding ll_csr_base

			if (!config->lc_reverse_edges || !config->lc_reverse_maps) {
				graph->in().set_edge_translation(false);
			}


			// Sort the in edges and load them

			in_sort->sort();

			xs_in_edge* buffer;
			size_t length;
			size_t index = 0;

			last_head = LL_NIL_NODE;
			last_tail = LL_NIL_NODE;

			while (in_sort->next_block(&buffer, &length)) {
				while (length --> 0) {

#ifdef LL_MLCSR_CONTINUATIONS
					if (last_head != buffer->head) {
						auto& vt_value = (*vt)[buffer->head];
						assert(LL_EDGE_LEVEL(vt_value.adj_list_start)
								== new_level);
						index = LL_EDGE_INDEX(vt_value.adj_list_start);
					}
#endif

					last_head = buffer->head;
					last_tail = buffer->tail;

					(*et)[index] = LL_VALUE_CREATE((node_t) buffer->tail);

					// TODO Do the out-to-in, in-to-out properties if desired

					index++;
					buffer++;

					if (print_progress) {
						if (index % step == 0) {
							fprintf(stderr, ".");
							if (index % (step * 10) == 0) {
								fprintf(stderr, "%lu", index / 1000000ul);
							}
						}
					}
				}
			}

			delete in_sort;
			in_sort = NULL;

			graph->in().finish_level_edges();
		}


		// Finish

		if (reverse) free(degrees_in);
		free(degrees_out);

		_last_has_more = _has_more;
		_has_more = false;

		return true;
	}


	/**
	 * Load the data into one or more queues of requests
	 *
	 * @param request_queues the request queues
	 * @param num_stripes the number of stripes (queues array length)
	 * @param config the loader configuration
	 * @return true if there are more edges to load
	 */
	bool load_to_request_queues(ll_la_request_queue** request_queues,
			size_t num_stripes, const ll_loader_config* config) {


		// Check features

		feature_vector_t features;
		features << LL_L_FEATURE(lc_max_edges);
		features << LL_L_FEATURE(lc_no_properties);

		config->assert_features(false /*direct*/, true /*error*/, features);


		// Initializie

		size_t max_edges = 0;
		size_t chunk_size = config->lc_max_edges;
		bool load_weight = !config->lc_no_properties;

		xs_w_edge e;
		bool has_more;

		while ((has_more = next_edge(&e.tail, &e.head, &e.weight))) {
			max_edges++;

			LL_D_NODE2_PRINT(e.tail, e.head, "%u --> %u\n", (unsigned) e.tail,
					(unsigned) e.head);

			ll_la_request_with_edge_properties* request;

			if (HasWeight && load_weight) {
				// XXX
				//LL_NOT_IMPLEMENTED;
			}

#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES
			request = new ll_la_add_edge_for_streaming_with_weights
				<node_t>((node_t) e.tail, (node_t) e.head);
#else
			request = new ll_la_add_edge
				<node_t>((node_t) e.tail, (node_t) e.head);
#endif

			size_t stripe = (e.tail >> (LL_ENTRIES_PER_PAGE_BITS+3))
				% num_stripes;
			request_queues[stripe]->enqueue(request);

			if (chunk_size > 0)
				if (max_edges % chunk_size == 0) break;
		}

		return has_more;
	}


	/**
	 * Load the graph into the writable representation
	 *
	 * @param graph the graph
	 * @param config the loader configuration
	 * @return true on no error
	 */
	bool load_incremental(ll_writable_graph* graph,
			const ll_loader_config* config) {


		// Check features

		feature_vector_t features;
		features << LL_L_FEATURE(lc_max_edges);
		features << LL_L_FEATURE(lc_no_properties);

		config->assert_features(false /*direct*/, true /*error*/, features);


		// Initializie

		size_t num_stripes = omp_get_max_threads();
		ll_la_request_queue* request_queues[num_stripes];
		for (size_t i = 0; i < num_stripes; i++) {
			request_queues[i] = new ll_la_request_queue();
		}

		LL_D_PRINT("Initialize\n");


		// TODO Deduplicate? Unordered?


		// TODO Create nodes

		bool has_more = true;
		while (has_more) {

			graph->tx_begin();

			for (size_t i = 0; i < num_stripes; i++)
				request_queues[i]->shutdown_when_empty(false);

			#pragma omp parallel
			{
				if (omp_get_thread_num() == 0) {

					has_more = this->load_to_request_queues(request_queues,
							num_stripes, config);

					// Add a worker

					for (size_t i = 0; i < num_stripes; i++)
						request_queues[i]->shutdown_when_empty();
					for (size_t i = 0; i < num_stripes; i++)
						request_queues[i]->run(*graph);
				}
				else {
					int t = omp_get_thread_num();
					for (size_t i = 0; i < num_stripes; i++, t++)
						request_queues[t % num_stripes]->worker(*graph);
				}
			}

			graph->tx_commit();

			if (has_more) break;
		}

		_last_has_more = _has_more;
		_has_more = has_more;

		for (size_t i = 0; i < num_stripes; i++) delete request_queues[i];

		return true;
	}


	/**
	 * Load the next batch of data
	 *
	 * @param graph the writable graph
	 * @param max_edges the maximum number of edges
	 * @return true if data was loaded, false if there are no more data
	 */
	virtual bool pull(ll_writable_graph* graph, size_t max_edges) {

		ll_loader_config config;
		config.lc_max_edges = max_edges;

		if (!load_incremental(graph, &config)) abort();

		return _last_has_more;
	}


	/**
	 * Load the next batch of data to request queues
	 *
	 * @param request_queues the request queues
	 * @param num_stripes the number of stripes (queues array length)
	 * @param max_edges the maximum number of edges
	 * @return true if data was loaded, false if there are no more data
	 */
	virtual bool pull(ll_la_request_queue** request_queues, size_t num_stripes,
			size_t max_edges) {

		ll_loader_config config;
		config.lc_max_edges = max_edges;

		bool has_more = load_to_request_queues(request_queues, num_stripes,
				&config);

		_last_has_more = _has_more;
		_has_more = has_more;

		return _last_has_more;
	}


private:

	/**
	 * Initialize the weights property (if applicable)
	 *
	 * @param graph the graph
	 * @param config the loader configuration
	 * @return the property, or NULL if not applicable
	 */
	 ll_mlcsr_edge_property<WeightType>* init_prop_weight(
			 ll_mlcsr_ro_graph* graph) {

		if (!HasWeight) return NULL;

		size_t new_level = graph->out().num_levels() - 1;
		size_t et_length = graph->out().edge_table_length(new_level);

		ll_mlcsr_edge_property<WeightType>* prop_weight = NULL;

		if (sizeof(WeightType) == 4) {
			prop_weight
				= reinterpret_cast<ll_mlcsr_edge_property<WeightType>*>
				(graph->get_edge_property_32("weight"));
			if (prop_weight == NULL) {
				prop_weight
					= reinterpret_cast<ll_mlcsr_edge_property<WeightType>*>
					(graph->create_uninitialized_edge_property_32
					 ("weight", WeightTypeCode));
				prop_weight->ensure_min_levels(new_level, et_length);
			}
		}
		else {
			if (sizeof(WeightType) != 8) abort();
			prop_weight
				= reinterpret_cast<ll_mlcsr_edge_property<WeightType>*>
				(graph->get_edge_property_64("weight"));
			if (prop_weight == NULL) {
				prop_weight
					= reinterpret_cast<ll_mlcsr_edge_property<WeightType>*>
					(graph->create_uninitialized_edge_property_64
					 ("weight", WeightTypeCode));
				prop_weight->ensure_min_levels(new_level, et_length);
			}
		}

		prop_weight->cow_init_level(et_length);

		return prop_weight;
	}
	

	/**
	 * Write a node with its out-edges and prep for the in-edges
	 *
	 * @param graph the graph
	 * @param et the edge table
	 * @param new_level the new level
	 * @param node the node
	 * @param adj_list the adjacency list
	 * @param weights the weights (if applicable)
	 * @param prop_weight the weights property (if applicable)
	 * @param in_sort the in-edges sorter (if applicable)
	 */
	void load_node_out(ll_mlcsr_ro_graph* graph, LL_ET<node_t>* et, size_t new_level,
			node_t node, std::vector<NodeType>& adj_list,
			std::vector<WeightType>& weights,
			ll_mlcsr_edge_property<WeightType>* prop_weight,
			ll_external_sort<xs_in_edge, xs_in_edge_comparator>* in_sort) {

		size_t et_index = graph->out().init_node(node, adj_list.size(), 0);
		edge_t edge = LL_EDGE_CREATE(new_level, et_index);

		for (size_t i = 0; i < adj_list.size(); i++) {
			LL_D_NODE2_PRINT(node, adj_list[i], "%ld --> %ld\n",
					(long) node, (long) adj_list[i]);
			(*et)[et_index + i] = LL_VALUE_CREATE((node_t) adj_list[i]);
		}

		if (in_sort != NULL) {
			xs_in_edge x;
			x.tail = node;
			for (size_t i = 0; i < adj_list.size(); i++) {
				x.head = adj_list[i];
#ifdef LL_LOAD_CREATE_REV_EDGE_MAP
				x.out_edge = edge + i;
#endif
				*in_sort << x;
			}
		}

		if (HasWeight) {
			for (size_t i = 0; i < weights.size(); i++) {
				prop_weight->cow_write(edge + i, weights[i]);
			}
		}
	}
	

	/**
	 * Write a node with its in-edges
	 *
	 * @param graph the graph
	 * @param et the edge table
	 * @param new_level the new level
	 * @param node the node
	 * @param adj_list the adjacency list
	 */
	void load_node_in(ll_mlcsr_ro_graph* graph, LL_ET<node_t>* et, size_t new_level,
			node_t node, std::vector<NodeType>& adj_list) {

		size_t et_index = graph->in().init_node(node, adj_list.size(), 0);

		for (size_t i = 0; i < adj_list.size(); i++) {
			(*et)[et_index + i] = LL_VALUE_CREATE((node_t) adj_list[i]);
		}
	}


	/**
	 * Load the graph directly into the read-only representation for the case
	 * in which the ll_edge_list_loader::stat() info is readily available
	 *
	 * @param graph the graph
	 * @param config the loader configuration
	 * @return true on no error
	 */
	bool load_direct_with_stat(ll_mlcsr_ro_graph* graph,
			const ll_loader_config* config) {


		// Check features

		feature_vector_t features;
		features << LL_L_FEATURE(lc_direction);
		features << LL_L_FEATURE(lc_reverse_edges);
		features << LL_L_FEATURE(lc_deduplicate);
		features << LL_L_FEATURE(lc_no_properties);
		features << LL_L_FEATURE(lc_max_edges);

		config->assert_features(false /*direct*/, true /*error*/, features);


		// Initialize the algorithm

		bool print_progress = config->lc_print_progress;
		bool reverse = config->lc_reverse_edges;
		bool load_weight = !config->lc_no_properties;

		size_t new_level = graph->num_levels();
		size_t max_nodes = 0;
		size_t max_edges = 0;

		xs_w_edge e;
		
		if (!stat(&max_nodes, &max_edges)) {
			LL_E_PRINT("The graph stat call failed\n");
			abort();
		}

		if (config->lc_max_edges > 0 && max_edges > config->lc_max_edges) {
			max_edges = config->lc_max_edges;
		}

		if (new_level > 0) {
			if (max_nodes < (size_t) graph->out().max_nodes()) {
				max_nodes = graph->out().max_nodes();
			}
		}

		if (config->lc_direction == LL_L_UNDIRECTED_DOUBLE) {
			max_edges *= 2;
		}


		// Initialize the new CSR level

		graph->partial_init_level(max_nodes, max_nodes, max_edges);
		LL_ET<node_t>* et = graph->out().edge_table(new_level);
		LL_D_PRINT("Nodes = %lu, edges = %lu\n", max_nodes, max_edges);

		ll_mlcsr_edge_property<WeightType>* prop_weight = NULL;
		if (load_weight) prop_weight = init_prop_weight(graph);


		// If the out-to-in, in-to-out properties are not enabled, disable
		// that feature in the corresponding ll_csr_base

		if (!config->lc_reverse_edges || !config->lc_reverse_maps) {
			graph->out().set_edge_translation(false);
			graph->in().set_edge_translation(false);
		}


		// Initialize the in-edges

		ll_external_sort<xs_in_edge, xs_in_edge_comparator>* in_sort = NULL;

		if (reverse) {
			graph->partial_init_level_in(max_nodes, max_nodes, max_edges);
			in_sort = new ll_external_sort<xs_in_edge,
					xs_in_edge_comparator>(config);
		}


		/*
		 *
		 * CASE 1: The input file is sorted
		 *
		 */

		// Try to load the data if it is sorted -- or discover that it is not,
		// abort, and then try again with the external sort

		size_t loaded_edges = 0;
		bool was_sorted = false;

		size_t step = 10 * 1000 * 1000ul;
		if (print_progress) {
			fprintf(stderr, "[<]");
		}

		if (config->lc_direction != LL_L_UNDIRECTED_DOUBLE) {

			std::vector<NodeType> adj_list_buffer;
			std::vector<WeightType> weight_buffer;

			NodeType last_tail = (NodeType) LL_NIL_NODE;
			NodeType last_head = (NodeType) LL_NIL_NODE;

			was_sorted = true;

			while (next_edge(&e.tail, &e.head, &e.weight)) {
				loaded_edges++;

				if (config->lc_max_edges > 0
						&& loaded_edges > config->lc_max_edges) {
					break;
				}

				LL_D_NODE2_PRINT(e.tail, e.head, "%ld --> %ld\n", (long) e.tail,
						(long) e.head);

				if (config->lc_direction == LL_L_UNDIRECTED_ORDERED) {
					if (e.tail > e.head) {
						unsigned x = e.tail; e.tail = e.head; e.head = x;
					}
				}
				if (config->lc_deduplicate && last_head == e.head
						&& last_tail == e.tail) {
					continue;
				}

				if ((last_tail != (NodeType) LL_NIL_NODE && last_tail > e.tail)
						|| (last_tail == e.tail && last_head > e.head)) {
					
					LL_D_PRINT("The input file is not sorted\n");

					was_sorted = false;
					loaded_edges = 0;

					rewind();
					graph->out().restart_init_level();
					if (in_sort != NULL) in_sort->clear();

					break;
				}


				// Init the node and write the edges after we moved to the next
				// node

				if (last_tail != e.tail && last_tail != (NodeType) LL_NIL_NODE) {
					load_node_out(graph, et, new_level, last_tail, adj_list_buffer,
							weight_buffer, prop_weight, in_sort);
					adj_list_buffer.clear();
					weight_buffer.clear();
				}

				last_head = e.head;
				last_tail = e.tail;


				// Load the edge into the buffer

				adj_list_buffer.push_back(e.head);
				if (HasWeight && load_weight) weight_buffer.push_back(e.weight);


				// Progress

				if (print_progress) {
					if (loaded_edges % step == 0) {
						fprintf(stderr, ".");
						if (loaded_edges % (step * 10) == 0) {
							fprintf(stderr, "%lu", loaded_edges / 1000000ul);
						}
					}
				}
			}


			// Finish the buffer

			if (was_sorted && last_tail != (NodeType) LL_NIL_NODE) {
				load_node_out(graph, et, new_level, last_tail, adj_list_buffer,
						weight_buffer, prop_weight, in_sort);
			}
		}


		/*
		 *
		 * CASE 2: The input file is not sorted
		 *
		 */

		// Now if the buffer was not sorted, load it using the external sort

		if (!was_sorted) {

			ll_external_sort<xs_w_edge, xs_w_edge_comparator>* out_sort
				= new ll_external_sort<xs_w_edge, xs_w_edge_comparator>(config);

			NodeType last_tail = (NodeType) LL_NIL_NODE;
			NodeType last_head = (NodeType) LL_NIL_NODE;

			size_t read_edges = 0;

			while (next_edge(&e.tail, &e.head, &e.weight)) {
				loaded_edges++;
				read_edges++;

				if (config->lc_max_edges > 0
						&& read_edges > config->lc_max_edges) {
					break;
				}

				if (config->lc_direction == LL_L_UNDIRECTED_ORDERED) {
					if (e.tail > e.head) {
						unsigned x = e.tail; e.tail = e.head; e.head = x;
					}
				}
				if (config->lc_deduplicate && last_head == e.head
						&& last_tail == e.tail) {
					continue;
				}

				last_head = e.head;
				last_tail = e.tail;

				*out_sort << e;

				if (config->lc_direction == LL_L_UNDIRECTED_DOUBLE) {
					if (e.tail != e.head) {
						unsigned x = e.tail; e.tail = e.head; e.head = x;
						*out_sort << e;
						loaded_edges++;
					}
				}


				// Progress

				if (print_progress) {
					if (loaded_edges % step == 0) {
						fprintf(stderr, ".");
						if (loaded_edges % (step * 10) == 0) {
							fprintf(stderr, "%lu", loaded_edges / 1000000ul);
						}
					}
				}
			}

			out_sort->sort();

			xs_w_edge* buffer;
			size_t length;

			std::vector<NodeType> adj_list_buffer;
			std::vector<WeightType> weight_buffer;

			last_tail = (NodeType) LL_NIL_NODE;
			last_head = (NodeType) LL_NIL_NODE;

			if (print_progress) {
				fprintf(stderr, "[+]");
			}

			while (out_sort->next_block(&buffer, &length)) {
				while (length --> 0) {

					if (config->lc_deduplicate && last_head == buffer->head
							&& last_tail == buffer->tail) {
						buffer++;
						continue;
					}


					// Init the node and write the edges after we moved to the
					// next node

					if (last_tail != buffer->tail
							&& last_tail != (NodeType) LL_NIL_NODE) {
						load_node_out(graph, et, new_level, last_tail,
								adj_list_buffer, weight_buffer, prop_weight,
								in_sort);
						adj_list_buffer.clear();
						weight_buffer.clear();
					}

					last_head = buffer->head;
					last_tail = buffer->tail;


					// Load the edge into the buffer

					adj_list_buffer.push_back(buffer->head);
					if (HasWeight && load_weight)
						weight_buffer.push_back(e.weight);

					buffer++;


					// Progress

					if (print_progress) {
						if (loaded_edges % step == 0) {
							fprintf(stderr, ".");
							if (loaded_edges % (step * 10) == 0) {
								fprintf(stderr, "%lu", loaded_edges / 1000000ul);
							}
						}
					}
				}
			}


			// Finish the buffer

			if (last_tail != (NodeType) LL_NIL_NODE) {
				load_node_out(graph, et, new_level, last_tail, adj_list_buffer,
						weight_buffer, prop_weight, in_sort);
			}
		}


		/*
		 *
		 * Finish up the out-edges and do the in-edges
		 *
		 */

		graph->out().finish_level_vertices();
		graph->out().finish_level_edges();

		if (HasWeight && load_weight) {
			prop_weight->finish_level();
		}

		if (reverse) {

			if (print_progress) {
				fprintf(stderr, "[I]");
			}


			// If the out-to-in, in-to-out properties are not enabled, disable
			// that feature in the corresponding ll_csr_base

			if (!config->lc_reverse_edges || !config->lc_reverse_maps) {
				graph->in().set_edge_translation(false);
			}


			// Sort the in-edges and load them

			in_sort->sort();

			xs_in_edge* buffer;
			size_t length;

			loaded_edges = 0;
			et = graph->in().edge_table(new_level);

			NodeType last_head = (NodeType) LL_NIL_NODE;

			std::vector<NodeType> adj_list_buffer;
			std::vector<WeightType> weight_buffer;

			while (in_sort->next_block(&buffer, &length)) {
				while (length --> 0) {

					// Init the node and write the edges after we moved to the
					// next node

					if (last_head != buffer->head && last_head != (NodeType) LL_NIL_NODE) {
						load_node_in(graph, et, new_level, last_head, adj_list_buffer);
						adj_list_buffer.clear();
					}

					last_head = buffer->head;


					// Load the edge into the buffer

					adj_list_buffer.push_back(buffer->tail);
					buffer++;


					// Progress

					if (print_progress) {
						if (loaded_edges % step == 0) {
							fprintf(stderr, ".");
							if (loaded_edges % (step * 10) == 0) {
								fprintf(stderr, "%lu", loaded_edges / 1000000ul);
							}
						}
					}
				}
			}


			// Finish the buffer

			if (last_head != (NodeType) LL_NIL_NODE) {
				load_node_in(graph, et, new_level, last_head, adj_list_buffer);
			}


			// Finish the in-edges

			delete in_sort;
			in_sort = NULL;

			graph->in().finish_level_vertices();
			graph->in().finish_level_edges();
		}


		// Finish

		_last_has_more = _has_more;
		_has_more = false;

		return true;
	}
};

#endif

