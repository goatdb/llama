/*
 * ll_data_source.h
 * LLAMA Graph Analytics
 *
 * Copyright 2015
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


#ifndef LL_DATA_SOURCE_H_
#define LL_DATA_SOURCE_H_

#include "llama/ll_common.h"
#include "llama/ll_mlcsr_graph.h"
#include "llama/ll_writable_graph.h"
#include "llama/loaders/ll_load_async_writable.h"

#include <algorithm>
#include <queue>
#include <vector>


/**
 * The pull-based data source
 */
class ll_data_source {

public:

	/**
	 * Create an instance of the data source wrapper
	 */
	inline ll_data_source() {}


	/**
	 * Destroy the data source
	 */
	virtual ~ll_data_source() {}


	/**
	 * Is this a simple data source? A simple data source has only edges with
	 * predefined node IDs.
	 *
	 * A simple data source needs to additionally implement:
	 *   * next_edge()
	 */
	virtual bool simple() { return false; }


	/**
	 * Load the next batch of data
	 *
	 * @param graph the writable graph
	 * @param max_edges the maximum number of edges
	 * @return true if data was loaded, false if there are no more data
	 */
	virtual bool pull(ll_writable_graph* graph, size_t max_edges) = 0;


	/**
	 * Load the next batch of data to request queues
	 *
	 * @param request_queues the request queues
	 * @param num_stripes the number of stripes (queues array length)
	 * @param max_edges the maximum number of edges
	 * @return true if data was loaded, false if there are no more data
	 */
	virtual bool pull(ll_la_request_queue** request_queues, size_t num_stripes,
			size_t max_edges) = 0;


	/**
	 * Get the next edge
	 *
	 * @param o_tail the output for tail
	 * @param o_head the output for head
	 * @return true if the edge was loaded, false if EOF or error
	 */
	virtual bool next_edge(node_t* o_tail, node_t* o_head) { abort(); }
};



/**
 * A simple pull-based data source
 */
class ll_simple_data_source : public ll_data_source {

public:

	/**
	 * Create an instance of the data source wrapper
	 */
	inline ll_simple_data_source() {}


	/**
	 * Destroy the data source
	 */
	virtual ~ll_simple_data_source() {}


	/**
	 * Is this a simple data source? A simple data source has only edges with
	 * predefined node IDs.
	 *
	 * A simple data source needs to additionally implement:
	 *   * next_edge()
	 */
	virtual bool simple() { return true; }


	/**
	 * Load the next batch of data
	 *
	 * @param graph the writable graph
	 * @param max_edges the maximum number of edges
	 * @return true if data was loaded, false if there are no more data
	 */
	virtual bool pull(ll_writable_graph* graph, size_t max_edges) {

		size_t num_stripes = omp_get_max_threads();
		ll_la_request_queue* request_queues[num_stripes];

		for (size_t i = 0; i < num_stripes; i++) {
			request_queues[i] = new ll_la_request_queue();
		}

		bool loaded = false;
		size_t chunk = num_stripes <= 1
			? std::min<size_t>(10000ul, max_edges)
			: max_edges;

		while (true) {

			graph->tx_begin();
			bool has_data = false;

			for (size_t i = 0; i < num_stripes; i++)
				request_queues[i]->shutdown_when_empty(false);

			#pragma omp parallel
			{
				if (omp_get_thread_num() == 0) {

					has_data = this->pull(request_queues, num_stripes, chunk);

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

			if (has_data)
				loaded = true;
			else
				break;
		}

		for (size_t i = 0; i < num_stripes; i++) delete request_queues[i];

		return loaded;
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

		node_t tail, head;
		size_t num_edges = 0;

		while (next_edge(&tail, &head)) {
			num_edges++;

			LL_D_NODE2_PRINT(tail, head, "%ld --> %ld\n",
					(long) tail, (long) head);

			ll_la_request_with_edge_properties* request;
#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES
			request = new ll_la_add_edge_for_streaming_with_weights<node_t>(
					tail, head);
#else
			request = new ll_la_add_edge<node_t>(tail, head);
#endif

			size_t stripe = (tail>>(LL_ENTRIES_PER_PAGE_BITS+3)) % num_stripes;
			request_queues[stripe]->enqueue(request);

			if (max_edges > 0 && num_edges >= max_edges) break;
		}

		return num_edges > 0;
	}
};



/**
 * A serial concatenation of multiple data sources
 */
class ll_concat_data_source : public ll_data_source {

	std::queue<ll_data_source*> _data_sources;
	ll_spinlock_t _lock;
	bool _simple;


public:

	/**
	 * Create an instance of the concatenated data source
	 */
	ll_concat_data_source() {

		_lock = 0;
		_simple = true;
	}


	/**
	 * Destroy the data source
	 */
	virtual ~ll_concat_data_source() {

		while (!_data_sources.empty()) {
			delete _data_sources.front();
			_data_sources.pop();
		}
	}


	/**
	 * Add a data source
	 *
	 * @param data_source the data source
	 */
	void add(ll_data_source* data_source) {

		ll_spinlock_acquire(&_lock);
		_data_sources.push(data_source);
		_simple = _simple && data_source->simple();
		ll_spinlock_release(&_lock);
	}


	/**
	 * Is this a simple data source?
	 */
	virtual bool simple() {
		return _simple;
	}


	/**
	 * Load the next batch of data
	 *
	 * @param graph the writable graph
	 * @param max_edges the maximum number of edges
	 * @return true if data was loaded, false if there are no more data
	 */
	virtual bool pull(ll_writable_graph* graph, size_t max_edges) {

		ll_spinlock_acquire(&_lock);

		if (_data_sources.empty()) {
			ll_spinlock_release(&_lock);
			return false;
		}

		ll_data_source* d = _data_sources.front();
		ll_spinlock_release(&_lock);

		while (true) {

			bool r = d->pull(graph, max_edges);
			if (r) return r;

			ll_spinlock_acquire(&_lock);

			if (d != _data_sources.front()) {
				LL_E_PRINT("Race condition\n");
				abort();
			}

			delete d;
			_data_sources.pop();

			if (_data_sources.empty()) {
				ll_spinlock_release(&_lock);
				return false;
			}

			d = _data_sources.front();
			ll_spinlock_release(&_lock);
		}
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

		ll_spinlock_acquire(&_lock);

		if (_data_sources.empty()) {
			ll_spinlock_release(&_lock);
			return false;
		}

		ll_data_source* d = _data_sources.front();
		ll_spinlock_release(&_lock);

		while (true) {

			bool r = d->pull(request_queues, num_stripes, max_edges);
			if (r) return r;

			ll_spinlock_acquire(&_lock);

			if (d != _data_sources.front()) {
				LL_E_PRINT("Race condition\n");
				abort();
			}

			delete d;
			_data_sources.pop();

			if (_data_sources.empty()) {
				ll_spinlock_release(&_lock);
				return false;
			}

			d = _data_sources.front();
			ll_spinlock_release(&_lock);
		}
	}


	/**
	 * Get the next edge
	 *
	 * @param o_tail the output for tail
	 * @param o_head the output for head
	 * @return true if the edge was loaded, false if EOF or error
	 */
	virtual bool next_edge(node_t* o_tail, node_t* o_head) {

		ll_spinlock_acquire(&_lock);

		if (_data_sources.empty()) {
			ll_spinlock_release(&_lock);
			return false;
		}

		ll_data_source* d = _data_sources.front();
		ll_spinlock_release(&_lock);

		while (true) {

			bool r = d->next_edge(o_tail, o_head);
			if (r) return r;

			ll_spinlock_acquire(&_lock);

			if (d != _data_sources.front()) {
				LL_E_PRINT("Race condition\n");
				abort();
			}

			delete d;
			_data_sources.pop();

			if (_data_sources.empty()) {
				ll_spinlock_release(&_lock);
				return false;
			}

			d = _data_sources.front();
			ll_spinlock_release(&_lock);
		}
	}
};



/**
 * A generic pull-based data source
 */
template <typename input_t>
class ll_generic_data_source {

public:

	/**
	 * Create an instance of class ll_generic_data_source
	 */
	inline ll_generic_data_source() {}


	/**
	 * Destroy an instance of the class
	 */
	virtual ~ll_generic_data_source() {}


	/**
	 * Get the next input item. The function returns a pointer to an internal
	 * buffer that does not need (and should not be) freed by the caller.
	 * 
	 * Depending on the implementation, this might or might not be thread safe.
	 *
	 * @return the pointer to the next item, or NULL if done
	 */
	virtual const input_t* next_input() = 0;
};



/**
 * An adapter for a generic data source for LLAMA
 */
template <typename input_t>
class ll_simple_data_source_adapter : public ll_simple_data_source {

	ll_generic_data_source<input_t>* _source;
	bool _own;

	std::queue<node_pair_t> _edge_buffer;
	ll_spinlock_t _edge_buffer_lock;

	size_t _num_inputs;


public:

	/**
	 * Create an instance of the data source adapter
	 *
	 * @param source the generic data source to wrap
	 * @param own true to own the data source and destroy it at the end
	 */
	ll_simple_data_source_adapter(ll_generic_data_source<input_t>* source,
			bool own=false) {

		_source = source;
		_own = own;

		_edge_buffer_lock = 0;
		_num_inputs = 0;
	}


	/**
	 * Destroy the instance of this class
	 */
	virtual ~ll_simple_data_source_adapter() {
		if (_own && _source != NULL) delete _source;
	}


	/**
	 * Get the data source
	 *
	 * @return the data source
	 */
	inline ll_generic_data_source<input_t>* source() {
		return _source;
	}


	/**
	 * Get the number of processed inputs
	 *
	 * @return the number of inputs
	 */
	inline size_t num_processed_inputs() {
		return _num_inputs;
	}


	/**
	 * Get the next edge
	 *
	 * @param o_tail the output for tail
	 * @param o_head the output for head
	 * @return true if the edge was loaded, false if EOF or error
	 */
	virtual bool next_edge(node_t* o_tail, node_t* o_head) {

		ll_spinlock_acquire(&_edge_buffer_lock);
		
		while (_edge_buffer.empty()) {

			const input_t* input = _source->next_input();
			if (input == NULL) {
				ll_spinlock_release(&_edge_buffer_lock);
				return false;
			}

			process_input(input);
			_num_inputs++;
		}

		node_pair_t e = _edge_buffer.front();
		_edge_buffer.pop();

		ll_spinlock_release(&_edge_buffer_lock);

		*o_tail = e.tail;
		*o_head = e.head;
		return true;
	}


protected:

	/**
	 * Add an edge to the buffer.
	 *
	 * Beware that this is intended to be only called from inside
	 * process_input().
	 *
	 * @param tail the tail
	 * @param head the head
	 */
	void add_edge(node_t tail, node_t head) {

		// This is only called from process_input(), which is only called from
		// next_edge() -- and this happens only while the _edge_buffer_lock is
		// held. Note that we would need to revisit this if we want more than
		// one loading thread.

		node_pair_t e;
		e.tail = tail;
		e.head = head;

		_edge_buffer.push(e);
	}


	/**
	 * Processs an input item and generate corresponding edges
	 *
	 * @param input the input item
	 */
	virtual void process_input(const input_t* input) = 0;
};


#endif
