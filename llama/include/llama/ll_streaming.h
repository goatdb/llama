/*
 * ll_streaming.h
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


#ifndef LL_STREAMING_H_
#define LL_STREAMING_H_

#include "llama/ll_common.h"
#include "llama/ll_mlcsr_graph.h"
#include "llama/ll_writable_graph.h"
#include "llama/loaders/ll_load_async_writable.h"

#include <algorithm>
#include <queue>
#include <vector>


/**
 * The stream config options
 */
#define LL_SC_UNIFORM		0
#define LL_SC_NORMAL 		1


/**
 * The streaming pull configuration
 */
class ll_stream_config {

	double sc_rdtsc_per_ms;


public:

	size_t sc_edges_per_second;
	size_t sc_max_edges_per_batch;

	int sc_batch_size_distribution;
	size_t sc_batch_size_unif_min;
	size_t sc_batch_size_unif_max;
	double sc_batch_size_norm_mean;
	double sc_batch_size_norm_stdev;


public:

	/**
	 * Create an instance of ll_stream_config
	 *
	 * @param rdtsc_per_ms the number of ticks per ms
	 */
	ll_stream_config(double rdtsc_per_ms = ll_rdtsc_per_ms()) {

		sc_rdtsc_per_ms = rdtsc_per_ms;

		sc_edges_per_second = 400 * 1000ul;
		sc_max_edges_per_batch = 0;

		sc_batch_size_distribution = LL_SC_NORMAL;
		sc_batch_size_unif_min = 10;
		sc_batch_size_unif_max = 100;
		sc_batch_size_norm_mean = sc_batch_size_unif_min
			+ (sc_batch_size_unif_max - sc_batch_size_unif_min) / 2.0;
		sc_batch_size_norm_stdev = sc_batch_size_unif_max
			- sc_batch_size_norm_mean;
	}


	/**
	 * Generate the next batch size
	 * 
	 * @return the next batch size
	 */
	size_t next_batch_size() const {

		double m;
		ssize_t r;

		switch (sc_batch_size_distribution) {
			case LL_SC_UNIFORM:
				return sc_batch_size_unif_min
					+ rand() % (sc_batch_size_unif_max-sc_batch_size_unif_min);
			case LL_SC_NORMAL:
				m = -6;
				for (int i = 0; i < 12; i++) m += rand() / (double) RAND_MAX;
				r = (ssize_t) (sc_batch_size_norm_mean
						+ m * sc_batch_size_norm_stdev);
				if (r < (ssize_t) sc_batch_size_unif_min)
					r = sc_batch_size_unif_min;
				if (r > (ssize_t) sc_batch_size_unif_max)
					r = sc_batch_size_unif_max;
				return r;
			default:
				abort();
		}
	}


	/**
	 * Get the value of rdtsc_per_ms
	 *
	 * @return the number of ticks per ms
	 */
	inline double rdtsc_per_ms() const {
		return sc_rdtsc_per_ms;
	}
};


/**
 * The streaming stats
 */
class ll_stream_stats {

public:

	size_t ss_requests_arrived;
	size_t ss_requests_processed;
	double ss_start;


public:

	/**
	 * Create an instance of ll_stream_stats
	 */
	ll_stream_stats() {
		ss_requests_arrived = 0;
		ss_requests_processed = 0;
		ss_start = ll_get_time_ms();
	}


	/**
	 * Get the total number of outstanding requests
	 *
	 * @return the number of outstanding requests
	 */
	inline size_t num_outstanding_requests() const {
		return (size_t) std::max(0l,
				((ssize_t) ss_requests_arrived
				 - (ssize_t) ss_requests_processed));
	}
};


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
 * A database loader that pulls continuously
 */
class ll_continuous_loader {

	size_t _num_stripes;
	ll_la_request_queue** _request_queues;

	ll_writable_graph* _graph;
	ll_data_source* _data_source;
	ll_stream_config _config;

	volatile bool _terminate;
	ll_spinlock_t* _lock;
	ll_stream_stats _stats;

	std::atomic<size_t> _requests_in_current_batch;


public:

	/**
	 * Create an instance of class ll_continuous_loader
	 *
	 * @param graph the writable graph
	 * @param data_source the data source
	 * @param config the streaming configuration
	 * @param lock the whole-graph lock (must be released to proceed)
	 */
	ll_continuous_loader(ll_writable_graph* graph,
			ll_data_source* data_source,
			const ll_stream_config* config,
			ll_spinlock_t* lock) 
		: _config(*config)
	{

		assert(graph != NULL);

		_terminate = false;
		_graph = graph;
		_data_source = data_source;
		_lock = lock;
		_requests_in_current_batch = 0;

		_num_stripes = omp_get_max_threads();
		_request_queues = (ll_la_request_queue**) malloc(
				sizeof(ll_la_request_queue*) * _num_stripes);

		for (size_t i = 0; i < _num_stripes; i++) {
			_request_queues[i] = new ll_la_request_queue();
		}
	}


	/**
	 * Destroy the object
	 */
	virtual ~ll_continuous_loader() {

		for (size_t i = 0; i < _num_stripes; i++) delete _request_queues[i];
		free(_request_queues);
	}


	/**
	 * Terminate the loader
	 */
	void terminate() {
		_terminate = true;
		__sync_synchronize();
	}


	/**
	 * Get the stats
	 *
	 * @return the stats
	 */
	inline const ll_stream_stats& stats() {
		return _stats;
	}


	/**
	 * Reset the batch counters
	 */
	void reset_batch_counters() {
		_requests_in_current_batch = 0;
	}


	/**
	 * Continuously load data. It supports pausing in processing the load
	 * requests, for example, while a checkpoint is running.
	 *
	 * Please note that this is a single-threaded function.
	 */
	void run() {

		_stats.ss_start = ll_get_time_ms();

		uint64_t dt_behind = 0;

		double time_last_msg = 0;
		bool print_msg = false;

		double rdtsc_per_ms = _config.rdtsc_per_ms();
		uint64_t t_last = ll_rdtsc();


		// Pull until we run out of data

		while (!_terminate) {

			uint64_t t_start = t_last;
			size_t batch_size = _config.next_batch_size();
			size_t org_batch_size = batch_size;

			size_t r = _requests_in_current_batch.load();
			if (_config.sc_max_edges_per_batch > 0) {
				if (r + batch_size > _config.sc_max_edges_per_batch) {
					if (r >= _config.sc_max_edges_per_batch) {
						batch_size = 0;
					}
					else {
						batch_size = _config.sc_max_edges_per_batch - r;
					}
				}
			}

			double expected_ms = 1000
				* (org_batch_size / (double) _config.sc_edges_per_second);
			uint64_t expected_dt = rdtsc_per_ms * expected_ms;

			if (batch_size > 0) {
				bool loaded = _data_source
					->pull(_request_queues, _num_stripes, batch_size);
				if (!loaded) return;
			}

			// TODO Make exact (could be less if this is the end of the stream)
			_stats.ss_requests_arrived += batch_size;
			_requests_in_current_batch += batch_size;

			uint64_t t_stop_at = t_start - dt_behind + expected_dt;

			uint64_t t;
			size_t processed = 0;
			while ((t = ll_rdtsc()) < t_stop_at) {
				if (ll_spinlock_try_acquire(_lock)) {
					bool r = false;
					for (size_t i = 0; i < _num_stripes; i++) {
						if (_request_queues[i]->process_next(*_graph)) {
							processed++;
							r = true;
						}
					}
					ll_spinlock_release(_lock);
					if (!r) break;
				}
				else {
					usleep(5);
				}
			}

			ATOMIC_ADD<size_t>(&_stats.ss_requests_processed, processed);

			size_t l = 0;
			for (size_t i = 0; i < _num_stripes; i++) {
				l += _request_queues[i]->size();
			}

			t_last = ll_rdtsc();
			int64_t behind = (int64_t) t_last - (int64_t) t_stop_at;
			double ms_behind = behind / rdtsc_per_ms;

			if (print_msg && (l > 100 * 1000 || ms_behind > 100)) {
				double ct = ll_get_time_ms();
				if (ct - time_last_msg > 1000) {
					time_last_msg = ct;
					LL_W_PRINT("Falling behind: %0.3lf Mreq, %0.3lf s \n",
							l / 1000000.0, ms_behind / 1000.0);
				}
			}

			if (behind >= 0) {
				dt_behind = behind;
				if (!print_msg && ms_behind > 100) {
					double ct = ll_get_time_ms();
					if (ct - time_last_msg > 1000) {
						time_last_msg = ct;
						LL_W_PRINT("Falling behind: %0.3lf s \n",
								ms_behind / 1000.0);
					}
				}
			}
			else {
				dt_behind = 0;
				usleep((size_t) ((1000.0 * -behind) / rdtsc_per_ms));
			}
		}
	}


	/**
	 * Drain the queues using all threads
	 */
	void drain() {

#	pragma omp parallel
		{
			int t = omp_get_thread_num() % _num_stripes;
			size_t processed = 0;

			for (size_t i = 0; i < _num_stripes; i++, t++) {
				t %= _num_stripes;
				while (true) {
					if (_request_queues[t]->process_next(*_graph)) {
						processed++;
					}
					else {
						break;
					}
				}
			}

			ATOMIC_ADD<size_t>(&_stats.ss_requests_processed, processed);
			//LL_I_PRINT("Drained %lu\n", processed);
		}
	}
};


typedef struct {
	node_t tail;
	node_t head;
} ll_cdl_edge_data_t;

bool operator< (const ll_cdl_edge_data_t& a, const ll_cdl_edge_data_t& b) {
	if (a.tail != b.tail) return a.tail < b.tail;
	return a.head < b.head;
}


/**
 * A database loader that pulls continuously
 */
class ll_continuous_direct_loader {

	size_t _num_stripes;
	std::vector<ll_cdl_edge_data_t>* _buffer;
	ll_spinlock_t _buffer_lock;

	ll_data_source* _data_source;
	ll_stream_config _config;

	volatile bool _terminate;
	ll_spinlock_t* _lock;
	ll_stream_stats _stats;

	std::atomic<size_t> _requests_in_current_batch;

	volatile bool _run_task;


protected:

	ll_writable_graph* _graph;


public:

	/**
	 * Create an instance of class ll_continuous_direct_loader
	 *
	 * @param graph the writable graph
	 * @param data_source the data source
	 * @param config the streaming configuration
	 * @param lock the whole-graph lock (must be released to proceed)
	 */
	ll_continuous_direct_loader(ll_writable_graph* graph,
			ll_data_source* data_source,
			const ll_stream_config* config,
			ll_spinlock_t* lock) 
		: _config(*config)
	{

#ifndef LL_S_SINGLE_SNAPSHOT
		assert(graph != NULL);
#endif

		_terminate = false;
		_run_task = false;

		_graph = graph;
		_data_source = data_source;
		_lock = lock;
		_requests_in_current_batch = 0;

		_num_stripes = omp_get_max_threads();
		_buffer = new std::vector<ll_cdl_edge_data_t>();
		_buffer_lock = 0;
	}


	/**
	 * Destroy the object
	 */
	virtual ~ll_continuous_direct_loader() {

		delete _buffer;
	}


	/**
	 * Terminate the loader
	 */
	void terminate() {
		_terminate = true;
		__sync_synchronize();
	}


	/**
	 * Get the stats
	 *
	 * @return the stats
	 */
	inline const ll_stream_stats& stats() {
		return _stats;
	}


	/**
	 * Reset the batch counters
	 */
	void reset_batch_counters() {
		_requests_in_current_batch = 0;
	}


	/**
	 * Continuously load data. It supports pausing in processing the load
	 * requests.
	 *
	 * Please note that this is a single-threaded function.
	 */
	void run() {

		_stats.ss_start = ll_get_time_ms();

		uint64_t dt_behind = 0;

		double time_last_msg = 0;
		bool print_msg = false;

		double rdtsc_per_ms = _config.rdtsc_per_ms();
		uint64_t t_last = ll_rdtsc();
		uint64_t t_run_task = t_last;
		uint64_t dt_last_task = 0;


		// Pull until we run out of data

		while (!_terminate) {

			uint64_t t_start = t_last;
			size_t batch_size = _config.next_batch_size();
			size_t org_batch_size = batch_size;

			size_t r = _requests_in_current_batch.load();
			if (_config.sc_max_edges_per_batch > 0) {
				if (r + batch_size > _config.sc_max_edges_per_batch) {
					if (r >= _config.sc_max_edges_per_batch) {
						batch_size = 0;
					}
					else {
						batch_size = _config.sc_max_edges_per_batch - r;
					}
				}
			}

			double expected_ms = 1000
				* (org_batch_size / (double) _config.sc_edges_per_second);
			uint64_t expected_dt = rdtsc_per_ms * expected_ms;

			size_t n = 0;
			if (batch_size > 0) {
				ll_spinlock_acquire(&_buffer_lock);
				for (size_t i = 0; i < batch_size; i++) {
					ll_cdl_edge_data_t x;
					if (!_data_source->next_edge(&x.tail, &x.head)) break;
					_buffer->push_back(x);
					//push_heap(_buffer->begin(), _buffer->end());
					n++;
				}
				ll_spinlock_release(&_buffer_lock);
				if (n == 0) return;
			}

			_stats.ss_requests_arrived += n;
			_requests_in_current_batch += n;

			uint64_t t_stop_at = t_start - dt_behind + expected_dt;

			if (_run_task && dt_behind <= 0) {
				_run_task = false;
				t_run_task = ll_rdtsc();
				ll_spinlock_acquire(&_buffer_lock);

				task(*_buffer);
				_stats.ss_requests_processed += _buffer->size();
				_buffer->clear();

				ll_spinlock_release(&_buffer_lock);
				dt_last_task = ll_rdtsc() - t_run_task;
			}

			t_last = ll_rdtsc();
			int64_t behind = (int64_t) t_last - (int64_t) t_stop_at;
			double ms_behind = behind / rdtsc_per_ms;

			if (behind >= 0) {
				dt_behind = behind;
				// TODO What is the right condition for this warning?
				if (!print_msg && ms_behind > 10000
						&& dt_last_task < 2 * dt_behind) {
					double ct = ll_get_time_ms();
					if (ct - time_last_msg > 1000) {
						time_last_msg = ct;
						LL_W_PRINT("Falling behind: %0.3lf s \n",
								ms_behind / 1000.0);
					}
				}
			}
			else {
				dt_behind = 0;
				usleep((size_t) ((1000.0 * -behind) / rdtsc_per_ms));
			}
		}
	}


	/**
	 * Swap the buffers and return the original buffer
	 *
	 * @return the buffer (must be deleted by the caller!)
	 */
	std::vector<ll_cdl_edge_data_t>* swap_buffers() {
		return swap_buffers(true);
	}


	/**
	 * Schedule the task
	 */
	void schedule_task() {
		_run_task = true;
		__sync_synchronize();
	}


protected:

	/**
	 * Run a task - override to do something useful with the buffer
	 *
	 * @param buffer the buffer
	 */
	virtual void task(std::vector<ll_cdl_edge_data_t>& buffer) {}


	/**
	 * Swap the buffers and return the original buffer
	 *
	 * @param lock true to lock
	 * @return the buffer (must be deleted by the caller!)
	 */
	std::vector<ll_cdl_edge_data_t>* swap_buffers(bool lock) {

		if (lock) ll_spinlock_acquire(&_buffer_lock);
		std::vector<ll_cdl_edge_data_t>* b = _buffer;
		_buffer = new std::vector<ll_cdl_edge_data_t>();
		if (lock) ll_spinlock_release(&_buffer_lock);

		//sort_heap(b->begin(), b->end());

		_stats.ss_requests_processed += b->size();

		return b;
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

#endif
