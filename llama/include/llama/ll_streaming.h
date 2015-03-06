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
#include "llama/ll_data_source.h"
#include "llama/ll_mlcsr_graph.h"
#include "llama/ll_writable_graph.h"
#include "llama/loaders/ll_load_async_writable.h"
#include "llama/loaders/ll_load_utils.h"

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

	size_t sc_max_edges_per_second;
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

		sc_max_edges_per_second = 0;		// Max speed
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

	size_t ss_last_batch_requests;


public:

	/**
	 * Create an instance of ll_stream_stats
	 */
	ll_stream_stats() {
		
		ss_requests_arrived = 0;
		ss_requests_processed = 0;
		
		ss_last_batch_requests = 0;

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
 * A database loader that pulls continuously into the writable graph. Call
 * advance() to advance the sliding window.
 *
 * The run() function keeps processing the pending requests in the queue while
 * throttling the edge ingest rate, but not if throttling is disabled.
 */
class ll_stream_writable_loader {

	size_t _num_stripes;
	ll_la_request_queue** _request_queues;

	ll_writable_graph* _graph;
	ll_data_source* _data_source;

	ll_stream_config _config;
	ll_loader_config _loader_config;
	int _window;
	int _keep;

	volatile bool _terminate;
	ll_spinlock_t _lock;
	ll_stream_stats _stats;

	std::atomic<size_t> _requests_in_current_batch;
	size_t _last_requests_processed;


public:

	/**
	 * Create an instance of class ll_stream_writable_loader
	 *
	 * @param graph the writable graph
	 * @param data_source the data source
	 * @param config the streaming configuration
	 * @param loader_config the loader configuration (used in advance())
	 * @param window the number of snapshots in the sliding window (-1 = all)
	 * @param keep the number of read-optimized levels to keep (-1 = all)
	 */
	ll_stream_writable_loader(ll_writable_graph* graph,
			ll_data_source* data_source,
			const ll_stream_config* config,
			const ll_loader_config* loader_config,
			int window, int keep=-1)
		: _config(*config), _loader_config(*loader_config)
	{

		assert(graph != NULL);

		_graph = graph;
		_data_source = data_source;
		_window = window;
		_keep = keep;

		_terminate = false;
		_lock = 0;
		_requests_in_current_batch = 0;
		_last_requests_processed = 0;

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
	virtual ~ll_stream_writable_loader() {

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

		int64_t dt_behind = 0;

		double time_last_msg = 0;
		bool print_msg = false;

		double rdtsc_per_ms = _config.rdtsc_per_ms();
		uint64_t t_last = ll_rdtsc();


		// Pull until we run out of data

		while (!_terminate) {

			uint64_t t_start = ll_rdtsc();
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

			double expected_ms = _config.sc_max_edges_per_second == 0 ? 0 
				: 1000 * (org_batch_size / (double) _config.sc_max_edges_per_second);
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

			size_t processed = 0;
			if (_config.sc_max_edges_per_second > 0) {
				uint64_t t;
				while ((t = ll_rdtsc()) < t_stop_at) {
					if (ll_spinlock_try_acquire(&_lock)) {
						bool r = false;
						for (size_t i = 0; i < _num_stripes; i++) {
							if (_request_queues[i]->process_next(*_graph)) {
								processed++;
								r = true;
							}
						}
						ll_spinlock_release(&_lock);
						if (!r) break;
					}
					else {
						usleep(5);
					}
				}
			}

			ATOMIC_ADD<size_t>(&_stats.ss_requests_processed, processed);

			size_t l = 0;
			for (size_t i = 0; i < _num_stripes; i++) {
				l += _request_queues[i]->size();
			}

			t_last = ll_rdtsc();
			int64_t behind = _config.sc_max_edges_per_second == 0 ? 0
				: (int64_t) t_last - (int64_t) t_stop_at;
			double ms_behind = behind / rdtsc_per_ms;

			if (print_msg && (l > 100 * 1000 || ms_behind > 100)) {
				double ct = ll_get_time_ms();
				if (ct - time_last_msg > 1000) {
					time_last_msg = ct;
					LL_W_PRINT("Falling behind: %0.3lf Mreq, %0.3lf s \n",
							l / 1000000.0, ms_behind / 1000.0);
				}
			}

			dt_behind = behind;
			if (behind >= 0) {
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
				usleep((size_t) ((1000.0 * -behind) / rdtsc_per_ms));
			}

			dt_behind += ll_rdtsc() - t_last;
		}
	}


	/**
	 * Drain the queues using all threads
	 */
	void drain() {

		// XXX Should I lock???

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


	/**
	 * Advance the sliding window
	 *
	 * @return the number of the level that was just created
	 */
	int advance() {

		ll_spinlock_acquire(&_lock);
		_stats.ss_last_batch_requests = _stats.ss_requests_processed
			- _last_requests_processed;
		_last_requests_processed = _stats.ss_requests_processed;

		_graph->checkpoint(&_loader_config);

		if (_window > 0 && _graph->num_levels() >= (size_t) _window) {
#ifdef LL_MIN_LEVEL
			_graph->set_min_level(_graph->num_levels() - _window);
#endif
			if (_graph->num_levels() >= (size_t) _window + 3) {
				_graph->delete_level(_graph->num_levels() - _window - 3);
			}
		}

		if (_keep > 0) _graph->ro_graph().keep_only_recent_versions(_keep);

		int l = ((int) _graph->ro_graph().num_levels()) - 1;
		ll_spinlock_release(&_lock);

		return l;
	}
};


/**
 * A database loader that pulls continuously into a buffer of node pairs. Call
 * swap_buffers() to obtain a loaded buffer. You can also use the built-in
 * task mechanism to perform loading actions using the loader thread.
 */
class ll_stream_buffer_loader {

	size_t _num_stripes;
	std::vector<node_pair_t>* _buffer;
	ll_spinlock_t _buffer_lock;

	ll_data_source* _data_source;
	ll_stream_config _config;

	volatile bool _terminate;
	volatile bool _run_task;

	std::atomic<size_t> _requests_in_current_batch;


protected:

	ll_stream_stats _stats;


public:

	/**
	 * Create an instance of class ll_stream_buffer_loader
	 *
	 * @param data_source the data source
	 * @param config the streaming configuration
	 */
	ll_stream_buffer_loader(ll_data_source* data_source,
			const ll_stream_config* config)
		: _config(*config)
	{
		_terminate = false;
		_run_task = false;

		_data_source = data_source;
		_requests_in_current_batch = 0;

		_num_stripes = omp_get_max_threads();
		_buffer = new std::vector<node_pair_t>();
		_buffer_lock = 0;
	}


	/**
	 * Destroy the object
	 */
	virtual ~ll_stream_buffer_loader() {

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

		int64_t dt_behind = 0;

		double time_last_msg = 0;
		bool print_msg = false;

		double rdtsc_per_ms = _config.rdtsc_per_ms();
		uint64_t t_last = ll_rdtsc();
		uint64_t t_run_task = t_last;
		uint64_t dt_last_task = 0;


		// Pull until we run out of data

		while (!_terminate) {

			uint64_t t_start = ll_rdtsc();
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

			double expected_ms = _config.sc_max_edges_per_second == 0 ? 0 
				: 1000 * (org_batch_size / (double) _config.sc_max_edges_per_second);
			uint64_t expected_dt = rdtsc_per_ms * expected_ms;

			size_t n = 0;
			if (batch_size > 0) {
				ll_spinlock_acquire(&_buffer_lock);
				for (size_t i = 0; i < batch_size; i++) {
					node_pair_t x;
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

			if (_run_task) {

				// Note that if we check dt_behind <= 0, we will never run!
				// And if we check t_stop_at < ll_rdtsc(), we get huge stalls!

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
			int64_t behind = _config.sc_max_edges_per_second == 0 ? 0
				: (int64_t) t_last - (int64_t) t_stop_at;
			double ms_behind = behind / rdtsc_per_ms;
			dt_behind = behind;

			if (behind >= 0) {
				
				/*if (rand() % 50 == 0)
					fprintf(stderr, "-- Falling behind: %7.3lf ms\t"
							"[%0.3lf Mreq]\n",
							ms_behind, _buffer->size() / 1000000.0);*/

				// TODO What is the right condition for this warning?
				if (!print_msg && ms_behind > 10000
						&& (int64_t) dt_last_task < 2 * dt_behind) {
					double ct = ll_get_time_ms();
					if (ct - time_last_msg > 1000) {
						time_last_msg = ct;
						LL_W_PRINT("Falling behind: %0.3lf s \n",
								ms_behind / 1000.0);
					}
				}
			}
			else {
				usleep((size_t) (1000.0 * -ms_behind));
				//dt_behind = 0;
				
				/*if (rand() % 50 == 0)
					fprintf(stderr, "%8ld --> %7ld tsc,\t%0.3lf --> %0.3lf ms,"
							"\t%6.3lf %%\t[%0.3lf Mreq]\n",
						behind, behind + (ll_rdtsc() - t_last),
						behind / rdtsc_per_ms,
						(behind + (ll_rdtsc() - t_last)) / rdtsc_per_ms,
						100.0 * (behind + (ll_rdtsc() - t_last)) / -behind,
						_buffer->size() / 1000000.0);*/
			}

			dt_behind += ll_rdtsc() - t_last;
		}
	}


	/**
	 * Swap the buffers and return the original buffer
	 *
	 * @return the buffer (must be deleted by the caller!)
	 */
	std::vector<node_pair_t>* swap_buffers() {
		return swap_buffers(true);
	}


protected:

	/**
	 * Run a task - override to do something useful with the buffer
	 *
	 * @param buffer the buffer
	 */
	virtual void task(std::vector<node_pair_t>& buffer) {}


	/**
	 * Schedule the task
	 */
	void schedule_task() {
		_run_task = true;
		__sync_synchronize();
	}


	/**
	 * Determine if a task is scheduled
	 *
	 * @return true if it is scheduled
	 */
	inline bool task_scheduled() {
		return _run_task;
	}


	/**
	 * Swap the buffers and return the original buffer
	 *
	 * @param lock true to lock
	 * @return the buffer (must be deleted by the caller!)
	 */
	std::vector<node_pair_t>* swap_buffers(bool lock) {

		if (lock) ll_spinlock_acquire(&_buffer_lock);
		std::vector<node_pair_t>* b = _buffer;
		_buffer = new std::vector<node_pair_t>();
		if (lock) ll_spinlock_release(&_buffer_lock);

		//sort_heap(b->begin(), b->end());

		_stats.ss_requests_processed += b->size();

		return b;
	}
};


/**
 * A database loader that pulls continuously into a buffer of node pairs and
 * creates a single-snashot LLAMA on demand.
 */
class ll_stream_single_snapshot_loader : public ll_stream_buffer_loader {

	ll_database* _database;
	const ll_loader_config* _loader_config;
	int _streaming_window;

	std::deque<std::vector<node_pair_t>*> _buffer_queue;
	ll_spinlock_t _buffer_queue_lock;

	ll_writable_graph* _graph;
	double _last_advance_ms;


public:

	/**
	 * Create the object
	 *
	 * @param database the database
	 * @param data_source the data source
	 * @param config the streaming configuration
	 * @param loader_config the loader config
	 * @param window the sliding window size
	 */
	ll_stream_single_snapshot_loader(ll_database* database,
			ll_data_source* data_source,
			const ll_stream_config* config,
			ll_loader_config* loader_config,
			int window)
		: ll_stream_buffer_loader(data_source, config),
		  _database(database), _loader_config(loader_config)
	{
		_streaming_window = window;

		_graph = NULL;
		_buffer_queue_lock = 0;
		_last_advance_ms = 0;
	}


	/**
	 * Destroy the object
	 */
	virtual ~ll_stream_single_snapshot_loader() {
		
		while (!_buffer_queue.empty()) {
			delete _buffer_queue.front();
			_buffer_queue.pop_front();
		}
	}


	/**
	 * Try to grab the loaded graph. Return NULL if it is not yet ready.
	 *
	 * @return the graph (must be freed by the caller), or NULL if not ready
	 */
	ll_writable_graph* try_grab_graph() {
		__COMPILER_FENCE;

		ll_writable_graph* g = *((ll_writable_graph* volatile*) &_graph);

		if (__sync_val_compare_and_swap(&_graph, g, NULL)) {
			return g;
		}
		else {
			return NULL;
		}
	}


	/**
	 * Grab the loaded graph. Block until it is ready (or actually, poll and
	 * sleep in a few microsecond intervals). Note that it is the caller's
	 * responsibility to make sure that generating the graph is in progress.
	 *
	 * @return the graph (must be freed by the caller)
	 */
	ll_writable_graph* wait_for_advance() {

		ll_writable_graph* w = try_grab_graph();

		while (w == NULL) {
			w = try_grab_graph();
			usleep(5);
		}

		return w;
	}


	/**
	 * Get the time it took to advance the sliding window
	 *
	 * @return the time in ms
	 */
	double last_advance_time_ms() {
		return _last_advance_ms;
	}


	/**
	 * Prepare and return the next graph using the current contents of the
	 * buffer and advance the sliding window. If there is already a prepared
	 * graph, return it without preparing a new graph.
	 *
	 * @return the graph (must be freed by the caller)
	 */
	ll_writable_graph* advance() {

		ll_writable_graph* w = try_grab_graph();
		if (w != NULL) return w;

		prepare_next_graph(true);
		return try_grab_graph();
	}


	/**
	 * Schedule the graph to be prepared in the background by the loader
	 * thread and return immediately
	 */
	void advance_in_background() {
		schedule_task();
	}


protected:

	/**
	 * Prepare the next graph using the current contents of the buffer and
	 * advance the sliding window
	 *
	 * @param lock true to lock
	 */
	void prepare_next_graph(bool lock) {

		if (_graph != NULL) {
			LL_W_PRINT("Falling behind! The graph was not yet grabbed, skipping task");
			return;
		}

		// TODO Or should I create a new database instead?
		ll_writable_graph* g = new ll_writable_graph(_database,
				IF_LL_PERSISTENCE(NULL,) 80*1000000/*XXX*/);

		std::vector<node_pair_t>* last_buffer = swap_buffers(lock);
		_stats.ss_last_batch_requests = last_buffer->size();

		ll_spinlock_acquire(&_buffer_queue_lock);

		_buffer_queue.push_back(last_buffer);
		while (_buffer_queue.size() > (size_t) _streaming_window) {
			delete _buffer_queue.front();
			_buffer_queue.pop_front();
		}

		double t_start = ll_get_time_ms();

		ll_node_pair_queue_loader* loader
			= new ll_node_pair_queue_loader(&_buffer_queue);
		bool b = loader->load_direct(&g->ro_graph(), _loader_config);
		if (!b) abort();
		delete loader;

		_last_advance_ms = ll_get_time_ms() - t_start;

		ll_spinlock_release(&_buffer_queue_lock);

		_graph = g;
		__sync_synchronize();
	}


	/**
	 * Run a task - override to do something useful with the buffer
	 *
	 * @param buffer the buffer
	 */
	virtual void task(std::vector<node_pair_t>& buffer) {
		prepare_next_graph(false);
	}
};


/**
 * A database loader that pulls continuously into a buffer and creates another
 * read-optimized level on demand.
 */
class ll_stream_ro_level_loader : public ll_stream_buffer_loader {

	const ll_loader_config* _loader_config;
	int _streaming_window;
	int _keep;

	ll_writable_graph* _graph;

	volatile int _last_good_level;
	int _last_returned_level;
	double _last_advance_ms;


public:

	/**
	 * Create the object
	 *
	 * @param graph the writable graph
	 * @param data_source the data source
	 * @param config the streaming configuration
	 * @param loader_config the loader config
	 * @param window the sliding window size
	 * @param keep the number of complete snapshots to keep (-1 = all)
	 */
	ll_stream_ro_level_loader(ll_writable_graph* graph,
			ll_data_source* data_source,
			const ll_stream_config* config,
			ll_loader_config* loader_config,
			int window, int keep=-1)
		: ll_stream_buffer_loader(data_source, config),
		  _loader_config(loader_config), _graph(graph)
	{
		_streaming_window = window;
		_keep = keep;
		_last_good_level = -1;
		_last_returned_level = -2;
		_last_advance_ms = 0;
	}


	/**
	 * Destroy the object
	 */
	virtual ~ll_stream_ro_level_loader() {
	}


	/**
	 * Get the last good level
	 *
	 * @return the last complete good level, or -1 if none
	 */
	inline int last_good_level() {
		return _last_good_level;
	}


	/**
	 * Grab the loaded graph. Block until it is ready (or actually, poll and
	 * sleep in a few microsecond intervals). Note that it is the caller's
	 * responsibility to make sure that generating the graph is in progress.
	 *
	 * @return the loaded level
	 */
	int wait_for_advance() {

		while (_last_returned_level == _last_good_level) {
			usleep(5);
		}

		_last_returned_level = _last_good_level;
		return _last_returned_level;
	}


	/**
	 * Get the time it took to advance the sliding window
	 *
	 * @return the time in ms
	 */
	double last_advance_time_ms() {
		return _last_advance_ms;
	}


	/**
	 * Schedule the graph to be prepared in the background by the loader
	 * thread and return immediately
	 */
	void advance_in_background() {
		schedule_task();
	}


protected:

	/**
	 * Run a task - override to do something useful with the buffer
	 *
	 * @param buffer the buffer
	 */
	virtual void task(std::vector<node_pair_t>& buffer) {

		ll_writable_graph& graph = *_graph;
		double t_start = ll_get_time_ms();

		_stats.ss_last_batch_requests = buffer.size();

		ll_node_pair_loader* loader = new ll_node_pair_loader(&buffer, false);
		bool b = loader->load_direct(&graph.ro_graph(), _loader_config);
		if (!b) abort();
		delete loader;

		if (_streaming_window > 0
				&& graph.num_levels() >= (size_t) _streaming_window) {
#ifdef LL_MIN_LEVEL
			graph.set_min_level(graph.num_levels() - _streaming_window);
#endif
			if (graph.num_levels() >= (size_t) _streaming_window + 3) {
				graph.delete_level(graph.num_levels()
						- _streaming_window - 3);
			}
		}

		if (_keep > 0) graph.ro_graph().keep_only_recent_versions(_keep);

		_last_good_level = graph.ro_graph().num_levels() - 1;
		__sync_synchronize();

		_last_advance_ms = ll_get_time_ms() - t_start;
	}
};


/**
 * The sliding window configuration
 */
class ll_sliding_window_config {

public:

	int swc_window_snapshots;
	double swc_advance_interval_ms;
	size_t swc_max_advances;
	size_t swc_drain_threshold;


public:

	/**
	 * Create an instance of class ll_sliding_window_config
	 */
	ll_sliding_window_config() {

		swc_window_snapshots = 1;
		swc_advance_interval_ms = 1000;
		swc_max_advances = 0;
		swc_drain_threshold = 100 * 1000;
	}
};


/**
 * The streaming sliding-window driver (SLOTH)
 */
class ll_sliding_window_driver {

	ll_database* _database;
	ll_data_source* _data_source;

	ll_stream_config _stream_config;
	ll_sliding_window_config _window_config;
	ll_loader_config _loader_config;
	int _num_threads;

#if defined(LL_S_DIRECT) && defined(LL_S_SINGLE_SNAPSHOT)
	typedef ll_stream_single_snapshot_loader stream_loader;
#elif defined(LL_S_DIRECT) && !defined(LL_S_SINGLE_SNAPSHOT)
	typedef ll_stream_ro_level_loader stream_loader;
#else /* ! LL_S_DIRECT */
	typedef ll_stream_writable_loader stream_loader;
#endif

	stream_loader* _loader;

	volatile bool _terminate;

	size_t _batch;
	bool _last_skipped;

	double _last_advance_ms;
	double _last_batch_ms;
	double _last_compute_ms;
	double _last_behind_ms;
	double _last_interval_ms;

	double _last_interval_start;


public:

	/**
	 * Create an instance of class ll_sliding_window_driver
	 *
	 * @param database the database
	 * @param data_source the data source
	 * @param stream_config the streaming configuration
	 * @param window_config the sliding window configuration
	 * @param loader_config the loader configuration
	 * @param num_threads the number of threads (-1 = all, must be >= 2)
	 */
	ll_sliding_window_driver(ll_database* database,
			ll_data_source* data_source,
			const ll_stream_config* stream_config,
			const ll_sliding_window_config* window_config,
			const ll_loader_config* loader_config,
			int num_threads=-1) {

		_database = database;
		_data_source = data_source;

		if (stream_config != NULL) _stream_config = *stream_config;
		if (window_config != NULL) _window_config = *window_config;
		if (loader_config != NULL) _loader_config = *loader_config;

		_num_threads = num_threads <= 0 ? omp_get_max_threads() : num_threads;
		assert(_num_threads >= 2);

#if defined(LL_S_DIRECT) && defined(LL_S_SINGLE_SNAPSHOT)
		_loader = new ll_stream_single_snapshot_loader(_database,
				_data_source, &_stream_config, &_loader_config,
				_window_config.swc_window_snapshots);
#elif defined(LL_S_DIRECT) && !defined(LL_S_SINGLE_SNAPSHOT)
		_loader = new ll_stream_ro_level_loader(_database->graph(),
				_data_source, &_stream_config, &_loader_config,
				_window_config.swc_window_snapshots,
				_window_config.swc_window_snapshots == 3 ? -1 : 2);
#else /* ! LL_S_DIRECT */
		_loader = new ll_stream_writable_loader(_database->graph(),
				_data_source, &_stream_config, &_loader_config,
				_window_config.swc_window_snapshots);
#endif

		_terminate = false;
		_batch = 0;
		_last_skipped = false;

		_last_advance_ms = 0;
		_last_batch_ms = 0;
		_last_compute_ms = 0;
		_last_behind_ms = 0;
		_last_interval_ms = 0;

		_last_interval_start = 0;
	}


	/**
	 * Destroy the class
	 */
	virtual ~ll_sliding_window_driver() {

		delete _loader;
	}


	/**
	 * Get the streaming stats
	 *
	 * @return the stats
	 */
	inline const ll_stream_stats& stream_stats() {
		return _loader->stats();
	}


	/**
	 * Get the stream loader configuration
	 *
	 * @return the configuration
	 */
	inline const ll_stream_config& stream_config() {
		return _stream_config;
	}


	/**
	 * Get the sliding window configuration
	 *
	 * @return the configuration
	 */
	inline const ll_sliding_window_config& window_config() {
		return _window_config;
	}


	/**
	 * Get the graph loader configuration
	 *
	 * @return the configuration
	 */
	inline const ll_loader_config& loader_config() {
		return _loader_config;
	}


	/**
	 * Get the batch number
	 *
	 * @return the batch number
	 */
	inline size_t batch() {
		return _batch;
	}


	/**
	 * Get the time it took to advance the sliding window
	 *
	 * @return the time in ms
	 */
	double last_advance_time_ms() {
		return _last_advance_ms;
	}


	/**
	 * Get the time it took to perfrom the batch in the execution thread
	 *
	 * @return the batch time in ms
	 */
	double last_batch_time_ms() {
		return _last_batch_ms;
	}


	/**
	 * Get the time it took to perform the computation
	 *
	 * @return the time in ms
	 */
	double last_compute_time_ms() {
		return _last_compute_ms;
	}


	/**
	 * Get the time of the last interval between starts of two batches
	 *
	 * @return the time in ms
	 */
	double last_interval_time_ms() {
		return _last_interval_ms;
	}


	/**
	 * Get the number of processed requests in the last batch
	 *
	 * @return the number of processed requests (in most cases, just edges)
	 */
	size_t last_batch_requests() {
		return _loader->stats().ss_last_batch_requests;
	}


	/**
	 * Flag the driver to terminate at the beginning of the next batch
	 */
	void terminate() {
		_loader->terminate();
		_terminate = true;
	}


	/**
	 * Run the driver
	 */
	void run() {

		// Initialize OpenMP for the driver

		int prev_max_threads = omp_get_max_threads();
		int prev_nested = omp_get_nested();

		int concurrent_load_threads = 1;
		int concurrent_task_threads = _num_threads - concurrent_load_threads;
		if (concurrent_task_threads <= 0) {
			LL_E_PRINT("Error: No threads left for execution\n");
			abort();
		}

		omp_set_num_threads(2);		// Probably not necessary
		omp_set_nested(1);


		// Initialize streaming

		_batch = 0;
		_last_behind_ms = 0;
		_last_skipped = false;
		_last_interval_start = ll_get_time_ms();

		volatile bool done = false;


		// Threads

#		pragma omp parallel sections
		{

			// The load section

#			pragma omp section
			{
				omp_set_num_threads(concurrent_load_threads);

				_loader->run();

				done = true;
				__sync_synchronize();
			}


			// The task execution section

#			pragma omp section
			{
				omp_set_num_threads(concurrent_task_threads);

				usleep(1000 * _window_config.swc_advance_interval_ms);

#ifdef LL_S_DIRECT
				_loader->advance_in_background();

				// TODO This is too much of a hack:
				_loader->wait_for_advance();
				usleep(1000 * _window_config.swc_advance_interval_ms);
				_loader->advance_in_background();
#endif

				while (!done) {

					if (_window_config.swc_max_advances > 0) {
						if (_batch >= _window_config.swc_max_advances) {
							_loader->terminate();
							break;
						}
					}

					if (_terminate) {
						_loader->terminate();
						break;
					}

					_batch++;
					_last_skipped = false;

					double t_batch_start = ll_get_time_ms();
					_last_interval_ms = t_batch_start - _last_interval_start;
					_last_interval_start = t_batch_start;

					before_batch();

#if defined(LL_S_DIRECT) && defined(LL_S_SINGLE_SNAPSHOT)

					ll_writable_graph* w = _loader->wait_for_advance();
					_last_advance_ms = _loader->last_advance_time_ms();
					_loader->advance_in_background();

					double t_compute_start = ll_get_time_ms();
					compute(w->ro_graph());
					_last_compute_ms = ll_get_time_ms() - t_compute_start;

					delete w;

#elif defined(LL_S_DIRECT) && !defined(LL_S_SINGLE_SNAPSHOT)

					int level = _loader->wait_for_advance();
					_last_advance_ms = _loader->last_advance_time_ms();
					_loader->advance_in_background();

					if (level < 0) {
						_last_skipped = true;
						skipped();
					}
					else {
						ll_mlcsr_ro_graph G_ro(&_database->graph()->ro_graph(),
								level);
						assert(G_ro.num_levels() > 0);
						double t_compute_start = ll_get_time_ms();
						compute(G_ro);
						_last_compute_ms = ll_get_time_ms() - t_compute_start;
					}

#else /* ! LL_S_DIRECT */

					if (_window_config.swc_drain_threshold > 0) {
						if (_loader->stats().num_outstanding_requests()
								> _window_config.swc_drain_threshold) {
							_loader->drain();
						}
					}

					double t_advance_start = ll_get_time_ms();
					_loader->advance();
					_last_advance_ms = ll_get_time_ms() - t_advance_start;

					double t_compute_start = ll_get_time_ms();
					compute(_database->graph()->ro_graph());
					_last_compute_ms = ll_get_time_ms() - t_compute_start;
#endif

					_loader->reset_batch_counters();
					_last_batch_ms = ll_get_time_ms() - t_batch_start;

					after_batch();


					// Take care of the sliding window timing

					if (_window_config.swc_advance_interval_ms > 0) {

						double t = _window_config.swc_advance_interval_ms
							- _last_batch_ms - _last_behind_ms;

						if (t > 0) {
							usleep((long) (t * 1000.0));
							_last_behind_ms = 0;
						}
						else {
							_last_behind_ms = -t;
							behind(-t);
						}
					}
				}
			}
		}


		// Finish
		
		finished();

		omp_set_nested(prev_nested);
		omp_set_num_threads(prev_max_threads);
	}


protected:

	/**
	 * Callback for before starting a batch in an execution thread
	 */
	virtual void before_batch() {}


	/**
	 * Callback for after completing a batch in an execution thread
	 */
	virtual void after_batch() {}


	/**
	 * Callback for skipping a round of computation (e.g. if the graph is not
	 * ready)
	 */
	virtual void skipped() {}


	/**
	 * Callback for being behind
	 *
	 * @param ms the number of ms we are behind the schedule
	 */
	virtual void behind(double ms) {}


	/**
	 * Callback for finishing the entire run
	 */
	virtual void finished() {}


	/**
	 * Run the computation
	 *
	 * @param G the graph
	 */
	virtual void compute(ll_mlcsr_ro_graph& G) {}
};

#endif
