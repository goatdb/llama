/*
 * sloth.h
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


#ifndef SLOTH_H_
#define SLOTH_H_

#ifndef LL_STREAMING
#	define LL_STREAMING
#endif

#include <llama.h>


/**
 * UI callbacks for a sliding window application
 */
class sloth_ui_callbacks {

public:

	/**
	 * Create an instance of sloth_ui_callbacks
	 */
	inline sloth_ui_callbacks() {}


	/**
	 * Destroy the instance of this class
	 */
	virtual ~sloth_ui_callbacks() {}


	/**
	 * Callback for starting the run
	 */
	virtual void before_run() {}


	/**
	 * Callback for finishing the entire run
	 */
	virtual void after_run() {}


	/**
	 * Callback for starting the batch
	 */
	virtual void before_batch() {}


	/**
	 * Callback for finishing the batch
	 */
	virtual void after_batch() {}


	/**
	 * Callback for being behind
	 *
	 * @param ms the number of ms we are behind the schedule
	 */
	virtual void behind(double ms) {}


	/**
	 * Callback for before starting a computation
	 */
	virtual void before_computation() {}


	/**
	 * Callback for after finishing a computation
	 */
	virtual void after_computation() {}
};



/**
 * SLOTH configuration
 */
class sloth_config {

public:

	ll_stream_config stream_config;
	ll_sliding_window_config window_config;
	ll_loader_config loader_config;

	int num_threads;
	ssize_t max_inputs_per_second;


	/**
	 * Create an instance of sloth_config
	 */
	inline sloth_config() {
		num_threads = -1;
		max_inputs_per_second = -1;
	}
};



/**
 * A sliding window application
 */
template <typename input_t>
class sloth_application {
	
	ll_database _database;
	sloth_ui_callbacks* _ui;
	double _rdtsc_per_ms;

	ssize_t _max_inputs_per_second;

	bool _initialized;
	size_t _last_batch_inputs;


public:

	/**
	 * Create an instance of class sloth_application
	 *
	 * @param data_source the generic data source
	 * @param stream_config the streaming configuration
	 * @param window_config the sliding window configuration
	 * @param loader_config the loader configuration
	 * @param num_threads the number of threads (-1 = all, must be >= 2)
	 * @param max_inputs_per_second the maximum ingest rate (<= 0 = unliminted)
	 */
	sloth_application(ll_generic_data_source<input_t>* data_source,
			const ll_stream_config* stream_config,
			const ll_sliding_window_config* window_config,
			const ll_loader_config* loader_config=NULL,
			int num_threads=-1, ssize_t max_inputs_per_second=-1)
		: _source(*this, data_source),
		  _driver(*this, &_database, &_source, stream_config, window_config,
				loader_config, num_threads) {

		_ui = NULL;
		_initialized = false;
		_last_batch_inputs = 0;
		_rdtsc_per_ms = stream_config->rdtsc_per_ms();
		_max_inputs_per_second = max_inputs_per_second;
	}


	/**
	 * Create an instance of class sloth_application
	 *
	 * @param data_source the generic data source
	 * @param config the configuration
	 */
	sloth_application(ll_generic_data_source<input_t>* data_source,
			const sloth_config* config)
		: _source(*this, data_source),
		  _driver(*this, &_database, &_source, &config->stream_config,
				  &config->window_config, &config->loader_config,
				  config->num_threads) {

		_ui = NULL;
		_initialized = false;
		_last_batch_inputs = 0;
		_rdtsc_per_ms = config->stream_config.rdtsc_per_ms();
		_max_inputs_per_second = config->max_inputs_per_second;
	}


	/**
	 * Destroy the application
	 */
	virtual ~sloth_application() {
	}


	/**
	 * Set the callbacks
	 *
	 * @param ui the UI callbacks
	 */
	virtual void set_ui(sloth_ui_callbacks* ui) {
		_ui = ui;
	}


	/**
	 * Get the maximum inputs per second
	 *
	 * @return the max inputs per second (0 = unlimited)
	 */
	inline size_t max_inputs_per_second() {
		return _max_inputs_per_second <= 0 ? 0 : _max_inputs_per_second;
	}


	/**
	 * Run the application
	 */
	void run() {

		if (!_initialized) {
			on_initialize(*_database.graph());
			_initialized = true;
		}

		if (_ui) _ui->before_run();
		_driver.run();
		if (_ui) _ui->after_run();
	}


	/**
	 * Flag the driver to terminate at the beginning of the next batch
	 */
	void terminate() {
		_driver.terminate();
	}


	/**
	 * Get the sliding window driver
	 * 
	 * @return the driver
	 */
	ll_sliding_window_driver& driver() {
		return _driver;
	}


	/**
	 * Get the number of processed inputs in the last batch
	 *
	 * @return the number of inputs
	 */
	size_t last_batch_inputs() {
		return _last_batch_inputs;
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
	inline void add_edge(node_t tail, node_t head) {
		_source.add_edge_helper(tail, head);
	}


	/**
	 * Custom graph initialization
	 *
	 * @param W the writable graph
	 */
	virtual void on_initialize(ll_writable_graph& W) {}


	/**
	 * Processs an input item and generate corresponding edges
	 *
	 * @param input the input item
	 */
	virtual void on_input(const input_t* input) = 0;


	/**
	 * Run the computation when the sliding window advances
	 *
	 * @param G the graph
	 */
	virtual void on_advance(ll_mlcsr_ro_graph& G) = 0;


private:

	/**
	 * The data source adapter
	 */
	class data_source_adapter : public ll_simple_data_source_adapter<input_t> {

		sloth_application<input_t>& _owner;
		int64_t _last_input_time;
		int64_t _ahead;


	public:

		/**
		 * Create an instance of the class
		 *
		 * @param owner the owner
		 * @param data_source the generic data source
		 */
		data_source_adapter(sloth_application<input_t>& owner,
				ll_generic_data_source<input_t>* data_source)
			: ll_simple_data_source_adapter<input_t>(data_source, false),
			_owner(owner) {

			_last_input_time = ll_rdtsc();
			_ahead = 0;
		}


		/**
		 * Add an edge to the buffer.
		 *
		 * @param tail the tail
		 * @param head the head
		 */
		inline void add_edge_helper(node_t tail, node_t head) {
			add_edge(tail, head);
		}


	protected:

		/**
		 * Processs an input item and generate corresponding edges
		 *
		 * @param input the input item
		 */
		virtual void process_input(const input_t* input) {

			_owner.on_input(input);

			if (_owner._max_inputs_per_second > 0) {
				
				int64_t t = ll_rdtsc();
				int64_t dt = t - _last_input_time;
				_last_input_time = t;

				// [expected]
				//   = (1000 * rdtsc / ms) / (input / s)
				//   = (rdtsc / s) / (input / s)
				//   = (rdtsc / s) * (s / input)
				//   = rdtsc / input
				int64_t expected = 1000 * _owner._rdtsc_per_ms
					/ _owner._max_inputs_per_second;

				_ahead += expected - dt;

				if (_ahead > 0) {
					double ahead_ms = _ahead / _owner._rdtsc_per_ms;
					usleep((long) (ahead_ms * 1000));
				}
			}
		}
	};

	data_source_adapter _source;


	/**
	 * The driver
	 */
	class application_driver : public ll_sliding_window_driver {

		sloth_application<input_t>& _owner;
		size_t _last_num_inputs;


	public:

		/**
		 * Create an instance of class application_driver
		 *
		 * @param owner the owner
		 * @param database the database
		 * @param data_source the data source
		 * @param stream_config the streaming configuration
		 * @param window_config the sliding window configuration
		 * @param loader_config the loader configuration
		 * @param num_threads the number of threads (-1 = all, must be >= 2)
		 */
		application_driver(sloth_application<input_t>& owner,
				ll_database* database,
				data_source_adapter* data_source,
				const ll_stream_config* stream_config,
				const ll_sliding_window_config* window_config,
				const ll_loader_config* loader_config,
				int num_threads)
		: ll_sliding_window_driver(database, data_source, stream_config,
				window_config, loader_config, num_threads), _owner(owner) {

			_last_num_inputs = 0;
		}


	protected:

		/**
		 * Callback for before starting a batch in an execution thread
		 */
		virtual void before_batch() {
			size_t l = _owner._source.num_processed_inputs();
			_owner._last_batch_inputs = l - _last_num_inputs;
			_last_num_inputs = l;
			if (_owner._ui) _owner._ui->before_batch();
		}


		/**
		 * Callback for after completing a batch in an execution thread
		 */
		virtual void after_batch() {
			if (_owner._ui) _owner._ui->after_batch();
		}


		/**
		 * Callback for skipping a round of computation (e.g. if the graph is
		 * not ready)
		 */
		virtual void skipped() {}


		/**
		 * Callback for being behind
		 *
		 * @param ms the number of ms we are behind the schedule
		 */
		virtual void behind(double ms) {
			if (_owner._ui) _owner._ui->behind(ms);
		}


		/**
		 * Callback for finishing the entire run
		 */
		virtual void finished() {}


		/**
		 * Run the computation
		 *
		 * @param G the graph
		 */
		virtual void compute(ll_mlcsr_ro_graph& G) {
			if (_owner._ui) _owner._ui->before_computation();
			_owner.on_advance(G);
			if (_owner._ui) _owner._ui->after_computation();
		}
	};

	application_driver _driver;
};

#endif
