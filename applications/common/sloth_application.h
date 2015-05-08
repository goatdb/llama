/*
 * sloth_application.h
 * LLAMA Graph Analytics
 *
 * Copyright 2015
 *      The President and Fellows of Harvard College.
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

#ifndef SLOTH_APPLICATION_H_
#define SLOTH_APPLICATION_H_

#include <sloth.h>


/**
 * The configuration
 */
class sloth_test_application_config {

public:

	// Streaming options
	
	sloth_config sloth;
	bool verbose;
	bool print_results;
	bool fail_if_behind;


	/**
	 * Create an instance of class sloth_test_application_config
	 */
	inline sloth_test_application_config() {

		verbose = false;
		print_results = true;
		fail_if_behind = false;
	}
};


/**
 * The abstract application class
 */
template <typename T, class Config>
class sloth_test_application : public sloth_application<T> {

	std::string _name;
	std::string _code;


protected:

	Config _config;


public:

	/**
	 * Create an instance of class sloth_test_application
	 *
	 * @param data_source the generic data source
	 * @param config the configuration
	 * @param name the application name
	 * @param code the application code
	 */
	sloth_test_application(ll_generic_data_source<T>* data_source,
			const Config* config, const char* name, const char* code)
		: sloth_application<T>(data_source, &config->sloth),
		_config(*config) {

		_name = name;
		_code = code;
	}


	/**
	 * Destroy the application
	 */
	virtual ~sloth_test_application() {
	}


	/**
	 * Get the application name
	 *
	 * @return the name
	 */
	inline const char* name() { return _name.c_str(); }


	/**
	 * Get the application code
	 *
	 * @return the code
	 */
	inline const char* code() { return _code.c_str(); }


	/**
	 * Print the results
	 *
	 * @param file the output file
	 */
	virtual void print_results(FILE* file) = 0;


	/**
	 * Get the configuration
	 *
	 * @return the configuration
	 */
	inline const Config& config() {
		return _config;
	}


	/**
	 * Get the number of nodes
	 *
	 * @return the number of nodes
	 */
	virtual size_t num_nodes() = 0;
};



//==========================================================================//
// UI for the Sloth Applications                                            //
//==========================================================================//

/**
 * UI for the Sloth Applications
 */
template <class Application>
class sloth_test_ui : public sloth_ui_callbacks {

	Application* _application;

	bool _warn_on_behind_batch;
	bool _fail_on_behind_batch;
	double _batch_behind_threshold;

	bool _warn_on_behind_rate;
	bool _fail_on_behind_rate;
	double _target_rate_threshold;

	size_t _behind_count_fail_threshold;

	size_t _count_consecutive_behind_batch;
	size_t _count_consecutive_behind_rate;
	size_t _last_behind_batch_batch;
	bool _failed;
	
	FILE* _info_file;
	bool _info_tty;
	FILE* _progress_file;
	bool _progress_tty;
	FILE* _results_file;


public:

	/**
	 * Create an instance of sloth_test_ui
	 *
	 * @param application an instance of sloth_test_application
	 */
	sloth_test_ui(Application* application) {

		_application = application;
		_application->set_ui(this);

		_warn_on_behind_batch = false;
		_fail_on_behind_batch = application->config().fail_if_behind;
		_warn_on_behind_rate = false;
		_fail_on_behind_rate = application->config().fail_if_behind;

		_batch_behind_threshold = 0.05;
		_target_rate_threshold = 0.05;
		_behind_count_fail_threshold = 2;

		_count_consecutive_behind_batch = 0;
		_count_consecutive_behind_rate = 0;
		_last_behind_batch_batch = 0;
		_failed = false;


		// Output files

		_info_file = stderr;
		_info_tty = ll_is_tty(_info_file);

		_progress_file = stderr;
		_progress_tty = ll_is_tty(_progress_file);

		_results_file = stderr;
	}


	/**
	 * Destroy the instance of this class
	 */
	virtual ~sloth_test_ui() {}


	/**
	 * Return true if the run failed
	 *
	 * @param true if the run failed
	 */
	inline bool failed() {
		return _failed;
	}


	/**
	 * Callback for starting the run
	 */
	virtual void before_run() {

#ifdef LL_S_SINGLE_SNAPSHOT
		const char* type = "Single-Snapshot";
		const char* code1 = "ss";
#else
		const char* type = "Multiversioned";
		const char* code1 = "mv";
#endif

#ifdef DEDUP
#	ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES
		const char* dedup = "Weighted (Type 3)";
		const char* code2 = "w";
#	else
		const char* dedup = "Simple (Type 2)";
		const char* code2 = "d";
#	endif
#else
		const char* dedup = "None (Type 1)";
		const char* code2 = "";
#endif

#ifdef LL_DELETIONS
		const char* dv = "Yes";
#else
		const char* dv = "No";
#endif

		fprintf(_info_file, "%sBenchmark Name       :%s %s\n",
				_info_tty ? LL_C_B_BLUE : "", _info_tty ? LL_C_RESET : "",
				_application->name());
		fprintf(_info_file, "%sBenchmark Code       :%s %s\n",
				_info_tty ? LL_C_B_BLUE : "", _info_tty ? LL_C_RESET : "",
				_application->code());

		fprintf(_info_file, "\n");
		
		fprintf(_info_file, "%sSLOTH Implementation :%s %s\n",
				_info_tty ? LL_C_B_BLUE : "", _info_tty ? LL_C_RESET : "",
				type);
		fprintf(_info_file, "%sSLOTH Deduplication  :%s %s\n",
				_info_tty ? LL_C_B_BLUE : "", _info_tty ? LL_C_RESET : "",
				dedup);
		fprintf(_info_file, "%sLLAMA Deletion Vector:%s %s\n",
				_info_tty ? LL_C_B_BLUE : "", _info_tty ? LL_C_RESET : "",
				dv);
		fprintf(_info_file, "%sConfiguration Code   :%s %s%s\n",
				_info_tty ? LL_C_B_BLUE : "", _info_tty ? LL_C_RESET : "",
				code1, code2);

		fprintf(_info_file, "\n");
		fflush(_info_file);
	}


	/**
	 * Callback for finishing the entire run
	 */
	virtual void after_run() {}


	/**
	 * Callback for starting the batch
	 */
	virtual void before_batch() {

		ll_sliding_window_driver& driver = _application->driver();
		
		fprintf(_progress_file, "%s%4ld: %s",
				_progress_tty ? LL_C_B_BLUE : "",
				driver.batch(),
				_progress_tty ? LL_C_RESET : "");
		fflush(_progress_file);
	}


	/**
	 * Callback for finishing the batch
	 */
	virtual void after_batch() {

		ll_sliding_window_driver& driver = _application->driver();

		if (_progress_tty) {
			fprintf(_progress_file, "\r%s%4ld: ",
					_progress_tty ? LL_C_B_BLUE : "",
					driver.batch());
		}

		double inputs_per_second = _application->last_batch_inputs()
			/ driver.last_interval_time_ms() * 1000.0;
		double edges_per_second = driver.last_batch_requests()
			/ driver.last_interval_time_ms() * 1000.0;

		fprintf(_progress_file, "%s"
				"%5.0lf K records/s, %5.0lf K edges/s, "
				"%6.3lf s adv, %6.3lf s run, "
				"%6.3lf GB MaxRSS, %6.3lf M nodes%s\n",
				_progress_tty ? LL_C_B_BLUE : "",
				inputs_per_second / 1000.0,
				edges_per_second / 1000.0,
				driver.last_advance_time_ms() / 1000.0,
				driver.last_compute_time_ms() / 1000.0,
				ll_get_maxrss_kb() / 1048576.0,
				_application->num_nodes() / 1000000.0,
				_progress_tty ? LL_C_RESET : "");
		fflush(_progress_file);


		// Deal with target rate threshold, but not for the first batch, which
		// is allowed to be behind

		if (_target_rate_threshold >= 0 && driver.batch() > 1) {

			size_t max_eps = driver.stream_config().sc_max_edges_per_second;
			size_t max_ips = _application->max_inputs_per_second();

			bool behind_eps = max_eps > 0 && edges_per_second - max_eps
					< -_target_rate_threshold * max_eps;
			bool behind_ips = max_ips > 0 && inputs_per_second - max_ips
					< -_target_rate_threshold * max_ips;

			if (behind_eps || behind_ips) {
				_count_consecutive_behind_rate++;

				if (_behind_count_fail_threshold > 0
						&& (_warn_on_behind_rate || _fail_on_behind_rate)
						&& _count_consecutive_behind_rate
						>= _behind_count_fail_threshold) {

					if (_progress_tty) {
						fprintf(_progress_file, "\r%s%4ld: ",
								_progress_tty ? LL_C_B_BLUE : "",
								driver.batch());
					}

					double p_eps = max_eps > 0 ? (max_eps - edges_per_second)
						/ max_eps : 0;
					double p_ips = max_ips > 0 ? (max_ips - inputs_per_second)
						/ max_ips : 0;

					fprintf(_progress_file, "%sRate %0.2lf%% below the target"
							"%s%s\n",
							_progress_tty ? (_fail_on_behind_rate
								? LL_C_B_RED : LL_C_B_YELLOW) : "",
							std::max(p_eps, p_ips) * 100,
							_fail_on_behind_rate ? ", stopping" : "",
							_progress_tty ? LL_C_RESET : "");

					if (_fail_on_behind_rate) {
						_application->terminate();
						_failed = true;
					}
				}
			}
			else {
				_count_consecutive_behind_rate = 0;
			}
		}


		// Print the results

		if (_application->config().print_results) {
			fprintf(_results_file, "\n");
			_application->print_results(_results_file);
			fprintf(_results_file, "\n");
			fflush(_results_file);
		}
	}


	/**
	 * Callback for being behind
	 *
	 * @param ms the number of ms we are behind the schedule
	 */
	virtual void behind(double ms) {

		if (!_warn_on_behind_batch && !_fail_on_behind_batch) return;
		
		ll_sliding_window_driver& driver = _application->driver();

		ssize_t t = driver.window_config().swc_advance_interval_ms;
		double p = t <= 0 ? 0 : ms/driver.last_interval_time_ms();
		if (p < _batch_behind_threshold) return;

		if (_last_behind_batch_batch + 1 < driver.batch()) {
			_count_consecutive_behind_batch = 0;
		}
		_count_consecutive_behind_batch++;
		_last_behind_batch_batch = driver.batch();

		if (_count_consecutive_behind_batch < _behind_count_fail_threshold) {
			return;
		}

		if (_progress_tty) {
			fprintf(_progress_file, "\r%s%4ld: ",
					_progress_tty ? LL_C_B_BLUE : "",
					driver.batch());
		}

		fprintf(_progress_file, " %sCompute threads are %6.3lf s behind",
				_progress_tty ? (_fail_on_behind_batch
					? LL_C_B_RED : LL_C_B_YELLOW) : "",
				ms / 1000.0);
		if (t > 0) fprintf(_progress_file, " (%0.2lf%%)", 100 * p);
		if (_fail_on_behind_batch) fprintf(_progress_file, ", stopping");

		fprintf(_progress_file, "%s\n", _progress_tty ? LL_C_RESET : "");
		fflush(_progress_file);

		if (_fail_on_behind_batch) {
			_application->terminate();
			_failed = true;
		}
	}



	/**
	 * Callback for before starting a computation
	 */
	virtual void before_computation() {}


	/**
	 * Callback for after finishing a computation
	 */
	virtual void after_computation() {}
};



//==========================================================================//
// Application Constructor and Metadata                                     //
//==========================================================================//

/**
 * The test application creator
 */
template <class Application, class Config, typename T>
class sloth_test_application_creator {

	/**
	 * Information about a test application
	 */
	typedef struct {

		std::string m_code_long;
		std::string m_code_short;
		std::string m_name;
		std::string m_description;

	} meta_t;

	std::vector<meta_t> _apps;


public:

	/**
	 * Create an object of type sloth_test_application_creator
	 */
	sloth_test_application_creator() {}


	/**
	 * Destroy an instance of sloth_test_application_creator
	 */
	virtual ~sloth_test_application_creator() {}


	/**
	 * Create an instance of the application
	 *
	 * @param code the application code
	 * @param data_source the data source
	 * @param config the configuration
	 * @return the application object, or NULL on error (error msg already printed)
	 */
	virtual Application* create(const char* code,
			ll_generic_data_source<T>* data_source, const Config* config) = 0;


protected:

	/**
	 * Add an application
	 *
	 * @param name the name
	 * @param code_long the long code
	 * @param code_short the short code
	 * @param description the description
	 */
	void add(const char* name, const char* code_long, const char* code_short,
			const char* description) {

		meta_t m;
		m.m_name = name;
		m.m_code_long = code_long;
		m.m_code_short = code_short;
		m.m_description = description;

		_apps.push_back(m);
	}
};


#endif /* SLOTH_APPLICATION_H_ */

