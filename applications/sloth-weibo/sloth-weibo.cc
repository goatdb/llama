/*
 * sloth-weibo.cc
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

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <getopt.h>
#include <libgen.h>
#include <unordered_map>

#include <sloth.h>

#include "weibo_data_source.h"



//==========================================================================//
// SLOTH Weibo Applications                                                 //
//==========================================================================//

/**
 * The configuration
 */
class sloth_weibo_config {

public:

	sloth_config sloth;
	bool verbose;
	bool print_results;

	double tr_retweet_probability;
	double tr_diff_threshold;
	size_t tr_max_iterations;
	size_t tr_top_n;


	/**
	 * Create an instance of class sloth_weibo_config
	 */
	inline sloth_weibo_config() {

		verbose = false;
		print_results = true;

		tr_retweet_probability = 0.05;
		tr_diff_threshold = 1e-10;
		tr_max_iterations = 10;
		tr_top_n = 10;
	}
};


/**
 * The application class
 */
class sloth_weibo_application : public sloth_application<tweet_t> {

	typedef std::unordered_map<std::string, node_t> string_to_node_map_t;

	std::vector<std::string> _users;
	string_to_node_map_t _users_to_nodes;
	node_t _next_available_node;


protected:

	sloth_weibo_config _config;


public:

	/**
	 * Create an instance of class sloth_weibo_application
	 *
	 * @param data_source the generic data source
	 * @param config the configuration
	 */
	sloth_weibo_application(ll_generic_data_source<tweet_t>* data_source,
			const sloth_weibo_config* config)
		: sloth_application<tweet_t>(data_source, &config->sloth),
		_config(*config) {

		_next_available_node = 0;
	}


	/**
	 * Destroy the application
	 */
	virtual ~sloth_weibo_application() {
	}


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
	inline const sloth_weibo_config& config() {
		return _config;
	}


	/**
	 * Get the number of users
	 *
	 * @return the number of users
	 */
	size_t num_users() {
		return _users.size();
	}


protected:

	/**
	 * Translate user ID to the corresponding node ID. Create a new node if it
	 * does not already exists.
	 *
	 * @param user the user name
	 * @return the user node ID
	 */
	node_t translate_user_name(const std::string& user) {

		string_to_node_map_t::iterator i = _users_to_nodes.find(user);

		if (i == _users_to_nodes.end()) {
			node_t n = _next_available_node++;
			_users_to_nodes[user] = n;
			_users.push_back(user);
			assert(_users.size() == _next_available_node);
			return n;
		}

		return i->second;
	}


	/**
	 * Get user by node ID
	 *
	 * @param node the node ID
	 * @return the user name, or NULL if out of range
	 */
	const char* user(node_t node) {
		if (node < 0 || node >= (ssize_t) _users.size()) return NULL;
		return _users[node].c_str();
	}


	/**
	 * Extract mentions edges from the tweet
	 *
	 * @param tweet the tweet
	 */
	void extract_mentions_edges(const tweet_t& tweet) {

		node_t sender = LL_NIL_NODE;

		for (const char* s = tweet.t_text; *s != '\0'; s++) {
			if (*s != '@') continue;
			
			const char* start = ++s;
			while (isalnum(*s)) s++;
			const char* end = s;

			std::string user = std::string(start, (long) end - (long) start);

			// Filter -- using what we know about the anonymized data set
			if (user == "ukn") continue;
			if (user.length() < 8) continue;
			if (*start != 'u') continue;

			if (sender == LL_NIL_NODE) {
				sender = translate_user_name(std::string(tweet.t_user));
			}

			add_edge(sender, translate_user_name(user));
			//fprintf(stderr, "%s\n", user.c_str());
		}
	}
};



/**
 * Application: User ranking
 */
class swa_user_ranking : public sloth_weibo_application {

	float* _tunk_rank;
	size_t _tunk_rank_n;

	struct top_user {

		float tu_score;
		unsigned tu_index;

		bool operator() (const top_user& a, const top_user& b) {
			return a.tu_score > b.tu_score;
		}
	};

	top_user* _top;
	size_t _top_n;


public:

	/**
	 * Create an instance of class swa_user_ranking 
	 *
	 * @param data_source the generic data source
	 * @param config the configuration
	 */
	swa_user_ranking(ll_generic_data_source<tweet_t>* data_source,
			const sloth_weibo_config* config)
		: sloth_weibo_application(data_source, config) {
	
		_tunk_rank = NULL;
		_tunk_rank_n = 0;

		_top = (top_user*) malloc(sizeof(top_user) * _config.tr_top_n);
		_top_n = 0;
	}


	/**
	 * Destroy the application
	 */
	virtual ~swa_user_ranking() {

		if (_tunk_rank) free(_tunk_rank);
		if (_top) free(_top);
	}


	/**
	 * Print the results
	 *
	 * @param file the output file
	 */
	virtual void print_results(FILE* file) {

		for (size_t i = 0; i < _top_n; i++) {
			fprintf(file, "%2lu: %s (%0.6lf)\n",
					i, user(_top[i].tu_index), _top[i].tu_score);
		}
	}


protected:


	/**
	 * Processs an input item and generate corresponding edges
	 *
	 * @param input the input item
	 */
	virtual void on_input(const tweet_t* input) {
		extract_mentions_edges(*input);
	}


	/**
	 * Run the computation when the sliding window advances
	 *
	 * @param G the graph
	 */
	virtual void on_advance(ll_mlcsr_ro_graph& G) {

		// TunkRank
		// http://thenoisychannel.com/2009/01/13/a-twitter-analog-to-pagerank

		ll_memory_helper m;

		size_t N = G.max_nodes();
		float p = _config.tr_retweet_probability;
		double diff = 0;
		size_t iteration = 0;

		if (_tunk_rank == NULL || _tunk_rank_n < N) {
			if (_tunk_rank) free(_tunk_rank);
			_tunk_rank_n = (size_t) (N * 1.05);
			_tunk_rank = (float*) malloc(sizeof(float) * _tunk_rank_n);
		}

		float* tr = _tunk_rank;
		float* tr_next = m.allocate<float>(N);

		ll_foreach_node_omp(n, G) tr[n] = 1.0f / N;
		ll_foreach_node_omp(n, G) tr_next[n] = 0;

		do {
			float total = 0;

			#pragma omp parallel
			{
				float total_prv = 0;

				#pragma omp for schedule(dynamic,4096)
				ll_foreach_node(n, G) {

					int degree = G.out_degree(n);
					if (degree == 0) continue;

					float delta = (1 + p * tr[n]) / degree;
					ll_foreach_out(w, G, n) {
						ATOMIC_ADD<float>(&tr_next[w], delta);
						total_prv += delta;
					}
				}

				ATOMIC_ADD(&total, total_prv);
			}

			diff = 0;

			#pragma omp parallel
			{
				double diff_prv = 0;

				#pragma omp for schedule(dynamic,4096)
				ll_foreach_node(n, G) {
					float t = tr_next[n] / total;
					diff_prv += (double) std::abs(t - tr[n]);
					tr[n] = t;
					tr_next[n] = 0;
				}

				ATOMIC_ADD(&diff, diff_prv);
			}

			iteration++;

			if (_config.verbose) {
				fprintf(stderr, "Iteration %lu: Diff = %lf\n", iteration,
						diff);
			}
		}
		while ((iteration < _config.tr_max_iterations)
				&& (diff >= _config.tr_diff_threshold));


		// Find the top N users

		_top_n = std::min(N, _config.tr_top_n);
		if (_top_n > 0) {

			for (size_t i = 0; i < _top_n; i++) {
				_top[i].tu_score = tr[i];
				_top[i].tu_index = i;
			}

			std::sort(_top, _top + _top_n, *_top);

			for (size_t i = _top_n; i < N; i++) {
				float r = tr[i];

				if (_top[_top_n-1].tu_score < r) {
					_top[_top_n-1].tu_score = r;
					_top[_top_n-1].tu_index = i;

					for (size_t j = _top_n-1; j > 0; j--) {
						if (_top[j-1].tu_score < _top[j].tu_score) {
							top_user tmp = _top[j-1];
							_top[j-1] = _top[j];
							_top[j] = tmp;
						}
					}
				}
			}
		}
	}
};



//==========================================================================//
// Data Source Test                                                         //
//==========================================================================//

/**
 * A simple data source test
 *
 * @param data_source the data source
 */
void data_source_test(weibo_data_source_csv& data_source) {

	for (size_t i = 0; i < 10; i++) {
		const tweet_t* t = data_source.next_input();
		if (t == NULL) break;
		printf("%s: %s\n", t->t_user, t->t_text);
	}
	
	double t_start = ll_get_time_ms();

	size_t max = 10 * 1e+6;
	size_t count = 0;

	for (size_t i = 0; i < max; i++) {
		data_source.next_input();
		count++;
	}

	double dt = ll_get_time_ms() - t_start;
	fprintf(stderr, "Read %lu tweets in %0.3lf seconds (%0.3lf Mt/s)\n",
			count, dt / 1000.0, count / dt / 1000.0);
};



//==========================================================================//
// UI for the Sloth Applications                                            //
//==========================================================================//

/**
 * UI for the Sloth Applications
 */
class sloth_weibo_ui : public sloth_ui_callbacks {

	sloth_weibo_application* _application;
	FILE* _progress_file;
	bool _progress_tty;
	FILE* _results_file;


public:

	/**
	 * Create an instance of sloth_weibo_ui
	 *
	 * @param application an instance of sloth_weibo_application
	 */
	sloth_weibo_ui(sloth_weibo_application* application) {

		_application = application;
		_application->set_ui(this);

		_progress_file = stderr;
		_progress_tty = ll_is_tty(_progress_file);

		_results_file = stderr;
	}


	/**
	 * Destroy the instance of this class
	 */
	virtual ~sloth_weibo_ui() {}


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

		fprintf(_progress_file, "%s"
				"%6.3lf M tweets/s, %6.3lf M edges/s, "
				"%6.3lf s adv, %6.3lf s run, "
				"%6.3lf GB MaxRSS, %6.3lf M users%s\n",
				_progress_tty ? LL_C_B_BLUE : "",
				_application->last_batch_inputs()
					/ driver.last_interval_time_ms() / 1000.0,
				driver.last_batch_requests()
					/ driver.last_interval_time_ms() / 1000.0,
				driver.last_advance_time_ms() / 1000.0,
				driver.last_compute_time_ms() / 1000.0,
				ll_get_maxrss_kb() / 1048576.0,
				_application->num_users() / 1000000.0,
				_progress_tty ? LL_C_RESET : "");
		fflush(_progress_file);

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



//==========================================================================//
// The Command-Line Arguments                                               //
//==========================================================================//

static const char* SHORT_OPTIONS = "A:hM:R:r:st:W:";

static struct option LONG_OPTIONS[] =
{
	{"advance"      , required_argument, 0, 'A'},
	{"help"         , no_argument      , 0, 'h'},
	{"max-batches"  , required_argument, 0, 'M'},
	{"rate"         , required_argument, 0, 'R'},
	{"run"          , required_argument, 0, 'r'},
	{"silent"       , no_argument      , 0, 's'},
	{"threads"      , required_argument, 0, 't'},
	{"window"       , required_argument, 0, 'W'},
	{0, 0, 0, 0}
};


/**
 * Print the usage information
 *
 * @param arg0 the first element in the argv array
 */
static void usage(const char* arg0) {

	char* s = strdup(arg0);
	char* p = basename(s);
	fprintf(stderr, "Usage: %s [OPTIONS] INPUT_FILE...\n\n", p);
	free(s);
	
	fprintf(stderr, "Options:\n");
	fprintf(stderr, "  -A, --advance N       Set the advance interval (seconds)\n");
	fprintf(stderr, "  -h, --help            Show this usage information and exit\n");
	fprintf(stderr, "  -M, --max-batches N   Set the maximum number of batches\n");
	fprintf(stderr, "  -R, --rate N          Set the max stream rate (edges/second)\n");
	fprintf(stderr, "  -r, --run APP         Run the given application\n");
	fprintf(stderr, "  -s, --silent          Silent mode (do not print results)\n");
	fprintf(stderr, "  -t, --threads N       Set the number of threads\n");
	fprintf(stderr, "  -W, --window N        Set the window size (seconds)\n");
}



//==========================================================================//
// The Main Function                                                        //
//==========================================================================//

/**
 * The main function
 */
int main(int argc, char** argv)
{
	std::setlocale(LC_ALL, "en_US.utf8");

	sloth_weibo_config config;
	const char* s_application = NULL;

	int advance_interval = -1;
	int window_size = -1;


	// Pase the command-line arguments

	int option_index = 0;
	while (true) {
		int c = getopt_long(argc, argv, SHORT_OPTIONS, LONG_OPTIONS, &option_index);

		if (c == -1) break;

		switch (c) {

			case 'A':
				advance_interval = atoi(optarg);
				break;

			case 'h':
				usage(argv[0]);
				return 0;

			case 'M':
				config.sloth.window_config.swc_max_advances = atoi(optarg);
				break;

			case 'R':
				config.sloth.stream_config.sc_max_edges_per_second = atoi(optarg);
				break;

			case 'r':
				s_application = optarg;
				break;

			case 's':
				config.print_results = false;
				break;

			case 't':
				config.sloth.num_threads = atoi(optarg);
				break;

			case 'W':
				window_size = atoi(optarg);
				break;

			case '?':
			case ':':
				return 1;

			default:
				abort();
		}
	}

	if (window_size >= 0) {
		int w = window_size * 1000;
		int a = advance_interval * 1000;

		if (advance_interval < 0) {
			config.sloth.window_config.swc_advance_interval_ms = w;
			config.sloth.window_config.swc_window_snapshots = 1;
		}
		else if (w % a == 0) {
			config.sloth.window_config.swc_advance_interval_ms = a;
			config.sloth.window_config.swc_window_snapshots = w / a;
		}
		else {
			fprintf(stderr, "Error: The advance interval does not divide "
					"the window size\n");
			return 1;
		}
	}
	else {
		fprintf(stderr, "Error: Specifying the advance interval without "
				"specifying the window size\n");
		return 1;
	}


	// Get the input files and create the data source

	std::vector<std::string> input_files;
	for (int i = optind; i < argc; i++) {
		input_files.push_back(std::string(argv[i]));
	}

	if (input_files.empty()) {
		fprintf(stderr, "Error: No input files are specified\n");
		return 1;
	}

	weibo_data_source_csv data_source(input_files);


	// Create an application

	sloth_weibo_application* application = NULL;

	if (s_application == NULL) {
		application = new swa_user_ranking(&data_source, &config);
	}
	else if (strcmp(s_application, "ur") == 0) {
		application = new swa_user_ranking(&data_source, &config);
	}
	else {
		fprintf(stderr, "Error: Unknown application \"%s\"\n", s_application);
		return 1;
	}

	sloth_weibo_ui ui(application); (void) ui;


	// Run the application
	
	application->run();


	// Finish
	
	if (application) delete application;

	return 0;
}
