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
#include <sstream>
#include <unordered_map>

#include <sloth.h>

#include "common/sloth_application.h"
#include "weibo_data_source.h"



//==========================================================================//
// Applications - Common                                                    //
//==========================================================================//

/**
 * The configuration
 */
class sloth_weibo_config : public sloth_test_application_config {

public:

	// Common options for algorithms
	
	size_t top_n;


	// TunkRank options
	
	double tr_retweet_probability;
	double tr_diff_threshold;
	size_t tr_max_iterations;


	// K-Exposure options
	
	size_t ke_min_count;
	size_t ke_min_k;


	/**
	 * Create an instance of class sloth_weibo_config
	 */
	inline sloth_weibo_config() {

		top_n = 10;

		tr_retweet_probability = 0.05;
		tr_diff_threshold = 1e-10;
		tr_max_iterations = 10;

		ke_min_count = 20;
		ke_min_k = 10;
	}
};


/**
 * The application class
 */
class sloth_weibo_application
	: public sloth_test_application<tweet_t, sloth_weibo_config> {

	typedef std::unordered_map<std::string, node_t> string_to_node_map_t;

	std::vector<std::string> _names;
	string_to_node_map_t _names_to_nodes;
	node_t _next_available_node;


public:

	/**
	 * Create an instance of class sloth_weibo_application
	 *
	 * @param data_source the generic data source
	 * @param config the configuration
	 * @param name the application name
	 * @param code the application code
	 */
	sloth_weibo_application(ll_generic_data_source<tweet_t>* data_source,
			const sloth_weibo_config* config,
			const char* name, const char* code)
		: sloth_test_application<tweet_t, sloth_weibo_config>(data_source,
				config, name, code) {
		_next_available_node = 0;
	}


	/**
	 * Destroy the application
	 */
	virtual ~sloth_weibo_application() {
	}


	/**
	 * Get the number of nodes
	 *
	 * @return the number of nodes
	 */
	virtual size_t num_nodes() {
		return _names.size();
	}


protected:

	/**
	 * Translate node name to the corresponding node ID. Create a new node if
	 * it does not already exists.
	 *
	 * @param name the node name
	 * @return the node ID
	 */
	node_t lookup_or_create_node(const std::string& name) {

		string_to_node_map_t::iterator i = _names_to_nodes.find(name);

		if (i == _names_to_nodes.end()) {
			node_t n = _next_available_node++;
			_names_to_nodes[name] = n;
			_names.push_back(name);
			assert((ssize_t) _names.size() == _next_available_node);
			return n;
		}

		return i->second;
	}


	/**
	 * Get node by node ID
	 *
	 * @param node the node ID
	 * @return the node name, or NULL if out of range
	 */
	const char* node_name(node_t node) {
		if (node < 0 || node >= (ssize_t) _names.size()) return NULL;
		return _names[node].c_str();
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
				sender = lookup_or_create_node(std::string(tweet.t_user));
			}

			add_edge(sender, lookup_or_create_node(user));
		}
	}
};



//==========================================================================//
// Application: TunkRank (user ranking)                                     //
//==========================================================================//

/**
 * Application: User ranking
 */
class swa_tunk_rank : public sloth_weibo_application {

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
	 * Create an instance of class swa_tunk_rank 
	 *
	 * @param data_source the generic data source
	 * @param config the configuration
	 */
	swa_tunk_rank(ll_generic_data_source<tweet_t>* data_source,
			const sloth_weibo_config* config)
		: sloth_weibo_application(data_source, config, "TunkRank", "tr") {
	
		_tunk_rank = NULL;
		_tunk_rank_n = 0;

		_top = (top_user*) malloc(sizeof(top_user) * _config.top_n);
		_top_n = 0;
	}


	/**
	 * Destroy the application
	 */
	virtual ~swa_tunk_rank() {

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
			fprintf(file, "%4lu: %s (%0.6lf)\n", i+1,
					node_name(_top[i].tu_index), _top[i].tu_score);
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

		// TODO Add a version that uses weights

#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES
		ll_mlcsr_edge_property<uint32_t>& weights
			= *G.get_edge_weights_streaming();
#endif

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

#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES
					ll_foreach_out_ext(e, w, G, n) {
						float d = weights[e] * delta;
#	ifdef _DEBUG
						if (weights[e] == 0) {
							LL_W_PRINT("Zero weight for edge %lx: "
									"%ld --> %ld\n",
									e, (long) n, (long) w);
						}
						assert(weights[e] > 0);
#	endif
						ATOMIC_ADD<float>(&tr_next[w], d);
						total_prv += d;
					}
#else
					ll_foreach_out(w, G, n) {
						ATOMIC_ADD<float>(&tr_next[w], delta);
						total_prv += delta;
					}
#endif
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

		_top_n = std::min(N, _config.top_n);
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
// Application: K-Exposure (controversial hashtag detection)                //
//==========================================================================//

/**
 * Application: K-Exposure (controversial hashtag detection)
 */
class swa_k_exposure : public sloth_weibo_application {

	ll_mlcsr_edge_property<uint32_t>* _k;
	size_t _num_hashtags;

	float* _tunk_rank;
	size_t _tunk_rank_n;

	struct top_hashtag {

		unsigned th_index;
		float th_score;
		unsigned th_count;
		unsigned th_max_k;

		bool operator() (const top_hashtag& a, const top_hashtag& b) {
			return a.th_score > b.th_score;
		}
	};

	top_hashtag* _top;
	size_t _top_n;


public:

	/**
	 * Create an instance of class swa_k_exposure 
	 *
	 * @param data_source the generic data source
	 * @param config the configuration
	 */
	swa_k_exposure(ll_generic_data_source<tweet_t>* data_source,
			const sloth_weibo_config* config)
		: sloth_weibo_application(data_source, config, "K-Exposure", "k-exp") {
	
		_k = NULL;
		_num_hashtags = 0;

		_tunk_rank = NULL;
		_tunk_rank_n = 0;

		_top = (top_hashtag*) malloc(sizeof(top_hashtag) * _config.top_n);
		_top_n = 0;
	}


	/**
	 * Destroy the application
	 */
	virtual ~swa_k_exposure() {

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
			top_hashtag& t = _top[i];
			fprintf(file, "%4lu: %s (F=%0.3f, N=%u, K=%u)\n", i+1,
					node_name(t.th_index), t.th_score, t.th_count, t.th_max_k);
		}
	}


protected:

	/**
	 * Custom graph initialization
	 *
	 * @param W the writable graph
	 */
	virtual void on_initialize(ll_writable_graph& W) {
		_k = W.ro_graph().create_uninitialized_edge_property_32("k", LL_T_INT32);

#ifdef LL_S_SINGLE_SNAPSHOT
		LL_E_PRINT("This app does not support LL_S_SINGLE_SNAPSHOT\n");
		exit(1);
#endif
	}


	/**
	 * Processs an input item and generate corresponding edges
	 *
	 * @param input the input item
	 */
	virtual void on_input(const tweet_t* input) {

		// Extract the "mentions" graph

		extract_mentions_edges(*input);


		// Get the hashtags

		node_t user = LL_NIL_NODE;

		for (const char* s = input->t_text; *s != '\0'; s++) {
			if (*s != '#') continue;
			
			const char* start = s++;
			while (*s != '\0' && *s != '#' && !isspace(*s)) s++;
			const char* end = s;

			if (*s != '#') {
				//LL_W_PRINT("Unterminated Weibo hashtag in tweet: %s\n",
						//input->t_text);
				continue;
			}

			std::string hashtag = std::string(start, (long) end - (long) start);

			if (user == LL_NIL_NODE) {
				user = lookup_or_create_node(std::string(input->t_user));
			}

			add_edge(lookup_or_create_node(hashtag), user);
			_num_hashtags++;
		}
	}


	/**
	 * Run the computation when the sliding window advances
	 *
	 * @param G the graph
	 */
	virtual void on_advance(ll_mlcsr_ro_graph& G) {

		// Compute "k" edge value for each new post represented as an edge
		// [hashtag] --> [user]


		// For each hashtag with new posts...

		ll_foreach_node_within_level_omp(n, G.out(), G.max_level(), 10240) {

			// First of all, is this a hashtag? If not, skip.

			if (*node_name(n) != '#') continue;


			// Get the sorted list of users who posted with the hashtag

			std::vector<node_t> posters;
			ll_foreach_out(u, G, n) posters.push_back(u);
			std::sort(posters.begin(), posters.end());


			// For each new post by user u...

			ll_foreach_edge_within_level(e, u, G.out(), n, G.max_level()) {

				// Count the users that u pays attention to that also posted
				// with the given hashtag n

				uint32_t k = 0;
				ll_foreach_out(t, G, u) {
					if (std::binary_search(posters.begin(), posters.end(), t)){
						k++;
					}
				}

				_k->cow_write(e, k);
			}
		}


		// Now compute the k histogram parameters for each hashtag
		
		// The minimum k to consider
		uint32_t min_k = 0;

		// The per-thread top table
		std::vector<top_hashtag> per_thread_top[omp_get_max_threads()];

		#pragma omp parallel
		{
			std::vector<top_hashtag>& top = per_thread_top[omp_get_thread_num()];

			#pragma omp for schedule(dynamic,4096)
			ll_foreach_node(n, G) {

				// Skip non-hashtags

				if (*node_name(n) != '#') continue;


				// Create a histogram of the k's

				std::vector<uint32_t> h;
				uint32_t count = 0;

				ll_foreach_out_ext(e, u, G, n) {
					(void) u;
					count++;

					uint32_t k = _k->get(e);
					while (h.size() <= k) h.push_back(0);
					h[k]++;
				}


				// Skip histograms that do not have any non-zero k's

				if (h.size() <= 1 && count > 0) continue;


				// Compute a few helper statistics

				uint32_t max_k = h.size() - 1;
				uint32_t max_v = 0;

				for (size_t i = min_k; i < h.size(); i++) {
					max_v = std::max(max_v, h[i]);
				}

				double max_p = max_v / (double) count;


				// Filter based on the configuration

				if (count < _config.ke_min_count) continue;
				if (max_k < _config.ke_min_k    ) continue;


				// Compute the persistence parameter

				double A = 0;
				for (size_t i = min_k; i < h.size() - 1; i++) {
					A += (h[i] + h[i+1]) / (double) count / 2;
				}

				double R = max_k * max_p;
				double F = A / R;


				// Add the per-thread top table

				top_hashtag th;
				th.th_index = n;
				th.th_score = F;
				th.th_max_k = max_k;
				th.th_count = count;

				if (top.size() < _config.top_n) {

					top.push_back(th);

					if (top.size() == _config.top_n) {
						std::sort(top.begin(), top.end(), th);
					}
				}
				else if (F > top[top.size()-1].th_score) {

					top[top.size()-1] = th;

					for (size_t j = top.size()-1; j > 0; j--) {
						if (top[j-1].th_score < top[j].th_score) {
							top_hashtag tmp = top[j-1];
							top[j-1] = top[j];
							top[j] = tmp;
						}
						else {
							break;
						}
					}
				}

				//if (--c >= 0) {
					//LL_D_PRINT("%ld %s --> %lu\n", n, node_name(n), h.size());
				//}
			}
		}


		// Merge the top tables

		std::vector<top_hashtag> top;
		for (int thread = 0; thread < omp_get_max_threads(); thread++) {
			for (size_t i = 0; i < per_thread_top[thread].size(); i++) {
				top.push_back(per_thread_top[thread][i]);
			}
		}

		top_hashtag th;
		memset(&th, 0, sizeof(th));

		std::sort(top.begin(), top.end(), th);

		_top_n = std::min(_config.top_n, top.size());
		for (size_t i = 0; i < _top_n; i++) {
			_top[i] = top[i];
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
// Helpers                                                                  //
//==========================================================================//

/**
 * The user interface
 */
typedef sloth_test_ui<sloth_weibo_application> sloth_weibo_ui;


/**
 * The application creator
 */
class sloth_weibo_application_creator
	: public sloth_test_application_creator<sloth_weibo_application,
		sloth_weibo_config, tweet_t> {

public:

	/**
	 * Create an object of type sloth_weibo_application_creator
	 */
	sloth_weibo_application_creator() {
		add("K-Exposure", "k-exposure", "k-exp", "A measure of tag stickiness");
		add("TunkRank", "tunkrank", "tr", "A PageRank analogue for Twitter");
	}


	/**
	 * Destroy an instance of sloth_weibo_application_creator
	 */
	virtual ~sloth_weibo_application_creator() {}


	/**
	 * Create an instance of the application
	 *
	 * @param code the application code
	 * @param data_source the data source
	 * @param config the configuration
	 * @return the application object, or NULL on error (error msg already printed)
	 */
	virtual sloth_weibo_application* create(const char* code,
			ll_generic_data_source<tweet_t>* data_source,
			const sloth_weibo_config* config) {
		
		if (code == NULL) {
			return new swa_tunk_rank(data_source, config);
		}
		else if (strcmp(code, "tunkrank") == 0 || strcmp(code, "tr") == 0) {
			return new swa_tunk_rank(data_source, config);
		}
		else if (strcmp(code, "k-exposure") == 0 || strcmp(code, "k-exp") == 0) {
			return new swa_k_exposure(data_source, config);
		}
		else {
			fprintf(stderr, "Error: Unknown application \"%s\"\n", code);
			return NULL;
		}
	}
};



//==========================================================================//
// The Command-Line Arguments                                               //
//==========================================================================//

static const char* SHORT_OPTIONS = "A:C:E:FhM:R:r:sTt:W:";

static struct option LONG_OPTIONS[] =
{
	{"advance"       , required_argument, 0, 'A'},
	{"csv-result"    , required_argument, 0, 'C'},
	{"edge-rate"     , required_argument, 0, 'E'},
	{"fail-if-behind", no_argument      , 0, 'F'},
	{"help"          , no_argument      , 0, 'h'},
	{"max-length"    , required_argument, 0, 'M'},
	{"rate"          , required_argument, 0, 'R'},
	{"run"           , required_argument, 0, 'r'},
	{"silent"        , no_argument      , 0, 's'},
	{"timeliness"    , no_argument      , 0, 'T'},
	{"threads"       , required_argument, 0, 't'},
	{"window"        , required_argument, 0, 'W'},
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
	fprintf(stderr, "  -C, --csv-result F    Append the result to the given CSV file\n");
	fprintf(stderr, "  -E, --edge-rate N     Set the max stream rate (edges/second)\n");
	fprintf(stderr, "  -F, --fail-if-behind  Fail if the actual rate drops below max\n");
	fprintf(stderr, "  -h, --help            Show this usage information and exit\n");
	fprintf(stderr, "  -M, --max-length N    Set the maximum experiment length (seconds)\n");
	fprintf(stderr, "  -R, --rate N          Set the max stream rate (inputs/second)\n");
	fprintf(stderr, "  -r, --run APP         Run the given application\n");
	fprintf(stderr, "  -s, --silent          Silent mode (do not print results)\n");
	fprintf(stderr, "  -T, --timeliness      Compute timeliness\n");
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
	sloth_weibo_application_creator creator;
	std::setlocale(LC_ALL, "en_US.utf8");

	bool stderr_tty = ll_is_tty(stderr);

	sloth_weibo_config config;
	const char* s_application = NULL;
	const char* csv_result = NULL;

	bool timeliness = false;
	double timeliness_window_size_accuracy_threshold = 0.05;
	std::vector<int> timeliness_steps;
	timeliness_steps.push_back(1000);
	timeliness_steps.push_back(100);

	int advance_interval = -1;
	int window_size = -1;
	int max_length = -1;


	// Pase the command-line arguments

	int option_index = 0;
	while (true) {
		int c = getopt_long(argc, argv, SHORT_OPTIONS, LONG_OPTIONS, &option_index);

		if (c == -1) break;

		switch (c) {

			case 'A':
				advance_interval = (int) (atof(optarg) * 1000 + 0.5);
				break;

			case 'C':
				csv_result = optarg;
				break;

			case 'E':
				config.sloth.stream_config.sc_max_edges_per_second = atoi(optarg);
				break;

			case 'F':
				config.fail_if_behind = true;
				break;

			case 'h':
				usage(argv[0]);
				return 0;

			case 'M':
				max_length = (int) (atof(optarg) * 1000 + 0.5);
				break;

			case 'R':
				config.sloth.max_inputs_per_second = atoi(optarg);
				break;

			case 'r':
				s_application = optarg;
				break;

			case 's':
				config.print_results = false;
				break;

			case 'T':
				timeliness = true;
				break;

			case 't':
				config.sloth.num_threads = atoi(optarg);
				break;

			case 'W':
				window_size = (int) (atof(optarg) * 1000 + 0.5);
				break;

			case '?':
			case ':':
				return 1;

			default:
				abort();
		}
	}

	if (csv_result != NULL) {
		if (!timeliness) {
			fprintf(stderr, "Error: Option -C/--csv-result works only with "
					"-T/--timeliness\n");
			return 1;
		}
	}


	// Configure the window

	if (timeliness) {
		if (advance_interval >= 0) {
			fprintf(stderr, "Error: Cannot combine -T/--timeliness and "
					"-A/--advance\n");
			return 1;
		}
		if (window_size < 0) {
			fprintf(stderr, "Error: Option -T/--timeliness requires "
					"-W/--window\n");
			return 1;
		}
	}
	else {
		if (window_size >= 0) {
			int w = window_size;
			int a = advance_interval;

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
		else if (advance_interval >= 0) {
			fprintf(stderr, "Error: Specifying the advance interval without "
					"specifying the window size\n");
			return 1;
		}

		if (max_length >= 0) {
			if (config.sloth.window_config.swc_advance_interval_ms <= 0) {
				fprintf(stderr, "Error: The advance interval is not set\n");
			}
			else {
				config.sloth.window_config.swc_max_advances = max_length
					/ config.sloth.window_config.swc_advance_interval_ms;
			}
		}
	}

#ifdef DEDUP
	config.sloth.loader_config.lc_deduplicate = true;
#endif


	// Get the input files

	std::vector<std::string> input_files;
	for (int i = optind; i < argc; i++) {
		input_files.push_back(std::string(argv[i]));
	}

	if (input_files.empty()) {
		fprintf(stderr, "Error: No input files are specified\n");
		return 1;
	}


	// Create the application and run it
	
	if (timeliness) {

		// Run timeliness estimation

		config.fail_if_behind = true;
		config.print_results = false;

		int good_advance_interval = 0;

		for (size_t ti = 0; ti < timeliness_steps.size(); ti++) {
			int step = timeliness_steps[ti];

			advance_interval = good_advance_interval;
			if (ti > 0) {
				advance_interval -= timeliness_steps[ti - 1];
			}

			bool done = false;
			while (!done) {
				advance_interval += step;
				
				if (ti > 0 && advance_interval >= good_advance_interval) {
					break;
				}

				fprintf(stderr, "\n%s================ Trying advance interval "
						"%0.2lf seconds ================%s\n\n",
						stderr_tty ? LL_C_B_CYAN : "",
						advance_interval / 1000.0,
						stderr_tty ? LL_C_RESET : "");


				// Configure the computation

				config.sloth.window_config.swc_advance_interval_ms
					= advance_interval;
				config.sloth.window_config.swc_window_snapshots
					= window_size / advance_interval;

				int w = config.sloth.window_config.swc_window_snapshots
					* advance_interval;
				if (w < window_size) {
					config.sloth.window_config.swc_window_snapshots++;
					w = config.sloth.window_config.swc_window_snapshots
						* advance_interval;
					double p = std::abs(w - window_size) / (double)window_size;
					if (p > timeliness_window_size_accuracy_threshold) {
						fprintf(stderr, "%sWarning: %sCannot set the window "
								"size with sufficient accuracy "
								"(we are %0.2lf%% off)\n\n",
								stderr_tty ? LL_C_B_YELLOW : "",
								stderr_tty ? LL_C_RESET : "",
								p * 100);
					}
				}

				if (max_length > 0) {
					config.sloth.window_config.swc_max_advances = max_length
						/ config.sloth.window_config.swc_advance_interval_ms;
				}


				// Create the data source and the application

				weibo_data_source_csv data_source(input_files);
				sloth_weibo_application* application
					= creator.create(s_application, &data_source, &config);
				if (!application) return 1;


				// Run the application

				sloth_weibo_ui ui(application);
				application->run();


				// If we did not fail, we have a good advance time

				if (!ui.failed()) {
					done = true;
					good_advance_interval = advance_interval;
					fprintf(stderr, "%s\nAdvance interval %0.2lf seconds "
							"works.%s\n\n",
							stderr_tty ? LL_C_B_GREEN : "",
							advance_interval / 1000.0,
							stderr_tty ? LL_C_RESET : "");
				}


				// If we failed, check to see if it's even worth trying more

				if (ui.failed()) {
					if (config.sloth.window_config.swc_window_snapshots <= 1) {
						done = true;
					}
				}


				// Clean up

				delete application;
			}


			// If good_advance_interval is zero, we must have failed

			if (good_advance_interval == 0) break;
		}


		// Report the results

		if (good_advance_interval > 0) {
			fprintf(stderr, "\n%s============================== SUCCESS "
					"==============================%s\n",
					stderr_tty ? LL_C_B_GREEN : "",
					stderr_tty ? LL_C_RESET : "");
			fprintf(stderr, "%s\nTimeliness:%s %0.2lf seconds\n",
					stderr_tty ? LL_C_B_GREEN : "",
					stderr_tty ? LL_C_RESET : "",
					advance_interval / 1000.0);
		}
		else {
			fprintf(stderr, "\n\n%s============================== FAILURE "
					"==============================%s\n",
					stderr_tty ? LL_C_B_RED : "",
					stderr_tty ? LL_C_RESET : "");
			fprintf(stderr, "%s\nTimeliness:%s N/A\n",
					stderr_tty ? LL_C_B_RED : "",
					stderr_tty ? LL_C_RESET : "");
		}


		// Write the results to a CSV file

		if (csv_result != NULL) {
			
			FILE* f;
			bool needs_header = false;
			
			if (strcmp(csv_result, "") == 0 || strcmp(csv_result, "-") == 0) {
				f = stdout;
				needs_header = true;
			}
			else {

				f = fopen(csv_result, "r");
				if (f == NULL) {
					needs_header = true;
				}
				else {
					fclose(f);
				}

				f = fopen(csv_result, "a");
				if (f == NULL) {
					fprintf(stderr, "Error: Cannot open or create the output "
							"CSV file: %s\n",
							strerror(errno));
				}
			}

			if (f != NULL) {

				if (needs_header) {
					fprintf(f, "input_rate,edge_rate,window_size,max_length,"
							"timeliness\n");
				}

				std::ostringstream ss;
				if (config.sloth.max_inputs_per_second > 0) {
					ss << config.sloth.max_inputs_per_second;
				}

				ss << ",";
				if (config.sloth.stream_config.sc_max_edges_per_second > 0) {
					ss << config.sloth.stream_config.sc_max_edges_per_second;
				}

				ss << "," << (window_size / 1000.0);

				ss << ",";
				if (max_length > 0) {
					ss << (max_length / 1000.0);
				}

				ss << ",";
				if (good_advance_interval > 0) {
					ss << (good_advance_interval / 1000.0);
				}

				fprintf(f, "%s\n", ss.str().c_str());

				if (f != stdout && f != stderr) fclose(f);
			}
		}
	}
	else {

		// Run normal computation

		// Create the data source and the application

		weibo_data_source_csv data_source(input_files);

		sloth_weibo_application* application
			= creator.create(s_application, &data_source, &config);
		if (!application) return 1;


		// Run the application
		
		sloth_weibo_ui ui(application); (void) ui;
		application->run();


		// Clean up

		delete application;
	}


	// Finish

	return 0;
}
