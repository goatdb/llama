/*
 * ll_config.h
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


#ifndef LL_CONFIG_H_
#define LL_CONFIG_H_

#include <cstdint>
#include <string>
#include <vector>


/*
 * Loader config constants
 */

#define LL_L_DIRECTED				0
#define LL_L_UNDIRECTED_DOUBLE		1
#define LL_L_UNDIRECTED_ORDERED		2


/*
 * Features for checking their support
 */

#define LL_L_FEATURE(feature)	(std::string(#feature))

typedef std::vector<std::string> feature_vector_t;

inline feature_vector_t& operator<< (feature_vector_t& v,
		const std::string& value) {
	v.push_back(value);
	return v;
}



/**
 * The loader configuration
 */
class ll_loader_config {

public:

	/// Graph directionality (one of the LL_L_* constants)
	int lc_direction;

	/// Whether to have the reverse edges
	bool lc_reverse_edges;

	/// Whether to have the out-in and in-out edge ID maps
	bool lc_reverse_maps;

	/// Whether to deduplicate the edges on ingest
	bool lc_deduplicate;

	/// Whether to ignore the properties (and load only the graph structure)
	bool lc_no_properties;

	/// The temporary directories
	std::vector<std::string> lc_tmp_dirs;

	/// Whether to print progress while loading
	bool lc_print_progress;

	/// The buffer size in bytes for external sort (0 = auto-configure)
	size_t lc_xs_buffer_size;

	/// The max number of edges to load
	size_t lc_max_edges;

	/// The partial load - part number (1-based, not 0-based)
	size_t lc_partial_load_part;

	/// The partial load - the total number of parts
	size_t lc_partial_load_num_parts;


public:

	/**
	 * Create an instance of ll_loader_config with the default configuration
	 */
	ll_loader_config() {

		lc_direction = LL_L_DIRECTED;
		lc_deduplicate = false;
		lc_reverse_edges = false;
		lc_reverse_maps = false;
		lc_no_properties = false;

		lc_tmp_dirs.clear();
		lc_print_progress = false;
		lc_xs_buffer_size = 0;

		lc_max_edges = 0;
		lc_partial_load_part = 0;
		lc_partial_load_num_parts = 0;
	}


	/**
	 * Assert (or check) that all enabled features are indeed supported by the
	 * loader
	 *
	 * @param direct true if the loader is a direct loader (false = incremental)
	 * @param error true to throw an error, false to just return the result
	 * @param features the vector of supported features (use LL_L_FEATURE())
	 * @return true if okay, or false if not (for error == false)
	 */
	bool assert_features(bool direct, bool error,
			const feature_vector_t& features) const {

		feature_vector_t enabled_features;
		std::vector<std::string> enabled_feature_names;

#		define FEATURE(x) \
			if ((ssize_t) (x) != 0) { \
				enabled_features << LL_L_FEATURE(x); \
				enabled_feature_names.push_back(std::string(#x)); \
			}

		FEATURE(lc_direction);
		FEATURE(lc_deduplicate);
		FEATURE(lc_no_properties);

		if (direct) FEATURE(lc_reverse_edges);
		if (direct) FEATURE(lc_reverse_maps);

		FEATURE(lc_max_edges);
		FEATURE(lc_partial_load_part);
		FEATURE(lc_partial_load_num_parts);

#		undef FEATURE

		for (size_t i = 0; i < enabled_features.size(); i++) {
			bool ok = false;
			for (size_t k = 0; k < features.size(); k++) {
				if (enabled_features[i] == features[k]) {
					ok = true;
					break;
				}
			}
			if (!ok) {
				if (error) {
					LL_E_PRINT("Feature not supported: %s\n",
							enabled_feature_names[i].c_str());
					abort();
				}
				else {
					return false;
				}
			}
		}

		if (lc_partial_load_num_parts > 0) {
			if (lc_partial_load_part <= 0
					|| lc_partial_load_part > lc_partial_load_num_parts) {
				LL_E_PRINT("The partial load part ID is out of bounds\n");
				abort();
			}
		}
		else {
			if (lc_partial_load_part != 0) {
				LL_E_PRINT("Partial load part ID without the number of parts\n");
				abort();
			}
		}

		return true;
	}
};

#endif

