/*
 * cross_validate.h
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


#ifndef LL_CROSS_VALIDATE_H
#define LL_CROSS_VALIDATE_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <float.h>
#include <limits.h>
#include <cmath>
#include <algorithm>
#include <omp.h>

#include "benchmarks/benchmark.h"
#include "tools/print.h"


template <class EXP_GRAPH, class EXP_GRAPH2>
node_t cross_validate_exp_out(EXP_GRAPH& G_org, EXP_GRAPH2& G_exp) {

	std::vector<node_t> v_exp;
	std::vector<node_t> v_org;

	if (G_org.max_nodes() != G_exp.max_nodes()) {
		fprintf(stderr, "Different number of nodes:\n");
		fprintf(stderr, "  Exp: %lu\n", G_exp.max_nodes());
		fprintf(stderr, "  Org: %lu\n", G_org.max_nodes());
		abort();
	}

	for (node_t u = 0; u < G_org.max_nodes(); u ++) 
	{
		v_exp.clear();
		v_org.clear();

		int count_exp = 0;
		ll_edge_iterator iter;
		G_exp.out_iter_begin(iter, u);
		for (edge_t v_idx = G_exp.out_iter_next(iter);
				v_idx != LL_NIL_EDGE;
				v_idx = G_exp.out_iter_next(iter)) {
			node_t v = LL_ITER_OUT_NEXT_NODE(G_exp, iter, v_idx);

			count_exp++;
			v_exp.push_back(v);
		}

		int count_org = 0;
		G_org.out_iter_begin(iter, u);
		for (edge_t v_idx = G_org.out_iter_next(iter);
				v_idx != LL_NIL_EDGE;
				v_idx = G_org.out_iter_next(iter)) {
			node_t v = LL_ITER_OUT_NEXT_NODE(G_org, iter, v_idx);

			count_org++;
			v_org.push_back(v);
		}

		if (count_org != count_exp) return u;

		std::sort(v_org.begin(), v_org.end());
		std::sort(v_exp.begin(), v_exp.end());
		
		for (size_t i = 0; i < v_org.size(); i++) {
			if (v_org[i] != v_exp[i]) {
				for (size_t j = 0; j < v_org.size(); j++) {
					printf("%8ld %8ld", v_org[j], v_exp[j]);
					if (v_org[j] != v_exp[j]) printf(" *");
					printf("\n");
				}
				return u;
			}
		}

		if (v_exp.size() != G_exp.out_degree(u)) return u;
	}

    return LL_NIL_NODE;
}


template <class EXP_GRAPH, class EXP_GRAPH2>
node_t cross_validate_exp_in(EXP_GRAPH& G_org, EXP_GRAPH2& G_exp) {

	std::vector<node_t> v_exp;
	std::vector<node_t> v_org;

	for (node_t u = 0; u < G_org.max_nodes(); u ++) 
	{
		v_exp.clear();
		v_org.clear();

		int count_exp = 0;
		ll_edge_iterator iter;
		G_exp.inm_iter_begin(iter, u);
		for (node_t v = G_exp.inm_iter_next(iter);
				v != LL_NIL_EDGE;
				v = G_exp.inm_iter_next(iter)) {
			count_exp++;
			v_exp.push_back(v);
		}

		int count_org = 0;
		G_org.inm_iter_begin(iter, u);
		for (node_t v = G_org.inm_iter_next(iter);
				v != LL_NIL_EDGE;
				v = G_org.inm_iter_next(iter)) {
			count_org++;
			v_org.push_back(v);
		}

		if (count_org != count_exp) return u;

		std::sort(v_org.begin(), v_org.end());
		std::sort(v_exp.begin(), v_exp.end());
		
		for (size_t i = 0; i < v_org.size(); i++) {
			if (v_org[i] != v_exp[i]) return u;
		}

		if (v_exp.size() != G_exp.in_degree(u)) return u;
	}

    return LL_NIL_NODE;
}


template <class EXP_GRAPH, class EXP_GRAPH2>
node_t cross_validate_exp_node_properties(EXP_GRAPH& G_org, EXP_GRAPH2& G_exp) {

	{
		auto node_properties_org = G_org.get_all_node_properties_32();
		auto node_properties_exp = G_exp.get_all_node_properties_32();

		for (node_t u = 0; u < G_org.max_nodes(); u ++) 
		{
			std::map<std::string, std::pair<short, uint64_t>> p_org;
			std::map<std::string, std::pair<short, uint64_t>> p_exp;

			for (auto it = node_properties_org.begin(); it != node_properties_org.end(); it++) {
				p_org[it->first] =
					std::pair<short, uint64_t>(it->second->type(), it->second->get(u));
			}

			for (auto it = node_properties_exp.begin(); it != node_properties_exp.end(); it++) {
				p_exp[it->first] =
					std::pair<short, uint64_t>(it->second->type(), it->second->get(u));
			}

			for (auto it = p_org.begin(); it != p_org.end(); it++) {
				auto it2 = p_exp.find(it->first);
				if (it2 == p_exp.end()) return u;
				if (it->second.first  != it2->second.first ) return u;
				if (it->second.second != it2->second.second) return u;
			}

			for (auto it = p_exp.begin(); it != p_exp.end(); it++) {
				auto it2 = p_org.find(it->first);
				if (it2 == p_org.end()) return u;
			}
		}
	}

	{
		auto node_properties_org = G_org.get_all_node_properties_64();
		auto node_properties_exp = G_exp.get_all_node_properties_64();

		for (node_t u = 0; u < G_org.max_nodes(); u ++) 
		{
			std::map<std::string, std::pair<short, uint64_t>> p_org;
			std::map<std::string, std::pair<short, uint64_t>> p_exp;

			for (auto it = node_properties_org.begin(); it != node_properties_org.end(); it++) {
				p_org[it->first] =
					std::pair<short, uint64_t>(it->second->type(), it->second->get(u));
			}

			for (auto it = node_properties_exp.begin(); it != node_properties_exp.end(); it++) {
				p_exp[it->first] =
					std::pair<short, uint64_t>(it->second->type(), it->second->get(u));
			}

			for (auto it = p_org.begin(); it != p_org.end(); it++) {
				auto it2 = p_exp.find(it->first);
				if (it2 == p_exp.end()) return u;
				if (it->second.first  != it2->second.first ) return u;
				if (it->second.second != it2->second.second) {
					if (it->second.first == LL_T_STRING) {
						if (it->second.second == 0 || it2->second.second == 0) return u;
						std::string* s1 = reinterpret_cast<std::string*>(it->second.second);
						std::string* s2 = reinterpret_cast<std::string*>(it2->second.second);
						if (*s1 != *s2) return u;
					}
					else {
						return u;
					}
				}
			}

			for (auto it = p_exp.begin(); it != p_exp.end(); it++) {
				auto it2 = p_org.find(it->first);
				if (it2 == p_org.end()) return u;
			}
		}
	}

    return LL_NIL_NODE;
}


template <class EXP_GRAPH>
edge_t cross_validate_exp_edge_properties(EXP_GRAPH& G_org, EXP_GRAPH& G_exp) {

	// TODO This comparison does not make that much sense, since the edge IDs
	// are different between the different instances of the graph

	/*{
		auto edge_properties_org = G_org.get_all_edge_properties_32();
		auto edge_properties_exp = G_exp.get_all_edge_properties_32();

		for (edge_t u = 0; u < G_org.max_edges(); u ++) 
		{
			std::map<std::string, std::pair<short, uint64_t>> p_org;
			std::map<std::string, std::pair<short, uint64_t>> p_exp;

			for (auto it = edge_properties_org.begin(); it != edge_properties_org.end(); it++) {
				p_org[it->first] =
					std::pair<short, uint64_t>(it->second->type(), it->second->get(u));
			}

			for (auto it = edge_properties_exp.begin(); it != edge_properties_exp.end(); it++) {
				p_exp[it->first] =
					std::pair<short, uint64_t>(it->second->type(), it->second->get(u));
			}

			for (auto it = p_org.begin(); it != p_org.end(); it++) {
				auto it2 = p_exp.find(it->first);
				if (it2 == p_exp.end()) return u;
				if (it->second.first  != it2->second.first ) return u;
				if (it->second.second != it2->second.second) return u;
			}

			for (auto it = p_exp.begin(); it != p_exp.end(); it++) {
				auto it2 = p_org.find(it->first);
				if (it2 == p_org.end()) return u;
			}
		}
	}

	{
		auto edge_properties_org = G_org.get_all_edge_properties_64();
		auto edge_properties_exp = G_exp.get_all_edge_properties_64();

		for (edge_t u = 0; u < G_org.max_edges(); u ++) 
		{
			std::map<std::string, std::pair<short, uint64_t>> p_org;
			std::map<std::string, std::pair<short, uint64_t>> p_exp;

			for (auto it = edge_properties_org.begin(); it != edge_properties_org.end(); it++) {
				p_org[it->first] =
					std::pair<short, uint64_t>(it->second->type(), it->second->get(u));
			}

			for (auto it = edge_properties_exp.begin(); it != edge_properties_exp.end(); it++) {
				p_exp[it->first] =
					std::pair<short, uint64_t>(it->second->type(), it->second->get(u));
			}

			for (auto it = p_org.begin(); it != p_org.end(); it++) {
				auto it2 = p_exp.find(it->first);
				if (it2 == p_exp.end()) return u;
				if (it->second.second != it2->second.second) {
					if (it->second.first == LL_T_STRING) {
						if (it->second.second == 0 || it2->second.second == 0) return u;
						std::string* s1 = reinterpret_cast<std::string*>(it->second.second);
						std::string* s2 = reinterpret_cast<std::string*>(it2->second.second);
						if (*s1 != *s2) return u;
					}
					else {
						return u;
					}
				}
			}

			for (auto it = p_exp.begin(); it != p_exp.end(); it++) {
				auto it2 = p_org.find(it->first);
				if (it2 == p_org.end()) return u;
			}
		}
	}*/

    return LL_NIL_EDGE;
}


/**
 * Cross-validate the entire graph against the provided file
 *
 * @param graph the graph to validate
 * @param cross_validate_with the file
 * @param cross_validate_file_type the file type
 * @param verbose true to be verbose
 * @param config the loader config (optional)
 * @return 0 if OK, or an error number
 */
template <class Graph>
int cross_validate_with_file(Graph& graph, const char* cross_validate_with,
		const char* cross_validate_file_type, bool verbose,
		ll_loader_config* config) {

    if (verbose)
		fprintf(stderr, "Loading %s for cross-validation\n", cross_validate_with);

	IF_LL_PERSISTENCE(ll_persistent_storage storage("db-c")); // XXX
    ll_writable_graph cv_graph(NULL /* XXX */,
			IF_LL_PERSISTENCE(&storage,) graph.max_nodes() * 2);

	// XXX Need error checking

	ll_file_loaders loaders;
	ll_file_loader* loader = loaders.loader_for(cross_validate_with);
	ll_loader_config loader_config;
	if (config == NULL) {
		loader_config.lc_reverse_edges = graph.has_reverse_edges();
		loader_config.lc_reverse_maps = graph.has_reverse_edges();
	}
	else {
		memcpy(&loader_config, config, sizeof(ll_loader_config));
	}
	loader->load_direct(&cv_graph, cross_validate_with, &loader_config);

	if (cv_graph.max_nodes() != graph.max_nodes()) {
		fprintf(stderr, "Cross-Validation failed: Different number of nodes:\n");
		fprintf(stderr, "  Exp: %lu\n", cv_graph.max_nodes());
		fprintf(stderr, "  Org: %lu\n", graph.max_nodes());
		return 1;
	}

    if (verbose) fprintf(stderr, "Cross-validating:\n  * out-edges\n");
    node_t n = cross_validate_exp_out(cv_graph, graph);
    if (n != LL_NIL_NODE) {
        fprintf(stderr, "Cross-Validation failed: "
				"Found difference in out-edges of node %llu\n",
                (unsigned long long) n);
        printf("%15s:", "Original"); print_exp_adj_out(cv_graph, n);
        printf("%15s:", "Experimental"); print_exp_adj_out(graph, n);
        printf("%15s:", "RO Graph"); print_exp_adj_out(graph.ro_graph(), n);
        auto& g = graph.ro_graph().out();
        for (size_t l = 0; l < g.num_levels(); l++) {
            printf("%13s %lu:", "RO Lev", l);
            print_exp_adj_within_level(g, n, l);
        }
        return 1;
    }


    if (graph.has_reverse_edges() && loader_config.lc_reverse_edges) {

        if (verbose) fprintf(stderr, "  * in-edges\n");
        n = cross_validate_exp_in(cv_graph, graph);
        if (n != LL_NIL_NODE) {
            fprintf(stderr, "Cross-Validation failed: "
					"Found difference in in-edges of node %llu\n",
                    (unsigned long long) n);
            printf("%15s:", "Original"); print_exp_adj_in(cv_graph, n);
            printf("%15s:", "Experimental"); print_exp_adj_in(graph, n);
            printf("%15s:", "RO Graph"); print_exp_adj_in(graph.ro_graph(), n);
            auto& g = graph.ro_graph().in();
            for (size_t l = 0; l < g.num_levels(); l++) {
                printf("%13s %lu:", "RO Lev", l);
                print_exp_adj_within_level(g, n, l);
            }
            return 1;
        }

        if (verbose) fprintf(stderr, "  * node properties\n");
        n = cross_validate_exp_node_properties(cv_graph.ro_graph(), graph.ro_graph());
        if (n != LL_NIL_NODE) {
            fprintf(stderr, "Cross-Validation failed: "
					"Found difference in properties of node %llu\n",
                    (unsigned long long) n);
            printf("%15s:", "Original");
            {
                auto p = cv_graph.ro_graph().get_all_node_properties_32();
                for (auto it = p.begin(); it != p.end(); it++)
                    printf(" %x", it->second->get(n));
            }
            {
                auto p = cv_graph.ro_graph().get_all_node_properties_64();
                for (auto it = p.begin(); it != p.end(); it++)
                    printf(" %lx", it->second->get(n));
            }
            printf("\n");
            printf("%15s:", "Experimental");
            {
                auto p = graph.ro_graph().get_all_node_properties_32();
                for (auto it = p.begin(); it != p.end(); it++)
                    printf(" %x", it->second->get(n));
            }
            {
                auto p = graph.ro_graph().get_all_node_properties_64();
                for (auto it = p.begin(); it != p.end(); it++)
                    printf(" %lx", it->second->get(n));
            }
            printf("\n");
            return 1;
        }

		// Note: the following just calls a stub for now...
		edge_t e = cross_validate_exp_edge_properties(cv_graph.ro_graph(), graph.ro_graph());
		if (e != LL_NIL_EDGE) {
			fprintf(stderr, "Cross-Validation failed: "
					"Found difference in properties of edge %llu\n",
					(unsigned long long) e);
			return 1;
		}

        if (verbose) fprintf(stderr, "  SUCCESS\n");
    }

    return 0;
}

#endif
