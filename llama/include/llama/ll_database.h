/*
 * ll_database.h
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


#ifndef LL_DATABASE_H_
#define LL_DATABASE_H_

#include <string>
#include <vector>

#include "llama/ll_common.h"
#include "llama/ll_config.h"
#include "llama/ll_persistent_storage.h"


/**
 * The database
 */
class ll_database {


public:

	/**
	 * Create a new database instance, or load it if it exists
	 *
	 * @param dir the database directory (if it is a persistent database)
	 */
	ll_database(const char* dir = NULL) {

		omp_set_num_threads(omp_get_max_threads());

		_dir = IFE_LL_PERSISTENCE(dir == NULL ? "db" : dir, "");
		IF_LL_PERSISTENCE(_storage = new ll_persistent_storage(_dir.c_str()));

		_graph = new ll_writable_graph(this, IF_LL_PERSISTENCE(_storage,)
				80 * 1000000 /* XXX */);
	}


	/**
	 * Destroy the in-memory representation of the database instance
	 */
	virtual ~ll_database() {
		
		delete _graph;
		IF_LL_PERSISTENCE(delete _storage);
	}


	/**
	 * Set the number of OpenMP threads
	 *
	 * @param n the number of threads
	 */
	void set_num_threads(int n) {
		// XXX This really belongs to some global runtime
		omp_set_num_threads(n);
	}


	/**
	 * Get the database directory, if this is a persistent database
	 *
	 * @return the database directory, or NULL if not persistent
	 */
	inline const char* directory() {
		return IFE_LL_PERSISTENCE(_dir.c_str(), NULL);
	}


#ifdef LL_PERSISTENCE

	/**
	 * Get the persistent storage
	 *
	 * @return the persistent storage
	 */
	inline ll_persistent_storage* storage() {
		return _storage;
	}
#endif


	/**
	 * Get the graph
	 *
	 * @return the graph
	 */
	inline ll_writable_graph* graph() {
		return _graph;
	}


	/**
	 * Get the loader config
	 *
	 * @return the loader config
	 */
	inline ll_loader_config* loader_config() {
		return &_loader_config;
	}


private:

	/// The graph
	ll_writable_graph* _graph;

	/// The persistent storage
	IF_LL_PERSISTENCE(ll_persistent_storage* _storage);

	/// The database directory
	std::string _dir;

	/// The loader configuration
	ll_loader_config _loader_config;
};

#endif

