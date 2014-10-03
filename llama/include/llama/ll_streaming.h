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

#include <queue>


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
	 * Load the next batch of data
	 *
	 * @param graph the writable graph
	 * @param max_edges the maximum number of edges
	 * @return true if data was loaded, false if there are no more data
	 */
	virtual bool pull(ll_writable_graph* graph, size_t max_edges) = 0;
};


/**
 * A serial concatenation of multiple data sources
 */
class ll_concat_data_source : public ll_data_source {

	std::queue<ll_data_source*> _data_sources;
	ll_spinlock_t _lock;


public:

	/**
	 * Create an instance of the concatenated data source
	 */
	ll_concat_data_source() {

		_lock = 0;
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
		ll_spinlock_release(&_lock);
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
};

#endif
