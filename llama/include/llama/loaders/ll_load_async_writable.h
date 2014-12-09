/*
 * ll_load_async_writable.h
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


#ifndef LL_LOAD_ASYNC_WRITABLE_H_
#define LL_LOAD_ASYNC_WRITABLE_H_

#include "llama/ll_writable_graph.h"


/**
 * A request
 */
class ll_la_request {

public:

	/// The next request in the queue
	ll_la_request* _next;


	/**
	 * Create an instance of ll_la_request
	 */
	ll_la_request() : _next(NULL) {}


	/**
	 * Destroy an instance of ll_la_request
	 */
	virtual ~ll_la_request() {}


	/**
	 * Perform the request
	 *
	 * @param graph the graph
	 */
	virtual void run(ll_writable_graph& graph) = 0;
};


/**
 * A NOP request
 */
class ll_la_nop : public ll_la_request {

public:

	/**
	 * Create a class of type ll_la_nop
	 */
	ll_la_nop() {}


	/**
	 * Perform the request
	 *
	 * @param graph the graph
	 */
	virtual void run(ll_writable_graph& graph) {}
};


/**
 * A request queue
 */
class ll_la_request_queue {

	ll_spinlock_t _lock;
	
	ll_la_request* volatile _head;
	ll_la_request* volatile _tail;
	volatile size_t _length;
	
	volatile bool _shutdown_when_empty;


	// Make sure that the data structure is at least one cache-line in size...
	// Do we need this?
	
	char __fill[64 - sizeof(_lock) - sizeof(_head) - sizeof(_tail)
		- sizeof(_shutdown_when_empty)];


public:

	/**
	 * Create an empty request queue
	 */
	ll_la_request_queue() {

		_lock = 0;
		_head = NULL;
		_tail = NULL;
		_length = 0;

		_shutdown_when_empty = false;
	}


	/**
	 * Destroy the queue
	 */
	virtual ~ll_la_request_queue() {

		while (_head != NULL) {
			ll_la_request* n = _head->_next;
			delete _head;
			_head = n;
		}
	}


	/**
	 * Insert into the queue
	 *
	 * @param request the request
	 */
	void enqueue(ll_la_request* request) {

		request->_next = NULL;

		ll_spinlock_acquire(&_lock);

		_length++;
	
		if (_tail != NULL)
			_tail->_next = request;
		_tail = request;
		if (_head == NULL) _head = request;

		ll_spinlock_release(&_lock);
	}


	/**
	 * Remove from the front of the queue
	 *
	 * @return the request, or NULL if empty
	 */
	ll_la_request* dequeue() {

		ll_spinlock_acquire(&_lock);
	
		if (_head == NULL) {
			ll_spinlock_release(&_lock);
			return NULL;
		}

		_length--;

		ll_la_request* r = (ll_la_request*) _head;
		_head = r->_next;
		if (_head == NULL) _tail = NULL;

		ll_spinlock_release(&_lock);

		return r;
	}


	/**
	 * Get the size of the queue
	 *
	 * @return the number of elements in the queue
	 */
	inline size_t size() const {
		return _length;
	}


	/**
	 * Shutdown when the queue is empty
	 *
	 * @param shutdown true to shutdown, false to cancel
	 */
	void shutdown_when_empty(bool shutdown = true) {
		_shutdown_when_empty = shutdown;
	}


	/**
	 * Process the requests from start to finish
	 *
	 * @param graph the graph
	 */
	void run(ll_writable_graph& graph) {

		ll_la_request* r;
		while ((r = dequeue()) != NULL) {
			r->run(graph);
			delete r;
		}
	}


	/**
	 * Process the requests, wait when the queue is empty until an explicit
	 * shutdown command
	 */
	void worker(ll_writable_graph& graph) {

		for (;;) {

			ll_la_request* r = dequeue();

			if (r == NULL) {
				if (_shutdown_when_empty) return;
				usleep(10);
			}
			else {
				r->run(graph);
				delete r;
			}
		}
	}


	/**
	 * Process the next request
	 *
	 * @return true if the request was processed, false if the queue was empty
	 */
	bool process_next(ll_writable_graph& graph) {

		ll_la_request* r = dequeue();
		if (r == NULL) return false;

		r->run(graph);
		delete r;

		return true;
	}
};


/**
 * A request with node properties
 */
class ll_la_request_with_node_properties : public ll_la_request {

	std::vector<std::pair<ll_mlcsr_node_property<uint32_t>*, uint32_t>>
		_properties_32;
	std::vector<std::pair<ll_mlcsr_node_property<uint64_t>*, uint64_t>>
		_properties_64;


protected:

	/**
	 * Set the properties
	 *
	 * @param graph the graph
	 * @param n the node
	 */
	void set_properties(ll_writable_graph& graph, node_t n) {

		for (size_t i = 0; i < _properties_32.size(); i++) {
			_properties_32[i].first->set(n, _properties_32[i].second);
		}

		for (size_t i = 0; i < _properties_64.size(); i++) {
			_properties_64[i].first->set(n, _properties_64[i].second);
		}
	}


public:

	/**
	 * Add a property
	 *
	 * @param property the property
	 * @param value the value
	 */
	void add_property_32(ll_mlcsr_node_property<uint32_t>* property,
			uint32_t value) {
		_properties_32.push_back(
				std::pair<ll_mlcsr_node_property<uint32_t>*, uint32_t>
				(property, value));
	}


	/**
	 * Add a property
	 *
	 * @param property the property
	 * @param value the value
	 */
	void add_property_64(ll_mlcsr_node_property<uint64_t>* property,
			uint64_t value) {
		_properties_64.push_back(
				std::pair<ll_mlcsr_node_property<uint64_t>*, uint64_t>
				(property, value));
	}
};


/**
 * A request with edge properties
 */
class ll_la_request_with_edge_properties : public ll_la_request {

	std::vector<std::pair<ll_mlcsr_edge_property<uint32_t>*, uint32_t>>
		_properties_32;
	std::vector<std::pair<ll_mlcsr_edge_property<uint64_t>*, uint64_t>>
		_properties_64;


protected:

	/**
	 * Set the properties
	 *
	 * @param graph the graph
	 * @param e the edge
	 */
	void set_properties(ll_writable_graph& graph, edge_t e) {

		for (size_t i = 0; i < _properties_32.size(); i++) {
			_properties_32[i].first->set(e, _properties_32[i].second);
		}

		for (size_t i = 0; i < _properties_64.size(); i++) {
			_properties_64[i].first->set(e, _properties_64[i].second);
		}
	}


public:

	/**
	 * Add a property
	 *
	 * @param property the property
	 * @param value the value
	 */
	void add_property_32(ll_mlcsr_edge_property<uint32_t>* property,
			uint32_t value) {
		_properties_32.push_back(
				std::pair<ll_mlcsr_edge_property<uint32_t>*, uint32_t>
				(property, value));
	}


	/**
	 * Add a property
	 *
	 * @param property the property
	 * @param value the value
	 */
	void add_property_64(ll_mlcsr_edge_property<uint64_t>* property,
			uint64_t value) {
		_properties_64.push_back(
				std::pair<ll_mlcsr_edge_property<uint64_t>*, uint64_t>
				(property, value));
	}
};


/**
 * A request to create a node and assign properties
 */
class ll_la_add_node : public ll_la_request_with_node_properties {

	node_t* _out;


public:

	/**
	 * Create a class of type ll_la_add_node
	 *
	 * @param out the output pointer
	 */
	ll_la_add_node(node_t* out = NULL) {
		_out = out;
	}


	/**
	 * Perform the request
	 *
	 * @param graph the graph
	 */
	virtual void run(ll_writable_graph& graph) {

		node_t n = graph.add_node();
		if (n == LL_NIL_NODE) abort();

		this->set_properties(graph, n);

		if (_out != NULL) *_out = n;
	}
};


/**
 * A request to create a node with a predetermined ID and assign properties
 */
class ll_la_add_node_with_id
	: public ll_la_request_with_node_properties {

	node_t _id;


public:

	/**
	 * Create a class of type ll_la_add_node_with_id
	 *
	 * @param id the new ID
	 */
	ll_la_add_node_with_id(node_t id) {
		_id = id;
	}


	/**
	 * Perform the request
	 *
	 * @param graph the graph
	 */
	virtual void run(ll_writable_graph& graph) {

		bool b = graph.add_node(_id);
		if (!b) abort();

		this->set_properties(graph, _id);
	}
};


/**
 * A request to add properties to an existing node
 */
class ll_la_set_node_properties : public ll_la_request_with_node_properties {

	node_t _n;


public:

	/**
	 * Create a class of type ll_la_set_node_properties
	 *
	 * @param n the node
	 */
	ll_la_set_node_properties(node_t n) {
		_n = n;
	}


	/**
	 * Perform the request
	 *
	 * @param graph the graph
	 */
	virtual void run(ll_writable_graph& graph) {
		this->set_properties(graph, _n);
	}
};


/**
 * A request to create an edge and assign properties
 */
template <typename Node>
class ll_la_add_edge : public ll_la_request_with_edge_properties {

	Node _source;
	Node _target;


public:

	/**
	 * Create a class of type ll_la_add_edge
	 *
	 * @param source the source vertex
	 * @param target the target vertex
	 */
	ll_la_add_edge(Node source, Node target) {
		_source = source;
		_target = target;
	}


	/**
	 * Perform the request
	 *
	 * @param graph the graph
	 */
	virtual void run(ll_writable_graph& graph) {

		edge_t e = graph.add_edge(_source, _target);
		this->set_properties(graph, e);
	}
};


/**
 * A request to create an edge if it does not already exist and assign
 * properties
 */
template <typename Node>
class ll_la_add_edge_if_not_exists
	: public ll_la_request_with_edge_properties {

	Node _source;
	Node _target;


public:

	/**
	 * Create a class of type ll_la_add_edge_if_not_exists
	 *
	 * @param source the source vertex
	 * @param target the target vertex
	 */
	ll_la_add_edge_if_not_exists(Node source, Node target) {
		_source = source;
		_target = target;
	}


	/**
	 * Perform the request
	 *
	 * @param graph the graph
	 */
	virtual void run(ll_writable_graph& graph) {
		edge_t e;
		if (graph.add_edge_if_not_exists(_source, _target, &e)) {
			// New edge
		}
		else {
			// Existing edge
		}

		this->set_properties(graph, e);
	}
};


#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES

/**
 * A request to create an edge for streaming with weights and assign
 * properties
 */
template <typename Node>
class ll_la_add_edge_for_streaming_with_weights
	: public ll_la_request_with_edge_properties {

	Node _source;
	Node _target;


public:

	/**
	 * Create a class of type ll_la_add_edge_for_streaming_with_weights
	 *
	 * @param source the source vertex
	 * @param target the target vertex
	 */
	ll_la_add_edge_for_streaming_with_weights(Node source, Node target) {
		_source = source;
		_target = target;
	}


	/**
	 * Perform the request
	 *
	 * @param graph the graph
	 */
	virtual void run(ll_writable_graph& graph) {
		edge_t e;
		graph.add_edge_for_streaming_with_weights(_source, _target, &e);
		this->set_properties(graph, e);
	}
};

#endif


#endif
