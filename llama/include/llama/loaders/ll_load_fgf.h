/*
 * ll_load_fgf.h
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


#ifndef LL_LOAD_FGF_H_
#define LL_LOAD_FGF_H_

#include <sstream>

#if defined(__linux__)
#include <endian.h>
#elif defined(__NetBSD__) // maybe also __FreeBSD__, not sure
#include <sys/endian.h>
#else
#include <machine/endian.h>
#endif

#include "llama/ll_mem_array.h"
#include "llama/ll_writable_graph.h"

#include "llama/loaders/ll_load_async_writable.h"
#include "llama/loaders/ll_load_utils.h"

#if defined(__APPLE__)
#include <arpa/inet.h>
#define be32toh		ntohl
#define be16toh		ntohs

inline uint64_t be64toh(uint64_t h) {

	uint64_t x;
	uint8_t* d = (uint8_t*) &x;
	uint8_t* s = (uint8_t*) &h;

	d[0] = s[7];
	d[1] = s[6];
	d[2] = s[5];
	d[3] = s[4];
	d[4] = s[3];
	d[5] = s[2];
	d[6] = s[1];
	d[7] = s[0];

	return x;
}
#endif

// XXX Broken - needs a STINGER-like node map
//#define LOAD_NODES_ASYNC


/**
 * A FGF property
 */
class ll_fgf_property_type {

	std::string _name;
	short _type;


public:

	int v_aux;
	int e_aux;


	/**
	 * Create an instace of class ll_fgf_property_type
	 *
	 * @param name the property name
	 * @param type the property type
	 */
	ll_fgf_property_type(const char* name, short type) {
		
		_name = name;
		_type = type;

		v_aux = -1;
		e_aux = -1;
	}


	/**
	 * Create an instace of class ll_fgf_property_type
	 *
	 * @param source the object to copy
	 */
	ll_fgf_property_type(const ll_fgf_property_type& source) {

		_name = source._name;
		_type = source._type;

		v_aux = source.v_aux;
		e_aux = source.e_aux;
	}


	/**
	 * Get the property name
	 *
	 * @return the property name
	 */
	inline const char* name() {
		return _name.c_str();
	}
	

	/**
	 * Get the property type
	 *
	 * @return the property type
	 */
	inline short type() {
		return _type;
	}


	/**
	 * Determine if the type is supported
	 *
	 * @return true if yes
	 */
	bool supported() {
		switch (_type) {
			case 0x1:
			case 0x12:
				return true;
			default:
				return false;
		}
	}
};


/**
 * A FGF vertex or edge type
 */
class ll_fgf_object_type  {

	std::string _name;
	size_t _size;


public:

	/**
	 * Create an instace of class ll_fgf_object_type
	 *
	 * @param name the type name
	 * @param size the number of objects
	 */
	ll_fgf_object_type(const char* name, size_t size) {
		_name = name;
		_size = size;
	}


	/**
	 * Create an instace of class ll_fgf_object_type
	 *
	 * @param source the object to copy
	 */
	ll_fgf_object_type(const ll_fgf_object_type& source) {
		_name = source._name;
		_size = source._size;
	}


	/**
	 * Get the object type name
	 *
	 * @return the object type name
	 */
	inline const char* name() {
		return _name.c_str();
	}
	

	/**
	 * Get the number of objects
	 *
	 * @return the size
	 */
	inline size_t size() {
		return _size;
	}
};


/**
 * A wrapper around Java's object stream
 */
class ll_java_os_reader {

	FILE* _file;
	size_t _offset;

	char* _buffer;
	size_t _buffer_offset;
	size_t _buffer_size;

	size_t _cursor;

	uint32_t _header;

public:


	/**
	 * Create an instance of ll_java_os_reader
	 *
	 * @param file the file
	 */
	ll_java_os_reader(FILE* file) {

		_file = file;
		_offset = ftell(file);

		uint32_t w;
		if (fread(&w, sizeof(w), 1, _file) != 1) abort();
		_header = be32toh(w);

		_buffer = NULL;
		_cursor = 0;
		_buffer_size = 0;
	}


	/**
	 * Destroy the class
	 */
	~ll_java_os_reader() {
		if (_buffer != NULL) free(_buffer);
	}


	/**
	 * Rewind
	 */
	void rewind() {
		
		fseek(_file, (long) _offset, SEEK_SET);

		_cursor = 0;
		_buffer_size = 0;

		uint32_t w;
		fread(&w, sizeof(w), 1, _file);
		_header = be32toh(w);
	}


	/**
	 * Read bytes
	 *
	 * @param buf the target buffer
	 * @param size the number of bytes
	 */
	void read(void* buf, size_t size) {
		while (size > 0) { 
			if (_cursor == _buffer_size) read_block();

			size_t l = size;
			if (_cursor + size > _buffer_size) {
				l = _buffer_size - _cursor;
			}

			memcpy(buf, _buffer + _cursor, l);

			size -= l;
			_cursor += l;
			buf = (void*) (((char*) buf) + l);
		}
	}


	/**
	 * Read the next byte
	 *
	 * @return the next byte
	 */
	inline uint8_t read_byte() {
		uint8_t w;
		read(&w, sizeof(w));
		return w;
	}


	/**
	 * Read the next short
	 *
	 * @return the next short
	 */
	inline uint16_t read_short() {
		uint16_t w;
		read(&w, sizeof(w));
		return be16toh(w);
	}


	/**
	 * Read the next int
	 *
	 * @return the next int
	 */
	inline uint32_t read_int() {
		uint32_t w;
		read(&w, sizeof(w));
		return be32toh(w);
	}


	/**
	 * Read the next long
	 *
	 * @return the next long
	 */
	inline uint64_t read_long() {
		uint64_t w;
		read(&w, sizeof(w));
		return be64toh(w);
	}


	/**
	 * Read the next string
	 *
	 * @return the next string
	 */
	inline std::string read_string() {
		uint16_t length = read_short();
		char str[length + 1];
		read(str, length);
		str[length] = '\0';
		return std::string(str);
	}


	/**
	 * Check that the next few bytes are what is expected
	 *
	 * @param magic what to expect
	 * @return true if it matches
	 */
	inline bool verify_magic(const char* magic) {
		for (const char* p = magic; *p != '\0'; p++) {
			if (read_byte() != *p) return false;
		}
		return true;
	}


	/**
	 * Read the next compressed word
	 *
	 * @return the next compressed word
	 */
	inline uint64_t read_compressed_word() {
		uint8_t b = read_byte();
		return b < 0xff ? b : read_long();
	}


private:

	/**
	 * Read the next block
	 */
	void read_block() {

		uint8_t magic = fgetc(_file);
		size_t size = 0;

		uint8_t b;
		uint32_t w;

		switch (magic) {
			case 0x77:
				if (fread(&b, sizeof(b), 1, _file) != 1) abort();
				size = b;
				break;
			case 0x7a:
				if (fread(&w, sizeof(w), 1, _file) != 1) abort();
				size = be32toh(w);
				break;
			default:
				fprintf(stderr, "Error in ll_java_os_reader: "
						"Wrong magic at offset %lx.\n", ftell(_file) - 1);
				abort();
		}

		if (_buffer == NULL || size > _buffer_size) {
			if (_buffer != NULL) free(_buffer);
			_buffer = (char*) malloc(size);
		}

		_buffer_size = size;
		_buffer_offset = ftell(_file);
		if (fread(_buffer, 1, _buffer_size, _file) != _buffer_size) abort();

		_cursor = 0;
	}
};


/**
 * A FGF file
 */
class ll_fgf_file {

	std::string _file_name;
	FILE* _file;
	int _format_version;

	size_t _num_vertices;
	size_t _max_edges;
	node_t _initial_vertex_id;
	edge_t _initial_edge_id;

	std::vector<ll_fgf_property_type> _property_types;
	std::vector<ll_fgf_object_type> _vertex_types;
	std::vector<ll_fgf_object_type> _edge_types;

	bool _ok;
	std::string _error_msg;

	size_t _vertices_offset;

public:


	/**
	 * Create an object of type ll_fgf_file
	 *
	 * @param file_name the file name
	 */
	ll_fgf_file(const char* file_name) {

		_ok = false;
		_error_msg = "";


		// Open the file

		_file_name = file_name;
		_file = fopen(file_name, "rb");
		if (_file == NULL) {
			_error_msg = "Could not open file ";
			_error_msg += _file_name;
			_error_msg += ": ";
			_error_msg += strerror(errno);
			return;
		}


		// Parse the header

		if (!verify_magic("FGF")) {
			_error_msg = "Wrong magic";
			return;
		}

		_format_version = next_byte() - (int) '0';
		if (_format_version != 1) {
			_error_msg = "Unsupported version";
			return;
		}

		size_t header_size = next_word();
		size_t header_left = header_size;

		if (header_left >= 8) {
			header_left -= 8;
			_initial_vertex_id = next_word();
		}
		else {
			_initial_vertex_id = 0;
		}

		if (header_left >= 8) {
			header_left -= 8;
			_initial_edge_id = next_word();
		}
		else {
			_initial_edge_id = 0;
		}

		if (header_left > 0) {
			fseek(_file, (long) header_left, SEEK_CUR);
		}


		// Parse the object counts

		if (!verify_magic("CNTS")) {
			_error_msg = "Wrong magic, expected \"CNTS\"";
			return;
		}

		size_t num_property_types = next_word();

		_num_vertices = 0;
		size_t num_vertex_types = next_word();
		for (size_t i = 0; i < num_vertex_types; i++) {
			std::string name = next_string();
			size_t size = next_word();
			_num_vertices += size;
			_vertex_types.push_back(ll_fgf_object_type(name.c_str(), size));
		}

		_max_edges = 0;
		size_t num_edge_types = next_word();
		for (size_t i = 0; i < num_edge_types; i++) {
			std::string name = next_string();
			size_t size = next_word();
			_max_edges += size;
			_edge_types.push_back(ll_fgf_object_type(name.c_str(), size));
		}


		// Parse the attributes

		if (!verify_magic("ATTR")) {
			_error_msg = "Wrong magic, expected \"ATTR\"";
			return;
		}

		for (size_t i = 0; i < num_property_types; i++) {
			std::string name = next_string();
			short type = (short) next_short();
			if (type == 0) {
				_error_msg = "The file contains a Java-only property type";
				return;
			}
			_property_types.push_back(ll_fgf_property_type(name.c_str(), type));
			if (!_property_types[_property_types.size() - 1].supported()) {
				_error_msg = "Unsupported property type ";
				_error_msg += type;
				return;
			}
		}


		// Success!
		
		_vertices_offset = ftell(_file);

		_ok = true;
	}


	/**
	 * Destroy the object
	 */
	virtual ~ll_fgf_file() {
		if (_file != NULL) fclose(_file);
	}
	

	/**
	 * Return true if the file open went okay
	 *
	 * @return true on no error
	 */
	inline bool okay() {
		return _ok;
	}
	

	/**
	 * Return the last error message on failure
	 *
	 * @return the last error message
	 */
	inline const char* error_message() {
		return _error_msg.c_str();
	}


	/**
	 * Get the file
	 *
	 * @return the file
	 */
	inline FILE* file() {
		return _file;
	}


	/**
	 * Rewind back to the beginning of the data section 
	 */
	void rewind() {
		fseek(_file, (long) _vertices_offset, SEEK_SET);
	}


	/**
	 * Load the level
	 *
	 * @param graph the target RO graph
	 * @param config the loader configuration
	 */
	template <class GraphRO> void load_ro(GraphRO& graph,
			const ll_loader_config* config) {

		_ok = false;

		bool load_properties = !config->lc_no_properties;

		size_t max_vertices = _initial_vertex_id + _num_vertices;
		//size_t max_edges = _initial_edge_id + _max_edges;
		size_t new_level = graph.num_levels();


		// Start new levels for the node properties

		if (load_properties) {
			ll_with(auto p = graph.get_all_node_properties_32()) {
				for (auto it = p.begin(); it != p.end(); it++)
					it->second->init_level(max_vertices);
			}
			ll_with(auto p = graph.get_all_node_properties_64()) {
				for (auto it = p.begin(); it != p.end(); it++)
					it->second->init_level(max_vertices);
			}
		}


		// Load the vertex properties

		std::vector<ll_mlcsr_node_property<uint32_t>*> node_properties_32;
		std::vector<ll_mlcsr_node_property<uint64_t>*> node_properties_64;

		node_t v = _initial_vertex_id;
		ll_java_os_reader node_reader(_file);

		for (size_t vt_i = 0; vt_i < _vertex_types.size(); vt_i++) {

			if (!node_reader.verify_magic("NODE")) {
				_error_msg = "Wrong magic, expected \"NODE\"";
				return;
			}

			node_reader.read_string();

			for (size_t v_i = 0; v_i < _vertex_types[vt_i].size(); v_i++) {

				// Load the properties

				size_t n = node_reader.read_compressed_word();
				for (size_t i = 0; i < n; i++) {
					size_t t_i = node_reader.read_compressed_word();
					assert(t_i >= 0 && t_i < _property_types.size());
					ll_fgf_property_type& t = _property_types[t_i];

					switch (t.type()) {
						case LL_T_STRING /* 0x01 */:
							if (load_properties) {
								if (t.v_aux < 0) {
									t.v_aux = node_properties_64.size();
									auto* p = graph.get_node_property_64(t.name());
									if (p != NULL) {
										node_properties_64.push_back(p);
									}
									else {
										node_properties_64.push_back(graph
												.create_uninitialized_node_property_64
												(t.name(), t.type(),
												 destructor64<std::string>));
										node_properties_64[t.v_aux]
											->ensure_min_levels(new_level, max_vertices);
										node_properties_64[t.v_aux]
											->init_level(max_vertices);
									}
								}
								node_properties_64[t.v_aux]->append_node(v,
										(long) new std::string(node_reader.read_string()));
							}
							else {
								node_reader.read_string();
							}
							break;
						case LL_T_INT32 /* 0x12 */:
							if (load_properties) {
								if (t.v_aux < 0) {
									t.v_aux = node_properties_32.size();
									auto* p = graph.get_node_property_32(t.name());
									if (p != NULL) {
										node_properties_32.push_back(p);
									}
									else {
										node_properties_32
											.push_back(graph
													.create_uninitialized_node_property_32
													(t.name(), t.type()));
										node_properties_32[t.v_aux]
											->ensure_min_levels(new_level, max_vertices);
										node_properties_32[t.v_aux]->init_level(max_vertices);
									}
								}
								node_properties_32[t.v_aux]->append_node(v,
										node_reader.read_int());
							}
							else {
								node_reader.read_int();
							}
							break;
						default:
							abort();
					}
				}

				v++;
			}
		}

		if (load_properties) {
			for (size_t i = 0; i < node_properties_32.size(); i++) {
				node_properties_32[i]->finish_level();
			}
			for (size_t i = 0; i < node_properties_64.size(); i++) {
				node_properties_64[i]->finish_level();
			}
		}


		// Compute the out-degrees

		size_t edges_start = ftell(_file);

		degree_t* degrees = (degree_t*) malloc(sizeof(degree_t)*max_vertices);
		memset(degrees, 0, sizeof(degree_t) * max_vertices);

		for (size_t et_i = 0; et_i < _edge_types.size(); et_i++) {
			ll_java_os_reader edge_reader(_file);

			if (!edge_reader.verify_magic("EDGE")) {
				_error_msg = "Wrong magic, expected \"EDGE\"";
				return;
			}

			edge_reader.read_string();

			for (size_t v_i = 0; v_i < _edge_types[et_i].size(); v_i++) {

				node_t head = edge_reader.read_long();
				node_t tail = edge_reader.read_long();

				(void) head;
				degrees[tail]++;


				// Skip the properties

				size_t n = edge_reader.read_compressed_word();
				for (size_t i = 0; i < n; i++) {
					size_t t_i = edge_reader.read_compressed_word();
					ll_fgf_property_type& t = _property_types[t_i];

					switch (t.type()) {
						case LL_T_STRING /* 0x01 */:
							edge_reader.read_string();
							break;
						case LL_T_INT32 /* 0x12 */:
							edge_reader.read_int();
							break;
						default:
							abort();
					}
				}
			}
		}


		// Initialize the level

		graph.init_level_from_degrees(max_vertices, degrees, NULL);


		// Start new levels for the edge properties

		if (load_properties) {
			ll_with(auto p = graph.get_all_edge_properties_32()) {
				for (auto it = p.begin(); it != p.end(); it++)
					it->second->cow_init_level(_max_edges);
			}
			ll_with(auto p = graph.get_all_edge_properties_64()) {
				for (auto it = p.begin(); it != p.end(); it++)
					it->second->cow_init_level(_max_edges);
			}
		}


		// Load the edges

		std::vector<ll_mlcsr_edge_property<uint32_t>*> edge_properties_32;
		std::vector<ll_mlcsr_edge_property<uint64_t>*> edge_properties_64;

		fseek(_file, (long) edges_start, SEEK_SET);

		degree_t* loc = degrees; degrees = NULL;
		memset(loc, 0, sizeof(degree_t) * max_vertices);

		for (size_t et_i = 0; et_i < _edge_types.size(); et_i++) {
			ll_java_os_reader edge_reader(_file);

			if (!edge_reader.verify_magic("EDGE")) {
				_error_msg = "Wrong magic, expected \"EDGE\"";
				return;
			}

			edge_reader.read_string();

			for (size_t v_i = 0; v_i < _edge_types[et_i].size(); v_i++) {

				node_t head = edge_reader.read_long();
				node_t tail = edge_reader.read_long();

				edge_t e = graph.out().write_value(tail, loc[tail]++, head);


				// Load the properties

				size_t n = edge_reader.read_compressed_word();
				for (size_t i = 0; i < n; i++) {
					size_t t_i = edge_reader.read_compressed_word();
					assert(t_i >= 0 && t_i < _property_types.size());
					ll_fgf_property_type& t = _property_types[t_i];

					// Assume that edge properties are dense - this makes things
					// a lot simpler during loading
					
					switch (t.type()) {
						case LL_T_STRING /* 0x01 */:
							if (load_properties) {
								if (t.e_aux < 0) {
									t.e_aux = edge_properties_64.size();
									auto* p = graph.get_edge_property_64(t.name());
									if (p != NULL) {
										edge_properties_64.push_back(p);
									}
									else {
										edge_properties_64.push_back(graph
												.create_uninitialized_edge_property_64
												(t.name(), t.type(),
													 destructor64<std::string>));
										edge_properties_64[t.e_aux]
											->ensure_min_levels(new_level, _max_edges);
										edge_properties_64[t.e_aux]
											->cow_init_level(_max_edges);
									}
								}
								edge_properties_64[t.e_aux]->cow_write(e,
										(long) new std::string(edge_reader.read_string()));
							}
							else {
								edge_reader.read_string();
							}
							break;
						case LL_T_INT32 /* 0x12 */:
							if (load_properties) {
								if (t.e_aux < 0) {
									t.e_aux = edge_properties_32.size();
									auto* p = graph.get_edge_property_32(t.name());
									if (p != NULL) {
										edge_properties_32.push_back(p);
									}
									else {
										edge_properties_32.push_back(graph
												.create_uninitialized_edge_property_32
												(t.name(), t.type()));
										edge_properties_32[t.e_aux]
											->ensure_min_levels(new_level, _max_edges);
										edge_properties_32[t.e_aux]
											->cow_init_level(_max_edges);
									}
								}
								edge_properties_32[t.e_aux]
									->cow_write(e, edge_reader.read_int());
							}
							else {
								edge_reader.read_int();
							}
							break;
						default:
							abort();
					}
				}
			}
		}

		// TODO Sort edges if necessary

		graph.finish_level_edges();


		// Finish edge properties

		if (load_properties) {
			for (size_t i = 0; i < edge_properties_32.size(); i++) {
				edge_properties_32[i]->finish_level();
			}
			for (size_t i = 0; i < edge_properties_64.size(); i++) {
				edge_properties_64[i]->finish_level();
			}
		}


		// Success!
		
		free(loc);

		_ok = verify_magic("ENDG");
	}


	/**
	 * Load the level into the writable representation
	 *
	 * @param graph the target RW graph
	 * @param config the loader configuration
	 */
	template <class GraphRW> void load_rw(GraphRW& graph,
			const ll_loader_config* config) {

		bool load_properties = !config->lc_no_properties;

		int p_int32;
		std::string p_str;

		_ok = false;

		size_t max_vertices = _initial_vertex_id + _num_vertices;
		//size_t max_edges = _initial_edge_id + _max_edges;
		size_t num_levels = graph.ro_graph().num_levels();

		graph.tx_begin();


		// Load the vertex properties

		node_t* node_map = (node_t*) malloc(sizeof(node_t) * _num_vertices);

		std::vector<ll_mlcsr_node_property<uint32_t>*> node_properties_32;
		std::vector<ll_mlcsr_node_property<uint64_t>*> node_properties_64;

		node_t file_v = _initial_vertex_id;
		ll_java_os_reader node_reader(_file);

		size_t num_stripes = omp_get_max_threads();
		#define LA_TO_STRIPE(x)		(((x) >> (LL_ENTRIES_PER_PAGE_BITS + 3)) % (num_stripes))
		ll_la_request_queue request_queue[num_stripes];


#ifdef LOAD_NODES_ASYNC
		#pragma omp parallel
		{
			if (omp_get_thread_num() == 0) {
#elif 0
			}}
#endif

		for (size_t vt_i = 0; vt_i < _vertex_types.size(); vt_i++) {

			if (!node_reader.verify_magic("NODE")) {
				_error_msg = "Wrong magic, expected \"NODE\"";
				graph.tx_abort();
				break; //XXX return;
			}

			node_reader.read_string();

			for (size_t v_i = 0; v_i < _vertex_types[vt_i].size(); v_i++) {

				// Create the vertex if it does not already exist

#ifdef LOAD_NODES_ASYNC
				ll_la_request_with_node_properties* request;
				if (graph.node_exists(file_v)) {
					if (load_properties) {
						request = new ll_la_set_node_properties(file_v);
						node_map[file_v - _initial_vertex_id] = file_v;
					}
				}
				else {
					request = new ll_la_add_node
						(&node_map[file_v - _initial_vertex_id]);
				}
#else
				node_t v;
				if (graph.node_exists(file_v)) {
					v = file_v;
				}
				else {
					v = graph.add_node();
					if (v == LL_NIL_NODE) {
						graph.tx_abort();
						abort();		// TODO What to do now?
					}
				}
				node_map[file_v - _initial_vertex_id] = v;
#endif


				// Load the properties

				size_t n = node_reader.read_compressed_word();
				for (size_t i = 0; i < n; i++) {
					size_t t_i = node_reader.read_compressed_word();
					assert(t_i >= 0 && t_i < _property_types.size());
					ll_fgf_property_type& t = _property_types[t_i];

					switch (t.type()) {
						case LL_T_STRING /* 0x01 */:
							p_str = node_reader.read_string();
							if (load_properties) {
								if (t.v_aux < 0) {
									t.v_aux = node_properties_64.size();
									auto* p = graph.get_node_property_64(t.name());
									if (p != NULL) {
										node_properties_64.push_back(p);
									}
									else {
										if (num_levels == 0) {
											node_properties_64.push_back(graph
													.ro_graph()
													.create_uninitialized_node_property_64
													(t.name(), t.type(),
														destructor64<std::string>));
											node_properties_64[t.v_aux]
												->ensure_min_levels(num_levels, 0);
											node_properties_64[t.v_aux]
												->writable_init(max_vertices);
											graph.get_node_property_64(t.name());
										}
										else {
											abort();	// XXX
										}
									}
								}
#ifdef LOAD_NODES_ASYNC
								request->add_property_64(node_properties_64[t.v_aux],
										(long) new std::string(p_str));
#else
								node_properties_64[t.v_aux]->set(v,
										(long) new std::string(p_str));
#endif
							}
							break;
						case LL_T_INT32 /* 0x12 */:
							p_int32 = node_reader.read_int();
							if (load_properties) {
								if (t.v_aux < 0) {
									t.v_aux = node_properties_32.size();
									auto* p = graph.get_node_property_32(t.name());
									if (p != NULL) {
										node_properties_32.push_back(p);
									}
									else {
										if (num_levels == 0) {
											node_properties_32.push_back(graph.ro_graph()
													.create_uninitialized_node_property_32
													(t.name(), t.type()));
											node_properties_32[t.v_aux]
												->ensure_min_levels(num_levels, 0);
											node_properties_32[t.v_aux]
												->writable_init(max_vertices);
											graph.get_node_property_32(t.name());
										}
										else {
											abort();	// XXX
										}
									}
								}
#ifdef LOAD_NODES_ASYNC
								request->add_property_32
									(node_properties_32[t.v_aux], p_int32);
#else
								node_properties_32[t.v_aux]->set(v, p_int32);
#endif
							}
							break;
						default:
							abort();
					}
				}

				file_v++;

#ifdef LOAD_NODES_ASYNC
				request_queue[LA_TO_STRIPE(file_v)].enqueue(request);
#endif
			}
		}

#ifdef LOAD_NODES_ASYNC
#if 0
		{
			{
#endif
				// Add a worker

				for (int i = 0; i < num_stripes; i++)
					request_queue[i].shutdown_when_empty();
				for (int i = 0; i < num_stripes; i++)
					request_queue[i].run(graph);
			}
			else {
				int t = omp_get_thread_num();
				for (int i = 0; i < num_stripes; i++, t++)
					request_queue[t % num_stripes].worker(graph);
			}
		}
#endif


		// Load the edges

		std::vector<ll_mlcsr_edge_property<uint32_t>*> edge_properties_32;
		std::vector<ll_mlcsr_edge_property<uint64_t>*> edge_properties_64;

		for (size_t i = 0; i < num_stripes; i++)
			request_queue[i].shutdown_when_empty(false);

#pragma omp parallel
		{
			if (omp_get_thread_num() == 0) {

				for (size_t et_i = 0; et_i < _edge_types.size(); et_i++) {
					ll_java_os_reader edge_reader(_file);

					if (!edge_reader.verify_magic("EDGE")) {
						_error_msg = "Wrong magic, expected \"EDGE\"";
						graph.tx_abort();
						break;//XXX return;
					}

					edge_reader.read_string();

					for (size_t v_i = 0; v_i < _edge_types[et_i].size(); v_i++) {

						node_t head = edge_reader.read_long();
						node_t tail = edge_reader.read_long();

						if (head >= _initial_vertex_id)
							head = node_map[head - _initial_vertex_id];
						if (tail >= _initial_vertex_id)
							tail = node_map[tail - _initial_vertex_id];

						ll_la_request_with_edge_properties* request;

#ifdef LL_S_WEIGHTS_INSTEAD_OF_DUPLICATE_EDGES
						request = new ll_la_add_edge_for_streaming_with_weights
							<node_t>(tail, head);
#else
						request = new ll_la_add_edge<node_t>(tail, head);
#endif


						// Load the properties

						size_t n = edge_reader.read_compressed_word();
						for (size_t i = 0; i < n; i++) {
							size_t t_i = edge_reader.read_compressed_word();
							assert(t_i >= 0 && t_i < _property_types.size());
							ll_fgf_property_type& t = _property_types[t_i];

							switch (t.type()) {
								case LL_T_STRING /* 0x01 */:
									p_str = edge_reader.read_string();
									if (load_properties) {
										if (t.e_aux < 0) {
											t.e_aux = edge_properties_64.size();
											auto* p = graph.get_edge_property_64(t.name());
											if (p != NULL) {
												edge_properties_64.push_back(p);
											}
											else {
												abort();	// XXX
											}
										}
										//edge_properties_64[t.e_aux]->set(e,
										//(long) new std::string(p_str));
										request->add_property_64(edge_properties_64[t.e_aux],
												(long) new std::string(p_str));
									}
									break;
								case LL_T_INT32 /* 0x12 */:
									p_int32 = edge_reader.read_int();
									if (load_properties) {
										if (t.e_aux < 0) {
											t.e_aux = edge_properties_32.size();
											auto* p = graph.get_edge_property_32(t.name());
											if (p != NULL) {
												edge_properties_32.push_back(p);
											}
											else {
												if (num_levels == 0) {
													edge_properties_32.push_back(graph
														.ro_graph()
														.create_uninitialized_edge_property_32
														(t.name(), t.type()));
													edge_properties_32[t.e_aux]
														->ensure_min_levels(num_levels,
																_max_edges);
													edge_properties_32[t.e_aux]
														->writable_init();
													//edge_properties_32[t.e_aux]
													//->cow_init_level(_max_edges);
													graph.get_edge_property_32(t.name());
												}
												else {
													abort();	// XXX
												}
											}
										}
										//edge_properties_32[t.e_aux]->set(e, p_int32);
										request->add_property_32
											(edge_properties_32[t.e_aux], p_int32);
									}
									break;
								default:
									abort();
							}
						}

						request_queue[LA_TO_STRIPE(tail)].enqueue(request);
					}
				}


				// Add a worker

				for (size_t i = 0; i < num_stripes; i++)
					request_queue[i].shutdown_when_empty();
				for (size_t i = 0; i < num_stripes; i++)
					request_queue[i].run(graph);
			}
			else {
				int t = omp_get_thread_num();
				for (size_t i = 0; i < num_stripes; i++, t++)
					request_queue[t % num_stripes].worker(graph);
			}
		}


		// Success!

		_ok = verify_magic("ENDG");

		if (_ok) {
			graph.tx_commit();
		}
		else {
			_error_msg = "Wrong magic, expected \"NODE\"";
			graph.tx_abort();
		}

		free(node_map);

#undef LA_TO_STRIPE
	}


protected:

	/**
	 * Read the next byte
	 *
	 * @return the next byte
	 */
	inline uint8_t next_byte() {
		return fgetc(_file);
	}


	/**
	 * Read the next short
	 *
	 * @return the next short
	 */
	inline uint16_t next_short() {
		uint16_t w;
		if (fread(&w, sizeof(w), 1, _file) != 1) abort();
		return be16toh(w);
	}


	/**
	 * Read the next int
	 *
	 * @return the next int
	 */
	inline uint32_t next_int32() {
		uint32_t w;
		if (fread(&w, sizeof(w), 1, _file) != 1) abort();
		return be32toh(w);
	}


	/**
	 * Read the next word
	 *
	 * @return the next word
	 */
	inline uint64_t next_word() {
		uint64_t w;
		if (fread(&w, sizeof(w), 1, _file) != 1) abort();
		return be64toh(w);
	}


	/**
	 * Read the next string
	 *
	 * @return the next string
	 */
	inline std::string next_string() {
		uint16_t length = next_short();
		char str[length + 1];
		if (fread(str, 1, length, _file) != length) abort();
		str[length] = '\0';
		return std::string(str);
	}


	/**
	 * Check that the next few bytes are what is expected
	 *
	 * @param magic what to expect
	 * @return true if it matches
	 */
	inline bool verify_magic(const char* magic) {
		for (const char* p = magic; *p != '\0'; p++) {
			if (next_byte() != *p) return false;
		}
		return true;
	}
};


/**
 * The FGF file loader
 */
class ll_loader_fgf : public ll_file_loader {

public:

	/**
	 * Create a new instance of ll_loader_fgf
	 */
	ll_loader_fgf() {}


	/**
	 * Destroy the instance
	 */
	virtual ~ll_loader_fgf() {}


	/**
	 * Determine if this file can be opened by this loader
	 *
	 * @param file the file
	 * @return true if it can be opened
	 */
	virtual bool accepts(const char* file) {

		return strcmp(ll_file_extension(file), "fgf") == 0;
	}


	/**
	 * Load directly into the read-only representation by creating a new
	 * level
	 *
	 * @param graph the graph
	 * @param file the file
	 * @param config the loader configuration
	 */
	virtual void load_direct(ll_mlcsr_ro_graph* graph, const char* file,
			const ll_loader_config* config) {


		// Check features

		feature_vector_t features;
		features << LL_L_FEATURE(lc_reverse_edges);
		features << LL_L_FEATURE(lc_reverse_maps);
		features << LL_L_FEATURE(lc_no_properties);

		config->assert_features(false /*direct*/, true /*error*/, features);


		// Load

		ll_fgf_file fgf(file);
		if (!fgf.okay()) {
			fprintf(stderr, "Error: %s\n", fgf.error_message());
			abort();
		}

		fgf.load_ro(*graph, config);
		if (!fgf.okay()) {
			fprintf(stderr, "Error: %s\n", fgf.error_message());
			abort();
		}

		if (config->lc_reverse_edges) {
			graph->make_reverse_edges();
		}
		else {
			graph->out().set_edge_translation(false);
			graph->in().set_edge_translation(false);
		}
	}


	/**
	 * Load incrementally into the writable representation
	 *
	 * @param graph the graph
	 * @param file the file
	 * @param config the loader configuration
	 */
	virtual void load_incremental(ll_writable_graph* graph, const char* file,
			const ll_loader_config* config) {


		// Check features

		feature_vector_t features;
		features << LL_L_FEATURE(lc_reverse_edges);
		features << LL_L_FEATURE(lc_reverse_maps);
		features << LL_L_FEATURE(lc_no_properties);

		config->assert_features(false /*direct*/, true /*error*/, features);


		// Load

		ll_fgf_file fgf(file);
		if (!fgf.okay()) {
			fprintf(stderr, "Error: %s\n", fgf.error_message());
			abort();
		}

		fgf.load_rw(*graph, config);
		if (!fgf.okay()) {
			fprintf(stderr, "Error: %s\n", fgf.error_message());
			abort();
		}
	}


	/**
	 * Create a data source object for the given file
	 *
	 * @param file the file
	 * @return the data source
	 */
	virtual ll_data_source* create_data_source(const char* file) {
		LL_NOT_IMPLEMENTED;
		return NULL;
	}
};

#endif	/* LL_LOAD_FGF_H_ */

