/*
 * ll_persistent_storage.h
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


#ifndef LL_PERSISTENT_STORAGE_H_
#define LL_PERSISTENT_STORAGE_H_

#include "llama/ll_common.h"
#include "llama/ll_growable_array.h"

#include <sys/stat.h>
#include <sys/types.h>

#include <cctype>
#include <cstring>
#include <dirent.h>
#include <string>
#include <unistd.h>


/*
 * Constants / Global Settings
 */

#define LL_BLOCK_SIZE						4096

#ifndef LL_LEVELS_PER_ML_FILE_BITS
#define LL_LEVELS_PER_ML_FILE_BITS			2
#endif
#define LL_LEVELS_PER_ML_FILE				(1 << LL_LEVELS_PER_ML_FILE_BITS)

#define LL_PERSISTENCE_SEPARATOR			"__"
#define LL_PERSISTENCE_HEADER_INDICATOR		"-"


/*
 * Persistence On-Disk Format
 * ==========================
 *
 *
 * File-Name Structure
 * -------------------
 *
 * Each persistence context is stored in one or more data files, each
 * containing LL_LEVELS_PER_ML_FILE snapshots (levels). The file name consists
 * of the persistence context namespace and name followed by its sequence
 * number, all separated by LL_PERSISTENCE_SEPARATOR.
 *
 * For example:
 *
 *   csr__out__0.dat
 *    ^    ^   ^
 *    |    |   |
 *    |    |   +------ multi-level (ML) file sequence number
 *    |    +---------- persistence context name
 *    +--------------- persistence namespace name
 *
 * A persistence context can also have a user-specified header file, which
 * follows the same naming convention as above, except that it has
 * LL_PERSISTENCE_HEADER_INDICATOR in place of the file sequence number.
 *
 *
 * ML File Structure
 * -----------------
 *
 * +------------+-----+------------+--------------+-----+--------------+
 * | Metadata 0 | ... | Metadata K | Level Data 0 | ... | Level Data K |
 * +------------+-----+------------+--------------+-----+--------------+
 *  \                             / \                                 /
 *   -----------------------------   ---------------------------------
 *      Metadata for each level             Data for each level
 *
 * The metadata are instances of ll_persistence_context::level_meta and among
 * other things contains the offsets for the header and indirection table
 * start for each level.
 *
 *
 * Level Data within an ML File (LAMA or LLAMA)
 * --------------------------------------------
 *
 * The following corresponds to ll_persistent_array_swcow:
 *
 * +--------+------------------------+-----------------------+-------------+
 * | Header | LAMA Indirection Table | Edge Table (optional) | LAMA Chunks |
 * +--------+------------------------+-----------------------+-------------+
 *
 * The header is defined in ll_persistent_array_swcow::header_t. The
 * LAMA indirection table contains ll_persistent_chunk for each of the
 * corresponding sub-ranges of LAMA indices (usually vertex IDs).
 *
 * The current construction process assumes a fixed-size edge table, and a
 * variable number of LAMA chunks until the level is finished. cow_init()
 * preallocates the maximum possible amount of space for the case in which all
 * chunks will be COW-ed from the previous level; the file and the
 * corresponding mmap-ed region will be truncated at cow_finish().
 *
 * TODO Move the edge table to a separate file, which would enable the users
 * to create it without knowing its size in advance -- which is for example
 * useful for junction tree construction.
 */



//==========================================================================//
// Classes ll_persistent_chunk and ll_large_persistent_chunk                //
//==========================================================================//

/**
 * A location of a persistent chunk, relative to some context
 */
struct ll_persistent_chunk {

	unsigned pc_level;
	unsigned pc_length;
	size_t pc_offset;
};


/**
 * A location of a large persistent chunk, relative to some context
 */
struct ll_large_persistent_chunk {

	unsigned pc_level;
	size_t pc_length;
	size_t pc_offset;
};


/**
 * Compare two persistent chunks
 *
 * @param a the first chunk
 * @param b the second chunk
 * @return true if they are equal
 */
inline bool operator== (const ll_persistent_chunk& a,
		const ll_persistent_chunk& b) {
	return a.pc_length == b.pc_length && a.pc_length == b.pc_length
		&& a.pc_offset == b.pc_offset;
}


/**
 * Compare two persistent chunks
 *
 * @param a the first chunk
 * @param b the second chunk
 * @return true if they are not equal
 */
inline bool operator!= (const ll_persistent_chunk& a,
		const ll_persistent_chunk& b) {
	return a.pc_length != b.pc_length || a.pc_length != b.pc_length
		|| a.pc_offset != b.pc_offset;
}


/**
 * Compare two persistent chunks
 *
 * @param a the first chunk
 * @param b the second chunk
 * @return true if they are equal
 */
inline bool operator== (const ll_large_persistent_chunk& a,
		const ll_large_persistent_chunk& b) {
	return a.pc_length == b.pc_length && a.pc_length == b.pc_length
		&& a.pc_offset == b.pc_offset;
}


/**
 * Compare two persistent chunks
 *
 * @param a the first chunk
 * @param b the second chunk
 * @return true if they are not equal
 */
inline bool operator!= (const ll_large_persistent_chunk& a,
		const ll_large_persistent_chunk& b) {
	return a.pc_length != b.pc_length || a.pc_length != b.pc_length
		|| a.pc_offset != b.pc_offset;
}



//==========================================================================//
// Class: ll_length_and_data                                                //
//==========================================================================//

/**
 * Length and data
 */
struct ll_length_and_data {
	size_t ld_length;
	char ld_data[0];
};



//==========================================================================//
// Class: ll_persistent_storage                                             //
//==========================================================================//

/**
 * The persistent storage
 */
class ll_persistent_storage {

public:

	/**
	 * Create an instance of ll_persistent_storage and initialize
	 *
	 * @param directory the database directory
	 */
	ll_persistent_storage(const char* directory) {

		struct stat st;


		// Set the database directory

		if (*directory == '\0') {
			_directory = ".";
		}
		else {
			_directory = directory;
		}


		// Create the database directory if it does not exist

		if (stat(_directory.c_str(), &st)) {
			if (mkdir(_directory.c_str(), 0755)) {
				perror("mkdir");
				LL_E_PRINT("Cannot create a directory\n");
				abort();
			}
		}
		else {
			if (!S_ISDIR(st.st_mode)) {
				LL_E_PRINT("Not a directory\n");
				abort();
			}
		}
	}


	/**
	 * Destroy the instance of the class
	 */
	~ll_persistent_storage() {
	}


	/**
	 * Get the directory name
	 *
	 * @return the directory name
	 */
	inline const char* directory() const {
		return _directory.c_str();
	}


	/**
	 * Get the collection of persience_context names within the given namespace
	 *
	 * @param ns the namespace
	 * @return the collection of names
	 */
	std::vector<std::string> list_context_names(const char* ns) {

		std::vector<std::string> v;

		DIR* dir;
		dirent* ent;

		if ((dir = opendir(directory())) == NULL) {
			perror("opendir");
			LL_E_PRINT("Cannot open the database directory\n");
			abort();
		}

		std::string p = ns;
		p += LL_PERSISTENCE_SEPARATOR;
		size_t l = p.length();

		while ((ent = readdir(dir)) != NULL) {
			size_t dl = strlen(ent->d_name);
			if (dl > l + 4
					&& strncmp(ent->d_name, p.c_str(), l) == 0
					&& strncmp(ent->d_name + (dl - 4), ".dat", 4) == 0) {

				char* s = strdup(ent->d_name + l);
				*(s + (strlen(s) - 4)) = '\0';

				char* x = strstr(s, LL_PERSISTENCE_SEPARATOR);
				if (x == NULL) {
					free(s);
					continue;
				}
				*x = '\0';

				std::string str = s;
				free(s);

				bool found = false;
				for (size_t i = 0; i < v.size(); i++) {
					if (v[i] == str) {
						found = true;
						break;
					}
				}

				if (!found) v.push_back(str);
			}
		}

		closedir(dir);
		return v;
	}


private:

	/// The database directory
	std::string _directory;
};



//==========================================================================//
// Class: ll_persistence_context                                            //
//==========================================================================//

/**
 * A persistence context, such as outgoing edges, incoming edges, or a
 * property. This is similar to a database table.
 *
 * A context consists of one or more multi-level (ML) files, each representing
 * at most LL_LEVELS_PER_ML_FILE consecutive levels. Each ML file starts with
 * an index table with the metadata for each of those levels.
 */
class ll_persistence_context {

protected:

	/**
	 * The metadata for each level
	 */
	struct level_meta {

		unsigned lm_level;			// The level
		unsigned lm_sub_level;		// The sub-level for 2D level structures

		unsigned lm_header_size;	// In non-zero, the length of the header
		unsigned lm_base_level;		// If non-zero, the level that this copies

		size_t lm_vt_size;			// Vertex table size (number of nodes)
		size_t lm_vt_partitions;	// The number of vertex table chunks/pages

		size_t lm_vt_offset;		// VT offset within the level file
		size_t lm_header_offset;	// The header offset
	};


public:

	/**
	 * Create an instance of the persistence context
	 *
	 * @param storage the persistent storage engine
	 * @param name the name - must be a valid file name prefix
	 * @param ns the namespace - must also be a valid file name prefix
	 */
	ll_persistence_context(ll_persistent_storage* storage, const char* name,
			const char* ns) {

		_storage = storage;
		_name = name;
		_namespace = ns;

		_fds_lock = 0;
		_lengths_lock = 0;
		_mmaped_regions_lock = 0;

		_header_fd = 0;
		_header_lock = 0;

		_auto_sync = true;


		// Check

		if (!is_name_valid(ns)) {
			LL_E_PRINT("Invalid namespace name: %s\n", ns);
			abort();
		}

		if (!is_name_valid(name)) {
			LL_E_PRINT("Invalid context name: %s\n", name);
			abort();
		}


		// Open all relevant files

		DIR* dir;
		dirent* ent;

		if ((dir = opendir(_storage->directory())) == NULL) {
			perror("opendir");
			LL_E_PRINT("Cannot open the database directory\n");
			abort();
		}

		_prefix = _namespace;
		_prefix += LL_PERSISTENCE_SEPARATOR;
		_prefix += _name;
		size_t nl = _prefix.length();

		while ((ent = readdir(dir)) != NULL) {
			size_t dl = strlen(ent->d_name);
			if (dl > nl + 4
					&& strncmp(ent->d_name, _prefix.c_str(), nl) == 0
					&& strncmp(ent->d_name + (dl - 4), ".dat", 4) == 0) {

				char* p = ent->d_name + nl;
				if (strncmp(p, LL_PERSISTENCE_SEPARATOR,
							strlen(LL_PERSISTENCE_SEPARATOR)) != 0) continue;
				p += strlen(LL_PERSISTENCE_SEPARATOR);


				// Construct the file name

				std::string s = _storage->directory();
				s = s + "/" + ent->d_name;


				// Header file

				if (strncmp(p, LL_PERSISTENCE_HEADER_INDICATOR,
							strlen(LL_PERSISTENCE_HEADER_INDICATOR)) == 0
						&& strcmp(p + strlen(LL_PERSISTENCE_HEADER_INDICATOR),
							".dat") == 0) {
					int f = open(s.c_str(), O_RDWR);
					if (f < 0) {
						perror("open");
						LL_E_PRINT("Cannot open %s\n", s.c_str());
						abort();
					}
					_header_fd = f;
					continue;
				}


				// ML data file

				char* e;
				long x = strtol(p, &e, 10);
				if (e != ent->d_name + (dl - 4) || x < 0
						|| (size_t) x > (LL_MAX_LEVEL
							>> LL_LEVELS_PER_ML_FILE_BITS)) {
					LL_W_PRINT("Invalid file name: %s\n", ent->d_name);
					continue;
				}
				
				int f = open(s.c_str(), O_RDWR);
				if (f < 0) {
					perror("open");
					LL_E_PRINT("Cannot open %s\n", s.c_str());
					abort();
				}

				while ((size_t) x >= _fds.size()) _fds.append(-1);
				_fds[x] = f;


				// Determine the length of the ML file

				off_t l = lseek(f, 0, SEEK_END);
				if (l == (off_t) -1) {
					perror("lseek");
					LL_E_PRINT("Cannot determine the size of %s\n", s.c_str());
					abort();
				}

				while ((size_t) x >= _lengths.size()) _lengths.append(0);
				_lengths[x] = l;

				while ((size_t) x >= _append_locks.size())
					_append_locks.append(0);
			}
		}

		closedir(dir);
	}


	/**
	 * Destroy the persistence context
	 */
	~ll_persistence_context() {

		for (size_t i = 0; i < _mmaped_regions.size(); i++) {
			if (_mmaped_regions[i].mr_address == NULL) continue;
			munmap(_mmaped_regions[i].mr_address, _mmaped_regions[i].mr_length);
		}

		for (size_t i = 0; i < _fds.size(); i++) {
			if (_fds[i] > 0) close(_fds[i]);
		}

		if (_header_fd > 0) close(_header_fd);
	}


	/**
	 * Get the name of the persistence context
	 *
	 * @return the name
	 */
	inline const char* name() const {
		return _name.c_str();
	}


	/**
	 * Read the header
	 *
	 * @return length + data (to be freed by the caller), or NULL if none
	 */
	ll_length_and_data* read_header() {

		ll_spinlock_acquire(&_header_lock);

		if (_header_fd <= 0) {
			ll_spinlock_release(&_header_lock);
			return NULL;
		}

		off_t l = lseek(_header_fd, 0, SEEK_END);
		if (l == (off_t) -1) {
			perror("lseek");
			LL_E_PRINT("Cannot determine the size of the header file for "
					"context %s\n", _name.c_str());
			ll_spinlock_release(&_header_lock);
			abort();
		}

		ll_length_and_data* ld = NULL;
		ld = (ll_length_and_data*) malloc(sizeof(ll_length_and_data) + l);
		ld->ld_length = (size_t) l;

		ssize_t r = pread(_header_fd, ld->ld_data, l, 0);
		if (r < (ssize_t) l) {
			perror("pread");
			LL_E_PRINT("Cannot read header information for context %s\n",
					_name.c_str());
			ll_spinlock_release(&_header_lock);
			abort();
		}

		ll_spinlock_release(&_header_lock);
		return ld;
	}


	/**
	 * Read the header without opening the context
	 *
	 * @param storage the persistent storage
	 * @param name the context name
	 * @param ns the context namespace
	 * @return length + data (to be freed by the caller), or NULL if none
	 */
	static ll_length_and_data* read_header(ll_persistent_storage* storage,
			const char* name, const char* ns) {

		std::string s = storage->directory();
		s += "/";
		s += ns;
		s += LL_PERSISTENCE_SEPARATOR;
		s += name;
		s += LL_PERSISTENCE_SEPARATOR;
		s += LL_PERSISTENCE_HEADER_INDICATOR;
		s += ".dat";

		int f = open(s.c_str(), O_RDONLY);
		if (f < 0) {
			perror("open");
			LL_E_PRINT("Cannot open %s\n", s.c_str());
			abort();
		}

		off_t l = lseek(f, 0, SEEK_END);
		if (l == (off_t) -1) {
			perror("lseek");
			LL_E_PRINT("Cannot determine the size of the header file for "
					"context %s\n", name);
			close(f);
			abort();
		}

		ll_length_and_data* ld = NULL;
		ld = (ll_length_and_data*) malloc(sizeof(ll_length_and_data) + l);
		ld->ld_length = (size_t) l;

		ssize_t r = pread(f, ld->ld_data, l, 0);
		if (r < (ssize_t) l) {
			perror("pread");
			LL_E_PRINT("Cannot read header information for context %s\n",
					name);
			close(f);
			abort();
		}

		close(f);
		return ld;
	}


	/**
	 * Write the header
	 *
	 * @param data the data
	 * @param length the length
	 */
	void write_header(void* data, size_t length) {

		ll_spinlock_acquire(&_header_lock);

		if (_header_fd <= 0) {

			std::string s = _storage->directory();
			s += "/";
			s += _prefix;
			s += LL_PERSISTENCE_SEPARATOR;
			s += LL_PERSISTENCE_HEADER_INDICATOR;
			s += ".dat";

			int f = open(s.c_str(), O_CREAT | O_EXCL | O_RDWR, 0777);
			if (f < 0) {
				perror("open");
				LL_E_PRINT("Cannot open %s\n", s.c_str());
				ll_spinlock_release(&_header_lock);
				abort();
			}

			_header_fd = f;
		}
		else {

			int r = ftruncate(_header_fd, 0);
			if (r != 0) {
				perror("ftruncate");
				LL_E_PRINT("Cannot truncate the header file for context %s\n",
						_name.c_str());
				ll_spinlock_release(&_header_lock);
				abort();
			}
		}

		ssize_t r = pwrite(_header_fd, data, length, 0);
		if (r < (ssize_t) length) {
			perror("pwrite");
			LL_E_PRINT("Cannot write header information for context %s\n",
					_name.c_str());
			ll_spinlock_release(&_header_lock);
			abort();
		}

		if (fsync(_header_fd) != 0) {
			LL_E_PRINT("fsync() failed: %s\n", strerror(errno));
			abort();
		}

		ll_spinlock_release(&_header_lock);
	}


	/**
	 * Do we have a header?
	 *
	 * @return true if we have header
	 */
	inline bool has_header() const {
		return _header_fd > 0;
	}


	/**
	 * Get the cap on the current number of multi-level files
	 *
	 * @return the number of files if all ML files are present
	 */
	inline size_t ml_num_files_cap() const {
		return _fds.size();
	}


	/**
	 * Determine if the given ML file exists
	 *
	 * @param fi the file index
	 * @return true if it exists and is a part of this context
	 */
	inline bool ml_check_file_index(size_t fi) const {
		return fi < _fds.size() && _fds[fi] > 0;
	}


	/**
	 * Given the ML file index, get the level number of the smallest
	 * represented level
	 *
	 * @param fi the file index
	 * @return the level base number
	 */
	inline size_t ml_base_level(size_t fi) const {
		return fi << LL_LEVELS_PER_ML_FILE_BITS;
	}


	/**
	 * Read the level-meta information from the given file
	 *
	 * @param fi the file index
	 * @return the buffer with the level metadata (must be freed by the caller)
	 */
	level_meta* read_level_meta(size_t fi) {

		size_t l = sizeof(level_meta) * LL_LEVELS_PER_ML_FILE;
		level_meta* buffer = (level_meta*) malloc(l);
		ssize_t r = pread(file_for_index(fi), buffer, l, 0);
		if (r < (ssize_t) l) {
			perror("pread");
			LL_E_PRINT("Cannot read level-meta information from ML file %lu\n",
					fi);
			abort();
		}

		return buffer;
	}


	/**
	 * Ensure that the file for the given level is open
	 *
	 * @param level the level number
	 */
	void ensure_file_for_level(size_t level) {

		size_t fi = ml_file_index(level);
		file_for_index(fi);
	}


	/**
	 * Extend the file to the given size
	 *
	 * WARNING: If fallocate() is not available, this will use ftruncate()
	 * instead -- but without checking that the new size is actually
	 * larger than the old size.
	 *
	 * @param fi the file index
	 * @param offset the offset where start
	 * @param length the length to add
	 */
	void extend_file(size_t fi, off_t offset, off_t length) {

#if defined(__linux__)
		int r = fallocate(file_for_index(fi), 0, offset, length);
		if (r != 0) {
			perror("fallocate");
			abort();
		}
#else
		// TODO Make this safer so that we always extend?
		int r = ftruncate(file_for_index(fi), offset + length);
		if (r != 0) {
			perror("ftruncate");
			abort();
		}
#endif
	}


	/**
	 * Allocate a page-aligned space in a file
	 *
	 * @param fi the file index
	 * @param size the size
	 * @return the offset
	 */
	size_t allocate_page_aligned_space(size_t fi, size_t size) {
		
		ll_spinlock_acquire(&_lengths_lock);
		
		if (*((int * volatile) &_append_locks[fi])) {
			LL_E_PRINT("Append locked for ML file %lu\n", fi);
			abort();
			ll_spinlock_release(&_lengths_lock);
		}

		size_t offset = _lengths[fi];
		if ((offset & (LL_BLOCK_SIZE-1)) != 0) {
			size_t x = LL_BLOCK_SIZE - (offset & (LL_BLOCK_SIZE-1));
			_lengths[fi] += x;
			offset += x;
		}
		size_t s = size;
		if ((s & (LL_BLOCK_SIZE-1)) != 0) {
			size_t x = LL_BLOCK_SIZE - (s & (LL_BLOCK_SIZE-1));
			s += x;
		}
		_lengths[fi] += s;

		ll_spinlock_release(&_lengths_lock);

		extend_file(fi, offset, s);

		return offset;
	}


	/**
	 * Preallocate a page-aligned space in a file
	 *
	 * @param fi the file index
	 * @param size the size
	 * @param p_offset pointer to store the offset
	 * @param p_address pointer to store the mmap-ed addres
	 * @param p_map_index pointer to store the internal index of the mapping
	 */
	void preallocate_and_mmap_page_aligned_space(size_t fi, size_t size,
			size_t* p_offset, void** p_address, size_t* p_map_index) {

		// TODO Make sure that this space is not wasted if the program crashes
		// before truncating the file...
		
		ll_spinlock_acquire(&_lengths_lock);
		
		if (*((int * volatile) &_append_locks[fi])) {
			LL_E_PRINT("Append locked for ML file %lu\n", fi);
			abort();
			ll_spinlock_release(&_lengths_lock);
		}
		
		*((int * volatile) &_append_locks[fi]) = 1;

		size_t offset = _lengths[fi];
		if ((offset & (LL_BLOCK_SIZE-1)) != 0) {
			size_t x = LL_BLOCK_SIZE - (offset & (LL_BLOCK_SIZE-1));
			_lengths[fi] += x;
			offset += x;
		}
		size_t s = size;
		if ((s & (LL_BLOCK_SIZE-1)) != 0) {
			size_t x = LL_BLOCK_SIZE - (s & (LL_BLOCK_SIZE-1));
			s += x;
		}
		_lengths[fi] += s;

		ll_spinlock_release(&_lengths_lock);

		extend_file(fi, offset, s);

		void* m = mmap(NULL, s, PROT_READ|PROT_WRITE, MAP_SHARED,
				file_for_index(fi), offset);

		if (m == MAP_FAILED) {
			perror("mmap");
			LL_E_PRINT("Cannot mmap ML file %u\n", (unsigned) fi);
			abort();
		}

		mmaped_region_t mr;
		mr.mr_address = m;
		mr.mr_length = s;
		mr.mr_offset = offset;
		mr.mr_file_index = fi;
		mr.mr_writable = true;

		ll_spinlock_acquire(&_mmaped_regions_lock);
		size_t mi = _mmaped_regions.size();
		_mmaped_regions.push_back(mr);
		ll_spinlock_release(&_mmaped_regions_lock);

		*p_offset = offset;
		*p_address = m;
		*p_map_index = mi;
	}


	/**
	 * Finish (and possibly truncate) a preallocated page-aligned space
	 * in a file and set its mmap memory protection scheme to read-only
	 *
	 * @param mi the map index
	 * @param size the final size
	 */
	void finish_preallocated_and_mmaped_page_aligned_space(size_t mi,
			size_t size) {

		ll_spinlock_acquire(&_mmaped_regions_lock);
		mmaped_region_t& mr = _mmaped_regions[mi];
		size_t fi = mr.mr_file_index;
		
		ll_spinlock_acquire(&_lengths_lock);

		assert(_lengths[fi] == mr.mr_offset + mr.mr_length);
		assert(size <= mr.mr_length);
		
		if (!*((int * volatile) &_append_locks[fi])) {
			LL_E_PRINT("Append NOT locked for ML file %lu\n", fi);
			abort();
		}

		size_t s = size;
		if ((s & (LL_BLOCK_SIZE-1)) != 0) {
			size_t x = LL_BLOCK_SIZE - (s & (LL_BLOCK_SIZE-1));
			s += x;
		}
		_lengths[fi] -= mr.mr_length - s;

		if (s == 0) {
			munmap(mr.mr_address, mr.mr_length);
			memset(&mr, 0, sizeof(mr));
		}
		else if (s < mr.mr_length) {
#if defined(__linux__)
			void* m = mremap(mr.mr_address, mr.mr_length, s, 0);
#elif defined(__APPLE__)
			munmap(mr.mr_address, mr.mr_length);
			void* m = mmap(mr.mr_address, mr.mr_length,
					PROT_READ | (mr.mr_writable ? PROT_WRITE : 0),
					MAP_SHARED | MAP_FIXED, file_for_index(mr.mr_file_index),
					mr.mr_offset);
			if (m == MAP_FAILED) {
				perror("mmap/mremap");
				abort();
			}
#else
			void* m = mremap(mr.mr_address, mr.mr_length,
							mr.mr_address, s, MAP_FIXED);
#endif
			if (m == MAP_FAILED) {
				perror("mremap");
				abort();
			}
			if (m != mr.mr_address) {
				LL_E_PRINT("mremap moved\n");
				abort();
			}
			
			mr.mr_length = s;

			if (ftruncate(file_for_index(fi), _lengths[fi])) {
				perror("ftruncate");
				abort();
			}
		}
		
		mr.mr_writable = false;
		if (mprotect(mr.mr_address, mr.mr_length, PROT_READ) != 0) {
			perror("mprotect");
			abort();
		}
		
		*((int * volatile) &_append_locks[fi]) = 0;

		ll_spinlock_release(&_lengths_lock);
		ll_spinlock_release(&_mmaped_regions_lock);
	}


	/**
	 * Allocate a new level
	 *
	 * @param level the level number
	 * @param headerSize the size of the header
	 * @param max_nodes the number of nodes
	 * @param numPartitions the number of partitions
	 * @return the corresponding level metadata (to be freed by the caller)
	 */
	level_meta* allocate_level(size_t level, size_t headerSize,
			size_t max_nodes, size_t numPartitions) {

		size_t fi = ml_file_index(level);
		int fd = file_for_index(fi);

		level_meta* l = (level_meta*) calloc(1, sizeof(level_meta));
		l->lm_level = level;
		l->lm_header_size = headerSize;
		l->lm_vt_size = max_nodes;
		l->lm_vt_partitions = numPartitions;

		l->lm_header_offset = allocate_page_aligned_space(fi, 
				sizeof(ll_persistent_chunk) * numPartitions);
		l->lm_vt_offset = l->lm_header_offset + headerSize;

		ssize_t r = pwrite(fd, l, sizeof(level_meta),
				(level - ml_base_level(fi)) * sizeof(level_meta));
		if (r < (ssize_t) sizeof(level_meta)) {
			perror("pwrite");
			LL_E_PRINT("Cannot write level-meta information to ML file %lu\n", fi);
			free(l);
			abort();
		}
		
		return l;
	}


	/**
	 * Allocate a level as a copy of another level
	 *
	 * @param level the level number
	 * @param source the source level meta
	 * @return the corresponding level metadata (to be freed by the caller)
	 */
	level_meta* duplicate_level(size_t level, const level_meta* source) {

		size_t fi = ml_file_index(level);
		int fd = file_for_index(fi);

		level_meta* l = (level_meta*) calloc(1, sizeof(level_meta));
		memcpy(l, source, sizeof(*l));
		l->lm_level = level;
		if (l->lm_base_level == 0) l->lm_base_level = source->lm_level;

		if (l->lm_header_size > 0) {
			l->lm_header_offset = allocate_page_aligned_space(fi,
					l->lm_header_size);
		}
		else {
			l->lm_header_offset = 0;
		}

		ssize_t r = pwrite(fd, l, sizeof(level_meta),
				(level - ml_base_level(fi)) * sizeof(level_meta));
		if (r < (ssize_t) sizeof(level_meta)) {
			perror("pwrite");
			LL_E_PRINT("Cannot write level-meta information to ML file %lu\n", fi);
			free(l);
			abort();
		}
		
		return l;
	}


	/**
	 * Mmap an existing level. This code assumes that all the previous levels
	 * are already mmap-ed. The persistent array will be mmap-ed as read-only.
	 *
	 * @param lm the level meta
	 * @param chunks the chunk table
	 * @param indirection the indirection table to populate
	 * @param prev_indirection the indirection table of the previous level
	 * @param zero_page the zero page
	 */
	void mmap_level_ro(level_meta* lm, ll_persistent_chunk* chunks,
			void** indirection, void** prev_indirection, void* zero_page) {

		// First, get the range of offsets

		size_t offset_from = (size_t) -1;
		size_t offset_to   = 0;

		for (size_t i = 0; i < lm->lm_vt_partitions; i++) {
			if (chunks[i].pc_level == lm->lm_level) {
				if (offset_from > chunks[i].pc_offset) {
					offset_from = chunks[i].pc_offset;
				}
				if (offset_to < chunks[i].pc_offset + chunks[i].pc_length) {
					offset_to = chunks[i].pc_offset + chunks[i].pc_length;
				}
			}
		}

		if (offset_to <= offset_from) return;


		// Mmap

		size_t ps = sysconf(_SC_PAGE_SIZE);
		offset_from -= offset_from % ps;
		offset_to   += (ps - (offset_to % ps)) % ps;

		size_t size = offset_to - offset_from;
		size_t fi = ml_file_index(lm->lm_level);
		void* m = mmap(NULL, size, PROT_READ, MAP_SHARED,
				file_for_index(fi), offset_from);

		if (m == MAP_FAILED) {
			perror("mmap");
			LL_E_PRINT("Cannot mmap level %u\n", (unsigned) lm->lm_level);
			abort();
		}

		mmaped_region_t mr;
		mr.mr_address = m;
		mr.mr_length = size;
		mr.mr_offset = offset_from;
		mr.mr_file_index = fi;
		mr.mr_writable = false;

		ll_spinlock_acquire(&_mmaped_regions_lock);
		_mmaped_regions.push_back(mr);
		ll_spinlock_release(&_mmaped_regions_lock);


		// Set the indirection table, assuming that the previous level has
		// been built properly

		for (size_t i = 0; i < lm->lm_vt_partitions; i++) {
			if (chunks[i].pc_length == 0) {
				indirection[i] = zero_page;
			}
			else if (chunks[i].pc_level == lm->lm_level) {
				indirection[i] = (char*) m + (chunks[i].pc_offset-offset_from);
			}
			else {
				indirection[i] = prev_indirection[i];
			}
		}
	}


	/**
	 * Mmap a large chunk
	 *
	 * @param chunk the chunk
	 * @param check_for_existing_mapping true to check for an existing mapping
	 * @param writable true to map it writable
	 * @return the address
	 */
	void* mmap_large_chunk(ll_large_persistent_chunk* pc,
			bool check_for_existing_mapping=true,
			bool writable=true) {

		size_t fi = ml_file_index(pc->pc_level);

		if (check_for_existing_mapping) {
			ll_spinlock_acquire(&_mmaped_regions_lock);
			for (size_t i = 0; i < _mmaped_regions.size(); i++) {
				mmaped_region_t& m = _mmaped_regions[i];
				if (m.mr_file_index == fi
						&& m.mr_writable == writable
						&& pc->pc_offset >= m.mr_offset 
						&& pc->pc_offset + pc->pc_length
							<= m.mr_offset + m.mr_length) {
					void* p = (char*) m.mr_address + (pc->pc_offset
							- m.mr_offset);
					ll_spinlock_release(&_mmaped_regions_lock);
					return p;
				}
			}
			ll_spinlock_release(&_mmaped_regions_lock);
		}

		size_t offset_from = pc->pc_offset;
		size_t offset_to = pc->pc_offset + pc->pc_length;

		size_t ps = sysconf(_SC_PAGE_SIZE);
		offset_from -= offset_from % ps;
		offset_to   += (ps - (offset_to % ps)) % ps;

		size_t size = offset_to - offset_from;
		void* m = mmap(NULL, size, PROT_READ | (writable ? PROT_WRITE : 0),
				MAP_SHARED, file_for_index(fi), offset_from);

		if (m == MAP_FAILED) {
			perror("mmap");
			LL_E_PRINT("Cannot mmap level %u\n", (unsigned) pc->pc_level);
			abort();
		}

		mmaped_region_t mr;
		mr.mr_address = m;
		mr.mr_length = size;
		mr.mr_offset = offset_from;
		mr.mr_file_index = fi;
		mr.mr_writable = writable;

		ll_spinlock_acquire(&_mmaped_regions_lock);
		_mmaped_regions.push_back(mr);
		ll_spinlock_release(&_mmaped_regions_lock);

		return (char*) m + (pc->pc_offset - offset_from);
	}


	/**
	 * Read the level header
	 *
	 * @param lm the level meta
	 * @param header the level header
	 */
	void read_level_header(level_meta* lm, void* header) {

		size_t fi = ml_file_index(lm->lm_level);
		int fd = file_for_index(fi);

		ssize_t r = pread(fd, header, lm->lm_header_size, lm->lm_header_offset);
		if (r < lm->lm_header_size) {
			perror("pread");
			LL_E_PRINT("Cannot read level header from ML file %lu\n", fi);
			abort();
		}
	}


	/**
	 * Write the level header
	 *
	 * @param lm the level meta
	 * @param header the level header
	 */
	void write_level_header(level_meta* lm, void* header) {

		size_t fi = ml_file_index(lm->lm_level);
		int fd = file_for_index(fi);

		ssize_t r = pwrite(fd, header, lm->lm_header_size, lm->lm_header_offset);
		if (r < lm->lm_header_size) {
			perror("pwrite");
			LL_E_PRINT("Cannot write level header to ML file %lu\n", fi);
			abort();
		}
	}


	/**
	 * Read the chunk table
	 *
	 * @param lm the level meta
	 * @param chunks the chunk table
	 */
	void read_chunk_table(level_meta* lm, ll_persistent_chunk* chunks) {

		size_t l = lm->lm_level;
		if (lm->lm_base_level != 0) l = lm->lm_base_level;

		size_t fi = ml_file_index(l);
		int fd = file_for_index(fi);

		size_t size = sizeof(ll_persistent_chunk) * lm->lm_vt_partitions;
		ssize_t r = pread(fd, chunks, size, lm->lm_vt_offset);
		if (r < (ssize_t) size) {
			perror("pread");
			LL_E_PRINT("[%s] Cannot read chunk table from ML file %lu\n",
					_name.c_str(), fi);
			LL_E_PRINT("[%s] Level=%u Offset=%lu Length=%lu Read=%ld\n",
					_name.c_str(), lm->lm_level,
					lm->lm_vt_offset, size, r);
			abort();
		}
	}


	/**
	 * Write the chunk table
	 *
	 * @param lm the level meta
	 * @param chunks the chunk table
	 */
	void write_chunk_table(level_meta* lm, ll_persistent_chunk* chunks) {

		assert(lm->lm_base_level == 0);

		size_t fi = ml_file_index(lm->lm_level);
		int fd = file_for_index(fi);

		size_t size = sizeof(ll_persistent_chunk) * lm->lm_vt_partitions;
		ssize_t r = pwrite(fd, chunks, size, lm->lm_vt_offset);
		if (r < (ssize_t) size) {
			perror("pwrite");
			LL_E_PRINT("Cannot write chunk table to ML file %lu\n", fi);
			abort();
		}
	}


	/**
	 * Allocate a new chunk. Do not yet update the level metadata
	 *
	 * @param chunk the chunk pointer
	 * @param level the level number
	 * @param size the size
	 */
	void allocate_chunk(ll_persistent_chunk* chunk, unsigned level, size_t size) {
		
		size_t fi = ml_file_index(level);

		chunk->pc_level = level;
		chunk->pc_length = size;
		chunk->pc_offset = allocate_page_aligned_space(fi, size);
	}


	/**
	 * Allocate a new large chunk
	 *
	 * @param chunk the chunk pointer
	 * @param level the level number
	 * @param size the size
	 */
	void allocate_large_chunk(ll_large_persistent_chunk* chunk, unsigned level,
			size_t size) {
		
		size_t fi = ml_file_index(level);

		chunk->pc_level = level;
		chunk->pc_length = size;
		chunk->pc_offset = allocate_page_aligned_space(fi, size);
	}


	/**
	 * Allocate many new chunks. Do not yet update the level metadata
	 *
	 * @param chunk the first chunk pointer
	 * @param length the length
	 * @param level the level number
	 * @param size the size
	 */
	void allocate_many_chunks(ll_persistent_chunk* chunk, size_t length,
			unsigned level, size_t size) {

		size_t fi = ml_file_index(level);
		size_t offset = allocate_page_aligned_space(fi, length * size);
		
		for (size_t i = 0; i < length; i++, chunk++) {
			chunk->pc_level = level;
			chunk->pc_length = size;
			chunk->pc_offset = offset;
			offset += size;
		}
	}


	/**
	 * Read the corresponding chunk
	 *
	 * @param chunk the chunk
	 * @return the data (must be freed by the caller)
	 */
	void* read(const ll_persistent_chunk& chunk) {

		void* buffer = malloc(chunk.pc_length);
		ssize_t r = pread(file_for_index(ml_file_index(chunk.pc_level)),
				buffer, chunk.pc_length, chunk.pc_offset);
		if (r < chunk.pc_length) {
			perror("pread");
			LL_E_PRINT("Cannot read chunk Level=%u, Offset=%lu, Length=%u\n",
					chunk.pc_level, chunk.pc_offset, chunk.pc_length);
			abort();
		}

		return buffer;
	}


	/**
	 * Write the corresponding chunk
	 *
	 * @param chunk the chunk
	 * @param buffer the buffer
	 * @return the written data
	 */
	void* write(const ll_persistent_chunk& chunk, void* buffer) {

		ssize_t r = pwrite(file_for_index(ml_file_index(chunk.pc_level)),
				buffer, chunk.pc_length, chunk.pc_offset);
		if (r < chunk.pc_length) {
			perror("pwrite");
			LL_E_PRINT("Cannot write chunk Level=%u, Offset=%lu, Length=%u\n",
					chunk.pc_level, chunk.pc_offset, chunk.pc_length);
			abort();
		}

		return buffer;
	}


	/**
	 * Translate a level number to a file index
	 *
	 * @param level the level number
	 * @return the multi-level file index number
	 */
	inline size_t ml_file_index(size_t level) {
		return level >> LL_LEVELS_PER_ML_FILE_BITS;
	}


	/**
	 * Determine if the given string is a valid name
	 *
	 * @param name the context name or namespace name to test
	 * @return true if it is a valid name
	 */
	static bool is_name_valid(const char* name) {

		for (const char* p = name; *p != '\0'; p++) {
			if (!(isalnum(*p) || *p == '-' || *p == '_')) {
				return false;
			}
		}

		if (strstr(name, LL_PERSISTENCE_SEPARATOR) != NULL) {
			return false;
		}

		return true;
	}


	/**
	 * Sync all files and mmap-ed regions for the given level
	 *
	 * @param level the level number
	 */
	void sync(size_t level) {

		size_t fi = ml_file_index(level);
		int fd = file_for_index(fi);

		for (size_t i = 0; i < _mmaped_regions.size(); i++) {
			if (_mmaped_regions[i].mr_address == NULL) continue;
			if (_mmaped_regions[i].mr_file_index == fi) {
				int r = msync(_mmaped_regions[i].mr_address,
						_mmaped_regions[i].mr_length, MS_SYNC);
				if (r != 0) {
					LL_E_PRINT("msync() failed: %s\n", strerror(errno));
					abort();
				}
			}
		}

		if (fsync(fd) != 0) {
			LL_E_PRINT("fsync() failed: %s\n", strerror(errno));
			abort();
		}
	}


	/**
	 * Sync all files and mmap-ed regions for the given level if _auto_sync is
	 * enabled
	 *
	 * @param level the level number
	 */
	void do_auto_sync(size_t level) {

		if (_auto_sync) sync(level);
	}


	/**
	 * Sync everything
	 */
	void sync() {

		for (size_t i = 0; i < _mmaped_regions.size(); i++) {
			if (_mmaped_regions[i].mr_address == NULL) continue;
			int r = msync(_mmaped_regions[i].mr_address,
					_mmaped_regions[i].mr_length, MS_SYNC);
			if (r != 0) {
				LL_E_PRINT("msync() failed: %s\n", strerror(errno));
				abort();
			}
		}

		for (size_t i = 0; i < _fds.size(); i++) {
			int fd = _fds[i];
			if (fsync(fd) != 0) {
				LL_E_PRINT("fsync() failed: %s\n", strerror(errno));
				abort();
			}
		}
	}


protected:

	/**
	 * Get a file or create it if it does not exist
	 *
	 * @param fi the file index number
	 * @return the file descriptor
	 */
	int file_for_index(size_t fi) {

		if (fi < _fds.size()) {
			int r = _fds[fi];
			if (r > 0) return r;
		}

		ll_spinlock_acquire(&_fds_lock);

		while (fi >= _fds.size()) _fds.append(-1);
		int f = _fds[fi];
		if (f > 0) {
			ll_spinlock_release(&_fds_lock);
			return f;
		}

		char sb[64];
		sprintf(sb, "%lu", fi);

		std::string s = _storage->directory();
		s += "/";
		s += _prefix;
		s += LL_PERSISTENCE_SEPARATOR;
		s += sb;
		s += ".dat";

		f = open(s.c_str(), O_CREAT | O_EXCL | O_RDWR, 0777);
		if (f < 0) {
			perror("open");
			LL_E_PRINT("Cannot open %s\n", s.c_str());
			abort();
		}
		_fds[fi] = f;

		void* b = calloc(LL_LEVELS_PER_ML_FILE, sizeof(level_meta));
		ssize_t r = pwrite(f, b, LL_LEVELS_PER_ML_FILE * sizeof(level_meta), 0);
		if (r < (ssize_t) (LL_LEVELS_PER_ML_FILE * sizeof(level_meta))) {
			perror("pwrite");
			LL_E_PRINT("Cannot write the file header of %s\n", s.c_str());
			abort();
		}
		free(b);


		// No need to acquire _lengths_lock, since that only applies to already
		// existing lengths

		while (fi >= _lengths.size()) _lengths.append(0);
		_lengths[fi] = LL_LEVELS_PER_ML_FILE * sizeof(level_meta);

		while (fi >= _append_locks.size()) _append_locks.append(0);


		// Finish

		ll_spinlock_release(&_fds_lock);
		return f;
	}


private:

	/// A mapped region
	typedef struct {
		void* mr_address;
		size_t mr_length;
		size_t mr_file_index;
		size_t mr_offset;
		bool mr_writable;
	} mmaped_region_t;

	/// The persistent storage manager
	ll_persistent_storage* _storage;

	/// The name
	std::string _name;

	/// The namespace
	std::string _namespace;

	/// The file name prefix
	std::string _prefix;

	/// The header file descriptor, if available
	int _header_fd;

	/// The header lock
	ll_spinlock_t _header_lock;

	/// File descriptors for multi-level files
	ll_growable_array<int, 6, ll_nop_deallocator<int>> _fds;

	/// The fds lock
	ll_spinlock_t _fds_lock;

	/// The current length of the file
	ll_growable_array<size_t, 6, ll_nop_deallocator<size_t>> _lengths;

	/// The lengths lock
	ll_spinlock_t _lengths_lock;

	/// The mmaped regions
	std::vector<mmaped_region_t> _mmaped_regions;

	/// The mmaped regions lock
	ll_spinlock_t _mmaped_regions_lock;

	/// The do-not-allocate flag for a file
	ll_growable_array<int, 6, ll_nop_deallocator<int>> _append_locks;

	/// True to automatically sync when finishing a level
	bool _auto_sync;
};



//==========================================================================//
// Class: ll_persistent_array_collection                                    //
//==========================================================================//

/**
 * A collection of persistent levels
 */
template <class A, typename T>
class ll_persistent_array_collection {

public:

	/**
	 * Create an instance of ll_persistent_array_collection
	 *
	 * @param storage the storage manager
	 * @param name the collection name - must be a valid filename prefix
	 * @param ns the namespace - must also be a valid file name prefix
	 */
	ll_persistent_array_collection(ll_persistent_storage* storage,
			const char* name, const char* ns) {

		_master = NULL;
		_persistence = new ll_persistence_context(storage, name, ns);


		// Create the level skeletons

		for (size_t fi = 0; fi < _persistence->ml_num_files_cap(); fi++) {
			if (!_persistence->ml_check_file_index(fi)) continue;

			ll_persistence_context::level_meta* lm
				= _persistence->read_level_meta(fi);

			for (size_t li = 0; li < LL_LEVELS_PER_ML_FILE; li++) {
				ll_persistence_context::level_meta& l = lm[li];
				if (l.lm_vt_offset == 0) continue;

				A* a = new A(this, &l);

				while (_levels.size() <= l.lm_level) _levels.push_back(NULL);
				_levels[l.lm_level] = a;
			}

			free(lm);
		}
	}


	/**
	 * Create a read-only clone of ll_persistent_array_collection
	 *
	 * @param master the master array collection
	 * @param level the max level
	 */
	ll_persistent_array_collection(ll_persistent_array_collection<A, T>* master,
			int level) {
		
		assert(master != NULL);

		_master = master;
		_persistence = master->_persistence;

		if (level >= (int) master->size()) level = (int) master->size() - 1;
		if (level < 0) level = 0;

		if (master->size() > 0) {
			for (int i = 0; i <= level; i++) {
				_levels.push_back(master->_levels[i]);
			}
		}
	}


	/**
	 * Destroy the instance
	 */
	~ll_persistent_array_collection() {

		if (_master != NULL) return;

		for (int l = _levels.size()-1; l >= 0; l--) {
			if (_levels[l] != NULL) {
				delete _levels[l];
				_levels[l] = NULL;
			}
		}

		delete _persistence;
	}


	/**
	 * Get the appropriate level
	 *
	 * @param index the index
	 * @return the level
	 */
	inline A* operator[] (int index) {
		return _levels[index];
	}


	/**
	 * Get the appropriate level
	 *
	 * @param index the index
	 * @return the level
	 */
	inline const A* operator[] (int index) const {
		return _levels[index];
	}


	/**
	 * Get the number of levels
	 *
	 * @return the number of levels - elements in the _level array
	 */
	inline size_t size() const {
		return _levels.size();
	}


	/**
	 * Get the max level
	 *
	 * @return the maximum level to consider
	 */
	inline int max_level() const {
		// Does not support LL_MLCSR_LEVEL_ID_WRAP
		return ((int) size()) - 1;
	}


	/**
	 * Determine if there is a previous level
	 *
	 * @param level the current level ID
	 * @param true if there is a previous level
	 */
	inline bool has_prev_level(int level) const {
		return level > 0 && (*this)[level-1] != NULL;
	}


	/**
	 * Get the previous level
	 *
	 * @param level the current level ID
	 * @return the previous level
	 */
	inline A* prev_level(int level) {
		int id = ((int) level) - 1;
		assert(id >= 0);
		return (*this)[id];
	}


	/**
	 * Get the previous level
	 *
	 * @param level the current level ID
	 * @return the previous level
	 */
	inline const A* prev_level(int level) const {
		int id = ((int) level) - 1;
		assert(id >= 0);
		return (*this)[id];
	}


	/**
	 * Get the latest level
	 *
	 * @return the latest level if available, or NULL otherwise
	 */
	A* latest_level() {
		if (_levels.empty()) return NULL;
		return (*this)[size() - 1];
	}


	/**
	 * Get the latest level
	 *
	 * @return the latest level if available, or NULL otherwise
	 */
	const A* latest_level() const {
		if (_levels.empty()) return NULL;
		return (*this)[size() - 1];
	}


	/**
	 * Get the next level ID, or fail if there is not enough space
	 *
	 * @return the next level ID
	 */
	int next_level_id() const {

		int new_level_id = _levels.size();
		if (new_level_id > (int) LL_MAX_LEVEL) {
			LL_E_PRINT("Maximum number of levels reached (%ld)\n",
						(ssize_t) LL_MAX_LEVEL + 1);
			abort();
		}

		return new_level_id;
	}


	/**
	 * Determine if the given level exists
	 *
	 * @param level the level number
	 * @return true if it exists
	 */
	bool level_exists(size_t level) const {
		return level < _levels.size() && _levels[level] != NULL;
	}


	/**
	 * Count the number of existing levels
	 *
	 * @return the number of existing levels
	 */
	size_t count_existing_levels() const {
		size_t x = 0;
		for (size_t i = 0; i < _levels.size(); i++) {
			if (_levels[i] != NULL) x++;
		}
		return x;
	}


	/**
	 * Determine if the collection is empty
	 *
	 * @return true if it is empty
	 */
	inline bool empty() const {
		return _levels.empty();
	}


	/**
	 * Get the capacity of the backing vector
	 *
	 * @return the capacity of the backing vector
	 */
	inline size_t capacity() const {
		return _levels.capacity();
	}


	/**
	 * Add a new level
	 *
	 * @param size the size
	 * @return the new level data structure
	 */
	A* new_level(size_t size) {

		assert(_master == NULL);

		A* a = new A(this, _levels.size(), size);
		_levels.push_back(a);
		return a;
	}


	/**
	 * Add an existing level
	 *
	 * @param a the level
	 */
	void push_back(A* a) {
		_levels.push_back(a);
	}


	/**
	 * Delete a level
	 *
	 * @param level the level number
	 */
	void delete_level(size_t level) {

		assert(_master == NULL);
		assert(level >= 0 && level < _levels.size());
		assert(_levels[level] != NULL);

		delete _levels[level];
		_levels[level] = NULL;
	}


	/**
	 * Delete all levels except the specified number of most recent levels
	 *
	 * @param keep the number of levels to keep
	 */
	void keep_only_recent_levels(size_t keep) {
		LL_NOT_IMPLEMENTED;
	}


	/**
	 * Get the persistence context
	 *
	 * @return the persistence context
	 */
	inline ll_persistence_context& persistence() {
		return *_persistence;
	}


private:

	/// The levels
	std::vector<A*> _levels;

	/// The persistence context
	ll_persistence_context* _persistence;

	/// The master object (if this is a read-only clone)
	ll_persistent_array_collection<A, T>* _master;
};



//==========================================================================//
// Class: ll_persistent_array_swcow                                         //
//==========================================================================//

/**
 * A persistent level consisting of chunks
 */
template <typename T>
class ll_persistent_array_swcow {

public:

	/**
	 * Create an instance of ll_persistent_array_swcow
	 *
	 * @param collection the collection of levels
	 * @param level the level number
	 * @param size the number of elements
	 */
	ll_persistent_array_swcow(
			ll_persistent_array_collection
				<ll_persistent_array_swcow<T>, T>* collection,
			int level, size_t size)
		: _persistence(collection->persistence()) {

		_collection = collection;
		_level = level;
		_size = size;

		size_t entries_per_page = 1 << LL_ENTRIES_PER_PAGE_BITS;
		_num_pages = (size + 4) / entries_per_page;
		if ((size + 4) % entries_per_page > 0) _num_pages++;

		// XXX This starts with a huge number of pages due to writable_init()
		// using the hard-coded absolute maximum number of nodes: currently
		// 80 million. This results on an alocation of a huge number of chunks
		// patg

		common_init();

		memset(&_header, 0, sizeof(_header));
	}


	/**
	 * Create an instance of ll_persistent_array_swcow from a serialized
	 * representation
	 *
	 * @param collection the collection of levels
	 * @param lm the level metadata
	 */
	ll_persistent_array_swcow(
			ll_persistent_array_collection
				<ll_persistent_array_swcow<T>, T>* collection,
			const ll_persistence_context::level_meta* lm)
		: _persistence(collection->persistence()) {

		_collection = collection;
		_level = lm->lm_level;
		_size = lm->lm_vt_size;
		_num_pages = lm->lm_vt_partitions;

		common_init();

		_level_meta = (ll_persistence_context::level_meta*)
			malloc(sizeof(*_level_meta));
		memcpy(_level_meta, lm, sizeof(*_level_meta));

		_persistence.read_level_header(_level_meta, &_header);

		if (_level > 0) {
			ll_persistent_chunk*& prev_chunks = (*_collection)[_level-1]->_chunks;
			if (prev_chunks != NULL) {
				free(prev_chunks);
				prev_chunks = NULL;
			}
		}

		_chunks = (ll_persistent_chunk*) malloc((_num_pages+1)*sizeof(*_chunks));
		memset(&_chunks[_num_pages], 0, sizeof(*_chunks));
		_persistence.read_chunk_table(_level_meta, _chunks);

		T** prev_indirection = NULL;
		if (_level > 0) {
			prev_indirection = (*_collection)[_level-1]->_indirection;
		}
		_persistence.mmap_level_ro(_level_meta, _chunks, (void**) _indirection,
				(void**) prev_indirection, (void*) _zero_page);
	}


private:

	/**
	 * Common initialization
	 */
	void common_init() {

		_level_meta = NULL;
		_edge_table_ptr = NULL;
		_chunks = NULL;

		_cow_spinlock = 0;
		_modified_chunks = 0;
		_highest_cowed_page = -1;
		_duplicate_of_prev_level = false;

		_finished_vertices = false;
		_finished_edges = false;

		_indirection = (T**) calloc(_num_pages + 1, sizeof(T*));

		_zero_page = (T*) malloc(sizeof(T) << LL_ENTRIES_PER_PAGE_BITS);
		memset(_zero_page, 0, sizeof(T) << LL_ENTRIES_PER_PAGE_BITS);
		memset(&_nil, 0, sizeof(_nil));
	}


public:

	/**
	 * Destroy the instance
	 */
	~ll_persistent_array_swcow() {

		// Debugging

		if (_finished_vertices) {
			assert(_edge_table_ptr == NULL || _finished_edges);
		}


		// Free the other data structures

		if (_level > 0) {
			T** prev_indirection = (*_collection)[_level-1]->_indirection;
			if (prev_indirection != _indirection) free(_indirection);
		}
		else {
			free(_indirection);
		}
		if (_chunks != NULL) free(_chunks);

		if (_level_meta != NULL) free(_level_meta);
		free(_zero_page);
	}


	/**
	 * Get the level number
	 *
	 * @return the level
	 */
	inline int level() const {
		return _level;
	}


	/**
	 * Return the array size
	 *
	 * @return the array size
	 */
	inline size_t size() const {
		return _size;
	}


	/**
	 * Return the number of chunks (pages)
	 *
	 * @return the number of chunks
	 */
	inline size_t chunks() const {
		return _num_pages;
	}


	/**
	 * Return the number of chunks (pages)
	 *
	 * @return the number of chunks
	 */
	inline size_t pages() const {
		return _num_pages;
	}


	/**
	 * Return the page
	 *
	 * @param index the page index
	 */
	inline T* page(size_t index) {
		return _indirection[index];
	}


	/**
	 * Return the in-memory size
	 *
	 * @return the number of bytes occupied by this instance
	 */
	size_t in_memory_size() const {
		// TODO
		return 0;
	}


	/**
	 * Set the edge table pointer
	 *
	 * @param ptrEdgeTable the edge table pointer
	 */
	void set_edge_table_ptr(LL_ET<node_t>** ptrEdgeTable) {
		assert(_edge_table_ptr == NULL);

		_edge_table_ptr = ptrEdgeTable;
		if (*_edge_table_ptr != NULL) {
			// If everything is working correctly, we should never get here
			LL_W_PRINT("The edge table in %s has been unnecessarily"
					" allocated.\n", _persistence.name());
			free(*_edge_table_ptr);
		}
		*_edge_table_ptr = (LL_ET<node_t>*)
			_persistence.mmap_large_chunk(&_header.h_et_chunk);
	}


	/**
	 * Get the number of edges, if applicable
	 *
	 * @return the number of edges
	 */
	size_t edges() const {
		return _header.h_et_size;
	}


	/**
	 * Init the vertex table as dense, which allows direct writes
	 *
	 * @param p_et the pointer to the edge table
	 * @param et_max_edges the number of edges (ET capacity)
	 */
	void dense_init(LL_ET<node_t>** p_et=NULL, size_t et_max_edges=0) {
		cow_init(p_et, et_max_edges);
	}


	/**
	 * Direct write into a dense table
	 *
	 * @param node the node
	 * @param value the value
	 */
	void dense_direct_write(node_t node, const T& value) {
		cow_write(node, value);
	}


	/**
	 * Finish writing
	 */
	void dense_finish(void) {
		cow_finish();
	}


	/**
	 * Init the vertex table as a copy of the previous level, which can be
	 * further modified using copy-on-write until finalized
	 *
	 * @param p_et the pointer to the edge table
	 * @param et_max_edges the number of edges (ET capacity)
	 */
	void cow_init(LL_ET<node_t>** p_et=NULL, size_t et_max_edges=0) {

		 assert(_edge_table_ptr == NULL);
		 assert(!_finished_vertices);
		 assert(_chunks == NULL);

		 assert(_level == 0 || (*_collection)[_level-1]->_level_meta != NULL);

		_edge_table_ptr = p_et;
		_header.h_et_size = et_max_edges;
		
		_persistence.ensure_file_for_level(_level);
		size_t fi = _persistence.ml_file_index(_level);


		// Create the vertex table

		if (_level == 0) {

			// Level 0: Set everything to the zero page

			for (size_t i = 0; i < _num_pages; i++) _indirection[i]=_zero_page;
			_chunks = (ll_persistent_chunk*) calloc(_num_pages + 1,
					sizeof(*_chunks));
		}
		else {

			// Levels > 1: Copy the indirection table, move the chunk table,
			// and extend them appropriately

			size_t prev_num_pages = (*_collection)[_level-1]->_num_pages;
			ll_persistent_chunk*& prev_chunks = (*_collection)[_level-1]->_chunks;
			T** prev_indirection = (*_collection)[_level-1]->_indirection;

			memcpy(_indirection, prev_indirection, sizeof(T*) * prev_num_pages);
			for (size_t i = prev_num_pages; i < _num_pages; i++)
				_indirection[i] = _zero_page;

			assert(prev_chunks != NULL);
			_chunks = (ll_persistent_chunk*) realloc(prev_chunks,
					sizeof(*_chunks) * _num_pages);

			if (_num_pages > prev_num_pages) {
				memset(&_chunks[prev_num_pages], 0,
						sizeof(*_chunks) * (_num_pages - prev_num_pages));
			}
			prev_chunks = NULL;
		}


		// Create the edge table

		if (_edge_table_ptr != NULL) {
			_persistence.allocate_large_chunk(&_header.h_et_chunk, _level,
					_header.h_et_size * sizeof(node_t));
			if (*_edge_table_ptr != NULL) {
				// If everything is working correctly, we should never get here
				LL_W_PRINT("The edge table in %s has been unnecessarily"
						" allocated.\n", _persistence.name());
				free(*_edge_table_ptr);
			}
			*_edge_table_ptr = (LL_ET<node_t>*)
				_persistence.mmap_large_chunk(&_header.h_et_chunk);
		}


		// Preallocate the vertex table

		_persistence.preallocate_and_mmap_page_aligned_space(fi,
				_num_pages * sizeof(T) * LL_ENTRIES_PER_PAGE,
				&_cow_current_offset, &_cow_current_address,
				&_cow_current_mapping);
	}


	/**
	 * Write using copy-on-write
	 *
	 * @param node the node
	 * @param value the value
	 */
	void cow_write(node_t node, const T& value) {
		assert(node < (node_t) _num_pages * LL_ENTRIES_PER_PAGE);

		size_t wp = node >> LL_ENTRIES_PER_PAGE_BITS;
		size_t wi = node & (LL_ENTRIES_PER_PAGE - 1);

		T* page = _indirection[wp];


		// Do a COW if we do not own the given level, or it is a zero page
		
		if (_chunks[wp].pc_level != _level || page == _zero_page) {
			ll_spinlock_acquire(&_cow_spinlock);
			page = *((T** volatile) &_indirection[wp]);
			if (*((unsigned* volatile) &_chunks[wp].pc_level) != _level
					|| page == _zero_page) {

				if ((ssize_t) wp > _highest_cowed_page) _highest_cowed_page=wp;
				_modified_chunks++;


				// Allocate the chunk

				ll_persistent_chunk prev_chunk = _chunks[wp];
				size_t size = prev_chunk.pc_length;
				if (size == 0) size = sizeof(T) * LL_ENTRIES_PER_PAGE;

				//_persistence.allocate_chunk(&_chunks[wp], _level, size);
				memset(&_chunks[wp], 0, sizeof(*_chunks));
				_chunks[wp].pc_level = _level;
				_chunks[wp].pc_offset = _cow_current_offset;
				_chunks[wp].pc_length = size;
				_cow_current_offset += size;


				// Copy or zero-allocate the new page

				//_indirection[wp] = page = (T*) malloc(size);
				_indirection[wp] = page = (T*) _cow_current_address;
				_cow_current_address = ((char*) _cow_current_address) + size;
				if (prev_chunk.pc_length > 0) {
					auto prev = (*_collection)[_level-1];
					memcpy(page, prev->_indirection[wp], size);
				}
				else {
					memset(page, 0, size);
				}
			}
			ll_spinlock_release(&_cow_spinlock);
		}


		// Write

		page[wi] = value;
	}


	/**
	 * Finish writing
	 */
	void cow_finish(void) {

		assert(_level_meta == NULL);
		assert(!_finished_vertices);

		// TODO Move the switch to finish_level_edges, so that we do not
		// deal with this issue of reloading the VT back before using it for
		// the edges.
		// 
		// Or perhaps we can use fallocate() and mmap before the level
		// creation? Maybe not, because then things might not end up too
		// consecutive... or maybe yes, dunno.

		_persistence.finish_preallocated_and_mmaped_page_aligned_space(
				_cow_current_mapping,
				_modified_chunks * sizeof(T) * LL_ENTRIES_PER_PAGE);


		// If there are no modified chunks, and this is not Level 0, make this
		// level a duplicate of the previous level. Otherwise write the chunk
		// table. The new level_meta has already been written out either by
		// allocate_level(), or it will be written out by duplicate_level().

		// TODO Shrink
		// TODO What if _highest_cowed_page+1 < _num_pages?

		if (_highest_cowed_page < 0 && _level > 0) {
			_duplicate_of_prev_level = true;
			assert(_modified_chunks == 0);
			_level_meta = _persistence.duplicate_level(_level,
					(*_collection)[_level-1]->_level_meta);
		}
		else {
			assert(_level == 0 || _modified_chunks != 0);
			if (_level_meta == NULL) {
				_level_meta = _persistence.allocate_level(_level,
						sizeof(header_t), _size, _num_pages);
						//_highest_cowed_page + 1);
			}
			_persistence.write_chunk_table(_level_meta, _chunks);
		}


		// Write the level header

		// TODO Have a way to mark the level's VT and ET complete?

		_persistence.write_level_header(_level_meta, &_header);
		

		// Sync if there are no edges; otherwise sync in finish_level_edges()

		if (_edge_table_ptr == NULL) _persistence.do_auto_sync(_level);
		

		// Finish

		_finished_vertices = true;
	}


	/**
	 * Finish the edges part of the level
	 */
	void finish_level_edges(void) {

		// mmap() the new level if it was modified, otherwise refer to the
		// same indirection table as the previous level

		T** prev_indirection = NULL;
		size_t prev_num_pages = 0;

		if (_level > 0) {
			prev_indirection = (*_collection)[_level-1]->_indirection;
			prev_num_pages = (*_collection)[_level-1]->_num_pages;
		}

		if (_duplicate_of_prev_level && prev_num_pages == _num_pages) {
			free(_indirection);
			_indirection = prev_indirection;
		}


		// Sync

		// TODO mprotect the edge table if configured to do so?

		if (_edge_table_ptr != NULL) _persistence.do_auto_sync(_level);


		// Finish

		_finished_edges = true;
	}


	/**
	 * Shrink the data
	 * 
	 * @param size the new size
	 */
	void shrink(size_t size) {

		if (_size < size) return;
		_size = size;

		// TODO What do I need to do to make this work properly?

		size_t entries_per_page = 1 << LL_ENTRIES_PER_PAGE_BITS;
		_num_pages = (size + 4) / entries_per_page;
		if ((size + 4) % entries_per_page > 0) _num_pages++;

		T** p = (T**) realloc(_indirection, (_num_pages + 1) * sizeof(T*));
		if (p != _indirection) {
			// TODO Do this properly
			LL_E_PRINT("realloc moved");
			abort();
		}
		_indirection = p;
	}


	/**
	 * Return the value associated with the given vertex
	 * 
	 * @param node the node id
	 * @return the associated value
	 */
	inline const T& operator[] (node_t node) const {
		assert(_indirection[node >> LL_ENTRIES_PER_PAGE_BITS] != NULL);
		return _indirection[node >> LL_ENTRIES_PER_PAGE_BITS]
			[node & (LL_ENTRIES_PER_PAGE - 1)];
	}


	/**
	 * Begin an iterator for nodes contained in this level of the vertex table
	 *
	 * @param iter the iterator variable
	 * @param start the start node ID
	 * @param end the last node ID (exclusive)
	 */
	void modified_node_iter_begin(ll_vertex_iterator& iter,
			node_t start = 0, node_t end = (node_t) -1) {

		memset(&iter, 0, sizeof(iter));
		iter.vi_next_node = start;
		iter.vi_end = end == -1 ? _size : std::min<node_t>(end, _size);
		
		if (_level > 0) {
			auto* prev = (*_collection)[_level-1];
			size_t page = iter.vi_next_node >> LL_ENTRIES_PER_PAGE_BITS;
			T* d = _indirection[page];

			if (page < prev->_num_pages) {
				T* p = prev->_indirection[page];

				if (d == p) {
					modified_node_iter_next(iter);
				}
				else {
					const T& dp = (*this)[iter.vi_next_node];
					const T& pp = (*prev)[iter.vi_next_node];
					if (dp == pp) modified_node_iter_next(iter);
				}
			}
		}
	}


	/**
	 * Get the next (maybe) modified node
	 *
	 * @param iter the iterator variable
	 * @return the next node, or NIL_NODE if not available
	 */
	node_t modified_node_iter_next(ll_vertex_iterator& iter) {

		node_t r = iter.vi_next_node++;
		if (r >= iter.vi_end) return LL_NIL_NODE;

		iter.vi_value = &(*this)[r];
			
		if (_level > 0) {
			auto* prev = (*_collection)[_level-1];

			while (iter.vi_next_node < (node_t) _size
					&& iter.vi_next_node < iter.vi_end) {
				size_t page = iter.vi_next_node >> LL_ENTRIES_PER_PAGE_BITS;
				assert(page < _num_pages);
				T* dc = _indirection[page];


				// If there is a corresponding page in prev

				if (dc != _zero_page && page < prev->_num_pages) {
					T* pc = prev->_indirection[page];
					
					// If the page in prev is identical

					if (dc == pc) {
						iter.vi_next_node = (iter.vi_next_node
								+ LL_ENTRIES_PER_PAGE)
							& ~(LL_ENTRIES_PER_PAGE - 1);
						continue;
					}
					else {

						// The page in prev is different

						const T* dp = &(*this)[iter.vi_next_node];
						const T* pp = &(*prev)[iter.vi_next_node];

						size_t i = iter.vi_next_node
							& (LL_ENTRIES_PER_PAGE - 1);

						while (i < LL_ENTRIES_PER_PAGE) {
							if (*dp != *pp) return r;

							iter.vi_next_node++;
							i++;
							dp++;
							pp++;
						}
					}
				}
				else {

					// This page is not in prev at all

					break;
				}
			}
		}

		return r;
	}


	/**
	 * Begin an iterator for nodes contained in this level of the vertex table
	 * and get the first node
	 *
	 * @param iter the iterator variable
	 * @param start the start node ID
	 * @param end the last node ID (exclusive)
	 * @return the next node, or NIL_NODE if not available
	 */
	inline node_t modified_node_iter_begin_next(ll_vertex_iterator& iter,
			node_t start = 0, node_t end = (node_t) -1) {
		modified_node_iter_begin(iter, start, end);
		return modified_node_iter_next(iter);
	}


	/**
	 * Delete a level
	 *
	 * @param level the level number
	 */
	void delete_level(size_t level) {
		LL_NOT_IMPLEMENTED;
	}


	/**
	 * Delete all levels except the specified number of most recent levels
	 *
	 * @param keep the number of levels to keep
	 */
	void keep_only_recent_levels(size_t keep) {
		LL_NOT_IMPLEMENTED;
	}


private:

	/// The header
	typedef struct {
		ll_large_persistent_chunk h_et_chunk;
		size_t h_et_size;
	} header_t;

	/// The collection
	ll_persistent_array_collection<ll_persistent_array_swcow<T>, T>*
		_collection;

	/// The persistence context
	ll_persistence_context& _persistence;

	/// The level metadata
	ll_persistence_context::level_meta* _level_meta;

	/// The level number
	size_t _level;

	/// The number of elements
	size_t _size;

	/// The number of pages
	size_t _num_pages;

	/// COW spinlock
	ll_spinlock_t _cow_spinlock;

	/// The highest COWed page number
	ssize_t _highest_cowed_page;

	/// The number of modified chunks
	size_t _modified_chunks;

	/// Is this a duplicate of the previous level?
	bool _duplicate_of_prev_level;

	/// The edge table pointer
	LL_ET<node_t>** _edge_table_ptr;

	/// The header
	header_t _header;

	/// The indirection table
	T** _indirection;

	/// The corresponding chunks
	ll_persistent_chunk* _chunks;

	/// The NIL element
	T _nil;

	/// The zero page
	T* _zero_page;

	/// Are the vertices finished?
	bool _finished_vertices;

	/// Are the edges finished?
	bool _finished_edges;
	
	/// The chunk offset for COW
	size_t _cow_current_offset;

	/// The mmaped region address for COW
	void* _cow_current_address;

	/// The current mapping index for COW
	size_t _cow_current_mapping;
};


#endif
