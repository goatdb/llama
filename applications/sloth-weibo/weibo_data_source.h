/*
 * weibo_data_source.h
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

#ifndef WEIBO_DATA_SOURCE_H_
#define WEIBO_DATA_SOURCE_H_

#include <sloth.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cctype>
#include <cwchar>
#include <string>
#include <vector>

#define WEIBO_CSV_HEADER	\
	"mid,retweeted_status_mid,uid,retweeted_uid,source,image,text,geo," \
	"created_at,deleted_last_seen,permission_denied"
#define WEIBO_CSV_NUM_FIELDS	11
#define WEIBO_CSV_FIELD_UID		2
#define WEIBO_CSV_FIELD_TEXT	6

//#define TWEET_USE_WCHAR_T


/**
 * A tweet
 */
typedef struct {

	char* t_user;

#ifdef TWEET_USE_WCHAR_T
	wchar_t* t_text;
#else
	char* t_text;
#endif

} tweet_t;


/**
 * A Weibo data source - reading saved tweets from a csv file
 */
class weibo_data_source_csv : public ll_generic_data_source<tweet_t> {

	std::vector<std::string> _file_names;
	std::vector<FILE*> _file_handles;

	size_t _file_index;
	FILE* _file;

	size_t _max_errors;
	size_t _num_errors;

	size_t _line_len;
	char* _line;
	size_t _line_number;

	size_t _w_text_len;
	wchar_t* _w_text;
	tweet_t _tweet;


public:

	/**
	 * Create an instance of class weibo_data_source_csv
	 *
	 * @param file the file name
	 */
	weibo_data_source_csv(const char* file) {

		_file_names.push_back(std::string(file));

		init();
	}


	/**
	 * Create an instance of class weibo_data_source_csv
	 *
	 * @param files the file names
	 */
	weibo_data_source_csv(const std::vector<const char*>& files) {

		for (size_t i = 0; i < files.size(); i++) {
			_file_names.push_back(std::string(files[i]));
		}

		init();
	}


	/**
	 * Create an instance of class weibo_data_source_csv
	 *
	 * @param files the file names
	 */
	weibo_data_source_csv(const std::vector<std::string>& files) {

		for (size_t i = 0; i < files.size(); i++) {
			_file_names.push_back(files[i]);
		}

		init();
	}


	/**
	 * Destroy an instance of the class
	 */
	virtual ~weibo_data_source_csv() {

		for (size_t i = 0; i < _file_handles.size(); i++) {
			if (_file_handles[i]) fclose(_file_handles[i]);
		}

		if (_line) free(_line);
		if (_w_text) free(_w_text);
	}


	/**
	 * Get the next input item. The function returns a pointer to an internal
	 * buffer that does not need (and should not be) freed by the caller.
	 * 
	 * Depending on the implementation, this might or might not be thread safe.
	 *
	 * @return the pointer to the next item, or NULL if done
	 */
	virtual const tweet_t* next_input() {

	next_input_restart:

		// Get the next line
		
		if (!next_line()) return NULL;


		// Get the fields

		char* fields[WEIBO_CSV_NUM_FIELDS];
		
		char* s = _line;
		fields[0] = s;

		for (size_t i = 1; i <= WEIBO_CSV_FIELD_TEXT; i++) {
			if ((s = strchr(s, ',')) == NULL) {
				LL_D_PRINT("Not enough fields on line %lu of %s\n",
						_line_number, _file_names[_file_index].c_str());
				if (_num_errors++ >= _max_errors) {
					LL_W_PRINT("Too many errors in %s, "
							"skipping the rest of the file\n",
							_file_names[_file_index].c_str());
					if (!next_file()) return NULL;
				}
				goto next_input_restart;
			}
			*(s++) = '\0';
			fields[i] = s;
		}

		char* text = fields[WEIBO_CSV_FIELD_TEXT];

		for (size_t i = WEIBO_CSV_NUM_FIELDS-1; i > WEIBO_CSV_FIELD_TEXT; i--){
			if ((s = strrchr(text, ',')) == NULL) {
				LL_D_PRINT("Not enough fields on line %lu of %s\n",
						_line_number, _file_names[_file_index].c_str());
				if (_num_errors++ >= _max_errors) {
					LL_W_PRINT("Too many errors in %s, "
							"skipping the rest of the file\n",
							_file_names[_file_index].c_str());
					if (!next_file()) return NULL;
				}
				goto next_input_restart;
			}
			*(s++) = '\0';
			fields[i] = s;
		}


		// Extract the required metadata

		_tweet.t_user = fields[WEIBO_CSV_FIELD_UID];


#ifdef TWEET_USE_WCHAR_T

		// Translate the text to wchar_t*

		size_t text_bytes = ((size_t) fields[WEIBO_CSV_FIELD_TEXT + 1])
			- ((size_t) fields[WEIBO_CSV_FIELD_TEXT]);

		if (_w_text_len + 1 < (size_t) text_bytes) {
			if (_w_text) free(_w_text);
			_w_text_len = text_bytes + 16;
			_w_text = (wchar_t*) malloc(_w_text_len * sizeof(wchar_t));
		}

		const char* text_p = text;
		std::mbstate_t m;
		memset(&m, 0, sizeof(m));

		size_t w = std::mbsrtowcs(_w_text, &text_p, _w_text_len - 1, &m);
		if (w == (size_t) -1) {
			LL_D_PRINT("Invalid multi-byte sequence on line %lu of %s\n",
					_line_number, _file_names[_file_index].c_str());
			if (_num_errors++ >= _max_errors) {
				LL_W_PRINT("Too many errors in %s, "
						"skipping the rest of the file\n",
						_file_names[_file_index].c_str());
				if (!next_file()) return NULL;
			}
			goto next_input_restart;
		}
		_w_text[w] = 0;

		_tweet.t_text = _w_text;

#else /* ! defined TWEET_USE_WCHAR_T */

		_tweet.t_text = fields[WEIBO_CSV_FIELD_TEXT];

#endif

		return &_tweet;
	}


private:

	/**
	 * Common initialization
	 */
	void init() {

		_file_index = 0;
		_file = NULL;

		_max_errors = 100;
		_num_errors = 0;

		_line_len = 256;
		_line = (char*) malloc(_line_len);
		_line_number = 0;

		_w_text_len = 256;
		_w_text = (wchar_t*) malloc(_w_text_len * sizeof(wchar_t));


		// Open all files first to avoid ugly surprises later

		for (size_t i = 0; i < _file_names.size(); i++) {
			FILE* f = fopen(_file_names[i].c_str(), "r");
			if (f == NULL) {
				LL_E_PRINT("Cannot open %s\n", _file_names[i].c_str());
				perror("fopen");
				abort();
			}
			_file_handles.push_back(f);
		}
	}


	/**
	 * Switch to the next file
	 *
	 * @return true if okay, false if finished
	 */
	bool next_file() {

		if (_file_index >= _file_handles.size()) return false;

		while (true) {

			// Get the next file handle

			if (_file == NULL) {

				if (_file_handles.empty()) return false;
				_file_index = 0;
				_file = _file_handles[0];
			}
			else {

				fclose(_file);
				_file_handles[_file_index] = NULL;

				_file_index++;

				if (_file_index >= _file_handles.size()) {
					_file = NULL;
					return false;
				}

				_file = _file_handles[_file_index];
			}


			// Go to the beginning just in case (we should there be already)

			rewind(_file);
			_line_number = 0;
			_num_errors = 0;


			// Check the header

			ssize_t read = getline(&_line, &_line_len, _file);
			if (read < 0) continue;
			_line_number++;

			while (read > 0 && isspace(_line[read-1])) _line[--read] = '\0';

			if (strcmp(_line, WEIBO_CSV_HEADER) != 0) {
				LL_W_PRINT("Invalid header in %s, skipping\n",
						_file_names[_file_index].c_str());
				continue;
			}

			return true;
		}
	}


	/**
	 * Read the next line to the internal _line buffer, trim it from the right,
	 * and skip empty lines.
	 *
	 * Note that this function is not thread safe.
	 *
	 * @return true if read, false if finished
	 */
	bool next_line() {

		if (_file == NULL) {
			if (!next_file()) return false;
		}

		while (true) {

			ssize_t read = getline(&_line, &_line_len, _file);
			if (read < 0) {
				if (!next_file()) return false;
				continue;
			}

			_line_number++;

			while (read > 0 && isspace(_line[read-1])) _line[--read] = '\0';
			if (_line[0] == '\0') continue;

			return true;
		}
	}
};



#endif

