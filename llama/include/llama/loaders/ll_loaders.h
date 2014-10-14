/*
 * ll_loaders.h
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


#ifndef LL_LOADERS_H_
#define LL_LOADERS_H_

#include <vector>

#include "llama/loaders/ll_load_fgf.h"
#include "llama/loaders/ll_load_net.h"
#include "llama/loaders/ll_load_xstream1.h"

#include "llama/loaders/ll_gen_erdosrenyi.h"
#include "llama/loaders/ll_gen_rmat.h"


/**
 * A collection of loaders
 */
class ll_file_loaders {

	std::vector<ll_file_loader*> _loaders;


public:

	/**
	 * Create an instace of ll_file_loaders
	 */
	ll_file_loaders() {

		_loaders.push_back(new ll_loader_fgf());
		_loaders.push_back(new ll_loader_net());
		_loaders.push_back(new ll_loader_xs1());

		_loaders.push_back(new ll_generator_erdos_renyi());
		_loaders.push_back(new ll_generator_rmat());
	}


	/**
	 * Destroy the loaders
	 */
	virtual ~ll_file_loaders() {

		for (size_t i = 0; i < _loaders.size(); i++) {
			delete _loaders[i];
		}
	}


	/**
	 * Get loaders for the given file
	 *
	 * @param file_name the file name
	 * @return a collection of loaders
	 */
	std::vector<ll_file_loader*> loaders_for(const char* file_name) {

		std::vector<ll_file_loader*> v;

		for (size_t i = 0; i < _loaders.size(); i++) {
			if (_loaders[i]->accepts(file_name)) v.push_back(_loaders[i]);
		}

		return v;
	}


	/**
	 * Get a loader for the given file. If multiple loaders apply, return just
	 * one of them.
	 *
	 * @param file_name the file name
	 * @return the loader, or NULL if none found
	 */
	ll_file_loader* loader_for(const char* file_name) {

		for (size_t i = 0; i < _loaders.size(); i++) {
			if (_loaders[i]->accepts(file_name)) return _loaders[i];
		}

		return NULL;
	}

};

#endif

