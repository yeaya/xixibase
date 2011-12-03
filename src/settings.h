/*
   Copyright [2011] [Yao Yuan(yeaya@163.com)]

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef SETTINGS__H
#define SETTINGS__H

#include "defines.h"
#include "xixibase.h"
#include "xixi_list.hpp"
#include "xixi_hash_map.hpp"

class Extension_Mime_Item : public xixi::hash_node_base<Const_Data, Extension_Mime_Item>, public xixi::list_node_base<Extension_Mime_Item> {
public:
	inline bool is_key(const Const_Data* p) const {
		return (externsion.size == p->size) && (memcmp(externsion.data, p->data, p->size) == 0);
	}

	Simple_Data externsion;
	Simple_Data mime_type;
};

class Settings {
public:
	Settings();
	~Settings();
	void init();
	string load_conf();
//	bool ext_to_mime(const string& ext, string& mime_type);
	const uint8_t* get_mime_type(const uint8_t* ext, uint32_t ext_size, uint32_t& mime_type_size);

	string home_dir;

	uint64_t maxbytes;
	uint32_t maxconns;
	uint16_t port;
	string inter;

	double factor;            // chunk size growth factor
	uint32_t pool_size;       // number of io_service to run
	uint32_t num_threads;     // number of threads to run
	uint32_t item_size_min;
	uint32_t item_size_max;

	uint32_t max_stats_group;

	uint32_t cache_expiration;
//	map<string, string> mime_map;
	xixi::list<Extension_Mime_Item> ext_mime_list;
	xixi::hash_map<Const_Data, Extension_Mime_Item> ext_mime_map;
	vector<string> welcome_file_list;
};

extern Settings settings_;

#endif // SETTINGS__H
