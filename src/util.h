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

#ifndef UTIL_H
#define UTIL_H

#include "defines.h"
#include <stdio.h>
#include <string.h>
#include "hash.h"

#define SAFE_DELETE(p) if (p != NULL) { delete p; p = NULL; }
#define BUF_CMP(buf1, len1, buf2, len2) (len1 == len2 && memcmp(buf1, buf2, len1) == 0)

extern bool safe_toui64(const char* data, uint32_t data_len, uint64_t& out);
extern bool safe_toi64(const char* data, uint32_t data_len, int64_t& out);
extern bool safe_toui32(const char* data, uint32_t data_len, uint32_t& out);
extern bool safe_toi32(const char* data, uint32_t data_len, int32_t& out);
extern const char* get_suffix(const char* key, uint32_t length, uint32_t& suffix_size);
extern char* memfind(char* data, uint32_t length, const char* sub, uint32_t sub_len);
extern int strcasecmp(char* str1, char* str2, uint32_t length);
extern void to_lower(char* buf, uint32_t length);

template <int a = 0>
class Util {
public:
	int static strcasecmp(const char* str1, const char* str2) {
		while (true) {
			char c1 = *str1;
			char c2 = *str2;

			str1++;
			str2++;

			if (c1 == '\0') {
				if (c2 == '\0') {
					return 0;
				} else {
					return -1;
				}
			}

			if (c2 == '\0') {
				if (c1 == '\0') {
					return 0;
				} else {
					return 1;
				}
			}

			if (c1 == c2) {
				continue;
			}

			if (c1 >= 'a' && c1 <= 'z') {
				c1 -= ('a' - 'A');
			}

			if (c2 >= 'a' && c2 <= 'z') {
				c2 -= ('a' - 'A');
			}

			if (c1 == c2) {
				continue;
			} else if (c1 > c2) {
				return 1;
			} else {
				return -1;
			}
		}
		return 0;
	}
};

template <int a = 0>
class Swap {
public:
	uint16_t static swap16(uint16_t t) {
		return (t << 8) | (t >> 8);
	}
	uint32_t static swap32(uint32_t t) {
		uint32_t t1 = swap16((uint16_t)t);
		uint32_t t2 = swap16(t >> 16);
		return t2 | (t1 << 16);
	}
	uint64_t static swap64(uint64_t t) {
		uint64_t t1 = swap32((uint32_t)t);
		uint64_t t2 = swap32(t >> 32);
		return t2 | (t1 << 32);
	}
};

#ifdef ENDIAN_LITTLE
#define hton16(x) Swap<>::swap16(x)
#define ntoh16(x) Swap<>::swap16(x)

#define hton32(x) Swap<>::swap32(x)
#define ntoh32(x) Swap<>::swap32(x)

#define hton64(x) Swap<>::swap64(x)
#define ntoh64(x) Swap<>::swap64(x)
#else
#define hton16(x) (uint16_t)(x)
#define ntoh16(x) (uint16_t)(x)

#define hton32(x) (uint32_t)(x)
#define ntoh32(x) (uint32_t)(x)

#define hton64(x) (uint64_t)(x)
#define ntoh64(x) (uint64_t)(x)
#endif

#define ENCODE_UINT8(buf, x) (*(buf) = (uint8_t)(x))
#define ENCODE_UINT16(buf, x) ((*(uint16_t*)(buf)) = hton16(x))
#define ENCODE_UINT32(buf, x) ((*(uint32_t*)(buf)) = hton32(x))
#define ENCODE_UINT64(buf, x) ((*(uint64_t*)(buf)) = hton64(x))

#define DECODE_UINT8(buf) (*(buf))
#define DECODE_UINT16(buf) (ntoh16(*(uint16_t*)(buf)))
#define DECODE_UINT32(buf) (ntoh32(*(uint32_t*)(buf)))
#define DECODE_UINT64(buf) (ntoh64(*(uint64_t*)(buf)))

template<class T>
struct Simple_Data_T {
	Simple_Data_T() {}
	Simple_Data_T(const void* d, T s) {
		data = d;
		size = s;
	}
	bool equal(Simple_Data_T<T>* sd) {
		return (size == sd->size) && (memcmp(data, sd->data, size) == 0);
	}
	const void* data;
	T size;
};

typedef Simple_Data_T<uint8_t> Simple_Data8;
typedef Simple_Data_T<uint16_t> Simple_Data16;
typedef Simple_Data_T<uint32_t> Simple_Data32;
typedef Simple_Data_T<uint64_t> Simple_Data64;

class Simple_Data;

struct Const_Data {
	Const_Data() : data(NULL), size(0) {}
	Const_Data(const uint8_t* d, uint32_t s) {
		data = d;
		size = s;
	}
	inline bool equal(const Const_Data* sd) const {
		return (size == sd->size) && (memcmp(data, sd->data, size) == 0);
	}
	inline uint32_t hash_value() const {
		return hash32(data, size, 0);
	}
	const uint8_t* data;
	uint32_t size;
};

class Simple_Data {
public:
	Simple_Data() : data(NULL), size(0) {}
	Simple_Data(const void* d, uint32_t s) {
		data = NULL;
		size = 0;
		if (d != NULL && s > 0) {
			data = (uint8_t*)malloc(s + 1);
			if (data != NULL) {
				memcpy(data, d, s);
				size = s;
				data[s] = '\0';
			}
		}
	}
	~Simple_Data() {
		if (data != NULL) {
			free(data);
			data = NULL;
		}
	}
	inline void set(const uint8_t* d) {
		if (data != NULL) {
			free(data);
			data = NULL;
		}
		if (d != NULL && size > 0) {
			data = (uint8_t*)malloc(size + 1);
			if (data != NULL) {
				memcpy(data, d, size);
				data[size] = '\0';
			}
		}
	}
	inline void set(const uint8_t* d, uint32_t s) {
		clear();
		if (d != NULL) {
			data = (uint8_t*)malloc(s + 1);
			if (data != NULL) {
				memcpy(data, d, s);
				size = s;
				data[s] = '\0';
			}
		}
	}
	inline void set(const Const_Data* cd) {
		clear();
		if (cd != NULL && cd->data != NULL) {
			data = (uint8_t*)malloc(cd->size + 1);
			if (data != NULL) {
				memcpy(data, cd->data, cd->size);
				size = cd->size;
				data[size] = '\0';
			}
		}
	}
	inline void clear() {
		if (data != NULL) {
			free(data);
			data = NULL;
		}
		size = 0;
	}
	inline bool equal(const Const_Data* sd) const {
		return (size == sd->size) && (memcmp(data, sd->data, size) == 0);
	}
	inline bool equal(const Simple_Data* sd) const {
		return (size == sd->size) && (memcmp(data, sd->data, size) == 0);
	}
	inline uint32_t hash_value() const {
		return hash32(data, size, 0);
	}
	uint8_t* data;
	uint32_t size;
};

template<class T>
class ObjectIDManager {
public:
	uint32_t put_obj(T* obj) {
		uint32_t id;
		if (!objects_id_.empty()) {
			id = objects_.size();
			objects_.push_back(obj);
		} else {
			id = objects_id_.front();
			objects_id_.pop_front();
			objects_[id] = obj;
		}
		return id;
	}
	T* get_obj(uint32_t id) {
		if (id < objects_.size()) {
			return objects_[id];
		}
		return NULL;
	}
	T* pop_obj() {
		if (!objects_.empty()) {
			T* obj = objects_.back();
			objects_.pop_back();
			return obj;
		}
		return NULL;
	}
	bool remove_obj(uint32_t id) {
		uint32_t size = objects_.size();
		if (size == 0) {
			return false;
		}
		if (id + 1 == size) {
			T* obj = objects_.back();
			objects_.pop_back();
			return obj;
		} else if (id < size) {
			objects_[id] = NULL;
			objects_id_.push_back(id);
		}
		return false;
	}
	vector<T*> objects_;
	list<uint32_t> objects_id_;
};

template<uint32_t DEFAULT_SIZE, uint32_t HIGHWAT_SIZE>
class Receive_Buffer {
public:
	Receive_Buffer() {
		read_buf_size_ = DEFAULT_SIZE;
		read_data_size_ = 0;
		read_buf_ = (uint8_t*)malloc(read_buf_size_);
		read_curr_ = read_buf_;
	}
	~Receive_Buffer() {
		if (read_buf_ != NULL) {
			free(read_buf_);
			read_buf_ = NULL;
		}
	}

	inline uint8_t* get_read_buf() {
		return read_curr_ + read_data_size_;
	}

	inline uint32_t get_read_buf_size() {
		return read_buf_size_ - (read_curr_ - read_buf_) - read_data_size_;
	}

	inline void handle_processed() {
		if (read_curr_ != read_buf_) {
			if (read_data_size_ > 0) {
				memmove(read_buf_, read_curr_, read_data_size_);
			}
			read_curr_ = read_buf_;
		}
		int size = get_read_buf_size();
		if (size < DEFAULT_SIZE) {
			int new_size = read_buf_size_ * 2;
			uint8_t* new_read_buf_ = (uint8_t*)realloc(read_buf_, new_size);
			if (new_read_buf_ == NULL) {
				//        LOG_WARNING("Couldn't realloc input buffer, size=" << new_size);
				return;
			}
			size_t offset  = read_curr_ - read_buf_;
			read_buf_ = new_read_buf_;
			read_curr_ = read_buf_ + offset;
			read_buf_size_ = new_size;
		}

		if (read_buf_size_ > HIGHWAT_SIZE && read_data_size_ < DEFAULT_SIZE) {
			if (read_curr_ != read_buf_) {
				memmove(read_buf_, read_curr_, (size_t)read_data_size_);
			}

			uint8_t* newbuf = (uint8_t*)realloc((void*)read_buf_, DEFAULT_SIZE);
			if (newbuf != NULL) {
				read_buf_ = newbuf;
				read_buf_size_ = DEFAULT_SIZE;
			}
			read_curr_ = read_buf_;
		}
	}
	uint8_t* read_curr_;
	uint32_t read_data_size_;
	uint8_t* read_buf_;
	uint32_t read_buf_size_;
};

#endif // UTIL_H
