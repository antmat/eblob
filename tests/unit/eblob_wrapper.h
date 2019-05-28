#pragma once

#include <boost/filesystem.hpp>

#include <library/blob.h>

#include "library/crypto/sha512.h"

#include <eblob/eblob.hpp>

#include <vector>
#include <string>


eblob_config make_default_config();


class item_t {
public:

	item_t(uint64_t key_, const eblob_key &hashed_key_, const std::vector<char>& value_);

	bool operator< (const item_t &rhs) const;

	bool checked = false;
	bool removed = false;
	uint64_t key;
	eblob_key hashed_key;

	std::vector<char> value;
};


class eblob_wrapper {
public:
	explicit eblob_wrapper(eblob_config config, bool cleanup_files_ = true);

	void start(eblob_config *config = nullptr);

	void restart(eblob_config *config = nullptr);

	void stop();

	~eblob_wrapper();

	eblob_backend *get();

	const eblob_backend *get() const;

	int insert_item(item_t &item);

	int remove_item(item_t &item);

public:
	eblob_config default_config_;

private:
	eblob_backend *backend_ = nullptr;
	bool cleanup_files = true;
};


class item_generator {
public:

	static uint64_t DEFAULT_RANDOM_SEED;

	explicit item_generator(eblob_wrapper &wrapper_, uint64_t seed_ = DEFAULT_RANDOM_SEED)
	: wrapper(wrapper_)
	, gen(std::mt19937(seed_))
	{
	}

	item_t generate_item(uint64_t key);

private:
	std::vector<char> generate_random_data(size_t datasize);

private:
	eblob_wrapper &wrapper;
	std::mt19937 gen;
	std::uniform_int_distribution<unsigned> dist;
};


eblob_key hash(std::string key);
