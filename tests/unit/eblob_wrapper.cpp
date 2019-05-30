#include "eblob_wrapper.h"

#include <boost/filesystem.hpp>

#include <library/blob.h>
#include <library/crypto/sha512.h>

#include <eblob/eblob.hpp>

#include <vector>


item_t::item_t(uint64_t key_, const eblob_key &hashed_key_, const std::vector<char> &value_)
: key(key_)
, value(value_)
{
	memcpy(hashed_key.id, hashed_key_.id, EBLOB_ID_SIZE);
}


bool item_t::operator< (const item_t &rhs) const {
	return eblob_id_cmp(hashed_key.id, rhs.hashed_key.id) < 0;
}


eblob_wrapper::eblob_wrapper(eblob_config &config, bool cleanup_files_)
	: default_config_(config)
	, cleanup_files(cleanup_files_) {
}


void eblob_wrapper::start(eblob_config *config) {
	if (config != nullptr)
		backend_ = eblob_init(config);
	else
		backend_ = eblob_init(&default_config_);
}


void eblob_wrapper::restart(eblob_config *config) {
	stop();
	start(config);
}


void eblob_wrapper::stop() {
	if (backend_) {
		eblob_cleanup(backend_);
		backend_ = nullptr;
	}
}


eblob_wrapper::~eblob_wrapper() {
	stop();
	if (cleanup_files) {
		boost::filesystem::remove_all(default_config_.chunks_dir);
	}
}


eblob_backend* eblob_wrapper::get() {
	return backend_;
}


const eblob_backend* eblob_wrapper::get() const {
	return backend_;
}


int eblob_wrapper::insert_item(item_t &item) {
	return eblob_write(get(), &item.hashed_key, item.value.data(), /* offset */ 0, item.value.size(),
			   BLOB_DISK_CTL_APPEND);
}


int eblob_wrapper::remove_item(item_t &item) {
	item.removed = true;
	return eblob_remove_hashed(get(), &item.key, sizeof(item.key));
}


item_t item_generator::generate_item(uint64_t key) {
	size_t datasize = 2 * (1 << 20);  // 2Mib
	if (dist(gen) % 10 != 0) {  // 90% of probability
		datasize = 1 + dist(gen) % (1 << 10);  // less or equal 1KiB
	}

	std::vector<char> data = generate_random_data(datasize);
	struct eblob_key hashed_key;
	eblob_hash(wrapper.get(), hashed_key.id, sizeof(hashed_key.id), &key, sizeof(key));
	return item_t(key, hashed_key, data);
}


std::vector<char> item_generator::generate_random_data(size_t datasize) {
	std::vector<char> data(datasize);
	for (auto &element : data) {
		element = dist(gen) % 26 + 'a';
	}

	return data;
}

uint64_t item_generator::DEFAULT_RANDOM_SEED = 42;

eblob_key hash(std::string key) {
	eblob_key ret;
	sha512_buffer(key.data(), key.size(), ret.id);
	return ret;
}
