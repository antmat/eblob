#pragma once

#include <boost/filesystem.hpp>

#include <library/blob.h>

#include "library/crypto/sha512.h"

#include <eblob/eblob.hpp>

#include <memory>
#include <vector>
#include <string>


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


class config_wrapper {
public:
	config_wrapper() {
		data_dir_ = mkdtemp(&data_dir_template_.front());
		data_path_ = data_dir_ + "/data";
		log_path_ = data_dir_ + "/log";
		logger_.reset(new ioremap::eblob::eblob_logger(log_path_.c_str(), EBLOB_LOG_DEBUG));

		config_.blob_flags = EBLOB_L2HASH | EBLOB_DISABLE_THREADS | EBLOB_AUTO_INDEXSORT;
		config_.sync = -2;
		config_.log = logger_->log();
		config_.file = static_cast<char*>(malloc(data_path_.size() + 1));
		strcpy(config_.file, data_path_.c_str());
		config_.blob_size = EBLOB_BLOB_DEFAULT_BLOB_SIZE;
		config_.records_in_blob = EBLOB_BLOB_DEFAULT_RECORDS_IN_BLOB;
		config_.defrag_percentage = EBLOB_DEFAULT_DEFRAG_PERCENTAGE;
		config_.defrag_timeout = EBLOB_DEFAULT_DEFRAG_TIMEOUT;
		config_.index_block_size = EBLOB_INDEX_DEFAULT_BLOCK_SIZE;
		config_.index_block_bloom_length = EBLOB_INDEX_DEFAULT_BLOCK_BLOOM_LENGTH;
		config_.blob_size_limit = UINT64_MAX;
		config_.defrag_time = EBLOB_DEFAULT_DEFRAG_TIME;
		config_.defrag_splay = EBLOB_DEFAULT_DEFRAG_SPLAY;
		config_.periodic_timeout = EBLOB_DEFAULT_PERIODIC_THREAD_TIMEOUT;
		config_.stat_id = 12345;
		config_.chunks_dir = static_cast<char*>(malloc(data_dir_.size() + 1));
		strcpy(config_.chunks_dir, data_dir_.c_str());
	}

	config_wrapper(config_wrapper &&rhs) {
		config_ = rhs.config_;
		rhs.config_.file = nullptr;
		rhs.config_.chunks_dir = nullptr;
		logger_ = std::move(rhs.logger_);
	};

	//config_wrapper& operator=(config_wrapper &&rhs) = default;

	~config_wrapper() {
		free(config_.file);
		free(config_.chunks_dir);
	}

	eblob_config& get() {
		return config_;
	}

	const eblob_config& get() const {
		return config_;
	}

public:
	std::string data_dir_template_ = "/tmp/eblob-test-XXXXXX";
	std::string data_dir_;
	std::string data_path_;
	std::string log_path_;
	std::unique_ptr<ioremap::eblob::eblob_logger> logger_;
	eblob_config config_;
};


class eblob_wrapper {
public:
	explicit eblob_wrapper(eblob_config &config, bool cleanup_files_ = true);

	void start(eblob_config *config = nullptr);

	void restart(eblob_config *config = nullptr);

	void stop();

	~eblob_wrapper();

	eblob_backend *get();

	const eblob_backend *get() const;

	int insert_item(item_t &item);

	int remove_item(item_t &item);

public:
	eblob_config &default_config_;

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
