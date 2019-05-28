#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE DEFRAG library test

#include <boost/test/included/unit_test.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/filesystem.hpp>

#include "eblob_wrapper.h"

#include "library/blob.h"
#include "library/datasort.h"
#include "library/list.h"

#include "eblob/eblob.hpp"

#include <vector>
#include <utility>
#include <random>
#include <iostream>
#include <functional>

#include <sysexits.h>
#include <err.h>


void initialize_eblob_config(eblob_config &config) {
	config.blob_size = 10 * (1ULL << 30); // 10Gib
	config.defrag_timeout = 0; // we don't want to autodefrag
	config.defrag_time = 0;
	config.defrag_splay = 0;
	config.blob_flags |= EBLOB_USE_VIEWS;
}


void fill_eblob(eblob_wrapper &wrapper,
		std::vector<item_t> &shadow_elems,
		item_generator &generator,
		size_t total_records) {
	for (size_t index = 0; index != total_records; ++index) {
		shadow_elems.push_back(generator.generate_item(index));
		BOOST_REQUIRE_EQUAL(wrapper.insert_item(shadow_elems.back()), 0);
	}
}


void init_datasort_cfg(eblob_wrapper &wrapper, datasort_cfg &cfg) {
	memset(&cfg, 0, sizeof(cfg));
	cfg.b = wrapper.get();
	cfg.log = cfg.b->cfg.log;
}


class iterator_private {
public:
	explicit iterator_private(std::vector<item_t> &items_)
	: items(items_)
	{
	}

	std::vector<item_t> &items;
	size_t number_checked = 0;
};


int iterate_callback(struct eblob_disk_control *dc,
		     struct eblob_ram_control *rctl __attribute_unused__,
		     int fd,
		     uint64_t data_offset,
		     void *priv,
		     void *thread_priv __attribute_unused__)
{
	// TODO: BOOST_REQUIRE_EQUAL used because of 54-th version of boost
	// Maybe it will be more comfortable with BOOST_TEST and error messages in more modern boost version.
	BOOST_REQUIRE(!(dc->flags & BLOB_DISK_CTL_REMOVE));  // removed dc occured

	iterator_private &ipriv = *static_cast<iterator_private*>(priv);
	auto &items = ipriv.items;
	BOOST_REQUIRE(ipriv.number_checked < items.size()); // index out of range

	auto &item = items[ipriv.number_checked];
	BOOST_REQUIRE(!item.removed);  // item removed
	BOOST_REQUIRE(!item.checked); //  item already checked
	BOOST_REQUIRE(dc->data_size == item.value.size());  // sizes mismatch

	std::vector<char> data(dc->data_size);
	int ret = __eblob_read_ll(fd, data.data(), dc->data_size, data_offset);
	BOOST_REQUIRE(ret == 0);  // can't read data
	BOOST_REQUIRE(data == item.value);  // contet of value differ

	item.checked = true;
	++ipriv.number_checked;
	return 0;
}


int datasort(eblob_wrapper &wrapper, const std::set<size_t> &indexes) {
	size_t number_bases = indexes.size();
	assert(!indexes.empty());
	assert(*indices.rbegin() < number_bases);

	datasort_cfg dcfg;
	memset(&dcfg, 0, sizeof(datasort_cfg));
	std::vector<eblob_base_ctl *> bctls(number_bases);
	eblob_base_ctl *bctl;
	size_t index = 0;
	size_t loop_index = 0;
	list_for_each_entry(bctl, &wrapper.get()->bases, base_entry) {
		if (indexes.count(loop_index)) {
			bctls[index] = bctl;
			++index;
		}

		++loop_index;
		if (index == number_bases)
			break;
	}

	init_datasort_cfg(wrapper, dcfg);
	dcfg.bctl = bctls.data();
	dcfg.bctl_cnt = bctls.size();
	return eblob_generate_sorted_data(&dcfg);
}


int iterate(eblob_wrapper &wrapper,
	    iterator_private &priv) {
	eblob_index_block full_range;
	memset(full_range.start_key.id, 0, EBLOB_ID_SIZE);
	memset(full_range.end_key.id, 0xff, EBLOB_ID_SIZE);

	eblob_iterate_callbacks callbacks;
	memset(&callbacks, 0, sizeof(callbacks));
	callbacks.iterator = iterate_callback;

	eblob_iterate_control ictl;
	memset(&ictl, 0, sizeof(struct eblob_iterate_control));
	ictl.b = wrapper.get();
	ictl.log = ictl.b->cfg.log;
	ictl.flags = EBLOB_ITERATE_FLAGS_ALL | EBLOB_ITERATE_FLAGS_READONLY;
	ictl.iterator_cb = callbacks;
	ictl.priv = &priv;
	ictl.range = &full_range;
	ictl.range_num = 1;
	return eblob_iterate(wrapper.get(), &ictl);
}


/**
 * 1) Make two bases with 100 records each.
 *    State: data.0(unsorted, 100 records), data.1(unsorted, 100 records)
 * 2) Defrag first base
 *    State: data.0(sorted, 100 records), data.1(unsorted, 100 records)
 * 3) Remove half of first base.
 *    State: data.0(sorted, 50 records), data.1(unsorted, 100 records)
 * 4) Defrag first base.
 *    State: data.0(sorted with view, 50 records), data.1(unsorted, 100 records)
 * 5) Check that bases contains all 150 records
 */
BOOST_AUTO_TEST_CASE(first_base_sorted_second_base_unsorted) {
	const size_t RECORDS_IN_BLOB = 100;
	const size_t TOTAL_RECORDS = 2 * RECORDS_IN_BLOB;
	const size_t RECORDS_TO_REMOVE = RECORDS_IN_BLOB / 2;

	auto config = make_default_config();
	initialize_eblob_config(config);
	config.records_in_blob = RECORDS_IN_BLOB;
	eblob_wrapper wrapper(config);
	wrapper.start();
	BOOST_REQUIRE(wrapper.get() != nullptr);

	item_generator generator(wrapper);
	std::vector<item_t> shadow_elems;

	fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS);

	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0}), 0);

	// Remove a half items from first base
	for (size_t index = 0; index != RECORDS_TO_REMOVE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[index]), 0);
	}

	shadow_elems = std::vector<item_t>(shadow_elems.begin() + RECORDS_TO_REMOVE, shadow_elems.end());
	std::sort(shadow_elems.begin(), shadow_elems.begin() + (RECORDS_IN_BLOB - RECORDS_TO_REMOVE));

	// TODO: need to check that we use view over base
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0}), 0);

	iterator_private priv(shadow_elems);
	BOOST_REQUIRE_EQUAL(iterate(wrapper, priv), 0);
	BOOST_REQUIRE_EQUAL(priv.number_checked, TOTAL_RECORDS - RECORDS_TO_REMOVE);
}


/**
 * 1) Make two bases with 100 records each.
 *    State: data.0(unsorted, 100 records), data.1(unsorted, 100 records)
 * 2) Defrag first base
 *    State: data.0(sorted, 100 records), data.1(unsorted, 100 records)
 * 3) Remove a half from each base
 *    State: data.0(sorted, 50 records), data.1(unsorted, 50 records)
 * 4) Defrag two bases with view on first base
 *    State: data.0(sorted, 100 records)
 * 5) Check that result base contains 100 records
 */
BOOST_AUTO_TEST_CASE(merge_sorted_and_unsorted_bases) {
	const size_t RECORDS_IN_BLOB = 100;
	const size_t TOTAL_RECORDS = 2 * RECORDS_IN_BLOB;
	const size_t RECORDS_TO_REMOVE_IN_BASE = RECORDS_IN_BLOB / 2;

	auto config = make_default_config();
	initialize_eblob_config(config);
	config.records_in_blob = RECORDS_IN_BLOB;
	eblob_wrapper wrapper(config);
	wrapper.start();
	BOOST_REQUIRE(wrapper.get() != nullptr);

	item_generator generator(wrapper);
	std::vector<item_t> shadow_elems;

	fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS);

	// Sort first base
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0}), 0);

	// Remove a half from first base
	for (size_t index = 0; index != RECORDS_TO_REMOVE_IN_BASE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[index]), 0);
	}

	// Remove a half from second base
	for (size_t index = 0; index != RECORDS_TO_REMOVE_IN_BASE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[RECORDS_IN_BLOB + index]), 0);
	}

	// Defrag two bases
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0, 1}), 0);
	shadow_elems.erase(shadow_elems.begin() + RECORDS_IN_BLOB, shadow_elems.begin() + RECORDS_IN_BLOB + RECORDS_TO_REMOVE_IN_BASE);
	shadow_elems.erase(shadow_elems.begin(), shadow_elems.begin() + (RECORDS_IN_BLOB - RECORDS_TO_REMOVE_IN_BASE));
	std::sort(shadow_elems.begin(), shadow_elems.end());

	iterator_private priv(shadow_elems);
	BOOST_REQUIRE_EQUAL(iterate(wrapper, priv), 0);
	BOOST_REQUIRE_EQUAL(priv.number_checked, TOTAL_RECORDS - 2 * RECORDS_TO_REMOVE_IN_BASE);
}


/**
 *  1) Make two bases with 100 records each.
 *     State: data.0(unsorted, 100 record), data.1(unsorted, 100 records)
 *  2) Defrag two bases separately
 *     State: data.0(sorted, 100 records), data.1(sorted, 100 records)
 *  3) Remove a half from each base
 *     State: data.0(sorted, 50 records), data.1(sorted, 50 records)
 *  4) Defrag two bases with view on first base
 *     State: data.0(sorted, 100 records)
 *  5) Check that result base contains 100 records
 */
BOOST_AUTO_TEST_CASE(merge_sorted_and_sorted_bases) {
	const size_t RECORDS_IN_BLOB = 100;
	const size_t TOTAL_RECORDS = 2 * RECORDS_IN_BLOB;
	const size_t RECORDS_TO_REMOVE_IN_BASE = RECORDS_IN_BLOB / 2;

	auto config = make_default_config();
	initialize_eblob_config(config);
	config.records_in_blob = RECORDS_IN_BLOB;
	eblob_wrapper wrapper(config);
	wrapper.start();

	BOOST_REQUIRE(wrapper.get() != nullptr);

	item_generator generator(wrapper);
	std::vector<item_t> shadow_elems;

	fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS);

	// Sort bases separately
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0}), 0);
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {1}), 0);

	// Remove a half from first base
	for (size_t index = 0; index != RECORDS_TO_REMOVE_IN_BASE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[index]), 0);
	}

	// Remove a half from second base
	for (size_t index = 0; index != RECORDS_TO_REMOVE_IN_BASE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[RECORDS_IN_BLOB + index]), 0);
	}

	// Defrag two bases
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0, 1}), 0);
	shadow_elems.erase(shadow_elems.begin() + RECORDS_IN_BLOB, shadow_elems.begin() + RECORDS_IN_BLOB + RECORDS_TO_REMOVE_IN_BASE);
	shadow_elems.erase(shadow_elems.begin(), shadow_elems.begin() + (RECORDS_IN_BLOB - RECORDS_TO_REMOVE_IN_BASE));
	std::sort(shadow_elems.begin(), shadow_elems.end());

	iterator_private priv(shadow_elems);
	BOOST_REQUIRE_EQUAL(iterate(wrapper, priv), 0);
	BOOST_REQUIRE_EQUAL(priv.number_checked, TOTAL_RECORDS - 2 * RECORDS_TO_REMOVE_IN_BASE);
}


/**
 *  1) Make two bases with 100 records each.
 *     State: data.0(unsorted, 100 record), data.1(unsorted, 100 records)
 *  3) Remove a half from each base
 *     State: data.0(unsorted, 50 records), data.1(unsorted, 50 records)
 *  4) Defrag two bases without view on bases
 *     State: data.0(sorted, 100 records)
 *  5) Check that result base contains 100 records
 */
BOOST_AUTO_TEST_CASE(merge_unsorted_and_unsorted_bases) {
	const size_t RECORDS_IN_BLOB = 100;
	const size_t TOTAL_RECORDS = 2 * RECORDS_IN_BLOB;
	const size_t RECORDS_TO_REMOVE_IN_BASE = RECORDS_IN_BLOB / 2;

	auto config = make_default_config();
	initialize_eblob_config(config);
	config.records_in_blob = RECORDS_IN_BLOB;
	eblob_wrapper wrapper(config);
	wrapper.start();

	BOOST_REQUIRE(wrapper.get() != nullptr);

	item_generator generator(wrapper);
	std::vector<item_t> shadow_elems;

	fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS);

	// Remove a half from first base
	for (size_t index = 0; index != RECORDS_TO_REMOVE_IN_BASE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[index]), 0);
	}

	// Remove a half from second base
	for (size_t index = 0; index != RECORDS_TO_REMOVE_IN_BASE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[RECORDS_IN_BLOB + index]), 0);
	}

	// Defrag two bases
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0, 1}), 0);
	shadow_elems.erase(shadow_elems.begin() + RECORDS_IN_BLOB, shadow_elems.begin() + RECORDS_IN_BLOB + RECORDS_TO_REMOVE_IN_BASE);
	shadow_elems.erase(shadow_elems.begin(), shadow_elems.begin() + (RECORDS_IN_BLOB - RECORDS_TO_REMOVE_IN_BASE));
	std::sort(shadow_elems.begin(), shadow_elems.end());

	iterator_private priv(shadow_elems);
	BOOST_REQUIRE_EQUAL(iterate(wrapper, priv), 0);
	BOOST_REQUIRE_EQUAL(priv.number_checked, TOTAL_RECORDS - 2 * RECORDS_TO_REMOVE_IN_BASE);
}


/**
 *  1) Make three bases with 100 records each.
 *     State: data.0(unsorted, 100 record), data.1(unsorted, 100 records), data.2(unsorted, 100 records)
 *  3) Remove elements from two bases
 *     State: data.0(unsorted, 0 records), data.1(unsorted, 0 records), data.2(unsorted, 100 records)
 *  4) Defrag all bases
 *     State: data.2(unsorted, 100 records)
 *  5) Check that result base contains 100 records
 */
BOOST_AUTO_TEST_CASE(remove_bases) {
	const size_t RECORDS_IN_BLOB = 100;
	const size_t TOTAL_RECORDS = 3 * RECORDS_IN_BLOB;
	const size_t RECORDS_TO_REMOVE = 2 * RECORDS_IN_BLOB;

	auto config = make_default_config();
	initialize_eblob_config(config);
	config.records_in_blob = RECORDS_IN_BLOB;
	eblob_wrapper wrapper(config);

	wrapper.start();
	BOOST_REQUIRE(wrapper.get() != nullptr);

	item_generator generator(wrapper);
	std::vector<item_t> shadow_elems;

	fill_eblob(wrapper, shadow_elems, generator, TOTAL_RECORDS);

	// Sort bases separately
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {0}), 0);
	BOOST_REQUIRE_EQUAL(datasort(wrapper, {1}), 0);

	// Remove all elements
	for (size_t index = 0; index != RECORDS_TO_REMOVE; ++index) {
		BOOST_REQUIRE_EQUAL(wrapper.remove_item(shadow_elems[index]), 0);
	}

	// Defrag eblob (last base should not be touched)
	BOOST_REQUIRE_EQUAL(eblob_defrag(wrapper.get()), 0);
	shadow_elems.erase(shadow_elems.begin() + RECORDS_IN_BLOB, shadow_elems.begin() + 2 * RECORDS_IN_BLOB);
	shadow_elems.erase(shadow_elems.begin(), shadow_elems.begin() + RECORDS_IN_BLOB);

	iterator_private priv(shadow_elems);
	BOOST_REQUIRE_EQUAL(iterate(wrapper, priv), 0);
	BOOST_REQUIRE_EQUAL(priv.number_checked, TOTAL_RECORDS - 2 * RECORDS_IN_BLOB);
}
