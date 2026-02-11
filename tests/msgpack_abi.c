/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  CMetrics
 *  ========
 *  Copyright 2026 The CMetrics Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <cmetrics/cmetrics.h>
#include <cmetrics/cmt_gauge.h>
#include <cmetrics/cmt_map.h>
#include <cmetrics/cmt_metric.h>
#include <cmetrics/cmt_encode_msgpack.h>
#include <cmetrics/cmt_decode_msgpack.h>
#include <string.h>

#include "cmt_tests.h"

static int contains_bytes(const char *buffer, size_t buffer_length,
                          const char *needle, size_t needle_length)
{
    size_t index;

    if (buffer == NULL || needle == NULL || needle_length == 0 ||
        buffer_length < needle_length) {
        return CMT_FALSE;
    }

    for (index = 0; index <= buffer_length - needle_length; index++) {
        if (memcmp(&buffer[index], needle, needle_length) == 0) {
            return CMT_TRUE;
        }
    }

    return CMT_FALSE;
}

void test_msgpack_abi_legacy_value_only_decode()
{
    int ret;
    size_t offset;
    char *payload;
    size_t payload_size;
    struct cmt *input_cmt;
    struct cmt *output_cmt;
    struct cmt_gauge *input_gauge;
    struct cmt_gauge *output_gauge;

    cmt_initialize();

    input_cmt = cmt_create();
    TEST_CHECK(input_cmt != NULL);
    if (input_cmt == NULL) {
        return;
    }

    input_gauge = cmt_gauge_create(input_cmt, "ns", "sub", "legacy_value", "legacy", 0, NULL);
    TEST_CHECK(input_gauge != NULL);
    if (input_gauge == NULL) {
        cmt_destroy(input_cmt);
        return;
    }

    cmt_gauge_set(input_gauge, 111, 42.25, 0, NULL);

    payload = NULL;
    payload_size = 0;
    ret = cmt_encode_msgpack_create(input_cmt, &payload, &payload_size);
    TEST_CHECK(ret == 0);
    if (ret != 0) {
        cmt_destroy(input_cmt);
        return;
    }

    TEST_CHECK(contains_bytes(payload, payload_size, "value_int64", 11) == CMT_FALSE);
    TEST_CHECK(contains_bytes(payload, payload_size, "value_uint64", 12) == CMT_FALSE);

    offset = 0;
    output_cmt = NULL;
    ret = cmt_decode_msgpack_create(&output_cmt, payload, payload_size, &offset);
    TEST_CHECK(ret == 0);
    TEST_CHECK(output_cmt != NULL);

    if (ret == 0 && output_cmt != NULL) {
        output_gauge = cfl_list_entry_first(&output_cmt->gauges, struct cmt_gauge, _head);
        TEST_CHECK(output_gauge != NULL);
        if (output_gauge != NULL) {
            TEST_CHECK(output_gauge->map->metric_static_set == CMT_TRUE);
            TEST_CHECK(cmt_metric_get_value_type(&output_gauge->map->metric) == CMT_METRIC_VALUE_DOUBLE);
            TEST_CHECK(cmt_metric_get_value(&output_gauge->map->metric) == 42.25);
        }
    }

    cmt_destroy(input_cmt);
    if (output_cmt != NULL) {
        cmt_decode_msgpack_destroy(output_cmt);
    }
    cmt_encode_msgpack_destroy(payload);
}

void test_msgpack_abi_typed_int64_decode()
{
    int ret;
    size_t offset;
    char *payload;
    size_t payload_size;
    struct cmt *input_cmt;
    struct cmt *output_cmt;
    struct cmt_gauge *input_gauge;
    struct cmt_gauge *output_gauge;
    int64_t expected_value;

    cmt_initialize();

    input_cmt = cmt_create();
    TEST_CHECK(input_cmt != NULL);
    if (input_cmt == NULL) {
        return;
    }

    input_gauge = cmt_gauge_create(input_cmt, "ns", "sub", "typed_int", "typed", 0, NULL);
    TEST_CHECK(input_gauge != NULL);
    if (input_gauge == NULL) {
        cmt_destroy(input_cmt);
        return;
    }

    expected_value = 9007199254740993LL;
    cmt_gauge_set(input_gauge, 123, 0, 0, NULL);
    cmt_metric_set_int64(&input_gauge->map->metric, 123, expected_value);
    input_gauge->map->metric_static_set = CMT_TRUE;

    payload = NULL;
    payload_size = 0;
    ret = cmt_encode_msgpack_create(input_cmt, &payload, &payload_size);
    TEST_CHECK(ret == 0);
    if (ret != 0) {
        cmt_destroy(input_cmt);
        return;
    }

    TEST_CHECK(contains_bytes(payload, payload_size, "value_type", 10) == CMT_TRUE);
    TEST_CHECK(contains_bytes(payload, payload_size, "value_int64", 11) == CMT_TRUE);

    offset = 0;
    output_cmt = NULL;
    ret = cmt_decode_msgpack_create(&output_cmt, payload, payload_size, &offset);
    TEST_CHECK(ret == 0);
    TEST_CHECK(output_cmt != NULL);

    if (ret == 0 && output_cmt != NULL) {
        output_gauge = cfl_list_entry_first(&output_cmt->gauges, struct cmt_gauge, _head);
        TEST_CHECK(output_gauge != NULL);
        if (output_gauge != NULL) {
            TEST_CHECK(output_gauge->map->metric_static_set == CMT_TRUE);
            TEST_CHECK(cmt_metric_get_value_type(&output_gauge->map->metric) == CMT_METRIC_VALUE_INT64);
            TEST_CHECK(cmt_metric_get_int64_value(&output_gauge->map->metric) == expected_value);
        }
    }

    cmt_destroy(input_cmt);
    if (output_cmt != NULL) {
        cmt_decode_msgpack_destroy(output_cmt);
    }
    cmt_encode_msgpack_destroy(payload);
}

TEST_LIST = {
    {"msgpack_abi_legacy_value_only_decode", test_msgpack_abi_legacy_value_only_decode},
    {"msgpack_abi_typed_int64_decode",        test_msgpack_abi_typed_int64_decode},
    {0}
};
