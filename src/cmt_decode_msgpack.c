/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  CMetrics
 *  ========
 *  Copyright 2021 Eduardo Silva <eduardo@calyptia.com>
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
#include <cmetrics/cmt_metric.h>
#include <cmetrics/cmt_map.h>
#include <cmetrics/cmt_sds.h>
#include <cmetrics/cmt_counter.h>
#include <cmetrics/cmt_gauge.h>

#include <mpack/mpack.h>

#define CMT_DECODE_MSGPACK_SUCCESS                    0
#define CMT_DECODE_MSGPACK_ALLOCATION_ERROR           1
#define CMT_DECODE_MSGPACK_CONSUME_ERROR              2
#define CMT_DECODE_MSGPACK_ENGINE_ERROR               3
#define CMT_DECODE_MSGPACK_UNEXPECTED_KEY_ERROR       4
#define CMT_DECODE_MSGPACK_UNEXPECTED_DATA_TYPE_ERROR 5

static int consume_double_tag(mpack_reader_t *reader, 
                              double *output_buffer)
{
    int         result;
    mpack_tag_t tag;

    tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader)) {
        return CMT_DECODE_MSGPACK_ENGINE_ERROR;
    }

    if (mpack_type_double != mpack_tag_type(&tag)) {
        printf("DATA TYPE : %d\n", mpack_tag_type(&tag));
        return CMT_DECODE_MSGPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    *output_buffer = mpack_tag_double_value(&tag);

    return 0;
}

static int consume_uint_tag(mpack_reader_t *reader, 
                            uint64_t *output_buffer)
{
    int         result;
    mpack_tag_t tag;

    tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader)) {
        return CMT_DECODE_MSGPACK_ENGINE_ERROR;
    }

    if (mpack_type_uint != mpack_tag_type(&tag)) {
        printf("DATA TYPE : %d\n", mpack_tag_type(&tag));
        return CMT_DECODE_MSGPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    *output_buffer = mpack_tag_uint_value(&tag);

    return 0;
}

static int consume_string_tag(mpack_reader_t *reader, 
                              cmt_sds_t *output_buffer)
{
    uint32_t    string_length;
    int         result;
    mpack_tag_t tag;

    tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader)) {
        return CMT_DECODE_MSGPACK_ENGINE_ERROR;
    }

    if (mpack_type_str != mpack_tag_type(&tag)) {
        return CMT_DECODE_MSGPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    string_length = mpack_tag_str_length(&tag);

    *output_buffer = cmt_sds_create_size(string_length + 1);

    if (NULL == *output_buffer) {
        return CMT_DECODE_MSGPACK_ALLOCATION_ERROR;
    }

    mpack_read_cstr(reader, *output_buffer, string_length + 1, string_length);

    if (mpack_ok != mpack_reader_error(reader)) {
        return CMT_DECODE_MSGPACK_ENGINE_ERROR;
    }

    mpack_done_str(reader);

    if (mpack_ok != mpack_reader_error(reader)) {
        return CMT_DECODE_MSGPACK_ENGINE_ERROR;
    }

    return 0;
}

static int unpack_opts(mpack_reader_t *reader, struct cmt_opts *opts)
{
    mpack_tag_t outer_tag;
    mpack_tag_t inner_tag;
    uint32_t    entry_index;
    uint32_t    entry_count;
    uint64_t    error_detected;
    cmt_sds_t   key_name;
    int         result;

    outer_tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader)) {
        return CMT_DECODE_MSGPACK_ENGINE_ERROR;
    }

    if (mpack_type_map != mpack_tag_type(&outer_tag)) {
        return CMT_DECODE_MSGPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    entry_count = mpack_tag_map_count(&outer_tag);

    error_detected = 0;
    for (entry_index = 0 ; 
        0 == error_detected && entry_index < entry_count ; 
        entry_index++) {
        result = consume_string_tag(reader, &key_name);

        if (0 != result) {
            return -3;
        }

        if (0 == strcmp(key_name, "ns")) {
            result = consume_string_tag(reader, &opts->namespace);

            if (0 != result) {
                error_detected = 1;
            }
        }
        else if (0 == strcmp(key_name, "ss")) {
            result = consume_string_tag(reader, &opts->subsystem);

            if (0 != result) {
                error_detected = 1;
            }
        }
        else if (0 == strcmp(key_name, "name")) {
            result = consume_string_tag(reader, &opts->name);

            if (0 != result) {
                error_detected = 1;
            }

        }
        else if (0 == strcmp(key_name, "desc")) {
            result = consume_string_tag(reader, &opts->description);

            if (0 != result) {
                error_detected = 1;
            }
        }
        else if (0 == strcmp(key_name, "fqname")) {
            result = consume_string_tag(reader, &opts->fqname);

            if (0 != result) {
                error_detected = 1;
            }
        }

        cmt_sds_destroy(key_name);
    }

    if (0 != error_detected) {
        return -4;
    }

    mpack_done_map(reader);

    if (mpack_ok != mpack_reader_error(reader))
    {
        return -5;
    }

    return 0;
}


static int unpack_labels(mpack_reader_t *reader, struct mk_list *label_keys)
{
    mpack_tag_t outer_tag;
    uint32_t entry_index;
    uint32_t entry_count;
    uint64_t error_detected;
    cmt_sds_t entry_value;
    struct cmt_map_label *label;
    int result;

    outer_tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader))
    {
        return -1;
    }

    if (mpack_type_array != mpack_tag_type(&outer_tag)) {
        return CMT_DECODE_MSGPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    entry_count = mpack_tag_array_count(&outer_tag);

    error_detected = 0;
    for (entry_index = 0 ; 
        0 == error_detected && entry_index < entry_count ; 
        entry_index++) {
        result = consume_string_tag(reader, &entry_value);

        if (0 != result) {
            return -3;
        }

        label = malloc(sizeof(struct cmt_map_label));

        if (NULL == label) {
            error_detected = 1;
        }
        else
        {
            label->name = entry_value;
            mk_list_add(&label->_head, label_keys);
        }
    }

    if (0 != error_detected) {
        return -4;
    }

    mpack_done_array(reader);

    if (mpack_ok != mpack_reader_error(reader))
    {
        return -5;
    }

    return 0;
}

/* This function leaks A TON */
static int unpack_metric(mpack_reader_t *reader, struct cmt_metric **out_metric)
{

    mpack_tag_t outer_tag;
    mpack_tag_t inner_tag;
    uint32_t entry_index;
    uint32_t entry_count;
    uint64_t uint_value;
    uint64_t error_detected;
    double   double_value;
    cmt_sds_t key_name;
    int result;
    struct cmt_metric *metric;

    if (NULL == *out_metric) {
        metric = malloc(sizeof(struct cmt_metric));

        if (NULL == metric) {
            return -999;
        }

        *out_metric = metric;
    }
    else
    {
        metric = *out_metric;
    }

    mk_list_init(&metric->labels);

    outer_tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader))
    {
        return -1;
    }

    if (mpack_type_map != mpack_tag_type(&outer_tag)) {
        return CMT_DECODE_MSGPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    entry_count = mpack_tag_map_count(&outer_tag);

    error_detected = 0;
    for (entry_index = 0 ; 
        0 == error_detected && entry_index < entry_count ; 
        entry_index++) {
        result = consume_string_tag(reader, &key_name);

        if (0 != result) {
            return -3;
        }

        if (0 == strcmp(key_name, "ts")) {
            result = consume_uint_tag(reader, &metric->timestamp);

            if (0 != result) {
                error_detected = 111;
            }
        }
        else if (0 == strcmp(key_name, "value")) {
            result = consume_double_tag(reader, &double_value);

            if (0 != result) {
                error_detected = 2222;
            }
            else {
                metric->val = cmt_math_d64_to_uint64(double_value);
            }
        }
        else if (0 == strcmp(key_name, "labels")) {
            result = unpack_labels(reader, &metric->labels);

            if (0 != result) {
                error_detected = 333;
            }
        }

        cmt_sds_destroy(key_name);
    }

    if (0 != error_detected) {
        return error_detected;
    }

    mpack_done_map(reader);

    if (mpack_ok != mpack_reader_error(reader))
    {
        return -5;
    }

    return 0;
}

static int unpack_values(mpack_reader_t *reader, struct cmt_map *map)
{

    mpack_tag_t outer_tag;
    uint32_t entry_index;
    uint32_t entry_count;
    uint64_t error_detected;
    cmt_sds_t entry_value;
    struct cmt_map_label *label;
    struct cmt_metric *metric;
    int result;

    outer_tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader))
    {
        return -1;
    }

    if (mpack_type_array != mpack_tag_type(&outer_tag)) {
        return CMT_DECODE_MSGPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    entry_count = mpack_tag_array_count(&outer_tag);

    error_detected = 0;
    for (entry_index = 0 ; 
        0 == error_detected && entry_index < entry_count ; 
        entry_index++) {

        if (0 == entry_index && 1 == map->metric_static_set) {
            metric = &map->metric;
        }
        else {
            metric = NULL;
        }

        result = unpack_metric(reader, &metric);

        if (0 != result) {
            return -3;
        }

        if(0 != entry_index || 0 == map->metric_static_set)
        {
            mk_list_add(&metric->_head, &map->metrics);
        }
    }

    if (0 != error_detected) {
        return -4;
    }

    mpack_done_array(reader);

    if (mpack_ok != mpack_reader_error(reader))
    {
        return -5;
    }

    return 0;
}


static int unpack_header(mpack_reader_t *reader, mpack_tag_t tag, struct cmt_map *map)
{
    uint64_t error_detected;
    uint64_t uint_value;
    cmt_sds_t key_name;
    int result;

    result = consume_string_tag(reader, &key_name);

    if (0 != result) {
        return CMT_DECODE_MSGPACK_CONSUME_ERROR;
    }

    result = strcmp(key_name, "type");

    if (0 != result) {
        return -1;
    }

    result = consume_uint_tag(reader, &uint_value);

    if (0 != result) {
        return -6;
    }

    map->type = uint_value;

    cmt_sds_destroy(key_name);

    result = consume_string_tag(reader, &key_name);

    if (0 != result) {
        return -1;
    }

    result = strcmp(key_name, "metric_static_set");

    if (0 != result) {
        return -7;
    }

    result = consume_uint_tag(reader, &uint_value);

    if (0 != result) {
        return -8;
    }

    map->metric_static_set = uint_value;

    cmt_sds_destroy(key_name);

    result = consume_string_tag(reader, &key_name);

    if (0 != result) {
        return -1;
    }

    result = strcmp(key_name, "opts");

    if (0 != result) {
        return -9;
    }

    result = unpack_opts(reader, map->opts);

    if (0 != result) {
        return -10;
    }

    cmt_sds_destroy(key_name);

    result = consume_string_tag(reader, &key_name);

    if (0 != result) {
        return -1;
    }

    result = strcmp(key_name, "labels");

    if (0 != result) {
        return -11;
    }

    result = unpack_labels(reader, &map->label_keys);

    if (0 != result) {
        return -12;
    }

    map->label_count = mk_list_size(&map->label_keys);

    return 0;
}


static int unpack_basic_type(mpack_reader_t *reader, struct cmt_map **map)
{
    cmt_sds_t key_name;
    mpack_tag_t tag;
    int result;

    *map = cmt_map_create(0, NULL, 0, NULL);

    (*map)->opts = malloc(sizeof(struct cmt_opts));

    if (NULL == (*map)->opts) {
        return CMT_DECODE_MSGPACK_ALLOCATION_ERROR;
    }

    tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader))
    {
        return CMT_DECODE_MSGPACK_ENGINE_ERROR;
    }

    if (mpack_type_map != mpack_tag_type(&tag)) {
        return CMT_DECODE_MSGPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    result = consume_string_tag(reader, &key_name);

    if (0 != result) {
        return CMT_DECODE_MSGPACK_CONSUME_ERROR;
    }

    result = strcmp(key_name, "header");

    if (0 != result) {
        return CMT_DECODE_MSGPACK_UNEXPECTED_KEY_ERROR;
    }

    tag = mpack_read_tag(reader);

    result = unpack_header(reader, tag, *map);

    if (0 != result) {
        return -4;
    }

    result = consume_string_tag(reader, &key_name);

    if (0 != result) {
        return CMT_DECODE_MSGPACK_CONSUME_ERROR;
    }

    result = strcmp(key_name, "values");

    if (0 != result) {
        return CMT_DECODE_MSGPACK_UNEXPECTED_KEY_ERROR;
    }

    result = unpack_values(reader, *map);

    if (0 != result) {
        return -3;
    }

    return 0;
}

static int append_unpacked_counter_to_metrics_context(struct cmt *context, 
                                                      struct cmt_map *map)
{
    struct cmt_counter *counter;

    counter = malloc(sizeof(struct cmt_counter));    

    if (NULL == counter) {
        return CMT_DECODE_MSGPACK_ALLOCATION_ERROR;
    }

    counter->map = map;

    memcpy(&counter->opts, map->opts, sizeof(struct cmt_opts));

    free(map->opts);

    map->opts = &counter->opts;

    mk_list_add(&counter->_head, &context->counters);

    return CMT_DECODE_MSGPACK_SUCCESS;
}

static int append_unpacked_gauge_to_metrics_context(struct cmt *context, 
                                                    struct cmt_map *map)
{
    struct cmt_gauge *gauge;

    gauge = malloc(sizeof(struct cmt_gauge));    

    if (NULL == gauge) {
        return CMT_DECODE_MSGPACK_ALLOCATION_ERROR;
    }

    gauge->map = map;

    memcpy(&gauge->opts, map->opts, sizeof(struct cmt_opts));

    free(map->opts);

    map->opts = &gauge->opts;

    mk_list_add(&gauge->_head, &context->counters);

    return CMT_DECODE_MSGPACK_SUCCESS;
}

/* Convert cmetrics msgpack payload and generate a CMetrics context */
int cmt_decode_msgpack(struct cmt **out_cmt, void *in_buf, size_t in_size)
{
    struct cmt     *cmt;
    struct cmt_map *map;
    mpack_reader_t  reader;
    int             result;

    cmt = cmt_create();

    if (NULL == cmt) {
        return CMT_DECODE_MSGPACK_ALLOCATION_ERROR;
    }

    mpack_reader_init_data(&reader, in_buf, in_size);

    while (CMT_DECODE_MSGPACK_SUCCESS == result) {
        result = unpack_basic_type(&reader, &map);

        if (CMT_DECODE_MSGPACK_SUCCESS == result) {
            if (CMT_COUNTER == map->type) {
                result = append_unpacked_counter_to_metrics_context(cmt, map);
            }
            else if (CMT_GAUGE == map->type) {
                result = append_unpacked_gauge_to_metrics_context(cmt, map);
            }
            else if (CMT_HISTOGRAM == map->type) {
                // result = append_unpacked_histogram_to_metrics_context(cmt, map);
            }
        }
    }

    if (CMT_DECODE_MSGPACK_SUCCESS != result) {

    }

    *out_cmt = cmt;

    return 0;
}
