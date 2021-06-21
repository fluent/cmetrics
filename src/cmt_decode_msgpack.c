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
#include <cmetrics/cmt_decode_msgpack.h>

#include <mpack/mpack.h>

static void destroy_label_list(struct mk_list *label_list)
{
    struct mk_list *tmp;
    struct mk_list *head;
    struct cmt_map_label *label;
    
    mk_list_foreach_safe(head, tmp, label_list) {
        label = mk_list_entry(head, struct cmt_map_label, _head);
        cmt_sds_destroy(label->name);
        mk_list_del(&label->_head);
        free(label);
    }
}

static int unpack_opts_ns(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_opts *opts;

    opts = (struct cmt_opts *) context;

    return cmt_mpack_consume_string_tag(reader, &opts->namespace);
}

static int unpack_opts_ss(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_opts *opts;
    
    opts = (struct cmt_opts *) context;

    return cmt_mpack_consume_string_tag(reader, &opts->subsystem);
}

static int unpack_opts_name(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_opts *opts;
    
    opts = (struct cmt_opts *) context;

    return cmt_mpack_consume_string_tag(reader, &opts->name);
}

static int unpack_opts_desc(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_opts *opts;
    
    opts = (struct cmt_opts *) context;

    return cmt_mpack_consume_string_tag(reader, &opts->description);
}

static int unpack_opts_fqname(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_opts *opts;
    
    opts = (struct cmt_opts *) context;

    return cmt_mpack_consume_string_tag(reader, &opts->fqname);
}

static int unpack_opts(mpack_reader_t *reader, struct cmt_opts *opts)
{
    struct cmt_mpack_map_entry_callback_t callbacks[] = {
                                                            {"ns",     unpack_opts_ns},
                                                            {"ss",     unpack_opts_ss},
                                                            {"name",   unpack_opts_name},
                                                            {"desc",   unpack_opts_desc},
                                                            {"fqname", unpack_opts_fqname},
                                                            {NULL,     NULL}
                                                        };

    return cmt_mpack_unpack_map(reader, callbacks, (void *) opts);
}

static int unpack_label(mpack_reader_t *reader, size_t index, void *context)
{
    int                   result;
    struct cmt_map_label *new_label;
    struct mk_list       *label_list;
    cmt_sds_t             label_name;

    label_list = (struct mk_list *) context;

    result = cmt_mpack_consume_string_tag(reader, &label_name);

    if (CMT_DECODE_MSGPACK_SUCCESS != result) {
        cmt_sds_destroy(label_name);

        return result;
    }

    new_label = malloc(sizeof(struct cmt_map_label));

    if (NULL == new_label) {
        cmt_sds_destroy(label_name);

        return CMT_DECODE_MSGPACK_ALLOCATION_ERROR;
    }
    else
    {
        new_label->name = label_name;

        mk_list_add(&new_label->_head, label_list);
    }

    return CMT_DECODE_MSGPACK_SUCCESS;
}

static int unpack_metric_ts(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_metric *metric;
    
    metric = (struct cmt_metric *) context;

    return cmt_mpack_consume_uint_tag(reader, &metric->timestamp);
}

static int unpack_metric_value(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_metric *metric;
    int                result;    
    double             value;

    metric = (struct cmt_metric *) context;

    result = cmt_mpack_consume_double_tag(reader, &value);

    if(CMT_DECODE_MSGPACK_SUCCESS == result) {
        metric->val = cmt_math_d64_to_uint64(value);
    }

    return result;
}

static int unpack_metric_labels(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_metric *metric;
    
    metric = (struct cmt_metric *) context;

    return cmt_mpack_unpack_array(reader, unpack_label, (void *) &metric->labels);
}

static int unpack_metric(mpack_reader_t *reader, struct cmt_metric **out_metric)
{
    int                                   result;
    struct cmt_metric                    *metric;
    uint8_t                               metric_allocation_flag;
    struct cmt_mpack_map_entry_callback_t callbacks[] =   {
                                                        {"ts",     unpack_metric_ts},
                                                        {"value",  unpack_metric_value},
                                                        {"labels", unpack_metric_labels},
                                                        {NULL,     NULL}
                                                    };


    if (NULL == *out_metric) {
        metric = malloc(sizeof(struct cmt_metric));

        if (NULL == metric) {
            return CMT_DECODE_MSGPACK_ALLOCATION_ERROR;
        }

        *out_metric = metric;
        metric_allocation_flag = 1;
    }
    else
    {
        metric = *out_metric;
        metric_allocation_flag = 0;
    }

    memset(metric, 0, sizeof(struct cmt_metric));

    mk_list_init(&metric->labels);

    result = cmt_mpack_unpack_map(reader, callbacks, (void *) metric);

    if (CMT_DECODE_MSGPACK_SUCCESS != result) {
        destroy_label_list(&metric->labels);

        if (1 == metric_allocation_flag) {
            free(metric);

            *out_metric = NULL;
        }
    }

    return result;
}

static int unpack_metric_array_entry(mpack_reader_t *reader, size_t index, void *context)
{
    size_t             preexisting_entry_count;
    int                result;
    struct cmt_metric *metric;
    struct cmt_map    *map;
    
    map = (struct cmt_map *) context;

    if (0 == index && 1 == map->metric_static_set) {
        metric = &map->metric;
    }
    else {
        metric = NULL;
    }

    result = unpack_metric(reader, &metric);

    if (CMT_DECODE_MSGPACK_SUCCESS == result) {
        if(0 != index || 0 == map->metric_static_set)
        {
            mk_list_add(&metric->_head, &map->metrics);
        }
    }

    return result;
}

static int unpack_header_type(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_map *map;
    uint64_t        value;
    int             result;
    
    map = (struct cmt_map *) context;

    result = cmt_mpack_consume_uint_tag(reader, &value);

    if (CMT_DECODE_MSGPACK_SUCCESS == result) {
        map->type = value;
    }

    return result;
}

static int unpack_header_metric_static_set(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_map *map;
    uint64_t        value;
    int             result;
    
    map = (struct cmt_map *) context;

    result = cmt_mpack_consume_uint_tag(reader, &value);

    if (CMT_DECODE_MSGPACK_SUCCESS == result) {
        map->metric_static_set = value;
    }

    return result;
}

static int unpack_header_opts(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_map *map;
    
    map = (struct cmt_map *) context;

    return unpack_opts(reader, map->opts);
}

static int unpack_header_labels(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_map *map;
    
    map = (struct cmt_map *) context;

    return cmt_mpack_unpack_array(reader, unpack_label, (void *) &map->label_keys);
}

static int unpack_basic_type_header(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_map             *map;
    int                         result;
    struct cmt_mpack_map_entry_callback_t callbacks[] =   {
                                                    {"type",              unpack_header_type},
                                                    {"metric_static_set", unpack_header_metric_static_set},
                                                    {"opts",              unpack_header_opts},
                                                    {"labels",            unpack_header_labels},
                                                    {NULL,                NULL}
                                                };

    map = (struct cmt_map *) context;

    result = cmt_mpack_unpack_map(reader, callbacks, (void *) map);

    if (CMT_DECODE_MSGPACK_SUCCESS == result) {
        map->label_count = mk_list_size(&map->label_keys);
    }

    return result;
}

static int unpack_basic_type_values(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_map *map;

    map = (struct cmt_map *) context;

    return cmt_mpack_unpack_array(reader, unpack_metric_array_entry, (void *) map);
}

static int unpack_basic_type(mpack_reader_t *reader, struct cmt_map **map)
{
    int                                   result;
    struct cmt_mpack_map_entry_callback_t callbacks[] =   {
                                                    {"header", unpack_basic_type_header},
                                                    {"values", unpack_basic_type_values},
                                                    {NULL,     NULL}
                                                };

    *map = cmt_map_create(0, NULL, 0, NULL);

    if (NULL == *map) {
        return CMT_DECODE_MSGPACK_ALLOCATION_ERROR;
    }

    (*map)->opts = malloc(sizeof(struct cmt_opts));

    if (NULL == (*map)->opts) {
        cmt_map_destroy(*map);

        return CMT_DECODE_MSGPACK_ALLOCATION_ERROR;
    }

    result = cmt_mpack_unpack_map(reader, callbacks, (void *) *map);

    if (CMT_DECODE_MSGPACK_SUCCESS != result) {
        cmt_map_destroy(*map);
        free((*map)->opts);

        *map = NULL;
    }

    return result;
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
        cmt_destroy(cmt);
    }
    else {
        *out_cmt = cmt;
    }

    return result;
}
