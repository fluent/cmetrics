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
#include <cmetrics/cmt_hash.h> 
#include <cmetrics/cmt_encode_prometheus_remote_write.h>
#include <protobuf-c/remote.pb-c.h>

static int metric_banner(struct cmt_prometheus_remote_write_context *context, 
                         struct cmt_map *map,
                         struct cmt_metric *metric)
{
    struct cmt_prometheus_metric_metadata *metadata_entry;
    struct cmt_opts *opts;

    opts = map->opts;

    metadata_entry = malloc(sizeof(struct cmt_prometheus_metric_metadata));

    if(NULL == metadata_entry)
    {
        return -1;
    }

    memset(metadata_entry, 0, sizeof(struct cmt_prometheus_metric_metadata));

    prometheus__metric_metadata__init(&metadata_entry->data);

    if (map->type == CMT_COUNTER) {
        metadata_entry->data.type = PROMETHEUS__METRIC_METADATA__METRIC_TYPE__COUNTER;
    }
    else if (map->type == CMT_GAUGE) {
        metadata_entry->data.type = PROMETHEUS__METRIC_METADATA__METRIC_TYPE__GAUGE;
    }

    metadata_entry->data.metric_family_name = opts->fqname;
    metadata_entry->data.help = opts->fqname;
    metadata_entry->data.unit = "unit";

    mk_list_add(&metadata_entry->_head, &context->metadata_entries);

    return 0;
}

static uint64_t calculate_label_set_hash(struct mk_list *label_values, uint64_t seed)
{
    struct cmt_map_label *label_value;
    struct mk_list *head;
    XXH64_state_t state;

    XXH64_reset(&state, 0);

    XXH64_update(&state, &seed, sizeof(uint64_t)); 

    mk_list_foreach(head, label_values) {
        label_value = mk_list_entry(head, struct cmt_map_label, _head);

        XXH64_update(&state, label_value->name, cmt_sds_len(label_value->name)); 
    }

    return XXH64_digest(&state);
}


static int format_metric(struct cmt_prometheus_time_series *time_series,
                         struct cmt_metric *metric, 
                         int add_timestamp)
{
    size_t entry_index;

    entry_index = time_series->entries_set++;

    time_series->data.samples[entry_index] = malloc(sizeof(Prometheus__Sample));

    if (NULL == time_series->data.samples[entry_index]) {
        return 1;
    }

    prometheus__sample__init(time_series->data.samples[entry_index]);

    time_series->data.samples[entry_index]->value = cmt_metric_get_value(metric);

    if (add_timestamp) {
        time_series->data.samples[entry_index]->timestamp = cmt_metric_get_timestamp(metric);
    }
    else {
        time_series->data.samples[entry_index]->timestamp = 0;
    }

    return 0;
}


static struct cmt_prometheus_time_series *set_up_timeseries_for_label_set(
        struct cmt_prometheus_remote_write_context *context, 
        struct cmt_map *map, 
        struct cmt_metric *metric)
{
    struct mk_list                    *head;
    size_t                             label_index;
    size_t                             label_count;
    struct cmt_map_label              *label_k;
    struct cmt_map_label              *label_v;
    uint64_t                           label_set_hash;
    struct cmt_metric                 *secondary_metric;
    uint64_t                           secondary_metric_label_set_hash;
    size_t                             label_set_hash_matches;
    uint8_t                            time_series_match;
    struct cmt_prometheus_time_series *time_series_entry;
    Prometheus__Label                **sample_label_list;
    Prometheus__Sample               **sample_value_list;
    struct cmt_label                  *static_label;

    label_set_hash = calculate_label_set_hash(&metric->labels, context->sequence_number);

    time_series_match = 0;

    mk_list_foreach(head, &context->time_series_entries) {
        time_series_entry = mk_list_entry(head, struct cmt_prometheus_time_series, _head);

        if (time_series_entry->label_set_hash == label_set_hash) {
            time_series_match = 1;

            break;
        }
    }

    if (0 == time_series_match) {
        /* Find out how many metric values share these label values */
        label_set_hash_matches = 1;

        mk_list_foreach(head, &map->metrics) {
            secondary_metric = mk_list_entry(head, struct cmt_metric, _head);

            if (metric != secondary_metric) {
                secondary_metric_label_set_hash = calculate_label_set_hash(
                                                        &secondary_metric->labels, 
                                                        context->sequence_number);

                if (secondary_metric_label_set_hash == label_set_hash) {
                    label_set_hash_matches++;
                }
            }
        }

        /* Allocate the memory required for the label and value lists, we need to add
         * one for the fixed __name__ label
         */
        label_count = mk_list_size(&context->cmt->static_labels->list) + 
                      mk_list_size(&metric->labels) + 
                      1;


        time_series_entry = calloc(1, sizeof(struct cmt_prometheus_time_series));

        if (NULL == time_series_entry) {
            return NULL;
        }

        memset(time_series_entry, 0, sizeof(struct cmt_prometheus_time_series));

        sample_label_list = calloc(label_count, sizeof(Prometheus__Label *));

        if (NULL == sample_label_list) {
            return NULL;
        }

        sample_value_list = calloc(label_set_hash_matches, sizeof(Prometheus__Sample *));

        if (NULL == sample_value_list) {
            return NULL;
        }

        /* Initialize the time series */
        prometheus__time_series__init(&time_series_entry->data);

        time_series_entry->data.n_labels  = label_count;
        time_series_entry->data.labels    = sample_label_list;
        time_series_entry->data.n_samples = label_set_hash_matches;
        time_series_entry->data.samples   = sample_value_list;

        time_series_entry->label_set_hash = label_set_hash;
        time_series_entry->entries_set = 0;

        /* Initialize the label list */
        label_index = 0;

        /* Add the __name__ label */

        sample_label_list[label_index] = calloc(1, sizeof(Prometheus__Label));

        if (NULL == sample_label_list[label_index]) {
            return NULL;
        }

        prometheus__label__init(sample_label_list[label_index]);

        sample_label_list[label_index]->name = cmt_sds_create("__name__");
        sample_label_list[label_index]->value = cmt_sds_create(map->opts->fqname);

        label_index++;

        /* Add the static labels */

        mk_list_foreach(head, &context->cmt->static_labels->list) {
            static_label = mk_list_entry(head, struct cmt_label, _head);

            sample_label_list[label_index] = calloc(1, sizeof(Prometheus__Label));

            if (NULL == sample_label_list[label_index]) {
                return NULL;
            }

            prometheus__label__init(sample_label_list[label_index]);

            sample_label_list[label_index]->name = static_label->key;
            sample_label_list[label_index]->value = static_label->val;

            label_index++;
        }

        /* Add the specific labels */

        if (label_count > 0) {
            label_k = mk_list_entry_first(&map->label_keys, struct cmt_map_label, _head);

            mk_list_foreach(head, &metric->labels) {
                label_v = mk_list_entry(head, struct cmt_map_label, _head);

                sample_label_list[label_index] = calloc(1, sizeof(Prometheus__Label));

                if (NULL == sample_label_list[label_index]) {
                    return NULL;
                }

                prometheus__label__init(sample_label_list[label_index]);

                sample_label_list[label_index]->name = label_k->name;
                sample_label_list[label_index]->value = label_v->name;

                label_index++;

                label_k = mk_list_entry_next(&label_k->_head, struct cmt_map_label,
                                             _head, &map->label_keys);
            }
        }

        /* Add the time series to the context so we can find it when we try to format 
         * a metric with these same labels;
         */
        mk_list_add(&time_series_entry->_head, &context->time_series_entries);
    }

    return time_series_entry;
}

static int format_metrics(struct cmt_prometheus_remote_write_context *context, struct cmt_map *map, int add_timestamp)
{
    struct cmt_prometheus_time_series *time_series;
    struct cmt_metric *metric;
    int metric_banner_printed; 
    struct mk_list *head;
    size_t entry_index;
    int result;

    context->sequence_number++;

    /* Simple metric, no labels */
    if (1 == map->metric_static_set) {
        /* Set up the time series if needed */
        time_series = set_up_timeseries_for_label_set(context, 
                                                      map, 
                                                      &map->metric);

        if (NULL == time_series) {
            return 1;
        }

        result = metric_banner(context, map, &map->metric);

        if (0 != result) {
            return 2;
        }

        result = format_metric(time_series, &map->metric, add_timestamp);

        if (0 != result) {
            return 3;
        }
    }

    entry_index = 0;
    metric_banner_printed = 0;

    mk_list_foreach(head, &map->metrics) {
        metric = mk_list_entry(head, struct cmt_metric, _head);

        if (0 == metric_banner_printed) {
            metric_banner_printed = 1;

            result = metric_banner(context, map, metric);

            if (0 != result) {
                return 4;
            }
        }

        /* Set up the time series if needed */
        time_series = set_up_timeseries_for_label_set(context, 
                                                      map, 
                                                      metric);

        if (NULL == time_series) {
            return 5;
        }

        result = format_metric(time_series, metric, add_timestamp);

        if (0 != result) {
            return 6;
        }

        entry_index++;
    }

    return 0;
}


cmt_sds_t render_remote_write_context_to_sds(struct cmt_prometheus_remote_write_context *context)
{
    struct cmt_prometheus_metric_metadata *metadata_entry;
    struct cmt_prometheus_time_series *time_series_entry;
    size_t write_request_size;
    cmt_sds_t result_buffer;
    struct mk_list *head;
    size_t entry_index;

    context->write_request.n_timeseries = mk_list_size(&context->time_series_entries);
    context->write_request.n_metadata   = mk_list_size(&context->metadata_entries);

    context->write_request.timeseries = calloc(context->write_request.n_timeseries, sizeof(Prometheus__TimeSeries *));

    if(NULL == context->write_request.timeseries)
    {
        return NULL;
    }

    context->write_request.metadata = calloc(context->write_request.n_metadata, sizeof(Prometheus__TimeSeries *));

    if(NULL == context->write_request.metadata)
    {
        free(context->write_request.timeseries);

        return NULL;
    }

    entry_index = 0;

    mk_list_foreach(head, &context->time_series_entries) {
        time_series_entry = mk_list_entry(head, struct cmt_prometheus_time_series, _head);

        context->write_request.timeseries[entry_index++] = &time_series_entry->data;
    }

    entry_index = 0;

    mk_list_foreach(head, &context->metadata_entries) {
        metadata_entry = mk_list_entry(head, struct cmt_prometheus_metric_metadata, _head);

        context->write_request.metadata[entry_index++] = &metadata_entry->data;
    }

    write_request_size = prometheus__write_request__get_packed_size(&context->write_request);

    result_buffer = cmt_sds_create_size(write_request_size);

    if(NULL != result_buffer)
    {
        prometheus__write_request__pack(&context->write_request, result_buffer);

        CMT_SDS_HEADER(result_buffer)->len = write_request_size;
    }

    free(context->write_request.timeseries);

    free(context->write_request.metadata);

    return result_buffer;
}

static void cmt_destroy_prometheus_remote_write_context(struct cmt_prometheus_remote_write_context *context)
{
    struct cmt_prometheus_time_series *time_series_entry;
    struct cmt_prometheus_metric_metadata *metadata_entry;
    size_t  entry_index;
    struct mk_list *head;
    struct mk_list *tmp;

    mk_list_foreach_safe(head, tmp, &context->time_series_entries) {
        time_series_entry = mk_list_entry(head, struct cmt_prometheus_time_series, _head);

        for (entry_index = 0 ; 
             entry_index < time_series_entry->data.n_labels ; 
             entry_index++) {
            free(time_series_entry->data.labels[entry_index]);
        }

        free(time_series_entry->data.labels);

        for (entry_index = 0 ; 
             entry_index < time_series_entry->data.n_samples ; 
             entry_index++) {
            free(time_series_entry->data.samples[entry_index]);
        }

        free(time_series_entry->data.samples);

        mk_list_del(&time_series_entry->_head);

        free(time_series_entry);
    }

    mk_list_foreach_safe(head, tmp, &context->metadata_entries) {
        metadata_entry = mk_list_entry(head, struct cmt_prometheus_metric_metadata, _head);

        mk_list_del(&metadata_entry->_head);

        free(metadata_entry);
    }
}

/* Format all the registered metrics in Prometheus Text format */
cmt_sds_t cmt_encode_prometheus_remote_write_create(struct cmt *cmt, int add_timestamp)
{
    struct cmt_prometheus_remote_write_context context;
    struct mk_list *head;
    struct cmt_counter *counter;
    struct cmt_gauge *gauge;
    cmt_sds_t buf;
    int result;

    memset(&context, 0, sizeof(struct cmt_prometheus_remote_write_context));

    prometheus__write_request__init(&context.write_request);

    context.cmt = cmt;

    mk_list_init(&context.time_series_entries);
    mk_list_init(&context.metadata_entries);

    /* Counters */
    mk_list_foreach(head, &cmt->counters) {
        counter = mk_list_entry(head, struct cmt_counter, _head);
        result = format_metrics(&context, counter->map, add_timestamp);

        if (0 != result) {
            break;
        }
    }

    if (0 == result) {
        /* Gauges */
        mk_list_foreach(head, &cmt->gauges) {
            gauge = mk_list_entry(head, struct cmt_gauge, _head);
            result = format_metrics(&context, gauge->map, add_timestamp);

            if (0 != result) {
                break;
            }
        }

        if (0 == result) {
            buf = render_remote_write_context_to_sds(&context);
        }
    }

    cmt_destroy_prometheus_remote_write_context(&context);

    return buf;
}

void cmt_encode_prometheus_remote_write_destroy(cmt_sds_t text)
{
    cmt_sds_destroy(text);
}
