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
#include <protobuf-c/remote.pb-c.h>
#include <cmetrics/cmt_encode_prometheus_write_request.h>


static void sds_cat_safe(cmt_sds_t *buf, const char *str, int len)
{
    cmt_sds_t tmp;

    tmp = cmt_sds_cat(*buf, str, len);
    if (!tmp) {
        return;
    }
    *buf = tmp;
}

static int metric_banner(struct cmt_prometheus_write_request_context *context, 
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

static int format_metric(struct cmt_prometheus_write_request_context *context, 
                          struct cmt_map *map,
                          struct cmt_metric *metric, int add_timestamp)
{
    struct cmt_map_label *label_k;
    struct cmt_map_label *label_v;
    struct cmt_opts *opts;
    struct mk_list *head;
    size_t sample_count;
    size_t sample_index;
    size_t label_count;
    size_t label_index;

    struct cmt_prometheus_time_series *time_series_entry;
    Prometheus__Sample               **sample_value_list;
    Prometheus__Label                **sample_label_list;

    opts = map->opts;

    label_count = mk_list_size(&metric->labels);
    sample_count = 1;

    time_series_entry = malloc(sizeof(struct cmt_prometheus_time_series));

    if (NULL == time_series_entry) {
        return 1;
    }

    memset(time_series_entry, 0, sizeof(struct cmt_prometheus_time_series));

    sample_value_list = malloc(sizeof(Prometheus__Sample *) * label_count);

    if (NULL == sample_value_list) {
        return 2;
    }

    sample_label_list = malloc(sizeof(Prometheus__Label *) * label_count);

    if (NULL == sample_label_list) {
        return 3;
    }

    prometheus__time_series__init(&time_series_entry->data);

    /* Metric info */
    time_series_entry->data.n_labels  = label_count;
    time_series_entry->data.labels    = sample_label_list;
    time_series_entry->data.n_samples = label_count;
    time_series_entry->data.samples   = sample_value_list;

    label_index = 0;
    sample_index = 0;

    if (label_count > 0) {
        label_k = mk_list_entry_first(&map->label_keys, struct cmt_map_label, _head);

        mk_list_foreach(head, &metric->labels) {
            label_v = mk_list_entry(head, struct cmt_map_label, _head);

            sample_label_list[label_index] = malloc(sizeof(Prometheus__Label));

            if (NULL == sample_label_list[label_index]) {
                return 4;
            }

            prometheus__label__init(sample_label_list[label_index]);

            sample_label_list[label_index]->name = label_k->name;
            sample_label_list[label_index]->value = label_v->name;

            label_index++;

            label_k = mk_list_entry_next(&label_k->_head, struct cmt_map_label,
                                         _head, &map->label_keys);
        }
    }

    sample_value_list[sample_index] = malloc(sizeof(Prometheus__Sample));

    if (NULL == sample_value_list[sample_index]) {
        return 5;
    }

    prometheus__sample__init(sample_value_list[sample_index]);

    sample_value_list[sample_index]->value = cmt_metric_get_value(metric);

    if (add_timestamp) {
        sample_value_list[sample_index]->timestamp = cmt_metric_get_timestamp(metric);
    }
    else {
        sample_value_list[sample_index]->timestamp = 0;
    }

    mk_list_add(&time_series_entry->_head, &context->time_series_entries);

    return 0;
}

static int format_metrics(struct cmt_prometheus_write_request_context *context, struct cmt_map *map, int add_timestamp)
{
    struct cmt_metric *metric;
    struct mk_list *head;
    int result;

    /* Simple metric, no labels */
    if (map->metric_static_set == 1) {
        result = metric_banner(context, map, &map->metric);

        if (0 != result) {
            return 1;
        }

        result = format_metric(context, map, &map->metric, add_timestamp);

        if (0 != result) {
            return 2;
        }
    }
    else
    {
        if (mk_list_size(&map->metrics) > 0) {
            metric = mk_list_entry_first(&map->metrics, struct cmt_metric, _head);
            result = metric_banner(context, map, metric);

            if (0 != result) {
                return 3;
            }
        }

        mk_list_foreach(head, &map->metrics) {
            metric = mk_list_entry(head, struct cmt_metric, _head);
            result = format_metric(context, map, metric, add_timestamp);

            if (0 != result) {
                return 4;
            }
        }        
    }

    return 0;
}


cmt_sds_t render_remote_write_context_to_sds(struct cmt_prometheus_write_request_context *write_request_context)
{
    struct cmt_prometheus_metric_metadata *metadata_entry;
    struct cmt_prometheus_time_series *time_series_entry;
    size_t write_request_size;
    cmt_sds_t result_buffer;
    struct mk_list *head;
    size_t entry_index;

    write_request_context->write_request.n_timeseries = mk_list_size(&write_request_context->time_series_entries);
    write_request_context->write_request.n_metadata   = mk_list_size(&write_request_context->metadata_entries);

    write_request_context->write_request.timeseries = malloc(sizeof(Prometheus__TimeSeries *) * write_request_context->write_request.n_timeseries);

    if(NULL == write_request_context->write_request.timeseries)
    {
        return NULL;
    }

    write_request_context->write_request.metadata   = malloc(sizeof(Prometheus__TimeSeries *) * write_request_context->write_request.n_metadata);

    if(NULL == write_request_context->write_request.metadata)
    {
        free(write_request_context->write_request.timeseries);

        return NULL;
    }

    entry_index = 0;

    mk_list_foreach(head, &write_request_context->time_series_entries) {
        time_series_entry = mk_list_entry(head, struct cmt_prometheus_time_series, _head);

        write_request_context->write_request.timeseries[entry_index++] = &time_series_entry->data;
    }

    entry_index = 0;

    mk_list_foreach(head, &write_request_context->metadata_entries) {
        metadata_entry = mk_list_entry(head, struct cmt_prometheus_metric_metadata, _head);

        write_request_context->write_request.metadata[entry_index++] = &metadata_entry->data;
    }

    write_request_size = prometheus__write_request__get_packed_size(&write_request_context->write_request);

    result_buffer = cmt_sds_create_size(write_request_size);

    if(NULL != result_buffer)
    {
        prometheus__write_request__pack(&write_request_context->write_request, result_buffer);

        CMT_SDS_HEADER(result_buffer)->len = write_request_size;
    }

    free(write_request_context->write_request.timeseries);

    free(write_request_context->write_request.metadata);

    return result_buffer;
}

static void cmt_destroy_prometheus_write_request_context(struct cmt_prometheus_write_request_context *write_request_context)
{
    struct cmt_prometheus_time_series *time_series_entry;
    struct cmt_prometheus_metric_metadata *metadata_entry;
    size_t  entry_index;
    struct mk_list *head;
    struct mk_list *tmp;

    mk_list_foreach_safe(head, tmp, &write_request_context->time_series_entries) {
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

    mk_list_foreach_safe(head, tmp, &write_request_context->metadata_entries) {
        metadata_entry = mk_list_entry(head, struct cmt_prometheus_metric_metadata, _head);

        mk_list_del(&metadata_entry->_head);

        free(metadata_entry);
    }
}

/* Format all the registered metrics in Prometheus Text format */
cmt_sds_t cmt_encode_prometheus_write_request_create(struct cmt *cmt, int add_timestamp)
{
    struct cmt_prometheus_write_request_context write_request_context;
    struct mk_list *head;
    struct cmt_counter *counter;
    struct cmt_gauge *gauge;
    cmt_sds_t buf;
    int result;

    memset(&write_request_context, 0, sizeof(struct cmt_prometheus_write_request_context));

    prometheus__write_request__init(&write_request_context.write_request);

    mk_list_init(&write_request_context.time_series_entries);
    mk_list_init(&write_request_context.metadata_entries);

    /* Counters */
    mk_list_foreach(head, &cmt->counters) {
        counter = mk_list_entry(head, struct cmt_counter, _head);
        result = format_metrics(&write_request_context, counter->map, add_timestamp);

        if (0 != result) {
            break;
        }
    }

    if (0 == result) {
        /* Gauges */
        mk_list_foreach(head, &cmt->gauges) {
            gauge = mk_list_entry(head, struct cmt_gauge, _head);
            result = format_metrics(&write_request_context, gauge->map, add_timestamp);

            if (0 != result) {
                break;
            }
        }

        if (0 == result) {
            buf = render_remote_write_context_to_sds(&write_request_context);
        }
    }

    cmt_destroy_prometheus_write_request_context(&write_request_context);

    return buf;
}

void cmt_encode_prometheus_write_request_destroy(cmt_sds_t text)
{
    cmt_sds_destroy(text);
}
