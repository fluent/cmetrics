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
#include <cmetrics/cmt_encode_prometheus_write_request.h>

#include <protobuf-c/remote.pb-c.h>
#include <snappy.h>


static int sds_cat_safe(cmt_sds_t *buf, const char *str, int len)
{
    cmt_sds_t tmp;

    tmp = cmt_sds_cat(*buf, str, len);

    if (NULL == tmp) {
        return 1;
    }

    *buf = tmp;

    return 0;
}

static int format_metric_banner(struct cmt_prometheus_write_request_context *context, 
                                struct cmt_map *map,
                                struct cmt_metric *metric)
{
    struct cmt_prometheus_metric_metadata *metadata_entry;

    metadata_entry = calloc(1, sizeof(struct cmt_prometheus_metric_metadata));

    if (NULL == metadata_entry) {
        return CMT_ENCODE_PROMETHEUS_REMOTE_WRITE_ALLOCATION_ERROR;
    }

    prometheus__metric_metadata__init(&metadata_entry->data);

    if (map->type == CMT_COUNTER) {
        metadata_entry->data.type = PROMETHEUS__METRIC_METADATA__METRIC_TYPE__COUNTER;
    }
    else if (map->type == CMT_GAUGE) {
        metadata_entry->data.type = PROMETHEUS__METRIC_METADATA__METRIC_TYPE__GAUGE;
    }

    metadata_entry->data.metric_family_name = map->opts->fqname;
    metadata_entry->data.help = map->opts->fqname;
    metadata_entry->data.unit = "unit";

    mk_list_add(&metadata_entry->_head, &context->metadata_entries);

    return CMT_ENCODE_PROMETHEUS_REMOTE_WRITE_SUCCESS;
}

static int format_metric(struct cmt_prometheus_time_series *time_series_entry, 
                         struct cmt_map *map,
                         struct cmt_metric *metric, 
                         int add_timestamp,
                         size_t sample_index)
{
    struct mk_list                    *head;
    struct cmt_map_label              *label_k;
    struct cmt_map_label              *label_v;
    size_t                             label_count;
    size_t                             label_index;

    Prometheus__Exemplar              *current_exemplar;
    Prometheus__Sample                *current_sample;
    Prometheus__Label                **sample_label_list;

    label_count = mk_list_size(&metric->labels);


    if (0 == label_count) {
        current_sample = calloc(1, sizeof(Prometheus__Sample));

        if (NULL == current_sample) {
            return 3;
        }

        prometheus__sample__init(current_sample);

        current_sample->value = cmt_metric_get_value(metric);

        if (add_timestamp) {
            current_sample->timestamp = cmt_metric_get_timestamp(metric);
        }
        else {
            current_sample->timestamp = 0;
        }

        time_series_entry->data.samples[sample_index] = current_sample;
    }
    else {
        current_exemplar = calloc(1, sizeof(Prometheus__Exemplar));

        if (NULL == current_exemplar) {
            return 3;
        }

        prometheus__exemplar__init(current_exemplar);

        current_exemplar->labels = calloc(label_count, sizeof(Prometheus__Label *));

        if (NULL == current_exemplar->labels) {
            return 3;
        }

        label_index = 0;

        if (label_count > 0) {
            label_k = mk_list_entry_first(&map->label_keys, struct cmt_map_label, _head);

            mk_list_foreach(head, &metric->labels) {
                label_v = mk_list_entry(head, struct cmt_map_label, _head);

                current_exemplar->labels[label_index] = calloc(1, sizeof(Prometheus__Label));

                if (NULL == current_exemplar->labels[label_index]) {
                    return 4;
                }

                prometheus__label__init(current_exemplar->labels[label_index]);

                current_exemplar->labels[label_index]->name = label_k->name;
                current_exemplar->labels[label_index]->value = label_v->name;

                label_index++;

                label_k = mk_list_entry_next(&label_k->_head, 
                                             struct cmt_map_label,
                                             _head, 
                                             &map->label_keys);
            }
        }

        current_exemplar->value = cmt_metric_get_value(metric);

        if (add_timestamp) {
            current_exemplar->timestamp = cmt_metric_get_timestamp(metric);
        }
        else {
            current_exemplar->timestamp = 0;
        }

        time_series_entry->data.exemplars[sample_index] = current_exemplar;
    }

    return 0;
}

static int format_metrics(struct cmt_prometheus_write_request_context *context, struct cmt_map *map, int add_timestamp)
{
    int                metric_banner_processed;
    int                result;
    struct cmt_metric *metric;
    struct mk_list    *head;


    // struct mk_list                    *head;
    struct cmt_map_label              *label;
    size_t                             label_count;
    size_t                             label_index;
    size_t                             sample_index;
    size_t                             sample_count;
    size_t                             exemplar_index;
    size_t                             exemplar_count;

    struct cmt_prometheus_time_series *time_series_entry;

    label_count = mk_list_size(&map->label_keys);
    exemplar_count = mk_list_size(&map->metrics);

    if (0 != map->metric_static_set) {
        sample_count = 1;
    }
    else {
        sample_count = 0;
    }

    time_series_entry = calloc(1, sizeof(struct cmt_prometheus_time_series));

    if (NULL == time_series_entry) {
        return 1;
    }

    prometheus__time_series__init(&time_series_entry->data);

    if (0 < label_count)
    {
        time_series_entry->data.n_labels  = label_count + 1;
        time_series_entry->data.labels    = calloc(label_count, sizeof(Prometheus__Label *));

        if (NULL == time_series_entry->data.labels) {
            return 3;
        }
    }

    if (0 < sample_count && 0)
    {
        time_series_entry->data.n_samples = sample_count;
        time_series_entry->data.samples   = calloc(sample_count, sizeof(Prometheus__Sample *));;

        if (NULL == time_series_entry->data.samples) {
            return 3;
        }
    }

    if (0 < exemplar_count)
    {
        time_series_entry->data.n_exemplars = exemplar_count;
        time_series_entry->data.exemplars   = calloc(exemplar_count, sizeof(Prometheus__Exemplar *));;

        if (NULL == time_series_entry->data.exemplars) {
            return 3;
        }
    }

    label_index = 0;

    time_series_entry->data.labels[label_index] = calloc(1, sizeof(Prometheus__Label));

    if (NULL == time_series_entry->data.labels[label_index]) {
        return 4;
    }

    prometheus__label__init(time_series_entry->data.labels[label_index]);

    time_series_entry->data.labels[label_index]->name = "__name__";
    time_series_entry->data.labels[label_index]->value = "TEST";

    label_index = 1;

    mk_list_foreach(head, &map->label_keys) {
        label = mk_list_entry(head, struct cmt_map_label, _head);

        time_series_entry->data.labels[label_index] = calloc(1, sizeof(Prometheus__Label));

        if (NULL == time_series_entry->data.labels[label_index]) {
            return 4;
        }

        prometheus__label__init(time_series_entry->data.labels[label_index]);

        time_series_entry->data.labels[label_index]->name = label->name;
        time_series_entry->data.labels[label_index]->value = "XXX";

        label_index++;
    }

    sample_index = 0;

/*
    if (map->metric_static_set == 1) {
        result = format_metric_banner(context, map, &map->metric);

        if (0 != result) {
            return 1;
        }

        result = format_metric(time_series_entry, map, &map->metric, add_timestamp, sample_index++);

        if (0 != result) {
            return 2;
        }
    }
*/
    exemplar_index = 0;
    metric_banner_processed = 0;

    mk_list_foreach(head, &map->metrics) {
        metric = mk_list_entry(head, struct cmt_metric, _head);

        if (0 == metric_banner_processed) {
            result = format_metric_banner(context, map, metric);

            if (0 != result) {
                return 3;
            }
        }

        result = format_metric(time_series_entry, map, metric, add_timestamp, exemplar_index++);

        if (0 != result) {
            return 4;
        }
    }        


    mk_list_add(&time_series_entry->_head, &context->time_series_entries);

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

    printf("write_request_context->write_request.n_timeseries = %lu\n", write_request_context->write_request.n_timeseries);
    printf("write_request_context->write_request.n_metadata = %lu\n", write_request_context->write_request.n_metadata);
    printf("\n");

    write_request_context->write_request.timeseries = calloc(write_request_context->write_request.n_timeseries, sizeof(Prometheus__TimeSeries *));

    if(NULL == write_request_context->write_request.timeseries)
    {
        return NULL;
    }

    write_request_context->write_request.metadata   = calloc(write_request_context->write_request.n_metadata, sizeof(Prometheus__TimeSeries *));

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


static void hex_dump_sds_as_str(cmt_sds_t buf)
{
    size_t idx;

    if (NULL != buf) {
        for (idx = 0 ; idx < cmt_sds_len(buf) ; idx++) {
            printf("\\x%02x", (unsigned char)buf[idx]);
        }
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

{
    struct snappy_env snappy_env;
    cmt_sds_t comp;
    size_t comp_len;
    comp = cmt_sds_create_size(2 * cmt_sds_alloc(buf));

    if (NULL == comp) {
        printf("ERROR 1\n");
        exit(0);
    }

    snappy_init_env(&snappy_env);
    result = snappy_compress(&snappy_env, buf, cmt_sds_alloc(buf), comp, &comp_len);
    snappy_free_env(&snappy_env);

    if (0 != result) {
        printf("ERROR 2\n");
        exit(0);
    }

    printf("Compressed %lu to %llu\n", cmt_sds_alloc(buf), comp_len);
    CMT_SDS_HEADER(comp)->len = comp_len;

    cmt_sds_destroy(buf);

    hex_dump_sds_as_str(comp);printf("\n\n\n");

    buf = comp;
}

    return buf;
}

void cmt_encode_prometheus_write_request_destroy(cmt_sds_t text)
{
    cmt_sds_destroy(text);
}
