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
#include <cmetrics/cmt_gauge.h>
#include <cmetrics/cmt_counter.h>
#include <cmetrics/cmt_summary.h>
#include <cmetrics/cmt_histogram.h>
#include <cmetrics/cmt_untyped.h>
#include <cmetrics/cmt_compat.h>
#include <cmetrics/cmt_variant.h>
#include <cmetrics/cmt_decode_opentelemetry.h>

static struct cmt_variant *clone_variant(Opentelemetry__Proto__Common__V1__AnyValue *source);

static int clone_array(struct cmt_array *target,
                       Opentelemetry__Proto__Common__V1__ArrayValue *source);
static int clone_array_entry(struct cmt_array *target,
                             Opentelemetry__Proto__Common__V1__AnyValue *source);
static int clone_kvlist(struct cmt_kvlist *target,
                                Opentelemetry__Proto__Common__V1__KeyValueList *source);
static int clone_kvlist_entry(struct cmt_kvlist *target,
                           Opentelemetry__Proto__Common__V1__KeyValue *source);


static struct cmt_variant *clone_variant(Opentelemetry__Proto__Common__V1__AnyValue *source)
{
    struct cmt_kvlist  *new_child_kvlist;
    struct cmt_array   *new_child_array;
    struct cmt_variant *result_instance;
    int                 result;

    if (source->value_case == OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_STRING_VALUE) {
        result_instance = cmt_variant_create_from_string(source->string_value);
    }
    else if (source->value_case == OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_BOOL_VALUE) {
        result_instance = cmt_variant_create_from_bool(source->bool_value);
    }
    else if (source->value_case == OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_INT_VALUE) {
        result_instance = cmt_variant_create_from_int(source->int_value);
    }
    else if (source->value_case == OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_DOUBLE_VALUE) {
        result_instance = cmt_variant_create_from_double(source->double_value);
    }
    else if (source->value_case == OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_KVLIST_VALUE) {
        new_child_kvlist = cmt_kvlist_create();

        if (new_child_kvlist == NULL) {
            return NULL;
        }

        result_instance = cmt_variant_create_from_kvlist(new_child_kvlist);

        if (result_instance == NULL) {
            cmt_kvlist_destroy(new_child_kvlist);

            return NULL;
        }

        result = clone_kvlist(new_child_kvlist, source->kvlist_value);

        if (result) {
            cmt_variant_destroy(result_instance);

            return NULL;
        }
    }
    else if (source->value_case == OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_KVLIST_VALUE) {
        new_child_array = cmt_array_create(source->array_value->n_values);

        if (new_child_array == NULL) {
            return NULL;
        }

        result_instance = cmt_variant_create_from_array(new_child_array);

        if (result_instance == NULL) {
            cmt_array_destroy(new_child_array);

            return NULL;
        }

        result = clone_array(new_child_array, source->array_value);

        if (result) {
            cmt_variant_destroy(result_instance);

            return NULL;
        }
    }
    else if (source->value_case == OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_BYTES_VALUE) {
        result_instance = cmt_variant_create_from_bytes(source->bytes_value.data, source->bytes_value.len);
    }

    return result_instance;
}


static int clone_array(struct cmt_array *target,
                       Opentelemetry__Proto__Common__V1__ArrayValue *source)
{
    int    result;
    size_t index;

    for (index = 0 ;
         index < source->n_values ;
         index++) {
        result = clone_array_entry(target, source->values[index]);

        if (result) {
            return result;
        }
    }

    return 0;
}

static int clone_array_entry(struct cmt_array *target,
                             Opentelemetry__Proto__Common__V1__AnyValue *source)
{
    struct cmt_variant *new_child_instance;
    int                 result;

    new_child_instance = clone_variant(source);

    if (new_child_instance == NULL) {
        return -1;
    }

    result = cmt_array_append(target, new_child_instance);

    if (result) {
        cmt_variant_destroy(new_child_instance);

        return -2;
    }

    return 0;
}

static int clone_kvlist(struct cmt_kvlist *target,
                        Opentelemetry__Proto__Common__V1__KeyValueList *source)
{
    int    result;
    size_t index;

    for (index = 0 ;
         index < source->n_values ;
         index++) {
        result = clone_kvlist_entry(target, source->values[index]);

        if (result) {
            return result;
        }
    }

    return 0;
}

static int clone_kvlist_entry(struct cmt_kvlist *target,
                              Opentelemetry__Proto__Common__V1__KeyValue *source)
{
    struct cmt_variant *new_child_instance;
    int                 result;

    new_child_instance = clone_variant(source->value);

    if (new_child_instance == NULL) {
        return -1;
    }

    result = cmt_kvlist_insert(target, source->key, new_child_instance);

    if (result) {
        cmt_variant_destroy(new_child_instance);

        return -2;
    }

    return 0;
}

static int decode_resource_entry(struct cmt *cmt,
    char *schema_url,
    Opentelemetry__Proto__Resource__V1__Resource *resource)
{
    struct cmt_variant *kvlist_value;
    struct cmt_kvlist  *attributes;
    struct cmt_array   *resources;
    int                 result;
    size_t              index;

    kvlist_value = cmt_kvlist_fetch(cmt->internal_metadata, "resource");

    if (kvlist_value == NULL) {
        return -1;
    }

    resources = kvlist_value->data.as_array;
    attributes = cmt_kvlist_create();

    if (attributes == NULL) {
        return -2;
    }

    result = cmt_array_append_kvlist(resources, attributes);

    if (result) {
        cmt_kvlist_destroy(attributes);

        return -3;
    }

    /* from now on, if we fail, we don't want to destroy the kvlist because
     * the recursive destruction process will take care of it and any existing
     * keys
     */

    if (schema_url != NULL) {
        result = cmt_kvlist_insert_string(attributes,
                                          "__cmetrics__schema_url",
                                          schema_url);

        if (result) {
            return -4;
        }
    }

    if (resource != NULL) {
        for (index = 0 ;
             index < resource->n_attributes ;
             index++) {

            result = clone_kvlist_entry(attributes,
                                        resource->attributes[index]);

            if (result) {
                return result;
            }
        }
    }

    return 0;
}

static int decode_instrumentation_library(struct cmt *cmt,
    size_t resource_index,
    Opentelemetry__Proto__Common__V1__InstrumentationLibrary *instrumentation_library)
{
    struct cmt_variant *kvlist_value;
    struct cmt_variant *array_value;
    struct cmt_kvlist  *attributes;
    struct cmt_array   *resources;
    int                 result;
    size_t              index;

    if (instrumentation_library == NULL) {
        return 0;
    }

    kvlist_value = cmt_kvlist_fetch(cmt->internal_metadata, "resource");

    if (kvlist_value == NULL) {
        return -1;
    }

    resources = kvlist_value->data.as_array;
    array_value = cmt_array_fetch_by_index(resources, resource_index);

    if (array_value == NULL) {
        return -2;
    }

    attributes = array_value->data.as_kvlist;

    if (attributes == NULL) {
        return -3;
    }

    if (instrumentation_library->name != NULL) {
        result = cmt_kvlist_insert_string(attributes,
                                          "__cmetrics__instrumentation_library_name",
                                          instrumentation_library->name);

        if (result) {
            return -4;
        }

    }

    if (instrumentation_library->version != NULL) {
        result = cmt_kvlist_insert_string(attributes,
                                          "__cmetrics__instrumentation_library_version",
                                          instrumentation_library->version);

        if (result) {
            return -5;
        }

    }

    return 0;
}

static int append_metric_label(struct cmt_map *map, char *name)
{
    struct cmt_map_label *label;

    label = malloc(sizeof(struct cmt_map_label));

    if (label == NULL) {
        cmt_errno();

        return -1;
    }

    label->name = cmt_sds_create(name);

    if (label->name == NULL) {
        cmt_errno();

        free(label);

        return -2;
    }

    mk_list_add(&label->_head, &map->label_keys);

    return 0;
}

static int decode_data_point_labels(struct cmt *cmt,
                                    struct cmt_map *map,
                                    size_t attribute_count,
                                    Opentelemetry__Proto__Common__V1__KeyValue **attribute_list)
{
    Opentelemetry__Proto__Common__V1__KeyValue *attribute;
    int                                         label_found;
    size_t                                      label_index;
    struct cmt_map_label                       *current_label;
    struct mk_list                             *label_iterator;
    size_t                                      attribute_index;
    uint16_t                                   *label_index_list;

    if (attribute_count == 0) {
        return 0;
    }

    label_index_list = calloc(attribute_count, sizeof(uint16_t));

    if (label_index_list == NULL) {
        return -1;
    }

    for (attribute_index = 0 ;
         attribute_index < attribute_count ;
         attribute_index++) {

        attribute = attribute_list[attribute_index];

        label_found = CMT_FALSE;
        label_index = 0;

        if (mk_list_is_empty(&map->label_keys) == CMT_FALSE) {
            mk_list_foreach(label_iterator, &map->label_keys) {
                current_label = mk_list_entry(label_iterator, struct cmt_map_label, _head);

                if (strcmp(current_label->name, attribute->key) == 0) {
                    label_found = CMT_TRUE;

                    break;
                }

                label_index++;
            }
        }

        if (label_found == CMT_FALSE) {

        }

        label_index_list[attribute_index] = label_index;
    }


    free(label_index_list);

    return 0;
}

static int decode_numerical_data_point(struct cmt *cmt,
                                       struct cmt_map *map,
                                       Opentelemetry__Proto__Metrics__V1__NumberDataPoint *data_point)
{
    struct cmt_metric *sample;
    double             value;

    sample = calloc(1, sizeof(struct cmt_metric));

    if (sample == NULL) {
        return -1;
    }

    value = 0;

    if (data_point->value_case == OPENTELEMETRY__PROTO__METRICS__V1__NUMBER_DATA_POINT__VALUE_AS_INT) {
        value = cmt_math_uint64_to_d64((uint64_t) data_point->as_int);
    }
    else if (data_point->value_case == OPENTELEMETRY__PROTO__METRICS__V1__NUMBER_DATA_POINT__VALUE_AS_DOUBLE) {
        value = data_point->as_double;
    }

    cmt_metric_set(sample, data_point->time_unix_nano, data_point->as_double);



    return -1;
}

static int decode_numerical_data_point_list(struct cmt *cmt,
                                            struct cmt_map *map,
                                            size_t data_point_count,
                                            Opentelemetry__Proto__Metrics__V1__NumberDataPoint **data_point_list)
{
    size_t index;
    int    result;

    for (index = 0 ;
         result == 0 &&
         index < data_point_count ; index++) {
        result = decode_numerical_data_point(cmt, map, data_point_list[index]);
    }

    return result;
}

static int decode_counter_entry(struct cmt *cmt,
    void *instance,
    Opentelemetry__Proto__Metrics__V1__Sum *metric)
{
    struct cmt_counter *counter;
    int                 result;

    counter = (struct cmt_counter *) instance;

    result = decode_numerical_data_point_list(cmt,
                                              counter->map,
                                              metric->n_data_points,
                                              metric->data_points);

    if (result == 0) {
        if (metric->aggregation_temporality == OPENTELEMETRY__PROTO__METRICS__V1__AGGREGATION_TEMPORALITY__AGGREGATION_TEMPORALITY_DELTA) {
            counter->aggregation_type = CMT_AGGREGATION_TYPE_DELTA;
        }
        else  if (metric->aggregation_temporality == OPENTELEMETRY__PROTO__METRICS__V1__AGGREGATION_TEMPORALITY__AGGREGATION_TEMPORALITY_CUMULATIVE) {
            counter->aggregation_type = CMT_AGGREGATION_TYPE_CUMULATIVE;
        }
        else {
            counter->aggregation_type = CMT_AGGREGATION_TYPE_UNSPECIFIED;
        }

        counter->allow_reset = !metric->is_monotonic;
    }

    return -1;
}

static int decode_gauge_entry(struct cmt *cmt,
    void *instance,
    Opentelemetry__Proto__Metrics__V1__Gauge *metric)
{
    struct cmt_gauge *gauge;

    gauge = (struct cmt_gauge *) instance;

    return -1;
}

static int decode_summary_entry(struct cmt *cmt,
    void *instance,
    Opentelemetry__Proto__Metrics__V1__Summary *metric)
{
    struct cmt_summary *summary;

    summary = (struct cmt_summary *) instance;

    return -1;
}

static int decode_histogram_entry(struct cmt *cmt,
    void *instance,
    Opentelemetry__Proto__Metrics__V1__Histogram *metric)
{
    struct cmt_histogram *histogram;

    histogram = (struct cmt_histogram *) instance;

    return -1;
}

static int decode_metrics_entry(struct cmt *cmt,
    size_t resource_index,
    Opentelemetry__Proto__Metrics__V1__Metric *metric)
{
    char           *metric_description;
    char           *metric_namespace;
    char           *metric_subsystem;
    char           *metric_name;
    void           *instance;
    int             result;
    struct cmt_map *map;

    metric_name = metric->name;
    metric_namespace = "ns_missing";
    metric_subsystem = "ss_missing";
    metric_description = metric->description;

    if (metric->data_case == OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_SUM) {
        instance = cmt_counter_create(cmt,
                                      metric_namespace,
                                      metric_subsystem,
                                      metric_name,
                                      metric_description,
                                      0, NULL);

        if (instance == NULL) {
            return -1;
        }

        result = decode_counter_entry(cmt, instance, metric->sum);

        if (result) {
            cmt_counter_destroy(instance);
        }
    }
    else if (metric->data_case == OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_GAUGE) {
        instance = cmt_gauge_create(cmt,
                                    metric_namespace,
                                    metric_subsystem,
                                    metric_name,
                                    metric_description,
                                    0, NULL);

        if (instance == NULL) {
            return -1;
        }

        result = decode_gauge_entry(cmt, map, metric->gauge);

        if (result) {
            cmt_gauge_destroy(instance);
        }
    }
    else if (metric->data_case == OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_SUMMARY) {
        instance = cmt_summary_create(cmt,
                                      metric_namespace,
                                      metric_subsystem,
                                      metric_name,
                                      metric_description,
                                      0, NULL);

        if (instance == NULL) {
            return -1;
        }

        result = decode_summary_entry(cmt, map, metric->summary);

        if (result) {
            cmt_summary_destroy(instance);
        }
    }
    else if (metric->data_case == OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_HISTOGRAM) {
        instance = cmt_histogram_create(cmt,
                                        metric_namespace,
                                        metric_subsystem,
                                        metric_name,
                                        metric_description,
                                        (struct cmt_histogram_buckets *) cmt,
                                        0, NULL);

        if (instance == NULL) {
            return -1;
        }

        ((struct cmt_histogram *) instance)->buckets = NULL;

        result = decode_histogram_entry(cmt, map, metric->histogram);

        if (result) {
            cmt_histogram_destroy(instance);
        }
    }

    if (!result) {

    }

    return 0;
}

static int decode_instrumentation_library_metrics_entry(struct cmt *cmt,
    size_t resource_index,
    Opentelemetry__Proto__Metrics__V1__InstrumentationLibraryMetrics *metrics)
{
    int result;
    size_t index;

    result = decode_instrumentation_library(cmt,
                                            resource_index,
                                            metrics->instrumentation_library);

    if (result) {
        return -1;
    }

    for (index = 0 ;
         result == 0 &&
         index < metrics->n_metrics ;
         index++) {
        result = decode_metrics_entry(cmt,
                                      resource_index,
                                      metrics->metrics[index]);
    }

    return result;
}

static int decode_resource_metrics_entry(struct cmt *cmt,
    size_t resource_metrics_index,
    Opentelemetry__Proto__Metrics__V1__ResourceMetrics *resource_metrics)
{
    struct cmt_variant *kvlist_value;
    struct cmt_kvlist  *attributes;
    struct cmt_array   *resources;
    int                 result;
    size_t              index;

    result = decode_resource_entry(cmt,
                                   resource_metrics->schema_url,
                                   resource_metrics->resource);

    if (result) {
        return -1;
    }

    for (index = 0 ;
         result == 0 &&
         index < resource_metrics->n_instrumentation_library_metrics ;
         index++) {
        result = decode_instrumentation_library_metrics_entry(cmt,
                    resource_metrics_index,
                    resource_metrics->instrumentation_library_metrics[index]);
    }

    return result;
}

static int decode_service_request(struct cmt *cmt,
    Opentelemetry__Proto__Collector__Metrics__V1__ExportMetricsServiceRequest *service_request)
{
    int                 result;
    size_t              index;

    if (service_request->n_resource_metrics > 0) {
        result = cmt_kvlist_insert_new_array(cmt->internal_metadata,
                                             "resource", service_request->n_resource_metrics);

        if (result != 0) {
            return -1;
        }
    }


    for (index = 0 ;
         result == 0 &&
         index < service_request->n_resource_metrics ;
         index++) {
        result = decode_resource_metrics_entry(cmt, index,
                                               service_request->resource_metrics[index]);
    }

    // printf("n_resource_metrics = %zu\n", service_request->n_resource_metrics);

    // if (service_request->n_resource_metrics > 0) {
    //     printf("n_instrumentation_library_metrics = %zu\n", service_request->resource_metrics[0]->n_instrumentation_library_metrics);

    //     if (service_request->resource_metrics[0]->n_instrumentation_library_metrics > 0) {
    //         printf("n_metrics = %zu\n", service_request->resource_metrics[0]->instrumentation_library_metrics[0]->n_metrics);

    //         {
    //             size_t metric_index;

    //             for (metric_index = 0 ;
    //                  metric_index < service_request->resource_metrics[0]->instrumentation_library_metrics[0]->n_metrics;
    //                  metric_index++) {

    //                 printf("name = [%s]\n", service_request->resource_metrics[0]->instrumentation_library_metrics[0]->metrics[metric_index]->name);
    //                 printf("\tdesc = [%s]\n", service_request->resource_metrics[0]->instrumentation_library_metrics[0]->metrics[metric_index]->description);
    //                 printf("\t\tunit = [%s]\n", service_request->resource_metrics[0]->instrumentation_library_metrics[0]->metrics[metric_index]->unit);

    //             }
    //         }
    //     }
    // }


    return 0;
}

int cmt_decode_opentelemetry_create(struct cmt **out_cmt, char *in_buf, size_t in_size,
                                    size_t *offset)
{
    Opentelemetry__Proto__Collector__Metrics__V1__ExportMetricsServiceRequest *service_request;
    int                                                                        result;
    struct cmt                                                                *cmt;

printf("\n\n");

    cmt = cmt_create();

    result = cmt_kvlist_insert_string(cmt->internal_metadata,
                                      "producer", "opentelemetry");

    if (result != 0) {
        cmt_destroy(cmt);

        return -1;
    }

    printf("DECODING OPENTELEMETRY : %zu\n", in_size);

    service_request = opentelemetry__proto__collector__metrics__v1__export_metrics_service_request__unpack(NULL, in_size - *offset, &in_buf[*offset]);

    printf("service_request = %p\n");

    if (service_request != NULL) {
        result = decode_service_request(cmt, service_request);

        opentelemetry__proto__collector__metrics__v1__export_metrics_service_request__free_unpacked(service_request, NULL);
    }

    return 0;
}

void cmt_decode_opentelemetry_destroy(struct cmt *cmt)
{
    if (NULL != cmt) {
        cmt_destroy(cmt);
    }
}
