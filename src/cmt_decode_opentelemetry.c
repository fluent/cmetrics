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
#include <cmetrics/cmt_untyped.h>
#include <cmetrics/cmt_compat.h>
#include <cmetrics/cmt_variant.h>
#include <cmetrics/cmt_decode_opentelemetry.h>
/*
static int clone_array(struct cmt_array *target,
                       Opentelemetry__Proto__Common__V1__ArrayValue *source);
static int clone_key_value_list(struct cmt_kvlist *target,
                                Opentelemetry__Proto__Common__V1__KeyValueList *source);
static int clone_key_value(struct cmt_kvlist *target,
                           Opentelemetry__Proto__Common__V1__KeyValue *source);

static int clone_array(struct cmt_array *target,
                       Opentelemetry__Proto__Common__V1__ArrayValue *source)
{
    int    result;
    size_t index;

    for (index = 0 ;
         index < source->n_values ;
         index++) {
        result = clone_key_value(target, source->values[index]);

        if (result) {
            return result;
        }
    }

    return 0;
}

static int clone_key_value_list(struct cmt_kvlist *target,
                                Opentelemetry__Proto__Common__V1__KeyValueList *source)
{
    int    result;
    size_t index;

    for (index = 0 ;
         index < source->n_values ;
         index++) {
        result = clone_key_value(target, source->values[index]);

        if (result) {
            return result;
        }
    }

    return 0;
}

static int clone_key_value(struct cmt_kvlist *target,
                           Opentelemetry__Proto__Common__V1__KeyValue *source)
{
    struct cmt_kvlist  *new_child_kvlist;
    struct cmt_array   *new_child_array;
    struct cmt_variant *result_instance;
    int                 result;

    if (source->value->value_case ==
        OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_STRING_VALUE) {
        result = cmt_kvlist_insert_string(target,
            source->key, source->value->string_value);
    }
    else if (source->value->value_case ==
        OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_BOOL_VALUE) {
        result = cmt_kvlist_insert_bool(target,
            source->key, source->value->bool_value);
    }
    else if (source->value->value_case ==
        OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_INT_VALUE) {
        result = cmt_kvlist_insert_int(target,
            source->key, source->value->int_value);
    }
    else if (source->value->value_case ==
        OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_DOUBLE_VALUE) {
        result = cmt_kvlist_insert_double(target,
            source->key, source->value->double_value);
    }
    else if (source->value->value_case ==
        OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_KVLIST_VALUE) {
        new_child_list = cmt_kvlist_create();

        if (new_child_list) {
            return -1;
        }

        result = cmt_kvlist_insert_kvlist(target,
            source->key,
            new_child_list);

        if (result) {
            return -2;
        }

        result = clone_key_value_list(new_child_list, source->value->kvlist_value);
    }
    else if (source->value->value_case ==
        OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_KVLIST_VALUE) {
        new_child_array = cmt_array_create(source->value->array_value->n_values);

        if (new_child_array) {
            return -1;
        }

        result = cmt_kvlist_insert_array(target,
            source->key,
            new_child_array);

        if (result) {
            return -2;
        }

        result = clone_array(new_child_array, source->value->array_value);
    }
    else if (source->value->value_case ==
        OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_BYTES_VALUE) {
        result = cmt_kvlist_insert_bytes(target,
            source->key,
            source->value->bytes_value.data,
            source->value->bytes_value.len);
    }
}
*/

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

static int decode_resource_metrics(struct cmt *cmt,
    size_t resource_metrics_index,
    Opentelemetry__Proto__Metrics__V1__ResourceMetrics *resource_metrics)
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


    if (resource_metrics->resource != NULL) {
        for (index = 0 ;
             index < resource_metrics->resource->n_attributes ;
             index++) {

            result = clone_kvlist_entry(attributes,
                                        resource_metrics->resource->attributes[index]);

            if (result) {
                return result;
            }
        }
    }


    return 0;
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


    for (index = 0 ; index < service_request->n_resource_metrics ; index++) {
        result = decode_resource_metrics(cmt, index,
                                         service_request->resource_metrics[index]);

        if (result) {
            return -1;
        }
    }

    printf("n_resource_metrics = %zu\n", service_request->n_resource_metrics);

    if (service_request->n_resource_metrics > 0) {
        printf("n_instrumentation_library_metrics = %zu\n", service_request->resource_metrics[0]->n_instrumentation_library_metrics);

        if (service_request->resource_metrics[0]->n_instrumentation_library_metrics > 0) {
            printf("n_metrics = %zu\n", service_request->resource_metrics[0]->instrumentation_library_metrics[0]->n_metrics);

            {
                size_t metric_index;

                for (metric_index = 0 ;
                     metric_index < service_request->resource_metrics[0]->instrumentation_library_metrics[0]->n_metrics;
                     metric_index++) {

                    printf("name = [%s]\n", service_request->resource_metrics[0]->instrumentation_library_metrics[0]->metrics[metric_index]->name);
                    printf("\tdesc = [%s]\n", service_request->resource_metrics[0]->instrumentation_library_metrics[0]->metrics[metric_index]->description);
                    printf("\t\tunit = [%s]\n", service_request->resource_metrics[0]->instrumentation_library_metrics[0]->metrics[metric_index]->unit);

                }
            }
        }
    }


    return 0;
}

int cmt_decode_opentelemetry_create(struct cmt **out_cmt, char *in_buf, size_t in_size,
                                    size_t *offset)
{
    Opentelemetry__Proto__Collector__Metrics__V1__ExportMetricsServiceRequest *service_request;
    int                                                                        result;
    struct cmt                                                                *cmt;

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
