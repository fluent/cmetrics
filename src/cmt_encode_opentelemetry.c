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
#include <cmetrics/cmt_hash.h> 
#include <cmetrics/cmt_encode_opentelemetry.h>


static void destroy_instrumentation_library_metric(
    Opentelemetry__Proto__Metrics__V1__InstrumentationLibraryMetrics *metric);

static Opentelemetry__Proto__Metrics__V1__InstrumentationLibraryMetrics *
    initialize_instrumentation_library_metric();

static void destroy_instrumentation_library_metric_list(
    Opentelemetry__Proto__Metrics__V1__InstrumentationLibraryMetrics **metric_list);

static Opentelemetry__Proto__Metrics__V1__InstrumentationLibraryMetrics **
    initialize_instrumentation_library_metric_list(
    size_t element_count);

static void destroy_resource_metric(
    Opentelemetry__Proto__Metrics__V1__ResourceMetrics *metric);

static void destroy_resource_metric_list(
    Opentelemetry__Proto__Metrics__V1__ResourceMetrics **metric_list);

static Opentelemetry__Proto__Metrics__V1__ResourceMetrics **
    initialize_resource_metric_list(
    size_t element_count);

static void destroy_attribute(
    Opentelemetry__Proto__Common__V1__KeyValue *attribute);

static Opentelemetry__Proto__Common__V1__KeyValue *
    initialize_string_attribute(char *key, char *value);

static void destroy_attribute_list(
    Opentelemetry__Proto__Common__V1__KeyValue **attribute_list);

static Opentelemetry__Proto__Common__V1__KeyValue **
    initialize_attribute_list(
    size_t element_count);

static void destroy_data_point(
    Opentelemetry__Proto__Metrics__V1__NumberDataPoint *data_point);

static Opentelemetry__Proto__Metrics__V1__NumberDataPoint *
    initialize_double_data_point(
    uint64_t start_time,
    uint64_t timestamp,
    double value,
    Opentelemetry__Proto__Common__V1__KeyValue **attribute_list,
    size_t attribute_count);

static int append_attribute_to_data_point(
    Opentelemetry__Proto__Metrics__V1__NumberDataPoint *data_point,
    Opentelemetry__Proto__Common__V1__KeyValue *attribute,
    size_t attribute_slot_hint);

static void destroy_data_point_list(
    Opentelemetry__Proto__Metrics__V1__NumberDataPoint **data_point_list);

static Opentelemetry__Proto__Metrics__V1__NumberDataPoint **
    initialize_data_point_list(
    size_t element_count);

static void destroy_metric(
    Opentelemetry__Proto__Metrics__V1__Metric *metric);

static Opentelemetry__Proto__Metrics__V1__Metric *
    initialize_metric(int type,
                      char *name,
                      char *description,
                      char *unit,
                      size_t data_point_count);

static int append_data_point_to_metric(
    Opentelemetry__Proto__Metrics__V1__Metric *metric,
    Opentelemetry__Proto__Metrics__V1__NumberDataPoint *data_point,
    size_t data_point_slot_hint);

static void destroy_metric_list(
    Opentelemetry__Proto__Metrics__V1__Metric **metric_list);

static Opentelemetry__Proto__Metrics__V1__Metric **
    initialize_metric_list(
    size_t element_count);


void cmt_encode_opentelemetry_destroy(cmt_sds_t text);
/*
int pack_metric_sample(struct cmt_opentelemetry_context *context,
                       struct cmt_map *map,
                       struct cmt_metric *metric,
                       int add_metadata)
{
    struct cmt_prometheus_time_series *time_series;
    int                                result;

    result = set_up_time_series_for_label_set(context, map, metric, &time_series);

    if (result != CMT_ENCODE_OPENTELEMETRY_SUCCESS) {
        return result;
    }

    if (add_metadata == CMT_TRUE) {
        result = pack_metric_metadata(context, map, metric);

        if (result != CMT_ENCODE_OPENTELEMETRY_SUCCESS) {
            return result;
        }
    }

    return append_metric_to_timeseries(time_series, metric);
}

int pack_basic_type(struct cmt_opentelemetry_context *context,
                    struct cmt_map *map)
{
    int                add_metadata;
    struct cmt_metric *metric;
    int                result;
    struct mk_list    *head;

    context->sequence_number++;
    add_metadata = CMT_TRUE;

    if (map->metric_static_set) {
        result = pack_metric_sample(context, map, &map->metric, add_metadata);

        if (result != CMT_ENCODE_OPENTELEMETRY_SUCCESS) {
            return result;
        }
    }

    mk_list_foreach(head, &map->metrics) {
        metric = mk_list_entry(head, struct cmt_metric, _head);

        result = pack_metric_sample(context, map, metric, add_metadata);

        if (result != CMT_ENCODE_OPENTELEMETRY_SUCCESS) {
            return result;
        }

        if (add_metadata) {
            add_metadata = CMT_FALSE;
        }
    }

    return CMT_ENCODE_OPENTELEMETRY_SUCCESS;
}
*/

static void destroy_instrumentation_library_metric(
    Opentelemetry__Proto__Metrics__V1__InstrumentationLibraryMetrics *metric)
{
    if (metric != NULL) {
        destroy_metric_list(metric->metrics);

        free(metric);
    }
}

static Opentelemetry__Proto__Metrics__V1__InstrumentationLibraryMetrics *
    initialize_instrumentation_library_metric()
{
    Opentelemetry__Proto__Metrics__V1__InstrumentationLibraryMetrics *metric;

    metric = \
        calloc(1, sizeof(Opentelemetry__Proto__Metrics__V1__InstrumentationLibraryMetrics));

    if (metric == NULL) {
        return NULL;
    }

    opentelemetry__proto__metrics__v1__instrumentation_library_metrics__init(
        metric);

    return metric;
}

static void destroy_instrumentation_library_metric_list(
    Opentelemetry__Proto__Metrics__V1__InstrumentationLibraryMetrics **metric_list)
{
    size_t element_index;

    if (metric_list != NULL) {
        for (element_index = 0 ;
             metric_list[element_index] != NULL ;
             element_index++) {
            destroy_instrumentation_library_metric(metric_list[element_index]);

            metric_list[element_index] = NULL;
        }

        free(metric_list);
    }
}

static Opentelemetry__Proto__Metrics__V1__InstrumentationLibraryMetrics **
    initialize_instrumentation_library_metric_list(
    size_t element_count)
{
    size_t                                                             element_index;
    Opentelemetry__Proto__Metrics__V1__InstrumentationLibraryMetrics **metric_list;

    metric_list = calloc(element_count + 1,
                         sizeof(Opentelemetry__Proto__Metrics__V1__InstrumentationLibraryMetrics *));

    if (metric_list == NULL) {
        return NULL;
    }

    for (element_index = 0 ;
         element_index < element_count ;
         element_index++) {
        metric_list[element_index] = initialize_instrumentation_library_metric();

        if (metric_list[element_index] == NULL) {
            destroy_instrumentation_library_metric_list(metric_list);

            return NULL;
        }
    }

    return metric_list;
}


static void destroy_resource_metric(
    Opentelemetry__Proto__Metrics__V1__ResourceMetrics *metric)
{
    if (metric != NULL) {

    }
}

static void destroy_resource_metric_list(
    Opentelemetry__Proto__Metrics__V1__ResourceMetrics **metric_list)
{
    size_t element_index;

    if (metric_list != NULL) {
        for (element_index = 0 ;
             metric_list[element_index] != NULL ;
             element_index++) {
            destroy_resource_metric(metric_list[element_index]);

            metric_list[element_index] = NULL;
        }

        free(metric_list);
    }
}

static Opentelemetry__Proto__Metrics__V1__ResourceMetrics **
    initialize_resource_metric_list(
    size_t element_count)
{
    size_t                                               element_index;
    Opentelemetry__Proto__Metrics__V1__ResourceMetrics **metric_list;

    metric_list = calloc(element_count + 1,
                         sizeof(Opentelemetry__Proto__Metrics__V1__ResourceMetrics *));

    if (metric_list == NULL) {
        return NULL;
    }

    for (element_index = 0 ;
         element_index < element_count ;
         element_index++) {
        metric_list[element_index] = \
            calloc(1, sizeof(Opentelemetry__Proto__Metrics__V1__ResourceMetrics));

        if (metric_list[element_index] == NULL) {
            destroy_resource_metric_list(metric_list);

            return NULL;
        }

        opentelemetry__proto__metrics__v1__resource_metrics__init(
            metric_list[element_index]);
    }

    return metric_list;
}



/*
 * OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE__NOT_SET = 0,
 * OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_STRING_VALUE = 1,
 * OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_BOOL_VALUE = 2,
 * OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_INT_VALUE = 3,
 * OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_DOUBLE_VALUE = 4,
 * OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_ARRAY_VALUE = 5,
 * OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_KVLIST_VALUE = 6,
 * OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_BYTES_VALUE = 7
 */
static void destroy_attribute(
    Opentelemetry__Proto__Common__V1__KeyValue *attribute)
{
    if (attribute != NULL) {
        if (attribute->value != NULL) {
            if (attribute->value->value_case == \
                OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_STRING_VALUE) {
                cmt_sds_destroy(attribute->value->string_value);
            }

            free(attribute->value);
        }

        if (attribute->key != NULL) {
            cmt_sds_destroy(attribute->key);
        }

        free(attribute);
    }
}

static Opentelemetry__Proto__Common__V1__KeyValue *
    initialize_string_attribute(char *key, char *value)
{
    Opentelemetry__Proto__Common__V1__KeyValue *attribute;

    attribute = calloc(1,
                       sizeof(Opentelemetry__Proto__Common__V1__KeyValue));

    if (attribute == NULL) {
        return NULL;
    }

    attribute->value = calloc(1,
                              sizeof(Opentelemetry__Proto__Common__V1__AnyValue));

    if (attribute->value == NULL) {
        destroy_attribute(attribute);

        return NULL;
    }

    attribute->value->string_value = cmt_sds_create(value);

    if (attribute->value->string_value == NULL) {
        destroy_attribute(attribute);

        return NULL;
    }

    attribute->value->value_case = OPENTELEMETRY__PROTO__COMMON__V1__ANY_VALUE__VALUE_STRING_VALUE;

    attribute->key = cmt_sds_create(key);

    if (attribute->key == NULL) {
        destroy_attribute(attribute);

        return NULL;
    }

    return attribute;
}

static void destroy_attribute_list(
    Opentelemetry__Proto__Common__V1__KeyValue **attribute_list)
{
    size_t element_index;

    if (attribute_list != NULL) {
        for (element_index = 0 ;
             attribute_list[element_index] != NULL ;
             element_index++) {
            destroy_attribute(attribute_list[element_index]);

            attribute_list[element_index] = NULL;
        }

        free(attribute_list);
    }
}

static Opentelemetry__Proto__Common__V1__KeyValue **
    initialize_attribute_list(
    size_t element_count)
{
    Opentelemetry__Proto__Common__V1__KeyValue **attribute_list;

    attribute_list = calloc(element_count + 1,
                            sizeof(Opentelemetry__Proto__Common__V1__KeyValue *));

    if (attribute_list == NULL) {
        return NULL;
    }

    return attribute_list;
}





static void destroy_data_point(
    Opentelemetry__Proto__Metrics__V1__NumberDataPoint *data_point)
{
    if (data_point != NULL) {
        destroy_attribute_list(data_point->attributes);

        free(data_point);
    }
}

static Opentelemetry__Proto__Metrics__V1__NumberDataPoint *
    initialize_double_data_point(
    uint64_t start_time,
    uint64_t timestamp,
    double value,
    Opentelemetry__Proto__Common__V1__KeyValue **attribute_list,
    size_t attribute_count)
{
    Opentelemetry__Proto__Metrics__V1__NumberDataPoint *data_point;

    data_point = calloc(1,
                        sizeof(Opentelemetry__Proto__Metrics__V1__NumberDataPoint));

    if (data_point == NULL) {
        return NULL;
    }

    data_point->start_time_unix_nano = start_time;
    data_point->time_unix_nano = timestamp;
    data_point->value_case = OPENTELEMETRY__PROTO__METRICS__V1__NUMBER_DATA_POINT__VALUE_AS_DOUBLE;
    data_point->as_double = value;
    data_point->attributes = attribute_list;
    data_point->n_attributes = attribute_count;

    return data_point;
}

static int append_attribute_to_data_point(
    Opentelemetry__Proto__Metrics__V1__NumberDataPoint *data_point,
    Opentelemetry__Proto__Common__V1__KeyValue *attribute,
    size_t attribute_slot_hint)
{
    size_t attribute_slot_index;


    for (attribute_slot_index = attribute_slot_hint ;
         attribute_slot_index < data_point->n_attributes;
         attribute_slot_index++) {
        if (data_point->attributes[attribute_slot_index] == NULL) {
            data_point->attributes[attribute_slot_index] = attribute;

            return 0;
        }
    }

    return -1;
}

static void destroy_data_point_list(
    Opentelemetry__Proto__Metrics__V1__NumberDataPoint **data_point_list)
{
    size_t element_index;

    if (data_point_list != NULL) {
        for (element_index = 0 ;
             data_point_list[element_index] != NULL ;
             element_index++) {
            destroy_data_point(data_point_list[element_index]);

            data_point_list[element_index] = NULL;
        }

        free(data_point_list);
    }
}

static Opentelemetry__Proto__Metrics__V1__NumberDataPoint **
    initialize_data_point_list(
    size_t element_count)
{
    Opentelemetry__Proto__Metrics__V1__NumberDataPoint **data_point_list;

    data_point_list = calloc(element_count + 1,
                         sizeof(Opentelemetry__Proto__Metrics__V1__NumberDataPoint *));

    if (data_point_list == NULL) {
        return NULL;
    }

    return data_point_list;
}








static void destroy_metric(
    Opentelemetry__Proto__Metrics__V1__Metric *metric)
{
    if (metric != NULL) {
        if (metric->name != NULL) {
            cmt_sds_destroy(metric->name);
            metric->name = NULL;
        }

        if (metric->description == NULL) {
            cmt_sds_destroy(metric->description);
            metric->description = NULL;
        }

        if (metric->unit == NULL) {
            cmt_sds_destroy(metric->unit);
            metric->unit = NULL;
        }

        if (metric->data_case == OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_SUM) {
            destroy_data_point_list(metric->sum->data_points);
        }
        else if (metric->data_case == OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_GAUGE) {
            destroy_data_point_list(metric->gauge->data_points);
        }

        free(metric);
    }
}

/*
 * opentelemetry__proto__metrics__v1__gauge__init
 * opentelemetry__proto__metrics__v1__sum__init
 * opentelemetry__proto__metrics__v1__histogram__init
 * opentelemetry__proto__metrics__v1__number_data_point__init
 */
static Opentelemetry__Proto__Metrics__V1__Metric *
    initialize_metric(int type,
                      char *name,
                      char *description,
                      char *unit,
                      size_t data_point_count)
{
    Opentelemetry__Proto__Metrics__V1__Metric *metric;

    metric = calloc(1,
                    sizeof(Opentelemetry__Proto__Metrics__V1__Metric));

    if (metric == NULL) {
        return NULL;
    }


    if (description == NULL) {
        description = "";
    }

    if (unit == NULL) {
        unit = "";
    }

    metric->name = cmt_sds_create(name);

    if (metric->name == NULL) {
        destroy_metric(metric);

        return NULL;
    }

    metric->description = cmt_sds_create(description);

    if (metric->description == NULL) {
        destroy_metric(metric);

        return NULL;
    }

    metric->unit = cmt_sds_create(unit);

    if (metric->unit == NULL) {
        destroy_metric(metric);

        return NULL;
    }

    if (type == CMT_COUNTER) {
        metric->sum = calloc(1,
                             sizeof(Opentelemetry__Proto__Metrics__V1__Sum));

        if (metric->sum == NULL) {
            destroy_metric(metric);

            return NULL;
        }

        opentelemetry__proto__metrics__v1__sum__init(metric->sum);

        metric->data_case = OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_SUM;
        metric->sum->data_points = initialize_data_point_list(data_point_count);

        if (metric->sum->data_points == NULL) {
            destroy_metric(metric);

            return NULL;
        }

        metric->sum->n_data_points = data_point_count;
    }
    else if (type == CMT_GAUGE) {
        metric->gauge = calloc(1,
                               sizeof(Opentelemetry__Proto__Metrics__V1__Gauge));

        if (metric->gauge == NULL) {
            destroy_metric(metric);

            return NULL;
        }

        opentelemetry__proto__metrics__v1__gauge__init(metric->gauge);

        metric->data_case = OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_GAUGE;
        metric->gauge->data_points = initialize_data_point_list(data_point_count);

        if (metric->gauge->data_points == NULL) {
            destroy_metric(metric);

            return NULL;
        }

        metric->gauge->n_data_points = data_point_count;
    }

    return metric;
}

static int append_data_point_to_metric(
    Opentelemetry__Proto__Metrics__V1__Metric *metric,
    Opentelemetry__Proto__Metrics__V1__NumberDataPoint *data_point,
    size_t data_point_slot_hint)
{
    Opentelemetry__Proto__Metrics__V1__NumberDataPoint **data_point_list;
    size_t                                               data_point_slot_index;
    size_t                                               data_point_slot_count;

    data_point_list = NULL;
    data_point_slot_count = 0;

    if (metric != NULL) {
        if (metric->data_case == OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_SUM) {
            data_point_list = metric->sum->data_points;
            data_point_slot_count = metric->sum->n_data_points;
        }
        else if (metric->data_case == OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_GAUGE) {
            data_point_list = metric->gauge->data_points;
            data_point_slot_count = metric->gauge->n_data_points;
        }
    }

    for (data_point_slot_index = data_point_slot_hint ;
         data_point_slot_index < data_point_slot_count;
         data_point_slot_index++) {
        if (data_point_list[data_point_slot_index] == NULL) {
            data_point_list[data_point_slot_index] = data_point;

            return 0;
        }
    }

    return -1;
}

static void destroy_metric_list(
    Opentelemetry__Proto__Metrics__V1__Metric **metric_list)
{
    size_t element_index;

    if (metric_list != NULL) {
        for (element_index = 0 ;
             metric_list[element_index] != NULL ;
             element_index++) {
            destroy_metric(metric_list[element_index]);

            metric_list[element_index] = NULL;
        }

        free(metric_list);
    }
}

static Opentelemetry__Proto__Metrics__V1__Metric **
    initialize_metric_list(
    size_t element_count)
{
    Opentelemetry__Proto__Metrics__V1__Metric **metric_list;

    metric_list = calloc(element_count + 1,
                         sizeof(Opentelemetry__Proto__Metrics__V1__Metric *));

    if (metric_list == NULL) {
        return NULL;
    }

    return metric_list;
}



/* Format all the registered metrics in opentelemetrys collector format */
cmt_sds_t cmt_encode_opentelemetry_create(struct cmt *cmt)
{
    struct cmt_opentelemetry_context context;
    struct cmt_untyped              *untyped;
    struct cmt_counter              *counter;
    int                              result;
    struct cmt_gauge                *gauge;
    struct mk_list                  *head;
    cmt_sds_t                        buf;

    buf = NULL;
    result = 0;

    memset(&context, 0, sizeof(struct cmt_opentelemetry_context));

    opentelemetry__proto__metrics__v1__metrics_data__init(&context.metrics_data);

    {
        Opentelemetry__Proto__Metrics__V1__NumberDataPoint *data_point;
        Opentelemetry__Proto__Common__V1__KeyValue         *attribute;
        gauge = NULL;

        context.metrics_data.resource_metrics = initialize_resource_metric_list(1);

        if (context.metrics_data.resource_metrics == NULL) {
            return NULL;
        }

        context.metrics_data.n_resource_metrics = 1;

        context.metrics_data.resource_metrics[0]->instrumentation_library_metrics = \
            initialize_instrumentation_library_metric_list(1);

        if (context.metrics_data.resource_metrics[0] == NULL) {
            return NULL;
        }

        context.metrics_data.resource_metrics[0]->n_instrumentation_library_metrics = 1;


        context.metrics_data.resource_metrics[0]->instrumentation_library_metrics[0]->metrics = \
            initialize_metric_list(1);

        if (context.metrics_data.resource_metrics[0]->instrumentation_library_metrics[0]->metrics == NULL) {
            return NULL;
        }

        context.metrics_data.resource_metrics[0]->instrumentation_library_metrics[0]->n_metrics = 1;


        context.metrics_data.resource_metrics[0]->instrumentation_library_metrics[0]->metrics[0] = \
            initialize_metric(CMT_COUNTER,
                              "TEST",
                              "TEST METRIC DESC",
                              "TEST METRIC UNIT",
                              1);

        if (context.metrics_data.resource_metrics[0]->instrumentation_library_metrics[0]->metrics[0] == NULL) {
            return NULL;
        }

        data_point = initialize_double_data_point(0, cmt_time_now(), 1.64f, NULL, 0);

        if (data_point == NULL) {
            return NULL;
        }

        attribute = initialize_string_attribute("test_attr_1", "test_value_1");

        if (attribute == NULL) {
            return NULL;
        }

        result = append_attribute_to_data_point(data_point, attribute, 0);

        if (result != 0) {
            return NULL;
        }

        result = append_data_point_to_metric(
            context.metrics_data.resource_metrics[0]->instrumentation_library_metrics[0]->metrics[0],
            data_point,
            0);

        if (result != 0) {
            return NULL;
        }

    }

    return NULL;

    context.cmt = cmt;

    /* Counters */
    mk_list_foreach(head, &cmt->counters) {
        counter = mk_list_entry(head, struct cmt_counter, _head);
        // result = pack_basic_type(&context, counter->map);

        if (result != CMT_ENCODE_OPENTELEMETRY_SUCCESS) {
            break;
        }
    }

    if (result == CMT_ENCODE_OPENTELEMETRY_SUCCESS) {
        /* Gauges */
        mk_list_foreach(head, &cmt->gauges) {
            gauge = mk_list_entry(head, struct cmt_gauge, _head);
            // result = pack_basic_type(&context, gauge->map);

            if (result != CMT_ENCODE_OPENTELEMETRY_SUCCESS) {
                break;
            }
        }

    }

    if (result == CMT_ENCODE_OPENTELEMETRY_SUCCESS) {
        /* Untyped */
        mk_list_foreach(head, &cmt->untypeds) {
            untyped = mk_list_entry(head, struct cmt_untyped, _head);
            // pack_basic_type(&context, untyped->map);
        }
    }

    // if (result == CMT_ENCODE_OPENTELEMETRY_SUCCESS) {
    //     buf = render_remote_write_context_to_sds(&context);
    // }

    // cmt_destroy_opentelemetry_context(&context);

    return buf;
}

void cmt_encode_opentelemetry_destroy(cmt_sds_t text)
{
    cmt_sds_destroy(text);
}
