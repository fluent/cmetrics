/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  CMetrics
 *  ========
 *  Copyright 2021-2022 The CMetrics Authors
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
#include <cmetrics/cmt_counter.h>
#include <cmetrics/cmt_untyped.h>
#include <cmetrics/cmt_summary.h>
#include <cmetrics/cmt_histogram.h>
#include <cmetrics/cmt_encode_prometheus.h>
#include <cmetrics/cmt_decode_opentelemetry.h>
#include <cmetrics/cmt_encode_opentelemetry.h>
#include <cmetrics/cmt_decode_prometheus_remote_write.h>
#include <cmetrics/cmt_encode_prometheus_remote_write.h>
#include <cmetrics/cmt_decode_statsd.h>

#include "cmt_tests.h"

static struct cmt *generate_encoder_test_data()
{
    double                        quantiles[5];
    struct cmt_histogram_buckets *buckets;
    double                        val;
    struct cmt                   *cmt;
    uint64_t                      ts;
    struct cmt_gauge             *g1;
    struct cmt_counter           *c1;
    struct cmt_summary           *s1;
    struct cmt_histogram         *h1;

    ts = 0;
    cmt = cmt_create();

    c1 = cmt_counter_create(cmt, "kubernetes", "network", "load_counter", "Network load counter",
                            2, (char *[]) {"hostname", "app"});

    cmt_counter_get_val(c1, 0, NULL, &val);
    cmt_counter_inc(c1, ts, 0, NULL);
    cmt_counter_add(c1, ts, 2, 0, NULL);
    cmt_counter_get_val(c1, 0, NULL, &val);

    cmt_counter_inc(c1, ts, 2, (char *[]) {"localhost", "cmetrics"});
    cmt_counter_get_val(c1, 2, (char *[]) {"localhost", "cmetrics"}, &val);
    cmt_counter_add(c1, ts, 10.55, 2, (char *[]) {"localhost", "test"});
    cmt_counter_get_val(c1, 2, (char *[]) {"localhost", "test"}, &val);
    cmt_counter_set(c1, ts, 12.15, 2, (char *[]) {"localhost", "test"});
    cmt_counter_set(c1, ts, 1, 2, (char *[]) {"localhost", "test"});

    g1 = cmt_gauge_create(cmt, "kubernetes", "network", "load_gauge", "Network load gauge", 0, NULL);

    cmt_gauge_get_val(g1, 0, NULL, &val);
    cmt_gauge_set(g1, ts, 2.0, 0, NULL);
    cmt_gauge_get_val(g1, 0, NULL, &val);
    cmt_gauge_inc(g1, ts, 0, NULL);
    cmt_gauge_get_val(g1, 0, NULL, &val);
    cmt_gauge_sub(g1, ts, 2, 0, NULL);
    cmt_gauge_get_val(g1, 0, NULL, &val);
    cmt_gauge_dec(g1, ts, 0, NULL);
    cmt_gauge_get_val(g1, 0, NULL, &val);
    cmt_gauge_inc(g1, ts, 0, NULL);

    buckets = cmt_histogram_buckets_create(3, 0.05, 5.0, 10.0);

    h1 = cmt_histogram_create(cmt,
                              "k8s", "network", "load_histogram", "Network load histogram",
                              buckets,
                              1, (char *[]) {"my_label"});

    cmt_histogram_observe(h1, ts, 0.001, 0, NULL);
    cmt_histogram_observe(h1, ts, 0.020, 0, NULL);
    cmt_histogram_observe(h1, ts, 5.0, 0, NULL);
    cmt_histogram_observe(h1, ts, 8.0, 0, NULL);
    cmt_histogram_observe(h1, ts, 1000, 0, NULL);

    cmt_histogram_observe(h1, ts, 0.001, 1, (char *[]) {"my_val"});
    cmt_histogram_observe(h1, ts, 0.020, 1, (char *[]) {"my_val"});
    cmt_histogram_observe(h1, ts, 5.0, 1, (char *[]) {"my_val"});
    cmt_histogram_observe(h1, ts, 8.0, 1, (char *[]) {"my_val"});
    cmt_histogram_observe(h1, ts, 1000, 1, (char *[]) {"my_val"});;

    quantiles[0] = 0.1;
    quantiles[1] = 0.2;
    quantiles[2] = 0.3;
    quantiles[3] = 0.4;
    quantiles[4] = 0.5;

    s1 = cmt_summary_create(cmt,
                            "k8s", "disk", "load_summary", "Disk load summary",
                            5, quantiles,
                            1, (char *[]) {"my_label"});

    quantiles[0] = 1.1;
    quantiles[1] = 2.2;
    quantiles[2] = 3.3;
    quantiles[3] = 4.4;
    quantiles[4] = 5.5;

    cmt_summary_set_default(s1, ts, quantiles, 51.612894511314444, 10, 0, NULL);

    quantiles[0] = 11.11;
    quantiles[1] = 0;
    quantiles[2] = 33.33;
    quantiles[3] = 44.44;
    quantiles[4] = 55.55;

    cmt_summary_set_default(s1, ts, quantiles, 51.612894511314444, 10, 1, (char *[]) {"my_val"});

    return cmt;
}

void test_opentelemetry()
{
    cfl_sds_t        reference_prometheus_context;
    cfl_sds_t        opentelemetry_context;
    struct cfl_list  decoded_context_list;
    cfl_sds_t        prometheus_context;
    struct cmt      *decoded_context;
    size_t           offset;
    int              result;
    struct cmt      *cmt;

    offset = 0;

    cmt_initialize();

    cmt = generate_encoder_test_data();
    TEST_CHECK(cmt != NULL);

    reference_prometheus_context = cmt_encode_prometheus_create(cmt, CMT_TRUE);
    TEST_CHECK(reference_prometheus_context != NULL);

    if (reference_prometheus_context != NULL) {
        opentelemetry_context = cmt_encode_opentelemetry_create(cmt);
        TEST_CHECK(opentelemetry_context != NULL);

        if (opentelemetry_context != NULL) {
            result = cmt_decode_opentelemetry_create(&decoded_context_list,
                                                     opentelemetry_context,
                                                     cfl_sds_len(opentelemetry_context),
                                                     &offset);

            if (TEST_CHECK(result == 0)) {
                decoded_context = cfl_list_entry_first(&decoded_context_list, struct cmt, _head);

                if (TEST_CHECK(result == 0)) {
                    prometheus_context = cmt_encode_prometheus_create(decoded_context,
                                                                      CMT_TRUE);
                    TEST_CHECK(prometheus_context != NULL);

                    if (prometheus_context != NULL) {
                        TEST_CHECK(strcmp(prometheus_context,
                                          reference_prometheus_context) == 0);

                        cmt_encode_prometheus_destroy(prometheus_context);
                    }
                }

                cmt_decode_opentelemetry_destroy(&decoded_context_list);
            }
        }

        cmt_encode_opentelemetry_destroy(opentelemetry_context);
        cmt_encode_prometheus_destroy(reference_prometheus_context);
    }

    cmt_destroy(cmt);
}

static cfl_sds_t generate_exponential_histogram_otlp_payload()
{
    Opentelemetry__Proto__Collector__Metrics__V1__ExportMetricsServiceRequest   request;
    Opentelemetry__Proto__Metrics__V1__ResourceMetrics                           resource_metrics;
    Opentelemetry__Proto__Metrics__V1__ScopeMetrics                              scope_metrics;
    Opentelemetry__Proto__Metrics__V1__Metric                                    metric;
    Opentelemetry__Proto__Metrics__V1__ExponentialHistogram                      exponential_histogram;
    Opentelemetry__Proto__Metrics__V1__ExponentialHistogramDataPoint             data_point;
    Opentelemetry__Proto__Metrics__V1__ExponentialHistogramDataPoint__Buckets    positive_buckets;
    Opentelemetry__Proto__Metrics__V1__ExponentialHistogramDataPoint__Buckets    negative_buckets;
    Opentelemetry__Proto__Metrics__V1__ResourceMetrics                          *resource_metrics_list[1];
    Opentelemetry__Proto__Metrics__V1__ScopeMetrics                             *scope_metrics_list[1];
    Opentelemetry__Proto__Metrics__V1__Metric                                   *metric_list[1];
    Opentelemetry__Proto__Metrics__V1__ExponentialHistogramDataPoint            *data_point_list[1];
    uint64_t                                                                      positive_bucket_counts[2];
    uint64_t                                                                      negative_bucket_counts[1];
    size_t                                                                        payload_size;
    unsigned char                                                                *packed_payload;
    cfl_sds_t                                                                     payload;

    opentelemetry__proto__collector__metrics__v1__export_metrics_service_request__init(&request);
    opentelemetry__proto__metrics__v1__resource_metrics__init(&resource_metrics);
    opentelemetry__proto__metrics__v1__scope_metrics__init(&scope_metrics);
    opentelemetry__proto__metrics__v1__metric__init(&metric);
    opentelemetry__proto__metrics__v1__exponential_histogram__init(&exponential_histogram);
    opentelemetry__proto__metrics__v1__exponential_histogram_data_point__init(&data_point);
    opentelemetry__proto__metrics__v1__exponential_histogram_data_point__buckets__init(&positive_buckets);
    opentelemetry__proto__metrics__v1__exponential_histogram_data_point__buckets__init(&negative_buckets);

    positive_bucket_counts[0] = 3;
    positive_bucket_counts[1] = 2;
    negative_bucket_counts[0] = 1;

    positive_buckets.offset = 0;
    positive_buckets.n_bucket_counts = 2;
    positive_buckets.bucket_counts = positive_bucket_counts;

    negative_buckets.offset = 0;
    negative_buckets.n_bucket_counts = 1;
    negative_buckets.bucket_counts = negative_bucket_counts;

    data_point.time_unix_nano = 1;
    data_point.start_time_unix_nano = 0;
    data_point.count = 7;
    data_point.has_sum = CMT_TRUE;
    data_point.sum = 8.0;
    data_point.scale = 0;
    data_point.zero_count = 1;
    data_point.zero_threshold = 0.0;
    data_point.positive = &positive_buckets;
    data_point.negative = &negative_buckets;

    data_point_list[0] = &data_point;
    exponential_histogram.n_data_points = 1;
    exponential_histogram.data_points = data_point_list;
    exponential_histogram.aggregation_temporality =
        OPENTELEMETRY__PROTO__METRICS__V1__AGGREGATION_TEMPORALITY__AGGREGATION_TEMPORALITY_CUMULATIVE;

    metric.name = "exp_hist";
    metric.data_case = OPENTELEMETRY__PROTO__METRICS__V1__METRIC__DATA_EXPONENTIAL_HISTOGRAM;
    metric.exponential_histogram = &exponential_histogram;

    metric_list[0] = &metric;
    scope_metrics.n_metrics = 1;
    scope_metrics.metrics = metric_list;

    scope_metrics_list[0] = &scope_metrics;
    resource_metrics.n_scope_metrics = 1;
    resource_metrics.scope_metrics = scope_metrics_list;

    resource_metrics_list[0] = &resource_metrics;
    request.n_resource_metrics = 1;
    request.resource_metrics = resource_metrics_list;

    payload_size = opentelemetry__proto__collector__metrics__v1__export_metrics_service_request__get_packed_size(&request);
    packed_payload = calloc(1, payload_size);
    if (packed_payload == NULL) {
        return NULL;
    }

    opentelemetry__proto__collector__metrics__v1__export_metrics_service_request__pack(&request,
                                                                                         packed_payload);

    payload = cfl_sds_create_len((char *) packed_payload, payload_size);
    free(packed_payload);

    return payload;
}

void test_opentelemetry_exponential_histogram()
{
    cfl_sds_t       payload;
    cfl_sds_t       first_prometheus_context;
    cfl_sds_t       second_payload;
    cfl_sds_t       second_prometheus_context;
    struct cfl_list first_decoded_context_list;
    struct cfl_list second_decoded_context_list;
    struct cmt     *first_decoded_context;
    struct cmt     *second_decoded_context;
    size_t          offset;
    int             result;

    cmt_initialize();

    payload = generate_exponential_histogram_otlp_payload();
    TEST_CHECK(payload != NULL);

    if (payload == NULL) {
        return;
    }

    offset = 0;
    result = cmt_decode_opentelemetry_create(&first_decoded_context_list,
                                             payload,
                                             cfl_sds_len(payload),
                                             &offset);
    TEST_CHECK(result == CMT_DECODE_OPENTELEMETRY_SUCCESS);

    if (result != CMT_DECODE_OPENTELEMETRY_SUCCESS) {
        cfl_sds_destroy(payload);
        return;
    }

    first_decoded_context = cfl_list_entry_first(&first_decoded_context_list, struct cmt, _head);
    TEST_CHECK(first_decoded_context != NULL);

    if (first_decoded_context == NULL) {
        cmt_decode_opentelemetry_destroy(&first_decoded_context_list);
        cfl_sds_destroy(payload);
        return;
    }

    first_prometheus_context = cmt_encode_prometheus_create(first_decoded_context, CMT_TRUE);
    TEST_CHECK(first_prometheus_context != NULL);

    if (first_prometheus_context != NULL) {
        TEST_CHECK(strstr(first_prometheus_context, "exp_hist_bucket{le=\"-1.0\"} 1 0") != NULL);
        TEST_CHECK(strstr(first_prometheus_context, "exp_hist_bucket{le=\"0.0\"} 2 0") != NULL);
        TEST_CHECK(strstr(first_prometheus_context, "exp_hist_bucket{le=\"2.0\"} 5 0") != NULL);
        TEST_CHECK(strstr(first_prometheus_context, "exp_hist_bucket{le=\"4.0\"} 7 0") != NULL);
        TEST_CHECK(strstr(first_prometheus_context, "exp_hist_bucket{le=\"+Inf\"} 7 0") != NULL);
        TEST_CHECK(strstr(first_prometheus_context, "exp_hist_sum 8 0") != NULL);
        TEST_CHECK(strstr(first_prometheus_context, "exp_hist_count 7 0") != NULL);
    }

    second_payload = cmt_encode_opentelemetry_create(first_decoded_context);
    TEST_CHECK(second_payload != NULL);

    if (second_payload != NULL) {
        offset = 0;
        result = cmt_decode_opentelemetry_create(&second_decoded_context_list,
                                                 second_payload,
                                                 cfl_sds_len(second_payload),
                                                 &offset);
        TEST_CHECK(result == CMT_DECODE_OPENTELEMETRY_SUCCESS);

        if (result == CMT_DECODE_OPENTELEMETRY_SUCCESS) {
            second_decoded_context = cfl_list_entry_first(&second_decoded_context_list, struct cmt, _head);
            TEST_CHECK(second_decoded_context != NULL);

            if (second_decoded_context != NULL) {
                second_prometheus_context = cmt_encode_prometheus_create(second_decoded_context, CMT_TRUE);
                TEST_CHECK(second_prometheus_context != NULL);

                if (second_prometheus_context != NULL && first_prometheus_context != NULL) {
                    TEST_CHECK(strcmp(first_prometheus_context, second_prometheus_context) == 0);
                    cmt_encode_prometheus_destroy(second_prometheus_context);
                }
            }

            cmt_decode_opentelemetry_destroy(&second_decoded_context_list);
        }

        cmt_encode_opentelemetry_destroy(second_payload);
    }

    if (first_prometheus_context != NULL) {
        cmt_encode_prometheus_destroy(first_prometheus_context);
    }

    cmt_decode_opentelemetry_destroy(&first_decoded_context_list);
    cfl_sds_destroy(payload);
}

void test_prometheus_remote_write()
{
    int ret;
    struct cmt *decoded_context;
    cfl_sds_t payload = read_file(CMT_TESTS_DATA_PATH "/remote_write_dump_originally_from_node_exporter.bin");

    cmt_initialize();

    ret = cmt_decode_prometheus_remote_write_create(&decoded_context, payload, cfl_sds_len(payload));
    TEST_CHECK(ret == CMT_DECODE_PROMETHEUS_REMOTE_WRITE_SUCCESS);

    cmt_decode_prometheus_remote_write_destroy(decoded_context);

    cfl_sds_destroy(payload);
}

void test_statsd()
{
    int ret;
    struct cmt *decoded_context;
    cfl_sds_t payload = read_file(CMT_TESTS_DATA_PATH "/statsd_payload.txt");
    size_t len = 0;
    cfl_sds_t text = NULL;
    int flags = 0;

    /* For strtok_r, fill the last byte as \0. */
    len = cfl_sds_len(payload);
    cfl_sds_set_len(payload, len + 1);
    payload[len] = '\0';

    cmt_initialize();

    flags |= CMT_DECODE_STATSD_GAUGE_OBSERVER;

    ret = cmt_decode_statsd_create(&decoded_context, payload, cfl_sds_len(payload), flags);
    TEST_CHECK(ret == CMT_DECODE_PROMETHEUS_REMOTE_WRITE_SUCCESS);
    text = cmt_encode_prometheus_create(decoded_context, CMT_FALSE);

    printf("%s\n", text);
    cmt_encode_prometheus_destroy(text);

    cmt_decode_statsd_destroy(decoded_context);

    cfl_sds_destroy(payload);
}


TEST_LIST = {
    {"opentelemetry", test_opentelemetry},
    {"opentelemetry_exponential_histogram", test_opentelemetry_exponential_histogram},
    {"prometheus_remote_write", test_prometheus_remote_write},
    {"statsd", test_statsd},
    { 0 }
};
