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
#include <cmetrics/cmt_encode_prometheus.h>
#include <cmetrics/cmt_encode_prometheus_remote_write.h>
#include <cmetrics/cmt_decode_prometheus_remote_write.h>
#include <cmetrics/cmt_decode_statsd.h>

#include "cmt_tests.h"

static cfl_sds_t generate_remote_write_payload(char *extra_label_name,
                                               char *extra_label_value)
{
    Prometheus__WriteRequest request;
    Prometheus__TimeSeries time_series;
    Prometheus__Label name_label;
    Prometheus__Label extra_label;
    Prometheus__Sample sample;
    Prometheus__TimeSeries *time_series_list[1];
    Prometheus__Label *label_list[2];
    Prometheus__Sample *sample_list[1];
    size_t payload_size;
    unsigned char *packed_payload;
    cfl_sds_t payload;

    prometheus__write_request__init(&request);
    prometheus__time_series__init(&time_series);
    prometheus__label__init(&name_label);
    prometheus__label__init(&extra_label);
    prometheus__sample__init(&sample);

    name_label.name = "__name__";
    name_label.value = "rw_metric";
    extra_label.name = extra_label_name;
    extra_label.value = extra_label_value;

    label_list[0] = &name_label;
    label_list[1] = &extra_label;
    time_series.n_labels = 2;
    time_series.labels = label_list;

    sample.value = 1.0;
    sample.timestamp = 123;
    sample_list[0] = &sample;
    time_series.n_samples = 1;
    time_series.samples = sample_list;

    time_series_list[0] = &time_series;
    request.n_timeseries = 1;
    request.timeseries = time_series_list;

    payload_size = prometheus__write_request__get_packed_size(&request);
    packed_payload = calloc(1, payload_size);
    if (packed_payload == NULL) {
        return NULL;
    }

    prometheus__write_request__pack(&request, packed_payload);
    payload = cfl_sds_create_len((char *) packed_payload, payload_size);
    free(packed_payload);

    return payload;
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

void test_prometheus_remote_write_missing_label_name_rejected()
{
    int ret;
    struct cmt *decoded_context = NULL;
    cfl_sds_t payload;

    cmt_initialize();

    payload = generate_remote_write_payload(NULL, "value");
    TEST_CHECK(payload != NULL);
    if (payload != NULL) {
        ret = cmt_decode_prometheus_remote_write_create(&decoded_context,
                                                        payload,
                                                        cfl_sds_len(payload));
        TEST_CHECK(ret != CMT_DECODE_PROMETHEUS_REMOTE_WRITE_SUCCESS);
        cfl_sds_destroy(payload);
    }
}

void test_prometheus_remote_write_missing_label_value_no_crash()
{
    int ret;
    struct cmt *decoded_context = NULL;
    cfl_sds_t payload;
    cfl_sds_t encoded_payload;

    cmt_initialize();

    payload = generate_remote_write_payload("zone", NULL);
    TEST_CHECK(payload != NULL);
    if (payload != NULL) {
        ret = cmt_decode_prometheus_remote_write_create(&decoded_context,
                                                        payload,
                                                        cfl_sds_len(payload));
        TEST_CHECK(ret == CMT_DECODE_PROMETHEUS_REMOTE_WRITE_SUCCESS);
        if (ret == CMT_DECODE_PROMETHEUS_REMOTE_WRITE_SUCCESS) {
            encoded_payload = cmt_encode_prometheus_remote_write_create(decoded_context);
            TEST_CHECK(encoded_payload != NULL);
            if (encoded_payload != NULL) {
                cmt_encode_prometheus_remote_write_destroy(encoded_payload);
            }
            cmt_decode_prometheus_remote_write_destroy(decoded_context);
        }
        cfl_sds_destroy(payload);
    }
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
    {"prometheus_remote_write", test_prometheus_remote_write},
    {"prometheus_remote_write_missing_label_name_rejected", test_prometheus_remote_write_missing_label_name_rejected},
    {"prometheus_remote_write_missing_label_value_no_crash", test_prometheus_remote_write_missing_label_value_no_crash},
    {"statsd", test_statsd},
    { 0 }
};
