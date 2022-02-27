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
#include <cmetrics/cmt_histogram.h>
#include <cmetrics/cmt_encode_prometheus.h>

#include "cmt_tests.h"

static void prometheus_encode_test(struct cmt *cmt)
{
    cmt_sds_t buf;

    buf = cmt_encode_prometheus_create(cmt, CMT_TRUE);
    printf("\n%s\n", buf);
    cmt_encode_prometheus_destroy(buf);
}

void test_histogram()
{
    int ret;
    double val;
    uint64_t ts;
    struct cmt *cmt;
    struct cmt_gauge *g;
    struct cmt_histogram *h;
    struct cmt_histogram_buckets *buckets;

    cmt_initialize();

    /* Timestamp */
    ts = cmt_time_now();

    /* CMetrics context */
    cmt = cmt_create();
    TEST_CHECK(cmt != NULL);

    /* Create buckets */
    buckets = cmt_histogram_buckets_create(3, 0.05, 5.0, 10.0);
    TEST_CHECK(buckets != NULL);

    /* Create a gauge metric type */
    h = cmt_histogram_create(cmt,
                             "k8s", "network", "load", "Network load",
                             buckets,
                             1, (char *[]) {"my_label"});
    TEST_CHECK(h != NULL);

    /* no labels */
    cmt_histogram_observe(h, ts, 0.001, 0, NULL);
    cmt_histogram_observe(h, ts, 0.020, 0, NULL);
    cmt_histogram_observe(h, ts, 5.0, 0, NULL);
    cmt_histogram_observe(h, ts, 8.0, 0, NULL);
    cmt_histogram_observe(h, ts, 1000, 0, NULL);
    prometheus_encode_test(cmt);

    /* defined labels: add a custom label value */
    cmt_histogram_observe(h, ts, 0.001, 1, (char *[]) {"my_val"});
    cmt_histogram_observe(h, ts, 0.020, 1, (char *[]) {"my_val"});
    cmt_histogram_observe(h, ts, 5.0, 1, (char *[]) {"my_val"});
    cmt_histogram_observe(h, ts, 8.0, 1, (char *[]) {"my_val"});
    cmt_histogram_observe(h, ts, 1000, 1, (char *[]) {"my_val"});;
    prometheus_encode_test(cmt);

    /* static label: register static label for the context */
    cmt_label_add(cmt, "static", "test");
    prometheus_encode_test(cmt);

    cmt_destroy(cmt);
}

void test_set_defaults()
{
    int ret;
    double val;
    uint64_t ts;
    uint64_t *bucket_defaults;
    struct cmt *cmt;
    struct cmt_gauge *g;
    struct cmt_histogram *h;
    struct cmt_histogram_buckets *buckets;

    cmt_initialize();

    /* Timestamp */
    ts = cmt_time_now();

    /* CMetrics context */
    cmt = cmt_create();
    TEST_CHECK(cmt != NULL);

    /* Create buckets */
    buckets = cmt_histogram_buckets_create(3, 0.05, 5.0, 10.0);
    TEST_CHECK(buckets != NULL);

    /* Create a gauge metric type */
    h = cmt_histogram_create(cmt,
                             "k8s", "network", "load", "Network load",
                             buckets,
                             1, (char *[]) {"my_label"});
    TEST_CHECK(h != NULL);

    /* set default buckets */
    bucket_defaults = calloc(1, sizeof(uint64_t) * 4);
    TEST_CHECK(bucket_defaults != NULL);

    /* set the number of "items" container on each bucket */
    bucket_defaults[0] = 1;
    bucket_defaults[1] = 2;
    bucket_defaults[2] = 3;
    bucket_defaults[3] = 4;

    /* no labels */
    cmt_histogram_set_buckets_default(h, ts, bucket_defaults, 0, NULL);
    prometheus_encode_test(cmt);

    /* defined labels: add a custom label value */
    cmt_histogram_set_buckets_default(h, ts, bucket_defaults,
                                      1, (char *[]) {"my_val"});
    prometheus_encode_test(cmt);

    /* static label: register static label for the context */
    cmt_label_add(cmt, "static", "test");
    prometheus_encode_test(cmt);

    cmt_destroy(cmt);

    free(bucket_defaults);
}

TEST_LIST = {
    {"histogram"   , test_histogram},
    {"set_defaults", test_set_defaults},
    { 0 }
};
