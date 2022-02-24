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
#include <cmetrics/cmt_gauge.h>
#include <cmetrics/cmt_counter.h>
#include <cmetrics/cmt_decode_opentelemetry.h>
#include <cmetrics/cmt_encode_opentelemetry.h>

#include "cmt_tests.h"

static struct cmt *generate_simple_encoder_test_data()
{

    double val;
    uint64_t ts;
    struct cmt *cmt;
    struct cmt_counter *c;

    cmt = cmt_create();

    c = cmt_counter_create(cmt, "kubernetes", "network", "load", "Network load",
                           2, (char *[]) {"hostname", "app"});

    ts = 0;

    cmt_counter_get_val(c, 0, NULL, &val);
    cmt_counter_inc(c, ts, 0, NULL);
    cmt_counter_add(c, ts, 2, 0, NULL);
    cmt_counter_get_val(c, 0, NULL, &val);

    cmt_counter_inc(c, ts, 2, (char *[]) {"localhost", "cmetrics"});
    cmt_counter_get_val(c, 2, (char *[]) {"localhost", "cmetrics"}, &val);
    cmt_counter_add(c, ts, 10.55, 2, (char *[]) {"localhost", "test"});
    cmt_counter_get_val(c, 2, (char *[]) {"localhost", "test"}, &val);
    cmt_counter_set(c, ts, 12.15, 2, (char *[]) {"localhost", "test"});
    cmt_counter_set(c, ts, 1, 2, (char *[]) {"localhost", "test"});

    return cmt;
}

static struct cmt *generate_encoder_test_data()
{
    double val;
    uint64_t ts;
    struct cmt *cmt;
    struct cmt_counter *c1;
    struct cmt_counter *c2;
    struct cmt_counter *c3;

    cmt = cmt_create();

    c1 = cmt_counter_create(cmt, "kubernetes", "network", "load", "Network load",
                            2, (char *[]) {"hostname", "app"});

    ts = 0;

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


    c2 = cmt_counter_create(cmt, "kubernetes", "network", "cpu", "CPU load",
                            2, (char *[]) {"hostname", "app"});

    ts = 0;

    cmt_counter_get_val(c2, 0, NULL, &val);
    cmt_counter_inc(c2, ts, 0, NULL);
    cmt_counter_add(c2, ts, 2, 0, NULL);
    cmt_counter_get_val(c2, 0, NULL, &val);

    cmt_counter_inc(c2, ts, 2, (char *[]) {"localhost", "cmetrics"});
    cmt_counter_get_val(c2, 2, (char *[]) {"localhost", "cmetrics"}, &val);
    cmt_counter_add(c2, ts, 10.55, 2, (char *[]) {"localhost", "test"});
    cmt_counter_get_val(c2, 2, (char *[]) {"localhost", "test"}, &val);
    cmt_counter_set(c2, ts, 12.15, 2, (char *[]) {"localhost", "test"});
    cmt_counter_set(c2, ts, 1, 2, (char *[]) {"localhost", "test"});

    /* a counter without subsystem */
    c3 = cmt_counter_create(cmt, "kubernetes", "", "cpu", "CPU load",
                            2, (char *[]) {"hostname", "app"});
    cmt_counter_set(c3, ts, 10, 0, NULL);

    return cmt;
}

void test_opentelemetry()
{
    uint64_t ts;
    cmt_sds_t payload;
    struct cmt *cmt;
    struct cmt_counter *c;
    struct cmt_gauge *g;
    FILE *sample_file;
    size_t sample_file_size;
    int result;
    size_t offset;

    sample_file = fopen("payload.bin", "rb+");

    fseek(sample_file, 0, SEEK_END);
    sample_file_size = ftell(sample_file);
    fseek(sample_file, 0, SEEK_SET);

    TEST_CHECK(0 < sample_file_size);

    payload = sds_alloc(sample_file_size);

    result = fread(payload, 1, sample_file_size, sample_file);
    TEST_CHECK(result == sample_file_size);

    fclose(sample_file);

    offset = 0;
    result = cmt_decode_opentelemetry_create(&cmt, payload, sample_file_size,
                                             &offset);

    // cmt_destroy(cmt);
}

TEST_LIST = {
    {"opentelemetry", test_opentelemetry},
    { 0 }
};
