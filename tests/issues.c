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
#include <cmetrics/cmt_counter.h>
#include <cmetrics/cmt_encode_msgpack.h>
#include <cmetrics/cmt_decode_msgpack.h>
#include <cmetrics/cmt_encode_text.h>

#include "cmt_tests.h"

static struct cmt *generate_encoder_test_data()
{
    double val;
    uint64_t ts;
    struct cmt *cmt;
    struct cmt_counter *c1;
    struct cmt_counter *c2;
    struct cmt_counter *c3;

    cmt = cmt_create();

    c1 = cmt_counter_create(cmt, "kubernetes", "", "load", "Network load",
                            2, (char *[]) {"hostname", "app"});
    cmt_counter_set(c1, ts, 10, 0, NULL);

    c2 = cmt_counter_create(cmt, "kubernetes", "", "cpu", "CPU load",
                            2, (char *[]) {"hostname", "app"});
    cmt_counter_set(c2, ts, 10, 0, NULL);

    return cmt;
}


void test_issue_54()
{
    int ret;
    size_t offset = 0;
    char *mp1_buf = NULL;
    size_t mp1_size = 1;
    char *mp2_buf = NULL;
    size_t mp2_size = 2;
    struct cmt *cmt1 = NULL;
    struct cmt *cmt2 = NULL;
    cmt_sds_t text_result;
    const char expected_text[] = "1970-01-01T00:00:00.000000000Z kubernetes_load{tag1=\"tag1\",tag2=\"tag2\"} = 10\n" \
                                 "1970-01-01T00:00:00.000000000Z kubernetes_cpu{tag1=\"tag1\",tag2=\"tag2\"} = 10\n";

    cmt_initialize();

    /* Generate context with data */
    cmt1 = generate_encoder_test_data();
    TEST_CHECK(NULL != cmt1);

    /* append static labels */
    cmt_label_add(cmt1, "tag1", "tag1");
    cmt_label_add(cmt1, "tag2", "tag2");

    /* CMT1 -> Msgpack */
    ret = cmt_encode_msgpack_create(cmt1, &mp1_buf, &mp1_size);
    TEST_CHECK(0 == ret);

    /* Msgpack -> CMT2 */
    ret = cmt_decode_msgpack_create(&cmt2, mp1_buf, mp1_size, &offset);
    TEST_CHECK(0 == ret);

    text_result = cmt_encode_text_create(cmt2);
    TEST_CHECK(NULL != text_result);
    TEST_CHECK(0 == strcmp(text_result, expected_text));

    cmt_destroy(cmt1);
    cmt_encode_text_destroy(text_result);
    cmt_decode_msgpack_destroy(cmt2);
    cmt_encode_msgpack_destroy(mp1_buf);
    cmt_encode_msgpack_destroy(mp2_buf);

    // TEST_CHECK(global_counter == EXPECTED_VALUE);
}

TEST_LIST = {
    {"issue_54", test_issue_54},
    { 0 }
};
