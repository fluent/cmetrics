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
#include <cmetrics/cmt_encode_prometheus.h>
#include <cmetrics/cmt_encode_text.h>

#include "cmt_tests.h"

static struct cmt *generate_encoder_test_data()
{
    int ret;
    double val;
    uint64_t ts;
    struct cmt *cmt;
    struct cmt_counter *c;

    cmt = cmt_create();

    c = cmt_counter_create(cmt, "kubernetes", "network", "load", "Network load",
                           2, (char *[]) {"hostname", "app"});

    ts = cmt_time_now();

    ret = cmt_counter_get_val(c, 0, NULL, &val);
    ret = cmt_counter_inc(c, ts, 0, NULL);
    ret = cmt_counter_add(c, ts, 2, 0, NULL);
    ret = cmt_counter_get_val(c, 0, NULL, &val);

    ret = cmt_counter_inc(c, ts, 2, (char *[]) {"localhost", "cmetrics"});
    ret = cmt_counter_get_val(c, 2, (char *[]) {"localhost", "cmetrics"}, &val);
    ret = cmt_counter_add(c, ts, 10.55, 2, (char *[]) {"localhost", "test"});
    ret = cmt_counter_get_val(c, 2, (char *[]) {"localhost", "test"}, &val);
    ret = cmt_counter_set(c, ts, 12.15, 2, (char *[]) {"localhost", "test"});
    ret = cmt_counter_set(c, ts, 1, 2, (char *[]) {"localhost", "test"});

    return cmt;
}

/*
 * perform the following data encoding and compare msgpack buffsers
 *
 * CMT -> MSGPACK -> CMT -> MSGPACK
 *          |                  |
 *          |---> compare <----|
 */

void test_cmt_to_msgpack()
{
    int ret;
    char *mp1_buf = NULL;
    size_t mp1_size = 1;
    char *mp2_buf = NULL;
    size_t mp2_size = 2;
    struct cmt *cmt1 = NULL;
    struct cmt *cmt2 = NULL;

    /* Generate context with data */
    cmt1 = generate_encoder_test_data();
    TEST_CHECK(cmt1 != NULL);

    /* CMT1 -> Msgpack */
    ret = cmt_encode_msgpack(cmt1, &mp1_buf, &mp1_size);
    TEST_CHECK(ret == 0);

    /* Msgpack -> CMT2 */
    ret = cmt_decode_msgpack(&cmt2, mp1_buf, mp1_size);
    TEST_CHECK(ret == 0);

    /* CMT2 -> Msgpack */
    ret = cmt_encode_msgpack(cmt2, &mp2_buf, &mp2_size);
    TEST_CHECK(ret == 0);

    /* Compare msgpacks */
    TEST_CHECK(mp1_size == mp2_size);
    TEST_CHECK(memcmp(mp1_buf, mp2_buf, mp1_size) == 0);

    cmt_destroy(cmt1);
    cmt_destroy(cmt2);
    free(mp1_buf);
    free(mp2_buf);
}

/*
 * perform the following data encoding and compare msgpack buffsers
 *
 * CMT -> MSGPACK -> CMT -> TEXT
 * CMT -> TEXT
 *          |                  |
 *          |---> compare <----|
 */

void test_cmt_to_msgpack_integrity()
{
    int ret;
    char *mp1_buf = NULL;
    size_t mp1_size = 0;
    char *text1_buf = NULL;
    size_t text1_size = 0;
    char *text2_buf = NULL;
    size_t text2_size = 0;
    struct cmt *cmt1 = NULL;
    struct cmt *cmt2 = NULL;

    /* Generate context with data */
    cmt1 = generate_encoder_test_data();
    TEST_CHECK(cmt1 != NULL);

    /* CMT1 -> Msgpack */
    ret = cmt_encode_msgpack(cmt1, &mp1_buf, &mp1_size);
    TEST_CHECK(ret == 0);

    /* Msgpack -> CMT2 */
    ret = cmt_decode_msgpack(&cmt2, mp1_buf, mp1_size);
    TEST_CHECK(ret == 0);

    /* CMT1 -> Text */
    text1_buf = cmt_encode_text_create(cmt1, 1);
    TEST_CHECK(text1_buf != NULL);

    /* CMT2 -> Text */
    text2_buf = cmt_encode_text_create(cmt2, 1);
    TEST_CHECK(text2_buf != NULL);

    /* Compare msgpacks */
    TEST_CHECK(memcmp(text1_buf, text2_buf, text1_size) == 0);

    cmt_destroy(cmt1);
    cmt_destroy(cmt2);

    free(mp1_buf);

    cmt_encode_text_destroy(text1_buf);
    cmt_encode_text_destroy(text2_buf);
}

TEST_LIST = {
    {"cmt_msgpack", test_cmt_to_msgpack},
    {"cmt_msgpack_integrity", test_cmt_to_msgpack_integrity},
    { 0 }
};
