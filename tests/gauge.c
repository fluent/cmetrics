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

#include "cmt_tests.h"

void test_gauge()
{
    double val;
    struct cmt *cmt;
    struct cmt_gauge *g;

    cmt = cmt_create();
    TEST_CHECK(cmt != NULL);

    /* Create a gauge metric type */
    g = cmt_gauge_create(cmt, "kubernetes", "network", "load", "Network load");
    TEST_CHECK(g != NULL);

    /* Default value */
    val = cmt_gauge_get_value(g);
    TEST_CHECK(val == 0.0);

    /* Increment by one */
    cmt_gauge_inc(g);
    val = cmt_gauge_get_value(g);
    TEST_CHECK(val == 1.0);

    /* Decrement by one */
    cmt_gauge_dec(g);
    val = cmt_gauge_get_value(g);
    TEST_CHECK(val == 0.0);

    /* Add two */
    cmt_gauge_add(g, 2);
    val = cmt_gauge_get_value(g);
    TEST_CHECK(val == 2.0);

    /* Sub two */
    cmt_gauge_sub(g, 2);
    val = cmt_gauge_get_value(g);
    TEST_CHECK(val == 0.0);

    cmt_destroy(cmt);
}

TEST_LIST = {
    {"basic", test_gauge},
    { 0 }
};
