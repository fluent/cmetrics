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
#include <cmetrics/cmt_math.h>

static inline void add(struct cmt_metric *metric, uint64_t timestamp, double val)
{
    double old;
    double new;
    uint64_t tmp;

    old = cmt_metric_get(metric);
    new = old + val;

    /*
     * This Bits handling is wrong... as a workaround we use
     * cmt_metric_set, but is not a real atomic operation
     * since the value might change while switching...
     */
    //FIXME tmp = cmt_math_d64_to_uint64(val);
    //FIXME __atomic_fetch_add(&metric->val, tmp, __ATOMIC_RELAXED);

    cmt_metric_set(metric, timestamp, new);
}

void cmt_metric_set(struct cmt_metric *metric, uint64_t timestamp, double val)
{
    uint64_t tmp;

    tmp = cmt_math_d64_to_uint64(val);
    __atomic_store_n(&metric->val, tmp, __ATOMIC_RELAXED);
    __atomic_store_n(&metric->timestamp, timestamp, __ATOMIC_RELAXED);
}

void cmt_metric_inc(struct cmt_metric *metric, uint64_t timestamp)
{
    add(metric, timestamp, 1);
}

void cmt_metric_dec(struct cmt_metric *metric, uint64_t timestamp)
{
    double volatile val = 1.0;

    add(metric, timestamp, val * -1);
}

void cmt_metric_add(struct cmt_metric *metric, uint64_t timestamp, double val)
{
    add(metric, timestamp, val);
}

void cmt_metric_sub(struct cmt_metric *metric, uint64_t timestamp, double val)
{
    add(metric, timestamp, (double volatile) val * -1);
}

double cmt_metric_get(struct cmt_metric *metric)
{
    uint64_t val;

    __atomic_load(&metric->val, &val, __ATOMIC_RELAXED);
    return cmt_math_uint64_to_d64(val);
}
