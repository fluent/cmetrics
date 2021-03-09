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
#include <cmetrics/cmt_log.h>
#include <cmetrics/cmt_math.h>
#include <cmetrics/cmt_gauge.h>

struct cmt_gauge *cmt_gauge_create(struct cmt *cmt,
                                   char *namespace, char *subsystem, char *name,
                                   char *help)
{
    struct cmt_gauge *gauge;

    if (!name || !help) {
        return NULL;
    }

    if (strlen(name) == 0 || strlen(help) == 0) {
        return NULL;
    }

    gauge = calloc(1, sizeof(struct cmt_gauge));
    if (!gauge) {
        cmt_errno();
        return NULL;
    }
    gauge->val = 0;
    cmt_opts_init(&gauge->opts, namespace, subsystem, name, help);
    mk_list_add(&gauge->_head, &cmt->gauges);

    return gauge;
}

int cmt_gauge_destroy(struct cmt_gauge *gauge)
{
    mk_list_del(&gauge->_head);
    cmt_opts_exit(&gauge->opts);
    free(gauge);
}

static inline void add(struct cmt_gauge *gauge, double val)
{
    uint64_t tmp;

    tmp = cmt_math_d64_to_uint64(val);
    __atomic_fetch_add(&gauge->val, tmp, __ATOMIC_RELAXED);
}

void cmt_gauge_set(struct cmt_gauge *gauge, double val)
{
    uint64_t tmp;

    tmp = cmt_math_d64_to_uint64(val);
    __atomic_store_n(&gauge->val, tmp, __ATOMIC_RELAXED);
}

void cmt_gauge_inc(struct cmt_gauge *gauge)
{
    add(gauge, 1);
}

void cmt_gauge_dec(struct cmt_gauge *gauge)
{
    add(gauge, -1);
}

void cmt_gauge_add(struct cmt_gauge *gauge, double val)
{
    add(gauge, val);
}

void cmt_gauge_sub(struct cmt_gauge *gauge, double val)
{
    add(gauge, val * -1);
}

double cmt_gauge_get_value(struct cmt_gauge *gauge)
{
    uint64_t val;

    __atomic_load(&gauge->val, &val, __ATOMIC_RELAXED);
    return (double) val;
}
