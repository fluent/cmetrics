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
#include <cmetrics/cmt_counter.h>

struct cmt_counter *cmt_counter_create(struct cmt *cmt,
                                       char *namespace, char *subsystem,
                                       char *name, char *help)
{
    struct cmt_counter *counter;

    if (!name || !help) {
        return NULL;
    }

    if (strlen(name) == 0 || strlen(help) == 0) {
        return NULL;
    }

    counter = calloc(1, sizeof(struct cmt_counter));
    if (!counter) {
        cmt_errno();
        return NULL;
    }
    counter->val = 0;
    cmt_opts_init(&counter->opts, namespace, subsystem, name, help);
    mk_list_add(&counter->_head, &cmt->counters);

    return counter;
}

int cmt_counter_destroy(struct cmt_counter *counter)
{
    mk_list_del(&counter->_head);
    cmt_opts_exit(&counter->opts);
    free(counter);
}

static inline void add(struct cmt_counter *counter, double val)
{
    uint64_t tmp;

    tmp = cmt_math_d64_to_uint64(val);
    __atomic_fetch_add(&counter->val, tmp, __ATOMIC_RELAXED);
}

void cmt_counter_inc(struct cmt_counter *counter)
{
    add(counter, 1);
}

void cmt_counter_add(struct cmt_counter *counter, double val)
{
    add(counter, val);
}
