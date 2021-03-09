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

#ifndef CMT_GAUGE_H
#define CMT_GAUGE_H

#include <cmetrics/cmetrics.h>
#include <cmetrics/cmt_opts.h>

struct cmt_gauge {
    uint64_t val;
    struct cmt_opts opts;
    struct mk_list _head;
};

struct cmt_gauge *cmt_gauge_create(struct cmt *cmt,
                                   char *namespace, char *subsystem, char *name,
                                   char *help);

int cmt_gauge_destroy(struct cmt_gauge *gauge);
void cmt_gauge_set(struct cmt_gauge *gauge, double val);
void cmt_gauge_inc(struct cmt_gauge *gauge);
void cmt_gauge_dec(struct cmt_gauge *gauge);
void cmt_gauge_add(struct cmt_gauge *gauge, double val);
void cmt_gauge_sub(struct cmt_gauge *gauge, double val);
double cmt_gauge_get_value(struct cmt_gauge *gauge);

#endif
