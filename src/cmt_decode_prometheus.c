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

#include "cmetrics/cmetrics.h"
#include "cmetrics/cmt_untyped.h"
#include "monkey/mk_core/mk_list.h"
#include <cmetrics/cmt_counter.h>
#include <cmetrics/cmt_sds.h>
#include <cmetrics/cmt_decode_prometheus.h>
#include <stdint.h>

int cmt_decode_prometheus_create(struct cmt **out_cmt, const char *in_buf)
{
    yyscan_t scanner;
    YY_BUFFER_STATE buf;
    struct cmt *cmt;
    struct cmt_decode_prometheus_context context;
    int result;

    cmt = cmt_create();

    if (cmt == NULL) {
        return CMT_DECODE_PROMETHEUS_ALLOCATION_ERROR;
    }

    memset(&context, 0, sizeof(context));
    context.cmt = cmt;
    cmt_decode_prometheus_lex_init(&scanner);
    buf = cmt_decode_prometheus__scan_string(in_buf, scanner);
    if (!buf) {
        cmt_destroy(cmt);
        return CMT_DECODE_PROMETHEUS_ALLOCATION_ERROR;
    }

    result = cmt_decode_prometheus_parse(scanner, &context);

    if (result == 0) {
        *out_cmt = cmt;
    }
    else {
        cmt_destroy(cmt);
    }

    cmt_decode_prometheus__delete_buffer(buf, scanner);
    cmt_decode_prometheus_lex_destroy(scanner);

    return result;
}

void cmt_decode_prometheus_destroy(struct cmt *cmt)
{
    cmt_destroy(cmt);
}

static int split_metric_name(cmt_sds_t metric_name, char **ns,
        char **subsystem, char **name)
{
    // split the name
    *ns = strdup(metric_name);
    if (!*ns) {
        return CMT_DECODE_PROMETHEUS_ALLOCATION_ERROR;
    }
    *subsystem = strchr(*ns, '_');
    if (!subsystem) {
        *name = metric_name;
        *ns = NULL;
    }
    else {
        **subsystem = 0;  // split
        (*subsystem)++;
        *name = strchr(*subsystem, '_');
        if (!(*name)) {
            *name = *subsystem;
            *subsystem = NULL;
        }
        else {
            **name = 0;
            (*name)++;
        }
    }
    return 0;
}

static int add_metric_counter(struct cmt_decode_prometheus_context *context)
{
    int i;
    size_t label_count;
    struct cmt_counter *c;

    c = cmt_counter_create(context->cmt,
            context->metric.ns,
            context->metric.subsystem,
            context->metric.name,
            context->metric.docstring,
            context->metric.label_count,
            context->metric.labels);


    for (i = 0; i < context->metric.sample_count; i++) {
        label_count = context->metric.samples[i].label_count;
        cmt_counter_set(c,
                context->metric.samples[i].timestamp,
                context->metric.samples[i].value,
                label_count,
                label_count ? context->metric.samples[i].label_values : NULL);
    }

    return 0;
}

static int add_metric_untyped(struct cmt_decode_prometheus_context *context)
{
    int i;
    size_t label_count;
    struct cmt_untyped *c;

    c = cmt_untyped_create(context->cmt,
            context->metric.ns,
            context->metric.subsystem,
            context->metric.name,
            context->metric.docstring ? context->metric.docstring : "(no information)",
            context->metric.label_count,
            context->metric.labels);


    for (i = 0; i < context->metric.sample_count; i++) {
        label_count = context->metric.samples[i].label_count;
        cmt_untyped_set(c,
                context->metric.samples[i].timestamp,
                context->metric.samples[i].value,
                label_count,
                label_count ? context->metric.samples[i].label_values : NULL);
    }

    return 0;
}

static int finish_metric(struct cmt_decode_prometheus_context *context)
{
    int i;
    int j;
    int rv;

    switch (context->metric.type) {
        case COUNTER:
            rv = add_metric_counter(context);
            break;
        default:
            rv = add_metric_untyped(context);
            break;
    }

    // after a metric is parsed, we free all allocated strings and reset the context
    for (i = 0; i < context->metric.sample_count; i++) {
        for (j = 0; j < context->metric.samples[i].label_count; j++) {
            cmt_sds_destroy(context->metric.samples[i].label_values[j]);
        }
    }
    for (i = 0; i < context->metric.label_count; i++) {
        cmt_sds_destroy(context->metric.labels[i]);
    }
    free(context->metric.ns);
    cmt_sds_destroy(context->metric.name_orig);
    cmt_sds_destroy(context->metric.docstring);
    memset(&context->metric,
            0,
            sizeof(struct cmt_decode_prometheus_context_metric));

    return rv;
}

static int parse_metric_name(
        struct cmt_decode_prometheus_context *context,
        cmt_sds_t metric_name)
{
    int ret = 0;

    if (context->metric.name_orig) {
        if (strcmp(context->metric.name_orig, metric_name)) {
            // new metric name means the current metric is finished
            ret = finish_metric(context);
        }
        else {
            // same metric with name already allocated, destroy and return
            cmt_sds_destroy(metric_name);
            return ret;
        }
    }

    if (!ret) {
        context->metric.name_orig = metric_name;
        ret = split_metric_name(metric_name,
                &(context->metric.ns),
                &(context->metric.subsystem),
                &(context->metric.name));
    }

    return ret;
}

static int parse_label(
        struct cmt_decode_prometheus_context *context,
        cmt_sds_t name, cmt_sds_t value)
{
    int sindex;
    int lindex;
    int i;
    cmt_sds_t label;

    if (context->metric.label_count >= CMT_DECODE_PROMETHEUS_MAX_LABEL_COUNT) {
        return -1;
    }

    // check if the label is already registered
    for (i = 0; i < context->metric.label_count; i++) {
        if (!strcmp(name, context->metric.labels[i])) {
            // found, free the name memory and use the existing one
            cmt_sds_destroy(name);
            name = context->metric.labels[i];
            break;
        }
    }
    if (i == context->metric.label_count) {
        // didn't found the label, add it now
        context->metric.labels[i] = name;
        context->metric.label_count++;
    }

    sindex = context->metric.sample_count;
    // Uncomment the following two lines when cmetrics support the following usage:
    //
    //     struct cmt *cmt = cmt_create();
    //     struct cmt_counter *c = cmt_counter_create(cmt, "metric", "name", "total", "Some metric description", 4, (char *[]) {"a", "b", "c", "d"});
    //     cmt_counter_set(c, 0, 5, 4, (char *[]) { "0", "1", NULL, NULL });
    //     cmt_counter_set(c, 0, 7, 4, (char *[]) { NULL, NULL, "2", "3" });
    //
    // context->metric.samples[sample_index].label_values[i] = value;
    lindex = context->metric.samples[sindex].label_count;
    context->metric.samples[sindex].label_values[lindex] = value;
    context->metric.samples[sindex].label_count++;
    return 0;
}

static void parse_sample(
        struct cmt_decode_prometheus_context *context,
        double value,
        uint64_t timestamp)
{
    size_t index = context->metric.sample_count;
    context->metric.samples[index].value = value;
    // prometheus text format metrics are in milliseconds, while cmetrics is in
    // nanoseconds, so multiply by 10e5
    context->metric.samples[index].timestamp = timestamp * 10e5;
    context->metric.sample_count++;
}

// called automatically by the generated parser code on error
static int cmt_decode_prometheus_error(void *yyscanner,
                                       struct cmt_decode_prometheus_context *context,
                                       const char *msg)
{
    fprintf(stderr, "%s\n", msg);
    return 0;
}
