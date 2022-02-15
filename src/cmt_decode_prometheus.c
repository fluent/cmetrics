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

#include <stdarg.h>
#include <stdint.h>

#include <cmetrics/cmetrics.h>
#include <cmetrics/cmt_gauge.h>
#include <cmetrics/cmt_untyped.h>
#include <cmetrics/cmt_counter.h>
#include <cmetrics/cmt_sds.h>
#include <cmetrics/cmt_decode_prometheus.h>
#include <monkey/mk_core/mk_list.h>

#include <cmt_decode_prometheus_parser.h>
#include <stdio.h>

static void reset_context(struct cmt_decode_prometheus_context *context)
{
    int i;
    struct mk_list *head;
    struct mk_list *tmp;
    struct cmt_decode_prometheus_context_sample *sample;

    while (mk_list_is_empty(&context->metric.samples) != 0) {
        sample = mk_list_entry_first(&context->metric.samples,
                struct cmt_decode_prometheus_context_sample, _head);
        for (i = 0; i < sample->label_count; i++) {
            cmt_sds_destroy(sample->label_values[i]);
        }
        mk_list_del(&sample->_head);
        free(sample);
    }

    for (i = 0; i < context->metric.label_count; i++) {
        cmt_sds_destroy(context->metric.labels[i]);
    }

    if (context->metric.ns) {
        free(context->metric.ns);
    }

    cmt_sds_destroy(context->strbuf);
    context->strbuf = NULL;
    cmt_sds_destroy(context->metric.name_orig);
    cmt_sds_destroy(context->metric.docstring);
    memset(&context->metric,
            0,
            sizeof(struct cmt_decode_prometheus_context_metric));
    mk_list_init(&context->metric.samples);
}

int cmt_decode_prometheus_create(struct cmt **out_cmt, const char *in_buf,
        char *errbuf, size_t errbuf_size)
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
    context.errbuf = errbuf;
    context.errbuf_size = errbuf_size;
    mk_list_init(&(context.metric.samples));
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
        if (context.errcode) {
            result = context.errcode;
        }
        reset_context(&context);
    }

    cmt_decode_prometheus__delete_buffer(buf, scanner);
    cmt_decode_prometheus_lex_destroy(scanner);

    return result;
}

void cmt_decode_prometheus_destroy(struct cmt *cmt)
{
    cmt_destroy(cmt);
}

static int report_error(struct cmt_decode_prometheus_context *context,
                         int errcode,
                         const char *format, ...)
{
    va_list args;
    va_start(args, format);
    context->errcode = errcode;
    if (context->errbuf && context->errbuf_size) {
        vsnprintf(context->errbuf, context->errbuf_size - 1, format, args);
    }
    va_end(args);
    return errcode;
}

static int split_metric_name(struct cmt_decode_prometheus_context *context,
        cmt_sds_t metric_name, char **ns,
        char **subsystem, char **name)
{
    // split the name
    *ns = strdup(metric_name);
    if (!*ns) {
        return report_error(context,
                CMT_DECODE_PROMETHEUS_ALLOCATION_ERROR,
                "memory allocation failed");
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

// Use this helper function to return a stub value for docstring when it is not
// available. This is necessary for now because the metric constructors require
// a docstring, even though it is not required by prometheus spec.
static const char *get_docstring(struct cmt_decode_prometheus_context *context)
{
    return context->metric.docstring ? context->metric.docstring : "(no information)";
}

static int add_metric_counter(struct cmt_decode_prometheus_context *context)
{
    int i;
    size_t label_count;
    struct cmt_counter *c;
    struct mk_list *head;
    struct mk_list *tmp;
    struct cmt_decode_prometheus_context_sample *sample;

    c = cmt_counter_create(context->cmt,
            context->metric.ns,
            context->metric.subsystem,
            context->metric.name,
            get_docstring(context),
            context->metric.label_count,
            context->metric.labels);

    if (!c) {
        return report_error(context,
                CMT_DECODE_PROMETHEUS_CMT_CREATE_ERROR,
                "cmt_counter_create failed");
    }

    mk_list_foreach_safe(head, tmp, &context->metric.samples) {
        sample = mk_list_entry(head, struct cmt_decode_prometheus_context_sample, _head);
        label_count = sample->label_count;
        if (cmt_counter_set(c,
                    sample->timestamp,
                    sample->value,
                    label_count,
                    label_count ? sample->label_values : NULL)) {
            return report_error(context,
                    CMT_DECODE_PROMETHEUS_CMT_SET_ERROR,
                    "cmt_counter_set failed");
        }
    }

    return 0;
}

static int add_metric_gauge(struct cmt_decode_prometheus_context *context)
{
    int i;
    size_t label_count;
    struct cmt_gauge *c;
    struct mk_list *head;
    struct mk_list *tmp;
    struct cmt_decode_prometheus_context_sample *sample;

    c = cmt_gauge_create(context->cmt,
            context->metric.ns,
            context->metric.subsystem,
            context->metric.name,
            get_docstring(context),
            context->metric.label_count,
            context->metric.labels);

    if (!c) {
        return report_error(context,
                CMT_DECODE_PROMETHEUS_CMT_CREATE_ERROR,
                "cmt_gauge_create failed");
    }

    mk_list_foreach_safe(head, tmp, &context->metric.samples) {
        sample = mk_list_entry(head, struct cmt_decode_prometheus_context_sample, _head);
        label_count = sample->label_count;
        if (cmt_gauge_set(c,
                    sample->timestamp,
                    sample->value,
                    label_count,
                    label_count ? sample->label_values : NULL)) {
            return report_error(context,
                    CMT_DECODE_PROMETHEUS_CMT_SET_ERROR,
                    "cmt_gauge_set failed");
        }
    }

    return 0;
}

static int add_metric_untyped(struct cmt_decode_prometheus_context *context)
{
    int i;
    size_t label_count;
    struct cmt_untyped *c;
    struct mk_list *head;
    struct mk_list *tmp;
    struct cmt_decode_prometheus_context_sample *sample;

    c = cmt_untyped_create(context->cmt,
            context->metric.ns,
            context->metric.subsystem,
            context->metric.name,
            get_docstring(context),
            context->metric.label_count,
            context->metric.labels);

    if (!c) {
        return report_error(context,
                CMT_DECODE_PROMETHEUS_CMT_CREATE_ERROR,
                "cmt_untyped_create failed");
    }

    mk_list_foreach_safe(head, tmp, &context->metric.samples) {
        sample = mk_list_entry(head, struct cmt_decode_prometheus_context_sample, _head);
        label_count = sample->label_count;
        if (cmt_untyped_set(c,
                    sample->timestamp,
                    sample->value,
                    label_count,
                    label_count ? sample->label_values : NULL)) {
            return report_error(context,
                    CMT_DECODE_PROMETHEUS_CMT_SET_ERROR,
                    "cmt_untyped_set failed");
        }
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
        case GAUGE:
            rv = add_metric_gauge(context);
            break;
        case HISTOGRAM:
        case SUMMARY:
            rv = report_error(context,
                    CMT_DECODE_PROMETHEUS_PARSE_UNSUPPORTED_TYPE,
                    "unsupported metric type: %s",
                    context->metric.type == HISTOGRAM ? "histogram" : "summary");
            break;
        default:
            rv = add_metric_untyped(context);
            break;
    }

    reset_context(context);

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
        ret = split_metric_name(context, metric_name,
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
    int lindex;
    int i;
    cmt_sds_t label;
    struct cmt_decode_prometheus_context_sample *sample;

    if (context->metric.label_count >= CMT_DECODE_PROMETHEUS_MAX_LABEL_COUNT) {
        cmt_sds_destroy(name);
        cmt_sds_destroy(value);
        return report_error(context,
                CMT_DECODE_PROMETHEUS_MAX_LABEL_COUNT_EXCEEDED,
                "maximum number of labels exceeded");
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

    sample = mk_list_entry_last(&context->metric.samples,
            struct cmt_decode_prometheus_context_sample, _head);
    // Uncomment the following two lines when cmetrics support the following usage:
    //     struct cmt *cmt = cmt_create();
    //     struct cmt_counter *c = cmt_counter_create(cmt, "metric", "name", "total", "Some metric description", 4, (char *[]) {"a", "b", "c", "d"});
    //     cmt_counter_set(c, 0, 5, 4, (char *[]) { "0", "1", NULL, NULL });
    //     cmt_counter_set(c, 0, 7, 4, (char *[]) { NULL, NULL, "2", "3" });
    //
    // In other words, when cmetrics allows samples of the same metric with
    // different keys
    //
    // samples->label_values[i] = value;
    lindex = sample->label_count;
    sample->label_values[lindex] = value;
    sample->label_count++;
    return 0;
}

static int sample_start(struct cmt_decode_prometheus_context *context)
{
    struct cmt_decode_prometheus_context_sample *sample;

    sample = malloc(sizeof(*sample));
    if (!sample) {
        return report_error(context,
                CMT_DECODE_PROMETHEUS_ALLOCATION_ERROR,
                "memory allocation failed");
    }

    memset(sample, 0, sizeof(*sample));
    mk_list_add(&sample->_head, &context->metric.samples);
    return 0;
}

static void parse_sample(
        struct cmt_decode_prometheus_context *context,
        double value,
        uint64_t timestamp)
{
    struct cmt_decode_prometheus_context_sample *sample;
    sample = mk_list_entry_last(&context->metric.samples,
            struct cmt_decode_prometheus_context_sample, _head);
    sample->value = value;
    // prometheus text format metrics are in milliseconds, while cmetrics is in
    // nanoseconds, so multiply by 10e5
    sample->timestamp = timestamp * 10e5;
}

// called automatically by the generated parser code on error
static int cmt_decode_prometheus_error(void *yyscanner,
                                       struct cmt_decode_prometheus_context *context,
                                       const char *msg)
{
    report_error(context, CMT_DECODE_PROMETHEUS_SYNTAX_ERROR, msg);
    return 0;
}
