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
#include <cmetrics/cmt_decode_prometheus.h>
#include <cmetrics/cmt_encode_prometheus.h>

#include "cmetrics/cmt_counter.h"
#include "cmetrics/cmt_sds.h"
#include "cmt_decode_prometheus_parser.h"
#include "cmt_tests.h"
#include "lib/acutest/acutest.h"

struct fixture {
    yyscan_t scanner;
    YY_BUFFER_STATE buf;
    YYSTYPE lval;
    struct cmt_decode_prometheus_context context;
    const char *text;
};

struct fixture *init(int start_token, const char *test)
{
    cmt_initialize();
    struct fixture *f = malloc(sizeof(*f));
    memset(f, 0, sizeof(*f));
    f->context.cmt = cmt_create();
    f->context.start_token = start_token;
    cmt_decode_prometheus_lex_init(&f->scanner);
    f->buf = cmt_decode_prometheus__scan_string(test, f->scanner);
    return f;
}

void destroy(struct fixture *f)
{
    cmt_decode_prometheus__delete_buffer(f->buf, f->scanner);
    cmt_decode_prometheus_lex_destroy(f->scanner);
    cmt_destroy(f->context.cmt);
    free(f);
}

int parse(struct fixture *f)
{
    return cmt_decode_prometheus_parse(f->scanner, &f->context);
}

void test_header_help()
{
    struct fixture *f = init(START_HEADER,
            "# HELP cmt_labels_test Static labels test\n"
            );

    TEST_CHECK(parse(f) == 0);

    TEST_CHECK(strcmp(f->context.metric.ns, "cmt") == 0);
    TEST_CHECK(strcmp(f->context.metric.subsystem, "labels") == 0);
    TEST_CHECK(strcmp(f->context.metric.name, "test") == 0);
    TEST_CHECK(strcmp(f->context.metric.docstring, "Static labels test") == 0);
    TEST_CHECK(f->context.metric.type == 0);
    cmt_sds_destroy(f->context.metric.name_orig);
    cmt_sds_destroy(f->context.metric.docstring);
    free(f->context.metric.ns);

    destroy(f);
}

void test_header_type()
{
    struct fixture *f = init(START_HEADER,
            "# TYPE cmt_labels_test counter\n"
            );
    TEST_CHECK(parse(f) == 0);

    TEST_CHECK(strcmp(f->context.metric.ns, "cmt") == 0);
    TEST_CHECK(strcmp(f->context.metric.subsystem, "labels") == 0);
    TEST_CHECK(strcmp(f->context.metric.name, "test") == 0);
    TEST_CHECK(f->context.metric.type == COUNTER);
    TEST_CHECK(f->context.metric.docstring == NULL);
    cmt_sds_destroy(f->context.metric.name_orig);
    free(f->context.metric.ns);

    destroy(f);
}

void test_header_help_type()
{
    struct fixture *f = init(START_HEADER,
            "# HELP cmt_labels_test Static labels test\n"
            "# TYPE cmt_labels_test summary\n"
            );

    TEST_CHECK(parse(f) == 0);

    TEST_CHECK(strcmp(f->context.metric.docstring, "Static labels test") == 0);
    TEST_CHECK(strcmp(f->context.metric.ns, "cmt") == 0);
    TEST_CHECK(strcmp(f->context.metric.subsystem, "labels") == 0);
    TEST_CHECK(strcmp(f->context.metric.name, "test") == 0);
    TEST_CHECK(f->context.metric.type == SUMMARY);
    cmt_sds_destroy(f->context.metric.name_orig);
    cmt_sds_destroy(f->context.metric.docstring);
    free(f->context.metric.ns);

    destroy(f);
}

void test_header_type_help()
{
    struct fixture *f = init(START_HEADER,
            "# TYPE cmt_labels_test gauge\n"
            "# HELP cmt_labels_test Static labels test\n"
            );

    TEST_CHECK(parse(f) == 0);

    TEST_CHECK(strcmp(f->context.metric.docstring, "Static labels test") == 0);
    TEST_CHECK(strcmp(f->context.metric.ns, "cmt") == 0);
    TEST_CHECK(strcmp(f->context.metric.subsystem, "labels") == 0);
    TEST_CHECK(strcmp(f->context.metric.name, "test") == 0);
    TEST_CHECK(f->context.metric.type == GAUGE);
    cmt_sds_destroy(f->context.metric.name_orig);
    cmt_sds_destroy(f->context.metric.docstring);
    free(f->context.metric.ns);

    destroy(f);
}

void test_labels()
{
    struct fixture *f = init(START_LABELS, "dev=\"Calyptia\",lang=\"C\"");
    TEST_CHECK(parse(f) == 0);
    TEST_CHECK(f->context.metric.label_count == 2);
    TEST_CHECK(strcmp(f->context.metric.labels[0], "dev") == 0);
    TEST_CHECK(strcmp(f->context.metric.samples[0].label_values[0], "Calyptia") == 0);
    TEST_CHECK(strcmp(f->context.metric.labels[1], "lang") == 0);
    TEST_CHECK(strcmp(f->context.metric.samples[0].label_values[1], "C") == 0);
    cmt_sds_destroy(f->context.metric.labels[0]);
    cmt_sds_destroy(f->context.metric.samples[0].label_values[0]);
    cmt_sds_destroy(f->context.metric.labels[1]);
    cmt_sds_destroy(f->context.metric.samples[0].label_values[1]);
    destroy(f);
}

void test_labels_trailing_comma()
{
    struct fixture *f = init(START_LABELS, "dev=\"Calyptia\",lang=\"C\",");
    TEST_CHECK(parse(f) == 0);
    TEST_CHECK(f->context.metric.label_count == 2);
    TEST_CHECK(strcmp(f->context.metric.labels[0], "dev") == 0);
    TEST_CHECK(strcmp(f->context.metric.samples[0].label_values[0], "Calyptia") == 0);
    TEST_CHECK(strcmp(f->context.metric.labels[1], "lang") == 0);
    TEST_CHECK(strcmp(f->context.metric.samples[0].label_values[1], "C") == 0);
    cmt_sds_destroy(f->context.metric.labels[0]);
    cmt_sds_destroy(f->context.metric.samples[0].label_values[0]);
    cmt_sds_destroy(f->context.metric.labels[1]);
    cmt_sds_destroy(f->context.metric.samples[0].label_values[1]);
    destroy(f);
}

void test_sample()
{
    cmt_sds_t result;
    const char expected[] = (
            "# HELP cmt_labels_test some docstring\n"
            "# TYPE cmt_labels_test counter\n"
            "cmt_labels_test{dev=\"Calyptia\",lang=\"C\"} 1 0\n"
            );

    struct fixture *f = init(0,
            "# HELP cmt_labels_test some docstring\n"
            "# TYPE cmt_labels_test counter\n"
            "cmt_labels_test{dev=\"Calyptia\",lang=\"C\",} 1 0\n"
            );

    TEST_CHECK(parse(f) == 0);
    result = cmt_encode_prometheus_create(f->context.cmt, CMT_TRUE);
    TEST_CHECK(strcmp(result, expected) == 0);
    cmt_sds_destroy(result);

    destroy(f);
}

void test_samples()
{
    cmt_sds_t result;
    const char expected[] = (
            "# HELP cmt_labels_test some docstring\n"
            "# TYPE cmt_labels_test counter\n"
            "cmt_labels_test{dev=\"Calyptia\",lang=\"C\"} 5 999999\n"
            "cmt_labels_test{dev=\"Calyptia\",lang=\"C++\"} 6 7777\n"

            );

    struct fixture *f = init(0,
            "# HELP cmt_labels_test some docstring\n"
            "# TYPE cmt_labels_test counter\n"
            "cmt_labels_test{dev=\"Calyptia\",lang=\"C\",} 5 999999\n"
            "cmt_labels_test{dev=\"Calyptia\",lang=\"C++\"} 6 7777\n"
            );

    TEST_CHECK(parse(f) == 0);
    result = cmt_encode_prometheus_create(f->context.cmt, CMT_TRUE);
    TEST_CHECK(strcmp(result, expected) == 0);
    cmt_sds_destroy(result);

    destroy(f);
}

void test_escape_sequences()
{
    cmt_sds_t result;
    // this "expected" value is not correct since the encoder doesn't handle
    // escape sequences. I'm adding it here to verify that the parser is
    // correctly treating escape sequences
    const char expected[] = (
        "# HELP msdos_file_access_time_seconds (no information)\n"
        "# TYPE msdos_file_access_time_seconds untyped\n"
        "msdos_file_access_time_seconds{path=\"C:\\DIR\\FILE.TXT\",error=\"Cannot find file:\n\"FILE.TXT\"\"} 1458255915 0\n"
        );

    struct fixture *f = init(0,
        "# Escaping in label values:\n"
        "msdos_file_access_time_seconds{path=\"C:\\\\DIR\\\\FILE.TXT\",error=\"Cannot find file:\\n\\\"FILE.TXT\\\"\"} 1.458255915e9\n"
        );

    TEST_CHECK(parse(f) == 0);
    result = cmt_encode_prometheus_create(f->context.cmt, CMT_TRUE);
    TEST_CHECK(strcmp(result, expected) == 0);
    cmt_sds_destroy(result);

    destroy(f);
}

void test_metric_without_labels()
{ 
    cmt_sds_t result;

    const char expected[] =
        "# HELP metric_without_timestamp_and_labels (no information)\n"
        "# TYPE metric_without_timestamp_and_labels untyped\n"
        "metric_without_timestamp_and_labels 12.470000000000001 0\n"
        ;

    struct fixture *f = init(0,
        "# Minimalistic line:\n"
        "metric_without_timestamp_and_labels 12.47\n"
        );

    TEST_CHECK(parse(f) == 0);
    result = cmt_encode_prometheus_create(f->context.cmt, CMT_TRUE);
    printf("\n%s\n", result);
    TEST_CHECK(strcmp(result, expected) == 0);
    cmt_sds_destroy(result);

    destroy(f);
}

void test_complete()
{
    int status;
    size_t offset;
    cmt_sds_t result;
    struct cmt *cmt;
    const char in_buf[] =
        "# TYPE http_requests_total counter\n"
        "# HELP http_requests_total The total number of HTTP requests.\n"
        "http_requests_total{method=\"post\",code=\"200\"} 1027 1395066363000\n"
        "http_requests_total{method=\"post\",code=\"400\"}    3 1395066363000\n"
        "\n"
        "# Escaping in label values:\n"
        "msdos_file_access_time_seconds{path=\"C:\\\\DIR\\\\FILE.TXT\",error=\"Cannot find file:\\n\\\"FILE.TXT\\\"\"} 1.458255915e9\n"
        ;
    const char expected[] =
        "# HELP http_requests_total The total number of HTTP requests.\n"
        "# TYPE http_requests_total counter\n"
        "http_requests_total{method=\"post\",code=\"200\"} 1027 1395066363000\n"
        "http_requests_total{method=\"post\",code=\"400\"} 3 1395066363000\n"
        "# HELP msdos_file_access_time_seconds (no information)\n"
        "# TYPE msdos_file_access_time_seconds untyped\n"
        "msdos_file_access_time_seconds{path=\"C:\\DIR\\FILE.TXT\",error=\"Cannot find file:\n\"FILE.TXT\"\"} 1458255915 0\n"
        ;

    cmt_initialize();
    status = cmt_decode_prometheus_create(&cmt, in_buf);
    TEST_CHECK(status == 0);
    result = cmt_encode_prometheus_create(cmt, CMT_TRUE);
    TEST_CHECK(strcmp(result, expected) == 0);
    printf("\n%s\n", result);
    cmt_sds_destroy(result);
    cmt_decode_prometheus_destroy(cmt);
}


TEST_LIST = {
    {"header_help", test_header_help},
    {"header_type", test_header_type},
    {"header_help_type", test_header_help_type},
    {"header_type_help", test_header_type_help},
    {"labels", test_labels},
    {"labels_trailing_comma", test_labels_trailing_comma},
    {"sample", test_sample},
    {"samples", test_samples},
    {"escape_sequences", test_escape_sequences},
    {"metric_without_labels", test_metric_without_labels},
    {"complete", test_complete},
    { 0 }
};
