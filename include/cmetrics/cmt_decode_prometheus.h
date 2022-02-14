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

#ifndef CMT_DECODE_PROMETHEUS_H
#define CMT_DECODE_PROMETHEUS_H

#include <cmetrics/cmetrics.h>

#define CMT_DECODE_PROMETHEUS_SUCCESS                    0
#define CMT_DECODE_PROMETHEUS_INSUFFICIENT_DATA          1
#define CMT_DECODE_PROMETHEUS_INVALID_ARGUMENT_ERROR     2
#define CMT_DECODE_PROMETHEUS_ALLOCATION_ERROR           3
#define CMT_DECODE_PROMETHEUS_CORRUPT_INPUT_DATA_ERROR   4
#define CMT_DECODE_PROMETHEUS_CONSUME_ERROR              5
#define CMT_DECODE_PROMETHEUS_ENGINE_ERROR               6
#define CMT_DECODE_PROMETHEUS_PENDING_MAP_ENTRIES        7
#define CMT_DECODE_PROMETHEUS_PENDING_ARRAY_ENTRIES      8
#define CMT_DECODE_PROMETHEUS_UNEXPECTED_KEY_ERROR       9
#define CMT_DECODE_PROMETHEUS_UNEXPECTED_DATA_TYPE_ERROR 10
#define CMT_DECODE_PROMETHEUS_ERROR_CUTOFF               20

// due to a bug in flex/bison code generation, this must be defined before
// including the generated headers
#define YYSTYPE CMT_DECODE_PROMETHEUS_STYPE

#define YY_DECL int yylex \
               (YYSTYPE * yylval_param, \
               void* yyscanner, \
               struct cmt_decode_prometheus_context *context)

#define CMT_DECODE_PROMETHEUS_MAX_LABEL_COUNT 128

struct cmt_decode_prometheus_context_sample {
    double value;
    uint64_t timestamp;
    size_t label_count;
    cmt_sds_t label_values[CMT_DECODE_PROMETHEUS_MAX_LABEL_COUNT];
};

struct cmt_decode_prometheus_context_metric {
    cmt_sds_t name_orig;
    char *ns;
    char *subsystem;
    char *name;
    int type;
    cmt_sds_t docstring;
    size_t label_count;
    cmt_sds_t labels[CMT_DECODE_PROMETHEUS_MAX_LABEL_COUNT];
    size_t sample_count;
    struct cmt_decode_prometheus_context_sample samples[50];
};

struct cmt_decode_prometheus_context {
    struct cmt *cmt;
    int start_token;
    cmt_sds_t strbuf;
    struct cmt_decode_prometheus_context_metric metric;
};

#include "cmt_decode_prometheus_parser.h"

int cmt_decode_prometheus_lex(YYSTYPE *yylval_param,
                              void *yyscanner,
                              struct cmt_decode_prometheus_context *context);

#ifndef FLEX_SCANNER
// lexer header should not be included in the generated lexer c file,
// which defines FLEX_SCANNER
#include "cmt_decode_prometheus_lexer.h"
#endif

int cmt_decode_prometheus_create(struct cmt **out_cmt, const char *in_buf);
void cmt_decode_prometheus_destroy(struct cmt *cmt);

#endif
