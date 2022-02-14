%define api.pure true
%define api.prefix {cmt_decode_prometheus_}
%define parse.error verbose

%param {void *yyscanner}
%param {struct cmt_decode_prometheus_context *context}

%{
// we inline cmt_decode_prometheus.c which contains all the actions to avoid
// having to export a bunch of symbols that are only used by the generated
// parser code
#include "cmt_decode_prometheus.c"
%}

%union {
    cmt_sds_t str;
    double fpoint;
    long long integer;
}

%token '=' '{' '}' ','
%token <str> IDENTIFIER QUOTED HELP TYPE METRIC_DOC
%token COUNTER GAUGE SUMMARY UNTYPED HISTOGRAM
%token START_HEADER START_LABELS START_SAMPLES
%token <fpoint> FPOINT
%token <integer> INTEGER

%type <integer> metric_type

%start start;

%%

start:
    START_HEADER header
  | START_LABELS labels
  | START_SAMPLES samples
  | metric {
    if (finish_metric(context)) {
        YYABORT;
    }
  }
;

metrics:
    metrics metric
  | metric
;

metric:
    header samples
  | samples
;

header:
    help type
  | help
  | type help
  | type
;

help:
    HELP METRIC_DOC {
        parse_metric_name(context, $1);
        context->metric.docstring = $2;
    }
;

type:
    TYPE metric_type {
        parse_metric_name(context, $1);
        context->metric.type = $2;
    }
;

metric_type:
    COUNTER { $$ = COUNTER; }
  | GAUGE { $$ = GAUGE; }
  | SUMMARY { $$ = SUMMARY; }
  | UNTYPED { $$ = UNTYPED; }
  | HISTOGRAM { $$ = HISTOGRAM; }
;

samples:
    samples sample
  | sample
;

sample:
    IDENTIFIER { 
        parse_metric_name(context, $1);
    } sample_data
;

sample_data:
    '{' labels '}' values
  | values
;

labels:
    labellist ','
  | labellist
;

labellist:
    labellist ',' label
  | label
;

label:
    IDENTIFIER '=' QUOTED {
        if (parse_label(context, $1, $3)) {
            YYABORT;
        }
    }
;

values:
    FPOINT INTEGER {
        parse_sample(context, $1, $2);
    }
  | FPOINT FPOINT {
        parse_sample(context, $1, $2);
    }
  | INTEGER FPOINT {
        parse_sample(context, $1, $2);
    }
  | INTEGER INTEGER {
        parse_sample(context, $1, $2);
    }
  | INTEGER {
        parse_sample(context, $1, 0);
    }
  | FPOINT {
        parse_sample(context, $1, 0);
    }
;

%%
