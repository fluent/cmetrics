/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  CMetrics
 *  ========
 *  Copyright 2021-2024 The CMetrics Authors
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
#include <cmetrics/cmt_map.h>
#include <cmetrics/cmt_counter.h>
#include <cmetrics/cmt_gauge.h>
#include <cmetrics/cmt_untyped.h>
#include <cmetrics/cmt_histogram.h>
#include <cmetrics/cmt_summary.h>
#include <cmetrics/cmt_filter.h>

static int compare_label_keys(struct cmt_map *src, const char *label_key,
                              void *compare_ctx, int (*compare)(void *compare_ctx, const char *str, size_t slen),
                              int flags)
{
    struct cfl_list *head;
    struct cmt_map_label *label;
    size_t label_key_size = 0;

    if (label_key != NULL) {
        label_key_size = strlen(label_key);
    }

    cfl_list_foreach(head, &src->label_keys) {
        label = cfl_list_entry(head, struct cmt_map_label, _head);
        /* compare label_keys for prefix */
        if (flags & CMT_FILTER_PREFIX) {
            if (strncmp(label->name, label_key, label_key_size) == 0) {
                return (flags & CMT_FILTER_EXCLUDE) ? CMT_FALSE : CMT_TRUE;
            }

            return (flags & CMT_FILTER_EXCLUDE) ? CMT_TRUE : CMT_FALSE;
        }

        /* compare label_keys for substring */
        if (flags & CMT_FILTER_SUBSTRING) {
            if (strstr(label->name, label_key) != NULL) {
                return (flags & CMT_FILTER_EXCLUDE) ? CMT_FALSE : CMT_TRUE;
            }

            return (flags & CMT_FILTER_EXCLUDE) ? CMT_TRUE : CMT_FALSE;
        }

        /* Compare with an external context (e.g. Onigmo).
         * flb_regex_match should take three arguments that are
         * flb_regex context, string and its string length.
         * The length of string is changed and not determined by label_key.
         */
        if (compare_ctx != NULL && compare != NULL) {
            return compare(compare_ctx, label->name, strlen(label->name));
        }
    }

    return CMT_FALSE;
}

static int filter_context_label_key(struct cmt *dst, struct cmt *src,
                                    const char *label_key,
                                    void *compare_ctx, int (*compare)(void *compare_ctx, const char *str, size_t slen),
                                    int flags)
{
    int ret;
    struct cfl_list *head;
    struct cmt_counter *counter;
    struct cmt_gauge *gauge;
    struct cmt_untyped *untyped;
    struct cmt_histogram *histogram;
    struct cmt_summary *summary;

     /* Counters */
    cfl_list_foreach(head, &src->counters) {
        counter = cfl_list_entry(head, struct cmt_counter, _head);

        if (compare_label_keys(counter->map, label_key, compare_ctx, compare, flags) == CMT_FALSE) {
            continue;
        }

        ret = cmt_cat_copy_counter(dst, counter);
        if (ret == -1) {
            return -1;
        }
    }

    /* Gauges */
    cfl_list_foreach(head, &src->gauges) {
        gauge = cfl_list_entry(head, struct cmt_gauge, _head);

        if (compare_label_keys(gauge->map, label_key, compare_ctx, compare, flags) == CMT_FALSE) {
            continue;
        }

        ret = cmt_cat_copy_gauge(dst, gauge);
        if (ret == -1) {
            return -1;
        }
    }

    /* Untyped */
    cfl_list_foreach(head, &src->untypeds) {
        untyped = cfl_list_entry(head, struct cmt_untyped, _head);

        if (compare_label_keys(untyped->map, label_key, compare_ctx, compare, flags) == CMT_FALSE) {
            continue;
        }

        ret = cmt_cat_copy_untyped(dst, untyped);
        if (ret == -1) {
            return -1;
        }
    }

    /* Histogram */
    cfl_list_foreach(head, &src->histograms) {
        histogram = cfl_list_entry(head, struct cmt_histogram, _head);

        if (compare_label_keys(histogram->map, label_key, compare_ctx, compare, flags) == CMT_FALSE) {
            continue;
        }

        ret = cmt_cat_copy_histogram(dst, histogram);
        if (ret == -1) {
            return -1;
        }
    }

    /* Summary */
    cfl_list_foreach(head, &src->summaries) {
        summary = cfl_list_entry(head, struct cmt_summary, _head);

        if (compare_label_keys(summary->map, label_key, compare_ctx, compare, flags) == CMT_FALSE) {
            continue;
        }

        ret = cmt_cat_copy_summary(dst, summary);
        if (ret == -1) {
            return -1;
        }
    }

    return CMT_FILTER_SUCCESS;
}

static int compare_fqname(struct cmt_opts *src, const char *fqname,
                          void *compare_ctx, int (*compare)(void *compare_ctx, const char *str, size_t slen),
                          int flags)
{
    /* compare fqname for prefix */
    if (flags & CMT_FILTER_PREFIX) {
        if (strncmp(src->fqname, fqname, strlen(fqname)) == 0) {
            return (flags & CMT_FILTER_EXCLUDE) ? CMT_FALSE : CMT_TRUE;
        }

        return (flags & CMT_FILTER_EXCLUDE) ? CMT_TRUE : CMT_FALSE;
    }

    /* compare fqname for substring */
    if (flags & CMT_FILTER_SUBSTRING) {
        if (strstr(src->fqname, fqname) != NULL) {
            return (flags & CMT_FILTER_EXCLUDE) ? CMT_FALSE : CMT_TRUE;
        }

        return (flags & CMT_FILTER_EXCLUDE) ? CMT_TRUE : CMT_FALSE;
    }

    /* Compare with an external context (e.g. Onigmo).
     * flb_regex_match should take three arguments that are
     * flb_regex context, string and its string length.
     * The length of string is changed and not determined by fqname.
     */
    if (compare_ctx != NULL && compare != NULL) {
        return compare(compare_ctx, src->fqname, strlen(src->fqname));
    }

    return CMT_FALSE;
}

static int filter_context_fqname(struct cmt *dst, struct cmt *src,
                                 const char *fqname,
                                 void *compare_ctx, int (*compare)(void *compare_ctx, const char *str, size_t slen),
                                 int flags)
{
    int ret;
    struct cfl_list *head;
    struct cmt_counter *counter;
    struct cmt_gauge *gauge;
    struct cmt_untyped *untyped;
    struct cmt_histogram *histogram;
    struct cmt_summary *summary;

     /* Counters */
    cfl_list_foreach(head, &src->counters) {
        counter = cfl_list_entry(head, struct cmt_counter, _head);

        if (compare_fqname(counter->map->opts, fqname, compare_ctx, compare, flags) == CMT_FALSE) {
            continue;
        }

        ret = cmt_cat_copy_counter(dst, counter);
        if (ret == -1) {
            return -1;
        }
    }

    /* Gauges */
    cfl_list_foreach(head, &src->gauges) {
        gauge = cfl_list_entry(head, struct cmt_gauge, _head);
        if (compare_fqname(gauge->map->opts, fqname, compare_ctx, compare, flags) == CMT_FALSE) {
            continue;
        }

        ret = cmt_cat_copy_gauge(dst, gauge);
        if (ret == -1) {
            return -1;
        }
    }

    /* Untyped */
    cfl_list_foreach(head, &src->untypeds) {
        untyped = cfl_list_entry(head, struct cmt_untyped, _head);
        if (compare_fqname(untyped->map->opts, fqname, compare_ctx, compare, flags) == CMT_FALSE) {
            continue;
        }

        ret = cmt_cat_copy_untyped(dst, untyped);
        if (ret == -1) {
            return -1;
        }
    }

    /* Histogram */
    cfl_list_foreach(head, &src->histograms) {
        histogram = cfl_list_entry(head, struct cmt_histogram, _head);
        if (compare_fqname(histogram->map->opts, fqname, compare_ctx, compare, flags) == CMT_FALSE) {
            continue;
        }

        ret = cmt_cat_copy_histogram(dst, histogram);
        if (ret == -1) {
            return -1;
        }
    }

    /* Summary */
    cfl_list_foreach(head, &src->summaries) {
        summary = cfl_list_entry(head, struct cmt_summary, _head);
        if (compare_fqname(summary->map->opts, fqname, compare_ctx, compare, flags) == CMT_FALSE) {
            continue;
        }

        ret = cmt_cat_copy_summary(dst, summary);
        if (ret == -1) {
            return -1;
        }
    }

    return CMT_FILTER_SUCCESS;
}

int cmt_filter(struct cmt *dst, struct cmt *src,
               const char *fqname, const char *label_key,
               void *compare_ctx, int (*compare)(void *compare_ctx, const char *str, size_t slen),
               int flags)
{
    int ret = CMT_FILTER_SUCCESS;

    if (!dst) {
        return CMT_FILTER_INVALID_ARGUMENT;
    }

    if (!src) {
        return CMT_FILTER_INVALID_ARGUMENT;
    }

    if ((flags & CMT_FILTER_PREFIX) &&
        (flags & CMT_FILTER_SUBSTRING)) {
        return CMT_FILTER_INVALID_FLAGS;
    }

    if (fqname != NULL || (compare_ctx != NULL && compare != NULL)) {
        ret = filter_context_fqname(dst, src, fqname, compare_ctx, compare, flags);
    }

    if (ret != CMT_FILTER_SUCCESS) {
        return CMT_FILTER_FAILED_OPERATION;
    }

    /* On callback mode, labels are not searched by default. */
    if (label_key != NULL ||
        (compare_ctx != NULL && compare != NULL && flags & CMT_FILTER_REGEX_SEARCH_LABELS)) {
        ret = filter_context_label_key(dst, src, label_key, compare_ctx, compare, flags);
    }

    if (ret != CMT_FILTER_SUCCESS) {
        return CMT_FILTER_FAILED_OPERATION;
    }

    return ret;
}
