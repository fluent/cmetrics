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


#ifndef CMT_ENCODE_PROMETHEUS_WRITE_REQUEST_H
#define CMT_ENCODE_PROMETHEUS_WRITE_REQUEST_H

#include <cmetrics/cmetrics.h>
#include <cmetrics/cmt_sds.h>
#include <protobuf-c/remote.pb-c.h>

struct cmt_prometheus_metric_metadata {
    Prometheus__MetricMetadata data;
    struct mk_list _head;
};

struct cmt_prometheus_time_series {
    Prometheus__TimeSeries data;
    struct mk_list _head;
};

struct cmt_prometheus_write_request_context
{
    struct mk_list           time_series_entries;
    struct mk_list           metadata_entries;
    Prometheus__WriteRequest write_request;
};

cmt_sds_t cmt_encode_prometheus_write_request_create(struct cmt *cmt, int add_timestamp);
void cmt_encode_prometheus_write_request_destroy(cmt_sds_t text);

#endif
