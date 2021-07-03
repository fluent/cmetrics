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
#include <cmetrics/cmt_metric.h>
#include <cmetrics/cmt_map.h>
#include <cmetrics/cmt_sds.h>
#include <cmetrics/cmt_counter.h>
#include <cmetrics/cmt_gauge.h>
#include <cmetrics/cmt_compat.h>
#include <cmetrics/cmt_encode_msgpack.h>
#include <cmetrics/cmt_decode_msgpack.h>

#include <mpack/mpack.h>

static int validate_string_entry(mpack_reader_t *reader, size_t index, void *context)
{
    char  *value;
    size_t length;

    if (NULL == reader ) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    return cmt_mpack_consume_string_tag_in_place(reader, &value, &length);
}

static int validate_uint_entry(mpack_reader_t *reader, size_t index, void *context)
{
    uint64_t value;

    if (NULL == reader ) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    return cmt_mpack_consume_uint_tag(reader, &value);
}

static int validate_double_entry(mpack_reader_t *reader, size_t index, void *context)
{
    double value;

    if (NULL == reader ) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    return cmt_mpack_consume_double_tag(reader, &value);
}

static int validate_opts(mpack_reader_t *reader)
{
    struct cmt_mpack_map_entry_callback_t callbacks[] = \
        {
            {"ns",   validate_string_entry},
            {"ss",   validate_string_entry},
            {"name", validate_string_entry},
            {"desc", validate_string_entry},
            {NULL,   NULL}
        };

    if (NULL == reader) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    return cmt_mpack_unpack_map(reader, callbacks, NULL);
}

static int validate_metric_labels(mpack_reader_t *reader, size_t index, void *context)
{
    if (NULL == reader) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    return cmt_mpack_unpack_array(reader, validate_uint_entry, context);
}

static int validate_metric(mpack_reader_t *reader)
{
    struct cmt_mpack_map_entry_callback_t callbacks[] = \
        {
            {"ts",     validate_uint_entry},
            {"value",  validate_double_entry},
            {"labels", validate_metric_labels},
            {NULL,     NULL}
        };

    if (NULL == reader) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    return cmt_mpack_unpack_map(reader, callbacks, NULL);
}

static int validate_metric_array_entry(mpack_reader_t *reader, size_t index, void *context)
{
    if (NULL == reader) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    return validate_metric(reader);
}

static int validate_meta_ver(mpack_reader_t *reader, size_t index, void *context)
{
    uint64_t value;
    int      result;

    if (NULL == reader) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    result = cmt_mpack_consume_uint_tag(reader, &value);

    if (CMT_DECODE_MSGPACK_SUCCESS == result) {
        if (MSGPACK_ENCODER_VERSION != value)  {
            result = CMT_DECODE_MSGPACK_VERSION_ERROR;
        }
    }

    return result;
}

static int validate_meta_opts(mpack_reader_t *reader, size_t index, void *context)
{
    if (NULL == reader) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    return validate_opts(reader);
}

static int validate_meta_label_dictionary(mpack_reader_t *reader, size_t index, void *context)
{
    if (NULL == reader) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    return cmt_mpack_unpack_array(reader, validate_string_entry, NULL);
}

static int validate_meta_static_labels(mpack_reader_t *reader, size_t index, void *context)
{
    if (NULL == reader) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    return cmt_mpack_unpack_array(reader, validate_uint_entry, context);
}

static int validate_meta_labels(mpack_reader_t *reader, size_t index, void *context)
{
    if (NULL == reader) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    return cmt_mpack_unpack_array(reader, validate_uint_entry, context);
}

static int validate_basic_type_meta(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_msgpack_validate_context  *validate_context;
    struct cmt_mpack_map_entry_callback_t callbacks[] = \
        {
            {"ver",              validate_meta_ver},
            {"type",             validate_uint_entry},
            {"opts",             validate_meta_opts},
            {"label_dictionary", validate_meta_label_dictionary},
            {"static_labels",    validate_meta_static_labels},
            {"labels",           validate_meta_labels},
            {NULL,               NULL}
        };

    if (NULL == context || 
        NULL == reader) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    validate_context = (struct cmt_msgpack_validate_context *) context;

    validate_context->meta_found = 1;

    if (0 == validate_context->in_depth_validation) {
        return cmt_mpack_discard(reader);
    }
    else {
        return cmt_mpack_unpack_map(reader, callbacks, context);
    }
}

static int validate_basic_type_values(mpack_reader_t *reader, size_t index, void *context)
{
    struct cmt_msgpack_validate_context *validate_context;

    if (NULL == context || 
        NULL == reader) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    validate_context = (struct cmt_msgpack_validate_context *) context;

    validate_context->values_found = 1;

    if (0 == validate_context->in_depth_validation) {
        return cmt_mpack_discard(reader);
    }
    else {
        return cmt_mpack_unpack_array(reader, 
                                      validate_metric_array_entry, 
                                      context);
    }
}

static int validate_basic_type(mpack_reader_t *reader, void *context)
{
    struct cmt_mpack_map_entry_callback_t callbacks[] = \
        {
            {"meta",   validate_basic_type_meta},
            {"values", validate_basic_type_values},
            {NULL,     NULL}
        };

    return cmt_mpack_unpack_map(reader, callbacks, (void *) context);
}

/* Validate cmetrics msgpack payload */
int cmt_validate_msgpack(void *in_buf, size_t in_size, int in_depth_validation)
{
    struct cmt_msgpack_validate_context context;
    mpack_reader_t                      reader;
    int                                 result;

    if (NULL == in_buf  || 
        0 == in_size    ) {
        return CMT_DECODE_MSGPACK_INVALID_ARGUMENT_ERROR;
    }

    mpack_reader_init_data(&reader, in_buf, in_size);

    memset(&context, 0, sizeof(struct cmt_msgpack_validate_context));

    context.in_depth_validation = in_depth_validation;

    result = validate_basic_type(&reader, &context);

    mpack_reader_destroy(&reader);

    if(CMT_DECODE_MSGPACK_SUCCESS != result)
    {
        return CMT_VALIDATE_MSGPACK_FAILURE;
    }

    if(1 != context.meta_found)
    {
        printf("META NOT FOUND\n");
        return CMT_VALIDATE_MSGPACK_FAILURE;
    }
    else if(1 != context.values_found)
    {
        printf("VALUES NOT FOUND\n");
        return CMT_VALIDATE_MSGPACK_FAILURE;
    }

    return CMT_VALIDATE_MSGPACK_SUCCESS;
}
