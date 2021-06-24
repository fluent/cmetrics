#include <cmetrics/cmt_mpack_utils.h>
#include <mpack/mpack.h>

int cmt_mpack_consume_double_tag(mpack_reader_t *reader, double *output_buffer)
{
    int         result;
    mpack_tag_t tag;

    tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader)) {
        return CMT_MPACK_ENGINE_ERROR;
    }

    if (mpack_type_double != mpack_tag_type(&tag)) {
        return CMT_MPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    *output_buffer = mpack_tag_double_value(&tag);

    return CMT_MPACK_SUCCESS;
}

int cmt_mpack_consume_uint_tag(mpack_reader_t *reader, uint64_t *output_buffer)
{
    int         result;
    mpack_tag_t tag;

    tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader)) {
        return CMT_MPACK_ENGINE_ERROR;
    }

    if (mpack_type_uint != mpack_tag_type(&tag)) {
        return CMT_MPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    *output_buffer = mpack_tag_uint_value(&tag);

    return CMT_MPACK_SUCCESS;
}

int cmt_mpack_consume_string_tag(mpack_reader_t *reader, cmt_sds_t *output_buffer)
{
    uint32_t    string_length;
    int         result;
    mpack_tag_t tag;

    tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader)) {
        return CMT_MPACK_ENGINE_ERROR;
    }

    if (mpack_type_str != mpack_tag_type(&tag)) {
        return CMT_MPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    string_length = mpack_tag_str_length(&tag);

    *output_buffer = cmt_sds_create_size(string_length + 1);

    if (NULL == *output_buffer) {
        return CMT_MPACK_ALLOCATION_ERROR;
    }

    cmt_sds_set_len(*output_buffer, string_length);

    mpack_read_cstr(reader, *output_buffer, string_length + 1, string_length);

    if (mpack_ok != mpack_reader_error(reader)) {
        return CMT_MPACK_ENGINE_ERROR;
    }

    mpack_done_str(reader);

    if (mpack_ok != mpack_reader_error(reader)) {
        return CMT_MPACK_ENGINE_ERROR;
    }

    return 0;
}

int cmt_mpack_unpack_map(mpack_reader_t *reader, 
                         struct cmt_mpack_map_entry_callback_t *callback_list, 
                         void *context)
{
    struct cmt_mpack_map_entry_callback_t *callback_entry;
    uint32_t                               entry_index;
    uint32_t                               entry_count;
    cmt_sds_t                              key_name;
    int                                    result;
    mpack_tag_t                            tag;

    tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader)) {
        return CMT_MPACK_ENGINE_ERROR;
    }

    if (mpack_type_map != mpack_tag_type(&tag)) {
        return CMT_MPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    entry_count = mpack_tag_map_count(&tag);

    result = 0;
    
    for (entry_index = 0 ; 0 == result && entry_index < entry_count ; entry_index++) {
        result = cmt_mpack_consume_string_tag(reader, &key_name);

        if (0 == result) {
            callback_entry = callback_list;

            while (NULL != callback_entry->identifier) {
                result = strcmp(callback_entry->identifier, key_name);

                if (0 == result) {
                    result = callback_entry->handler(reader, entry_index, context);

                    break;
                }
                else {
                    result = CMT_MPACK_UNEXPECTED_KEY_ERROR;
                }

                callback_entry++;
            }

            cmt_sds_destroy(key_name);
        }
    }

    if (CMT_MPACK_SUCCESS == result) {
        mpack_done_map(reader);

        if (mpack_ok != mpack_reader_error(reader))
        {
            return CMT_MPACK_PENDING_MAP_ENTRIES;
        }        
    }

    return CMT_MPACK_SUCCESS;
}

int cmt_mpack_unpack_array(mpack_reader_t *reader, 
                           cmt_mpack_unpacker_entry_callback_fn_t entry_processor_callback, 
                           void *context)
{
    uint32_t              entry_index;
    uint32_t              entry_count;
    mpack_tag_t           tag;
    int                   result;

    tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader))
    {
        return CMT_MPACK_ENGINE_ERROR;
    }

    if (mpack_type_array != mpack_tag_type(&tag)) {
        return CMT_MPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    entry_count = mpack_tag_array_count(&tag);

    result = 0;

    for (entry_index = 0 ; 0 == result && entry_index < entry_count ; entry_index++) {
        result = entry_processor_callback(reader, entry_index, context);
    }

    if (CMT_MPACK_SUCCESS == result) {
        mpack_done_array(reader);

        if (mpack_ok != mpack_reader_error(reader))
        {
            return CMT_MPACK_PENDING_ARRAY_ENTRIES;
        }
    }

    return 0;
}
