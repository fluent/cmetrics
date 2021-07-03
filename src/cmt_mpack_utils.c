#include <cmetrics/cmt_mpack_utils.h>
#include <mpack/mpack.h>

int cmt_mpack_consume_double_tag(mpack_reader_t *reader, double *output_buffer)
{
    int         result;
    mpack_tag_t tag;

    if (NULL == output_buffer) {
        return CMT_MPACK_INVALID_ARGUMENT_ERROR;
    }

    if (NULL == reader) {
        return CMT_MPACK_INVALID_ARGUMENT_ERROR;
    }

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

    if (NULL == output_buffer) {
        return CMT_MPACK_INVALID_ARGUMENT_ERROR;
    }

    if (NULL == reader) {
        return CMT_MPACK_INVALID_ARGUMENT_ERROR;
    }

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

int cmt_mpack_consume_string_tag_in_place(mpack_reader_t *reader, 
                                          char **output_buffer,
                                          size_t *output_buffer_length)
{
    uint32_t    string_length;
    int         result;
    mpack_tag_t tag;

    if (NULL == output_buffer) {
        return CMT_MPACK_INVALID_ARGUMENT_ERROR;
    }

    if (NULL == reader) {
        return CMT_MPACK_INVALID_ARGUMENT_ERROR;
    }

    tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader)) {
        return CMT_MPACK_ENGINE_ERROR;
    }

    if (mpack_type_str != mpack_tag_type(&tag)) {
        return CMT_MPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    string_length = mpack_tag_str_length(&tag);

    /* This validation only applies to cmetrics and its use cases, we know 
     * for a fact that our label names and values are not supposed to be really
     * long so a huge value here probably means that the data stream got corrupted.
     */

    if (CMT_MPACK_MAX_STRING_LENGTH < string_length) {
        return CMT_MPACK_CORRUPT_INPUT_DATA_ERROR;
    }

    *output_buffer = mpack_read_bytes_inplace(reader, string_length);

    if (mpack_ok != mpack_reader_error(reader)) {
        *output_buffer = NULL;

        return CMT_MPACK_ENGINE_ERROR;
    }

    mpack_done_str(reader);

    if (mpack_ok != mpack_reader_error(reader)) {        
        *output_buffer = NULL;

        return CMT_MPACK_ENGINE_ERROR;
    }

    *output_buffer_length = string_length;

    return CMT_MPACK_SUCCESS;
}

int cmt_mpack_consume_string_tag(mpack_reader_t *reader, cmt_sds_t *output_buffer)
{
    uint32_t    string_length;
    int         result;
    mpack_tag_t tag;

    if (NULL == output_buffer) {
        return CMT_MPACK_INVALID_ARGUMENT_ERROR;
    }

    if (NULL == reader) {
        return CMT_MPACK_INVALID_ARGUMENT_ERROR;
    }

    tag = mpack_read_tag(reader);

    if (mpack_ok != mpack_reader_error(reader)) {
        return CMT_MPACK_ENGINE_ERROR;
    }

    if (mpack_type_str != mpack_tag_type(&tag)) {
        return CMT_MPACK_UNEXPECTED_DATA_TYPE_ERROR;
    }

    string_length = mpack_tag_str_length(&tag);

    /* This validation only applies to cmetrics and its use cases, we know 
     * for a fact that our label names and values are not supposed to be really
     * long so a huge value here probably means that the data stream got corrupted.
     */

    if (CMT_MPACK_MAX_STRING_LENGTH < string_length) {
        return CMT_MPACK_CORRUPT_INPUT_DATA_ERROR;
    }

    *output_buffer = cmt_sds_create_size(string_length + 1);

    if (NULL == *output_buffer) {
        return CMT_MPACK_ALLOCATION_ERROR;
    }

    cmt_sds_set_len(*output_buffer, string_length);

    mpack_read_cstr(reader, *output_buffer, string_length + 1, string_length);

    if (mpack_ok != mpack_reader_error(reader)) {
        cmt_sds_destroy(*output_buffer);

        *output_buffer = NULL;

        return CMT_MPACK_ENGINE_ERROR;
    }

    mpack_done_str(reader);

    if (mpack_ok != mpack_reader_error(reader)) {
        cmt_sds_destroy(*output_buffer);
        
        *output_buffer = NULL;

        return CMT_MPACK_ENGINE_ERROR;
    }

    return CMT_MPACK_SUCCESS;
}

int cmt_mpack_unpack_map(mpack_reader_t *reader, 
                         struct cmt_mpack_map_entry_callback_t *callback_list, 
                         void *context)
{
    size_t                                 key_name_length;
    struct cmt_mpack_map_entry_callback_t *callback_entry;
    uint32_t                               entry_index;
    uint32_t                               entry_count;
    char                                  *key_name;
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

    /* This validation only applies to cmetrics and its use cases, we know 
     * how our schema looks and how many entries the different fields have and none
     * of those exceed the number we set CMT_MPACK_MAX_MAP_ENTRY_COUNT to which is 10.
     * Making these sanity checks optional or configurable in runtime might be worth
     * the itme and complexity cost but that's something I don't know at the moment.
     */

    if (CMT_MPACK_MAX_MAP_ENTRY_COUNT < entry_count) {
        return CMT_MPACK_CORRUPT_INPUT_DATA_ERROR;
    }

    result = 0;
    
    for (entry_index = 0 ; 0 == result && entry_index < entry_count ; entry_index++) {
        result = cmt_mpack_consume_string_tag_in_place(reader, &key_name, &key_name_length);

        if (CMT_MPACK_SUCCESS == result) {
            callback_entry = callback_list;

            result = CMT_MPACK_UNEXPECTED_KEY_ERROR;

            while (CMT_MPACK_UNEXPECTED_KEY_ERROR == result &&
                   NULL != callback_entry->identifier) {

                if (0 == strncmp(callback_entry->identifier, key_name, key_name_length)) {
                    result = callback_entry->handler(reader, entry_index, context);
                }

                callback_entry++;
            }
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

    /* This validation only applies to cmetrics and its use cases, we know 
     * that in our schema we have the following arrays : 
     *     label text dictionary (strings)
     *     dimension labels (indexes)
     *     metric values
     *         dimension values
     *
     * IMO none of these arrays should be huge so I think using 65535 as a limit
     * gives us more than enough wiggle space (in reality I don't expect any of these
     * arrays to hold more than 128 values but I could be wrong as that probably depends
     * on the flush interval)
     */

    if (CMT_MPACK_MAX_ARRAY_ENTRY_COUNT < entry_count) {
        return CMT_MPACK_CORRUPT_INPUT_DATA_ERROR;
    }

    result = CMT_MPACK_SUCCESS;

    for (entry_index = 0 ;
         CMT_MPACK_SUCCESS == result && entry_index < entry_count ;
         entry_index++) {
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

int cmt_mpack_discard(mpack_reader_t *reader)
{
    mpack_discard(reader);

    return mpack_reader_error(reader);
}
