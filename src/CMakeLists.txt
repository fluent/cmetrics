set(src
  cmt_gauge.c
  cmt_counter.c
  cmt_log.c
  cmt_opts.c
  cmt_metric.c
  cmt_map.c
  cmetrics.c
  )

# Static Library
add_library(cmetrics-static STATIC ${src})
target_link_libraries(cmetrics-static xxhash)
