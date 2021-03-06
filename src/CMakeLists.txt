set(src
  cmt_gauge.c
  cmt_counter.c
  cmt_log.c
  cmt_opts.c
  cmetrics.c
  )

# Static Library
add_library(cmetric-static STATIC ${src})

add_executable(cmt-test main.c)
target_link_libraries(cmt-test cmetric-static)
