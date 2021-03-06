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

#include <stdio.h>
#include <stdlib.h>

#include <cmetrics/cmetrics.h>
#include <cmetrics/cmt_gauge.h>

int main()
{
  double val = 10.20304050607080;
  struct cmt *cmt;
  struct cmt_gauge *g;

  cmt = cmt_create();

  g = cmt_gauge_create(cmt, "ns", "sub", "temp", "this is a test");
  printf("fqname => '%s', help => '%s'\n", g->opts.fqname, g->opts.help);

  cmt_gauge_set(g, val);
  cmt_gauge_inc(g);
  printf("UINT64 => %" PRIu64 "\n", g->val);
  cmt_gauge_dec(g);
  printf("UINT64 => %" PRIu64 "\n", g->val);
  cmt_gauge_sub(g, 8);
  printf("UINT64 => %" PRIu64 "\n", g->val);
  cmt_gauge_add(g, 100);
  printf("UINT64 => %" PRIu64 "\n", g->val);

  printf("DOUBLE => %f\n", cmt_math_uint64_to_d64(g->val));

  cmt_destroy(cmt);
}
