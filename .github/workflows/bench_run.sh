#!/usr/bin/env bash

# this segment is a little complicated so its split into its own script so it can be tested locally

GITHUB_EVENT_NUMBER=$1

LOG_PAGE=$GITHUB_SERVER_URL/$GITHUB_REPOSITORY/runs/$GITHUB_RUN_ID

set -e; set -u
set -o pipefail # Needed to make the cargo bench fail early on e.g. compiler error.

cargo bench --bench redis_benches -- --baseline master --noplot | tee benches_log.txt -a
cargo bench --bench chain_benches -- --baseline master --noplot | tee benches_log.txt -a

COUNT=`grep -o "Performance has regressed." benches_log.txt | wc -l`
mkdir -p comment_info
if [ "$COUNT" != "0" ]; then
  echo "$COUNT benchmarks reported regressed performance. Please check the benchmark workflow logs for details: $LOG_PAGE" > comment_info/message.txt
  echo "$GITHUB_EVENT_NUMBER" > ./comment_info/issue_number.txt
fi
