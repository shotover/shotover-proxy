#!/usr/bin/env bash

set -e; set -u
set -o pipefail # Needed to make the cargo bench fail early on e.g. compiler error

GITHUB_EVENT_NUMBER=$1
LOG_PAGE=$GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID

# The extra complexity allows us to restore back to the original ref in more cases
ORIGINAL_REF=`git rev-parse --abbrev-ref HEAD`
if [ "$ORIGINAL_REF" == "HEAD" ]; then
  ORIGINAL_REF=`git rev-parse HEAD`
fi

echo -e "\nBenchmarking main as a baseline"
git fetch origin
git checkout origin/$GITHUB_BASE_REF
cargo bench --bench redis_benches -- --save-baseline master --noplot
cargo bench --bench chain_benches -- --save-baseline master --noplot

echo -e "\nBenchmarking PR branch against main as a baseline"
git checkout $ORIGINAL_REF
cargo bench --bench redis_benches -- --baseline master --noplot | tee benches_log.txt -a
cargo bench --bench chain_benches -- --baseline master --noplot | tee benches_log.txt -a

# grep returns non zero exit code when it doesnt find anything so we need to disable pipefail
set +o pipefail
COUNT=`grep -o "Performance has regressed." benches_log.txt | wc -l`
set -o pipefail

mkdir -p comment_info
if [ "$COUNT" != "0" ]; then
  echo "$COUNT benchmarks reported regressed performance. Please check the benchmark workflow logs for details: $LOG_PAGE" > comment_info/message.txt
  echo "$GITHUB_EVENT_NUMBER" > ./comment_info/issue_number.txt
fi
