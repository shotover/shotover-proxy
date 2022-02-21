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
cargo bench --all-features --bench redis_benches -- --save-baseline master --noplot
cargo bench --all-features --bench chain_benches -- --save-baseline master --noplot
cargo bench --all-features --bench cassandra_benches -- --save-baseline master --noplot

echo -e "\nBenchmarking PR branch against main as a baseline"
git checkout $ORIGINAL_REF
cargo bench --all-features --bench redis_benches -- --baseline-lenient master --noplot | tee benches_log.txt -a
cargo bench --all-features --bench chain_benches -- --baseline-lenient master --noplot | tee benches_log.txt -a
cargo bench --all-features --bench cassandra_benches -- --baseline-lenient master --noplot | tee benches_log.txt -a

# grep returns non zero exit code when it doesnt find anything so we need to disable pipefail
set +o pipefail
COUNT_REGRESS=`grep -o "Performance has regressed." benches_log.txt | wc -l`
COUNT_IMPROVE=`grep -o "Performance has improved." benches_log.txt | wc -l`
set -o pipefail

mkdir -p comment_info
if [[ "$COUNT_REGRESS" != "0" || "$COUNT_IMPROVE" != "0" ]]; then
  echo "$COUNT_REGRESS benchmark regressed. $COUNT_IMPROVE benchmark improved. Please check the benchmark workflow logs for full details: $LOG_PAGE" > comment_info/message.txt
  echo "$GITHUB_EVENT_NUMBER" > ./comment_info/issue_number.txt

  # grep returns non zero exit code when it doesnt find anything so we need to disable -e
  set +e

  if [[ "$COUNT_REGRESS" != "0" ]]; then
    echo "\`\`\`" >> comment_info/message.txt
    # Kind of brittle but -B5 includes the 5 lines prior which always happens to includes the bench name + results
    grep -B6 "Performance has regressed." benches_log.txt >> comment_info/message.txt
    echo "\`\`\`" >> comment_info/message.txt
  fi

  if [[ "$COUNT_IMPROVE" != "0" ]]; then
    echo "\`\`\`" >> comment_info/message.txt
    grep -B6 "Performance has improved." benches_log.txt >> comment_info/message.txt
    echo "\`\`\`" >> comment_info/message.txt
  fi

  set -e
fi
