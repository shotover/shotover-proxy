use itertools::Itertools;

async fn http_request_metrics() -> String {
    let url = "http://localhost:9001/metrics";
    reqwest::get(url).await.unwrap().text().await.unwrap()
}

pub async fn get_metrics_value(key: &str) -> String {
    let actual = http_request_metrics().await;

    for actual_line in actual.lines() {
        if let Some(actual_value) = actual_line.strip_prefix(key) {
            return actual_value.trim().to_owned();
        }
    }
    panic!("key {key:?} was not found in metrics output:\n{actual}");
}

/// Asserts that the `expected` lines of keys are included in the metrics.
/// The `previous` lines are excluded from the assertion, allowing for better error messages when checking for added lines.
/// The keys are removed to keep the output deterministic.
pub async fn assert_metrics_has_keys(previous: &str, expected: &str) {
    let actual = http_request_metrics().await;

    let previous: Vec<&str> = previous.lines().filter(|x| !x.is_empty()).collect();
    let expected_sorted: Vec<&str> = expected
        .lines()
        .filter(|line| !line.is_empty())
        .sorted()
        .collect();
    let actual_sorted: Vec<&str> = actual
        .lines()
        .map(|x| {
            // Strip numbers from the end
            x.trim_end_matches(|c: char| {
                ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ' ', '.'].contains(&c)
            })
        })
        .filter(|line| {
            !line.is_empty() && previous.iter().all(|previous| !line.starts_with(previous))
        })
        .sorted()
        .collect();

    let expected_string = expected_sorted.join("\n");
    let actual_string = actual_sorted.join("\n");

    // Manually recreate assert_eq because it formats the strings poorly
    assert!(
        expected_string == actual_string,
        "expected:\n{expected_string}\nbut was:\n{actual_string}"
    );
}

/// Asserts that the metrics contains a key with the corresponding value
/// Use this to make assertions on specific keys that you know are deterministic
pub async fn assert_metrics_key_value(key: &str, value: &str) {
    let actual_value = get_metrics_value(key).await;
    assert!(
        value == actual_value,
        "Expected metrics key {key:?} to have value {value:?} but it was instead {actual_value:?}"
    );
}
