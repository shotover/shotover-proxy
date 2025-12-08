use itertools::Itertools;

async fn http_request_metrics_on_port(observability_port: Option<&str>) -> String {
    let observability_port = observability_port.unwrap_or("9001");
    let url = format!("http://localhost:{observability_port}/metrics");
    reqwest::get(url).await.unwrap().text().await.unwrap()
}

async fn http_request_metrics() -> String {
    http_request_metrics_on_port(None).await
}

fn extract_metric_value(actual: &str, key: &str) -> String {
    actual
        .lines()
        .find_map(|line| line.strip_prefix(key).map(|rest| rest.trim().to_owned()))
        .unwrap_or_else(|| {
            panic!("key {key:?} was not found in metrics output:\n{actual}");
        })
}

pub async fn get_metrics_value(key: &str) -> String {
    let actual = http_request_metrics().await;
    extract_metric_value(&actual, key)
}

pub async fn get_metrics_value_on_port(key: &str, observability_port: &str) -> String {
    let actual = http_request_metrics_on_port(Some(observability_port)).await;
    extract_metric_value(&actual, key)
}

///Asserts that the `expected` keys are present in the actual metrics output
///Does not ensure key ordering requirements or that no extra keys are present
pub async fn assert_metrics_contains_keys(expected: &str) {
    let actual = http_request_metrics().await;
    let actual_sorted = get_sorted_metric_output_with_no_values(&actual, Vec::new());
    let missing_keys: Vec<&str> = expected
        .lines()
        .filter(|line| !line.is_empty() && !actual_sorted.contains(line))
        .collect();

    assert!(
        missing_keys.is_empty(),
        "The following expected keys were not found in metrics output:\n{:?}\nFull metrics output:\n{}",
        missing_keys,
        actual
    );
}

///Asserts that the given metric key are NOT present in the actual metrics output
pub async fn assert_metrics_contains_keys_not(expected: &str) {
    let actual = http_request_metrics().await;
    let actual_sorted = get_sorted_metric_output_with_no_values(&actual, Vec::new());
    let naughty_keys: Vec<&str> = expected
        .lines()
        .filter(|line| !line.is_empty() && actual_sorted.contains(line))
        .collect();
    assert!(
        naughty_keys.is_empty(),
        "The following key was unexpectedly found in metrics output:\n{:?}\nFull metrics output:\n{}",
        naughty_keys,
        actual
    );
}

/// Asserts that the `expected` lines of keys match the metric output. Does not ensure ordering of the keys.
/// The `previous` lines are excluded from the assertion, allowing for better error messages when checking for added lines.
/// The keys are removed to keep the output deterministic.
pub async fn assert_metrics_equals_keys(previous: &str, expected: &str) {
    let actual = http_request_metrics().await;

    let previous: Vec<&str> = previous.lines().filter(|x| !x.is_empty()).collect();
    let expected_sorted: Vec<&str> = expected
        .lines()
        .filter(|line| !line.is_empty())
        .sorted()
        .collect();
    let actual_sorted = get_sorted_metric_output_with_no_values(&actual, previous);

    let expected_string = expected_sorted.join("\n");
    let actual_string = actual_sorted.join("\n");

    // Manually recreate assert_eq because it formats the strings poorly
    assert!(
        expected_string == actual_string,
        "expected:\n{expected_string}\nbut was:\n{actual_string}"
    );
}

fn get_sorted_metric_output_with_no_values<'a>(
    actual: &'a str,
    previous: Vec<&'a str>,
) -> Vec<&'a str> {
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
    actual_sorted
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

/// Asserts that the metrics contains a key with the corresponding value
/// Use this to make assertions on specific keys that you know are deterministic
pub async fn assert_metrics_key_value_on_port(key: &str, value: &str, observability_port: &str) {
    let actual_value = get_metrics_value_on_port(key, observability_port).await;
    assert!(
        value == actual_value,
        "Expected metrics key {key:?} to have value {value:?} but it was instead {actual_value:?}"
    );
}
