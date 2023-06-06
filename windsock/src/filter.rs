use crate::bench::Tags;
use anyhow::{anyhow, Result};

struct FilterTag {
    key: String,
    values: Vec<String>,
}

pub(crate) struct Filter {
    filter: Vec<FilterTag>,
}

impl Filter {
    pub(crate) fn from_query(query: &str) -> Result<Filter> {
        let mut filter = vec![];
        for pair in query.split_whitespace() {
            let mut iter = pair.split('=');
            let key = iter.next().unwrap().to_owned();
            let values = match iter.next() {
                Some(rhs) => rhs.split('|').map(|x| x.to_owned()).collect(),
                None => {
                    return Err(anyhow!(
                        "Expected exactly one '=' but found no '=' in tag {pair:?}"
                    ))
                }
            };
            if iter.next().is_some() {
                return Err(anyhow!(
                    "Expected exactly one '=' but found multiple '=' in tag {pair:?}"
                ));
            }
            filter.push(FilterTag { key, values })
        }
        Ok(Filter { filter })
    }

    pub(crate) fn matches(&self, tags: &Tags) -> bool {
        for FilterTag { key, values } in &self.filter {
            match tags.0.get(key) {
                Some(check_value) => {
                    if !values.contains(check_value) {
                        return false;
                    }
                }
                None => return false,
            }
        }
        true
    }
}
