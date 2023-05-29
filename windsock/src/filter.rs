use crate::bench::Tags;
use anyhow::{anyhow, Result};

pub(crate) struct Filter {
    filter: Vec<[String; 2]>,
}

impl Filter {
    pub(crate) fn from_query(query: &str) -> Result<Filter> {
        let mut filter = vec![];
        for pair in query.split_whitespace() {
            let mut iter = pair.split('=');
            let key = iter.next().unwrap();
            let value = match iter.next() {
                Some(value) => value,
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
            filter.push([key.to_owned(), value.to_owned()])
        }
        Ok(Filter { filter })
    }

    pub(crate) fn matches(&self, tags: &Tags) -> bool {
        for [key, value] in &self.filter {
            match tags.0.get(key) {
                Some(other_value) => {
                    if value != other_value {
                        return false;
                    }
                }
                None => return false,
            }
        }
        true
    }
}
