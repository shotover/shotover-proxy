use crate::run_command;
use semver::Version;
use std::path::Path;

pub struct VersionTag {
    /// e.g. "v0.1.1"
    pub tag: String,
    /// e.g. "0.1.x"
    pub semver_range: String,
    /// e.g. 0.1.1
    pub version: Version,
}

impl VersionTag {
    fn new(tag: &str) -> Option<VersionTag> {
        let version = Version::parse(tag.strip_prefix("v")?).ok()?;

        // ignore any prerelease or otherwise unusual tags
        if !version.pre.is_empty() || !version.build.is_empty() {
            return None;
        }

        let semver_range = if version.major != 0 {
            format!("{}.Y.Z", version.major)
        } else {
            format!("0.{}.Z", version.minor)
        };
        Some(VersionTag {
            tag: tag.to_owned(),
            version,
            semver_range,
        })
    }
}

pub fn get_versions_of_repo(repo_path: &Path) -> Vec<VersionTag> {
    let mut versions: Vec<VersionTag> = run_command(repo_path, "git", &["tag"])
        .unwrap()
        .lines()
        .filter_map(VersionTag::new)
        .filter(|x| x.version >= Version::new(0, 1, 0))
        .collect();

    // reverse sort the list of versions
    versions.sort_by_key(|x| x.version.clone());
    versions.reverse();

    // Filter out any versions with duplicate semver range, keeping the first item.
    // Keeping the first items leaves us with the most recent item due to the previous reverse sort.
    let mut known = vec![];
    versions.retain(|version| {
        let any_known = known
            .iter()
            .any(|known_range| &version.semver_range == known_range);
        known.push(version.semver_range.clone());
        !any_known
    });

    versions
}
