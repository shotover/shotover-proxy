use cargo_metadata::{Metadata, MetadataCommand, Package, semver::Version};

fn main() {
    let cargo_meta = MetadataCommand::new().exec().unwrap();
    let mut deps = vec![];
    for package in &cargo_meta.packages {
        if is_direct_dependency(&cargo_meta, package) {
            deps.push(Dep {
                name: package.name.clone(),
                version: package.version.clone(),
                description: package.description.clone().unwrap_or_default(),
            });
        }
    }

    let mut table = MarkdownTableGenerator::new();
    for dep in deps {
        // Remove newlines and take only the first sentence.
        let description = dep
            .description
            .replace("\n", " ")
            .split(". ")
            .next()
            .unwrap()
            .to_owned();

        table.rows.push(vec![
            dep.name.clone(),                         // software
            dep.version.to_string(),                  // version
            "N".to_owned(),                           // EOL
            description,                              // Description
            "N".to_owned(),                           // In PCI Scope
            "Open Source".to_owned(),                 // Type of License
            "Rust Crate".to_owned(),                  // Library type
            "Source".to_owned(),                      // Sourcing - Download Type
            format!("crates.io/crates/{}", dep.name), // Sourcing - Download Source
            "SHA256".to_owned(),                      // Sourcing - Verification method
            "N".to_owned(),                           // Sourcing Policy Exception
            "Cadence and Shotover".to_owned(),        // Responsible Team
            "".to_owned(),                            // Comment
        ]);
    }

    table.display();
}

/// Returns true for direct dependencies
/// Returns false for transitive dependencies
/// Returns false for workspace member crate
fn is_direct_dependency(cargo_meta: &Metadata, check_package: &Package) -> bool {
    for member_package_id in &cargo_meta.workspace_members {
        if check_package.id == *member_package_id {
            // is a workspace member crate
            return false;
        }
    }
    for member_package_id in &cargo_meta.workspace_members {
        for possible_package in &cargo_meta.packages {
            if &possible_package.id == member_package_id
                && possible_package
                    .dependencies
                    .iter()
                    .any(|x| x.name == check_package.name && x.req.matches(&check_package.version))
            {
                // is a direct dependency
                return true;
            }
        }
    }

    // is a transitive dependency
    false
}

#[derive(Debug)]
struct Dep {
    name: String,
    version: Version,
    description: String,
}

struct MarkdownTableGenerator {
    rows: Vec<Vec<String>>,
}

impl MarkdownTableGenerator {
    fn new() -> Self {
        let rows = vec![
            vec![
                "Software".to_owned(),
                "Version".to_owned(),
                "EOL".to_owned(),
                "Description".to_owned(),
                "In PCI Scope ".to_owned(),
                "Type of License(Open Source, Proprietary)".to_owned(),
                "Library type ".to_owned(),
                "Sourcing - Download Type(Source, Binary, Bytecode, n/a)".to_owned(),
                "Sourcing - Download source".to_owned(),
                "Sourcing - Verification method(Public KeySHA512, SHA512, n/a)".to_owned(),
                "Sourcing - Policy Exception(if Yes, please provide the rationale".to_owned(),
                "Responsible Team".to_owned(),
                "Comment".to_owned(),
            ],
            vec![],
        ];
        MarkdownTableGenerator { rows }
    }

    fn display(&self) {
        let mut column_widths = vec![];
        for (column_i, _) in self.rows.first().unwrap().iter().enumerate() {
            column_widths.push(
                self.rows
                    .iter()
                    .map(|row| row.get(column_i).map(|cell| cell.len()).unwrap_or_default())
                    .max()
                    .unwrap(),
            )
        }

        for row in &self.rows {
            let mut row_string = "".to_owned();

            if row.is_empty() {
                // Empty row indicates header seperator, which we handle as a special case
                for width in &column_widths {
                    if !row_string.is_empty() {
                        row_string.push('-');
                    }
                    row_string.push_str(&format!("|-{:-<width$}", "", width = width));
                }
            } else {
                for (cell, width) in row.iter().zip(column_widths.iter()) {
                    if !row_string.is_empty() {
                        row_string.push(' ');
                    }
                    row_string.push_str(&format!("| {:width$}", cell, width = width));
                }
            }
            println!("{row_string}");
        }
    }
}
