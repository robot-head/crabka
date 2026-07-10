//! Raw pprof emission from merged profile trees.

use std::collections::HashMap;

use crate::{
    PprofProfile, ProfileType, Tree,
    proto::{Function, Line, Location, Profile, Sample, ValueType},
};

#[must_use]
pub fn tree_to_pprof(tree: &Tree, profile_type: &ProfileType) -> PprofProfile {
    let mut builder = PprofBuilder::new(profile_type);
    let (root, nodes) = tree.snapshot();
    let mut path = Vec::new();
    collect_samples(root, root, &nodes, &mut path, &mut builder);
    builder.finish()
}

#[must_use]
pub fn tree_to_pprof_with_max_nodes(
    tree: &Tree,
    profile_type: &ProfileType,
    max_nodes: i64,
) -> PprofProfile {
    let mut builder = PprofBuilder::new(profile_type);
    for (path, value) in tree.sample_paths(max_nodes) {
        builder.add_sample(&path, value);
    }
    builder.finish()
}

fn collect_samples(
    root: usize,
    node_idx: usize,
    nodes: &[crate::tree::TreeSnapshotNode],
    path: &mut Vec<String>,
    builder: &mut PprofBuilder,
) {
    if node_idx != root {
        path.push(nodes[node_idx].name.clone());
    }
    if nodes[node_idx].self_ != 0 && !path.is_empty() {
        builder.add_sample(path, nodes[node_idx].self_);
    }
    for child in &nodes[node_idx].children {
        collect_samples(root, *child, nodes, path, builder);
    }
    if node_idx != root {
        path.pop();
    }
}

struct PprofBuilder {
    profile: Profile,
    strings: HashMap<String, i64>,
    locations: HashMap<String, u64>,
}

impl PprofBuilder {
    fn new(profile_type: &ProfileType) -> Self {
        let mut profile = Profile {
            string_table: vec![String::new()],
            ..Default::default()
        };
        let mut strings = HashMap::from([(String::new(), 0)]);
        let sample_type = intern_string(
            &mut profile.string_table,
            &mut strings,
            &profile_type.sample_type,
        );
        let sample_unit = intern_string(
            &mut profile.string_table,
            &mut strings,
            &profile_type.sample_unit,
        );
        let period_type = intern_string(
            &mut profile.string_table,
            &mut strings,
            &profile_type.period_type,
        );
        let period_unit = intern_string(
            &mut profile.string_table,
            &mut strings,
            &profile_type.period_unit,
        );
        profile.sample_type.push(ValueType {
            r#type: sample_type,
            unit: sample_unit,
        });
        profile.period_type = Some(ValueType {
            r#type: period_type,
            unit: period_unit,
        });
        profile.default_sample_type = sample_type;
        Self {
            profile,
            strings,
            locations: HashMap::new(),
        }
    }

    fn add_sample(&mut self, root_to_leaf: &[String], value: i64) {
        let location_id = root_to_leaf
            .iter()
            .rev()
            .map(|name| self.location_id(name))
            .collect();
        self.profile.sample.push(Sample {
            location_id,
            value: vec![value],
            label: Vec::new(),
        });
    }

    fn location_id(&mut self, name: &str) -> u64 {
        if let Some(id) = self.locations.get(name) {
            return *id;
        }
        let name_ref = intern_string(&mut self.profile.string_table, &mut self.strings, name);
        let id = u64::try_from(self.profile.function.len() + 1).expect("function id fits u64");
        self.profile.function.push(Function {
            id,
            name: name_ref,
            system_name: name_ref,
            filename: 0,
            start_line: 0,
        });
        self.profile.location.push(Location {
            id,
            mapping_id: 0,
            address: 0,
            line: vec![Line {
                function_id: id,
                line: 0,
                column: 0,
            }],
            is_folded: false,
        });
        self.locations.insert(name.to_string(), id);
        id
    }

    fn finish(self) -> PprofProfile {
        self.profile.into()
    }
}

fn intern_string(strings: &mut Vec<String>, index: &mut HashMap<String, i64>, value: &str) -> i64 {
    if let Some(slot) = index.get(value) {
        return *slot;
    }
    let slot = i64::try_from(strings.len()).expect("string index fits i64");
    strings.push(value.to_string());
    index.insert(value.to_string(), slot);
    slot
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Frame, PprofProfile, ProfileType, Tree};

    fn frame(name: &str) -> Frame {
        Frame {
            function: name.to_string(),
            file: String::new(),
            line: 0,
        }
    }

    #[test]
    fn tree_to_pprof_round_trips_and_conserves_total() {
        let profile_type =
            ProfileType::parse("process_cpu:cpu:nanoseconds:cpu:nanoseconds").unwrap();
        let mut tree = Tree::new();
        tree.add_stack(&[frame("leaf_a"), frame("root_fn")], 7);
        tree.add_stack(&[frame("leaf_b"), frame("root_fn")], 3);

        let profile = tree_to_pprof(&tree, &profile_type);
        let back = PprofProfile::decode(&profile.encode()).unwrap();
        let inner = back.inner();
        let total: i64 = inner
            .sample
            .iter()
            .map(|sample| sample.value.iter().sum::<i64>())
            .sum();
        let sample_type = inner.sample_type[0];

        assert_eq!(total, 10);
        assert_eq!(inner.sample.len(), 2);
        assert_eq!(
            inner.sample.iter().all(|sample| sample.value != vec![0]),
            true
        );
        assert_eq!(inner.function.iter().all(|function| function.id > 0), true);
        assert_eq!(inner.location.iter().all(|location| location.id > 0), true);
        assert_eq!(
            inner
                .sample
                .iter()
                .flat_map(|sample| sample.location_id.iter())
                .all(|location_id| *location_id > 0),
            true
        );
        assert_eq!(
            sample_paths(inner),
            vec![
                vec!["leaf_a".to_string(), "root_fn".to_string()],
                vec!["leaf_b".to_string(), "root_fn".to_string()],
            ]
        );
        assert_eq!(
            inner.string_table[usize::try_from(sample_type.r#type).unwrap()].as_str(),
            "cpu"
        );
        assert_eq!(
            inner.string_table[usize::try_from(sample_type.unit).unwrap()].as_str(),
            "nanoseconds"
        );
    }

    #[test]
    fn tree_to_pprof_with_max_nodes_emits_synthetic_other() {
        let profile_type =
            ProfileType::parse("process_cpu:cpu:nanoseconds:cpu:nanoseconds").unwrap();
        let mut tree = Tree::new();
        for idx in 0..10 {
            tree.add_stack(&[frame(&format!("leaf{idx}"))], 1);
        }

        let profile = tree_to_pprof_with_max_nodes(&tree, &profile_type, 4);
        let inner = profile.inner();
        let total: i64 = inner
            .sample
            .iter()
            .map(|sample| sample.value.iter().sum::<i64>())
            .sum();

        assert_eq!(inner.sample.len() <= 4, true);
        assert_eq!(total, 10);
        assert_eq!(
            inner.string_table.iter().any(|value| value == "other"),
            true
        );
    }

    fn sample_paths(profile: &crate::proto::Profile) -> Vec<Vec<String>> {
        let mut paths = profile
            .sample
            .iter()
            .map(|sample| {
                sample
                    .location_id
                    .iter()
                    .map(|location_id| {
                        let location = profile
                            .location
                            .iter()
                            .find(|location| location.id == *location_id)
                            .expect("location id exists");
                        let function_id = location.line[0].function_id;
                        let function = profile
                            .function
                            .iter()
                            .find(|function| function.id == function_id)
                            .expect("function id exists");
                        profile.string_table[usize::try_from(function.name).unwrap()].clone()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        paths.sort();
        paths
    }
}
