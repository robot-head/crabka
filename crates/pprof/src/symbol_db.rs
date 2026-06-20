//! Deduplicated on-block symbol DB artifact.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_wincode::SerdeCompat;
use wincode::{Deserialize as WincodeDeserialize, Serialize as WincodeSerialize};

use crate::error::ProfileError;
use crate::frame::{Frame, SymbolSource};

/// One inlined line within a location.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LineRec {
    pub function_id: u32,
    pub line: i32,
}

/// A program location and its inlined lines, innermost first.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LocationRec {
    pub address: u64,
    pub mapping_id: u32,
    pub lines: Vec<LineRec>,
}

/// A function record. String fields index into `SymbolDb`'s string table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FunctionRec {
    pub name: u32,
    pub system_name: u32,
    pub filename: u32,
    pub start_line: i64,
}

/// A binary mapping record.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MappingRec {
    pub memory_start: u64,
    pub memory_limit: u64,
    pub file_offset: u64,
    pub filename: u32,
    pub build_id: u32,
    pub has_functions: bool,
    pub has_filenames: bool,
    pub has_line_numbers: bool,
    pub has_inline_frames: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TreeNode {
    parent: i32,
    location_ref: i32,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
struct Partition {
    nodes: Vec<TreeNode>,
    #[serde(skip)]
    children: HashMap<(i32, i32), u32>,
}

impl Partition {
    fn rebuild_children(&mut self) {
        self.children.clear();
        for (idx, node) in self.nodes.iter().enumerate() {
            let idx = u32::try_from(idx).expect("node index fits u32");
            self.children.insert((node.parent, node.location_ref), idx);
        }
    }
}

/// Deduplicated symbol database for a profile block.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct SymbolDb {
    strings: Vec<String>,
    #[serde(skip)]
    string_index: HashMap<String, u32>,
    functions: Vec<FunctionRec>,
    #[serde(skip)]
    function_index: HashMap<FunctionRec, u32>,
    locations: Vec<LocationRec>,
    #[serde(skip)]
    location_index: HashMap<LocationRec, u32>,
    mappings: Vec<MappingRec>,
    #[serde(skip)]
    mapping_index: HashMap<MappingRec, u32>,
    partitions: HashMap<u64, Partition>,
}

impl SymbolDb {
    #[must_use]
    pub fn new() -> Self {
        let mut db = Self::default();
        db.ensure_init();
        db
    }

    fn ensure_init(&mut self) {
        if self.strings.is_empty() {
            self.strings.push(String::new());
            self.string_index.insert(String::new(), 0);
        }
    }

    pub fn intern_string(&mut self, value: &str) -> u32 {
        self.ensure_init();
        if let Some(index) = self.string_index.get(value) {
            return *index;
        }
        let index = u32::try_from(self.strings.len()).expect("string table overflow");
        self.strings.push(value.to_string());
        self.string_index.insert(value.to_string(), index);
        index
    }

    #[must_use]
    pub fn string(&self, index: u32) -> &str {
        self.strings
            .get(usize::try_from(index).expect("u32 fits usize"))
            .map_or("", String::as_str)
    }

    pub fn intern_function(&mut self, function: FunctionRec) -> u32 {
        if let Some(index) = self.function_index.get(&function) {
            return *index;
        }
        let index = u32::try_from(self.functions.len()).expect("function table overflow");
        self.functions.push(function);
        self.function_index.insert(function, index);
        index
    }

    pub fn intern_location(&mut self, location: LocationRec) -> u32 {
        if let Some(index) = self.location_index.get(&location) {
            return *index;
        }
        let index = u32::try_from(self.locations.len()).expect("location table overflow");
        self.locations.push(location.clone());
        self.location_index.insert(location, index);
        index
    }

    pub fn intern_mapping(&mut self, mapping: MappingRec) -> u32 {
        if let Some(index) = self.mapping_index.get(&mapping) {
            return *index;
        }
        let index = u32::try_from(self.mappings.len()).expect("mapping table overflow");
        self.mappings.push(mapping);
        self.mapping_index.insert(mapping, index);
        index
    }

    pub fn intern_stacktrace(&mut self, partition: u64, location_refs: &[u32]) -> u32 {
        let part = self.partitions.entry(partition).or_default();
        let mut parent = -1;
        for location_ref in location_refs.iter().rev() {
            let location_ref = i32::try_from(*location_ref).expect("location ref fits i32");
            let key = (parent, location_ref);
            if let Some(child) = part.children.get(&key) {
                parent = i32::try_from(*child).expect("node index fits i32");
                continue;
            }
            let idx = u32::try_from(part.nodes.len()).expect("node table overflow");
            part.nodes.push(TreeNode {
                parent,
                location_ref,
            });
            part.children.insert(key, idx);
            parent = i32::try_from(idx).expect("node index fits i32");
        }
        u32::try_from(parent.max(0)).expect("leaf node index")
    }

    #[must_use]
    pub fn resolve(&self, partition: u64, stacktrace_id: u32) -> Vec<Frame> {
        let Some(part) = self.partitions.get(&partition) else {
            return Vec::new();
        };
        let mut frames = Vec::new();
        let mut current = i32::try_from(stacktrace_id).unwrap_or(-1);
        while current >= 0 {
            let Some(node) = part
                .nodes
                .get(usize::try_from(current).expect("non-negative"))
            else {
                break;
            };
            if let Some(location) = self
                .locations
                .get(usize::try_from(node.location_ref).expect("non-negative"))
            {
                for line in &location.lines {
                    let function = self
                        .functions
                        .get(usize::try_from(line.function_id).expect("u32 fits usize"));
                    frames.push(Frame {
                        function: function
                            .map_or("", |func| self.string(func.name))
                            .to_string(),
                        file: function
                            .map_or("", |func| self.string(func.filename))
                            .to_string(),
                        line: line.line,
                    });
                }
            }
            current = node.parent;
        }
        frames
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        <SerdeCompat<SymbolDb> as WincodeSerialize>::serialize(self).expect("SymbolDb serializes")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProfileError> {
        let mut db = <SerdeCompat<SymbolDb> as WincodeDeserialize>::deserialize(bytes)
            .map_err(|err| ProfileError::Decode(err.to_string()))?;
        db.rebuild_indexes();
        Ok(db)
    }

    fn rebuild_indexes(&mut self) {
        self.string_index = self
            .strings
            .iter()
            .enumerate()
            .map(|(idx, value)| (value.clone(), u32::try_from(idx).expect("idx fits u32")))
            .collect();
        self.function_index = self
            .functions
            .iter()
            .enumerate()
            .map(|(idx, value)| (*value, u32::try_from(idx).expect("idx fits u32")))
            .collect();
        self.location_index = self
            .locations
            .iter()
            .enumerate()
            .map(|(idx, value)| (value.clone(), u32::try_from(idx).expect("idx fits u32")))
            .collect();
        self.mapping_index = self
            .mappings
            .iter()
            .enumerate()
            .map(|(idx, value)| (*value, u32::try_from(idx).expect("idx fits u32")))
            .collect();
        for partition in self.partitions.values_mut() {
            partition.rebuild_children();
        }
    }
}

impl SymbolSource for SymbolDb {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame> {
        SymbolDb::resolve(self, partition, id)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn db_with_abc() -> (SymbolDb, [u32; 3]) {
        let mut db = SymbolDb::new();
        let mk = |db: &mut SymbolDb, name: &str| {
            let name_ref = db.intern_string(name);
            let filename_ref = db.intern_string(&format!("{name}.go"));
            let function = db.intern_function(FunctionRec {
                name: name_ref,
                system_name: name_ref,
                filename: filename_ref,
                start_line: 1,
            });
            db.intern_location(LocationRec {
                address: 0,
                mapping_id: 0,
                lines: vec![LineRec {
                    function_id: function,
                    line: 10,
                }],
            })
        };
        let a = mk(&mut db, "a");
        let b = mk(&mut db, "b");
        let c = mk(&mut db, "c");
        (db, [a, b, c])
    }

    #[test]
    fn string_zero_is_empty() {
        let db = SymbolDb::default();
        assert!(db.string(0) == "");
    }

    #[test]
    fn identical_stacks_dedup_to_same_leaf() {
        let (mut db, [a, b, c]) = db_with_abc();
        let id1 = db.intern_stacktrace(0, &[a, b, c]);
        let id2 = db.intern_stacktrace(0, &[a, b, c]);
        assert!(id1 == id2);
    }

    #[test]
    fn divergent_stacks_get_distinct_leaves_but_share_prefix() {
        let (mut db, [a, b, c]) = db_with_abc();
        let abc = db.intern_stacktrace(0, &[a, b, c]);
        let ab = db.intern_stacktrace(0, &[a, b]);
        assert!(abc != ab);
        let other = db.intern_stacktrace(1, &[a, b, c]);
        assert!(db.resolve(1, other).len() == 3);
    }

    #[test]
    fn resolve_climbs_leaf_to_root() {
        let (mut db, [a, b, c]) = db_with_abc();
        let id = db.intern_stacktrace(0, &[a, b, c]);
        let frames = db.resolve(0, id);
        let names: Vec<&str> = frames.iter().map(|frame| frame.function.as_str()).collect();
        assert!(names == vec!["a", "b", "c"]);
    }

    #[test]
    fn resolve_expands_inlined_frames_innermost_first() {
        let mut db = SymbolDb::new();
        let outer = db.intern_string("outer");
        let inner = db.intern_string("inner");
        let outer_fn = db.intern_function(FunctionRec {
            name: outer,
            system_name: outer,
            filename: 0,
            start_line: 1,
        });
        let inner_fn = db.intern_function(FunctionRec {
            name: inner,
            system_name: inner,
            filename: 0,
            start_line: 1,
        });
        let loc = db.intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![
                LineRec {
                    function_id: inner_fn,
                    line: 5,
                },
                LineRec {
                    function_id: outer_fn,
                    line: 9,
                },
            ],
        });
        let id = db.intern_stacktrace(0, &[loc]);
        let frames = db.resolve(0, id);
        let names: Vec<&str> = frames.iter().map(|frame| frame.function.as_str()).collect();
        assert!(names == vec!["inner", "outer"]);
    }

    #[test]
    fn encode_decode_round_trips() {
        let (mut db, [a, b, c]) = db_with_abc();
        let id = db.intern_stacktrace(0, &[a, b, c]);
        let bytes = db.encode();
        let back = SymbolDb::decode(&bytes).unwrap();
        assert!(back.resolve(0, id) == db.resolve(0, id));
    }

    #[test]
    fn symbol_source_impl_delegates_to_resolve() {
        let (mut db, [a, b, c]) = db_with_abc();
        let id = db.intern_stacktrace(0, &[a, b, c]);
        let source: &dyn SymbolSource = &db;
        assert!(source.resolve(0, id) == db.resolve(0, id));
    }
}
