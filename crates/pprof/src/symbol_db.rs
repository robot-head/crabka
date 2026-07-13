//! Deduplicated on-block symbol DB artifact.

use std::{borrow::Cow, collections::HashMap};

use serde::{Deserialize, Serialize};
use serde_wincode::SerdeCompat;
use wincode::{Deserialize as WincodeDeserialize, Serialize as WincodeSerialize};

use crate::{
    error::ProfileError,
    frame::{Frame, SymbolSource},
};

/// Stacktrace id reserved for samples with no frames (e.g. a goroutine profile's
/// occasional stackless record). It is kept distinct from node `0` — the first
/// real stacktrace's root — so stackless samples resolve to no frames instead of
/// borrowing another stack's root, and are thus excluded from flamegraph totals
/// while remaining counted in series sums.
pub const EMPTY_STACKTRACE_ID: u32 = u32::MAX;

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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MappingRec {
    pub memory_start: u64,
    pub memory_limit: u64,
    pub file_offset: u64,
    pub filename: u32,
    pub build_id: u32,
    pub symbolization: MappingSymbolization,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MappingSymbolization(u8);

impl MappingSymbolization {
    const FUNCTIONS: u8 = 1;
    const FILENAMES: u8 = 1 << 1;
    const LINE_NUMBERS: u8 = 1 << 2;
    const INLINE_FRAMES: u8 = 1 << 3;

    #[must_use]
    pub fn from_parts(parts: (bool, bool, bool, bool)) -> Self {
        let (has_functions, has_filenames, has_line_numbers, has_inline_frames) = parts;
        let mut flags = 0;
        if has_functions {
            flags |= Self::FUNCTIONS;
        }
        if has_filenames {
            flags |= Self::FILENAMES;
        }
        if has_line_numbers {
            flags |= Self::LINE_NUMBERS;
        }
        if has_inline_frames {
            flags |= Self::INLINE_FRAMES;
        }
        Self(flags)
    }

    #[must_use]
    pub fn has_functions(self) -> bool {
        self.0 & Self::FUNCTIONS != 0
    }

    #[must_use]
    pub fn has_filenames(self) -> bool {
        self.0 & Self::FILENAMES != 0
    }

    #[must_use]
    pub fn has_line_numbers(self) -> bool {
        self.0 & Self::LINE_NUMBERS != 0
    }

    #[must_use]
    pub fn has_inline_frames(self) -> bool {
        self.0 & Self::INLINE_FRAMES != 0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawLocation {
    pub address: u64,
    pub mapping: MappingRec,
    pub filename: String,
    pub build_id: String,
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

    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
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
    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn string(&self, index: u32) -> &str {
        self.strings
            .get(usize::try_from(index).expect("u32 fits usize"))
            .map_or("", String::as_str)
    }

    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn intern_function(&mut self, function: FunctionRec) -> u32 {
        if let Some(index) = self.function_index.get(&function) {
            return *index;
        }
        let index = u32::try_from(self.functions.len()).expect("function table overflow");
        self.functions.push(function);
        self.function_index.insert(function, index);
        index
    }

    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn intern_location(&mut self, location: LocationRec) -> u32 {
        if let Some(index) = self.location_index.get(&location) {
            return *index;
        }
        let index = u32::try_from(self.locations.len()).expect("location table overflow");
        self.locations.push(location.clone());
        self.location_index.insert(location, index);
        index
    }

    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn intern_mapping(&mut self, mapping: MappingRec) -> u32 {
        if let Some(index) = self.mapping_index.get(&mapping) {
            return *index;
        }
        let index = u32::try_from(self.mappings.len()).expect("mapping table overflow");
        self.mappings.push(mapping);
        self.mapping_index.insert(mapping, index);
        index
    }

    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn intern_stacktrace(&mut self, partition: u64, location_refs: &[u32]) -> u32 {
        if location_refs.is_empty() {
            return EMPTY_STACKTRACE_ID;
        }
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

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn copy_partition_from(
        &mut self,
        source: &SymbolDb,
        source_partition: u64,
        dest_partition: u64,
    ) -> Result<(), ProfileError> {
        let Some(partition) = source.partitions.get(&source_partition) else {
            return Ok(());
        };
        if self
            .partitions
            .get(&dest_partition)
            .is_some_and(|partition| !partition.nodes.is_empty())
        {
            return Err(ProfileError::Store(format!(
                "destination symbol partition {dest_partition} is not empty"
            )));
        }

        let strings = source
            .strings
            .iter()
            .map(|value| self.intern_string(value))
            .collect::<Vec<_>>();
        let mappings = source
            .mappings
            .iter()
            .map(|mapping| {
                self.intern_mapping(MappingRec {
                    memory_start: mapping.memory_start,
                    memory_limit: mapping.memory_limit,
                    file_offset: mapping.file_offset,
                    filename: remap_index(mapping.filename, &strings),
                    build_id: remap_index(mapping.build_id, &strings),
                    symbolization: mapping.symbolization,
                })
            })
            .collect::<Vec<_>>();
        let functions = source
            .functions
            .iter()
            .map(|function| {
                self.intern_function(FunctionRec {
                    name: remap_index(function.name, &strings),
                    system_name: remap_index(function.system_name, &strings),
                    filename: remap_index(function.filename, &strings),
                    start_line: function.start_line,
                })
            })
            .collect::<Vec<_>>();
        let locations = source
            .locations
            .iter()
            .map(|location| {
                self.intern_location(LocationRec {
                    address: location.address,
                    mapping_id: remap_index(location.mapping_id, &mappings),
                    lines: location
                        .lines
                        .iter()
                        .map(|line| LineRec {
                            function_id: remap_index(line.function_id, &functions),
                            line: line.line,
                        })
                        .collect(),
                })
            })
            .collect::<Vec<_>>();

        let nodes = partition
            .nodes
            .iter()
            .map(|node| {
                let location_ref = if node.location_ref >= 0 {
                    i32::try_from(remap_index(
                        u32::try_from(node.location_ref).expect("non-negative"),
                        &locations,
                    ))
                    .expect("location index fits i32")
                } else {
                    node.location_ref
                };
                TreeNode {
                    parent: node.parent,
                    location_ref,
                }
            })
            .collect::<Vec<_>>();
        let mut copied = Partition {
            nodes,
            children: HashMap::new(),
        };
        copied.rebuild_children();
        self.partitions.insert(dest_partition, copied);
        Ok(())
    }

    #[must_use]
    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn resolve(&self, partition: u64, stacktrace_id: u32) -> Vec<Frame> {
        if stacktrace_id == EMPTY_STACKTRACE_ID {
            return Vec::new();
        }
        let Some(part) = self.partitions.get(&partition) else {
            return Vec::new();
        };
        let mut frames = Vec::new();
        let mut current = i32::try_from(stacktrace_id).unwrap_or(-1);
        for _ in 0..part.nodes.len() {
            if current < 0 {
                break;
            }
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
                            .map_or(Cow::Borrowed(""), |func| {
                                drop_go_type_parameters(self.string(func.name))
                            })
                            .into_owned(),
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
    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn raw_locations(&self, partition: u64, stacktrace_id: u32) -> Vec<RawLocation> {
        if stacktrace_id == EMPTY_STACKTRACE_ID {
            return Vec::new();
        }
        let Some(part) = self.partitions.get(&partition) else {
            return Vec::new();
        };
        let mut locations = Vec::new();
        let mut current = i32::try_from(stacktrace_id).unwrap_or(-1);
        for _ in 0..part.nodes.len() {
            if current < 0 {
                break;
            }
            let Some(node) = part
                .nodes
                .get(usize::try_from(current).expect("non-negative"))
            else {
                break;
            };
            if let Some(location) = self
                .locations
                .get(usize::try_from(node.location_ref).expect("non-negative"))
                && let Some(mapping) = self
                    .mappings
                    .get(usize::try_from(location.mapping_id).expect("u32 fits usize"))
            {
                locations.push(RawLocation {
                    address: location.address,
                    mapping: *mapping,
                    filename: self.string(mapping.filename).to_string(),
                    build_id: self.string(mapping.build_id).to_string(),
                });
            }
            current = node.parent;
        }
        locations
    }

    #[must_use]
    /// # Panics
    /// Panics if decoded profile indexes reference a missing string, mapping, function, or location that validation promised was present.
    pub fn encode(&self) -> Vec<u8> {
        <SerdeCompat<SymbolDb> as WincodeSerialize>::serialize(self).expect("SymbolDb serializes")
    }

    /// # Errors
    /// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
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

fn remap_index(index: u32, remapped: &[u32]) -> u32 {
    remapped
        .get(usize::try_from(index).expect("u32 fits usize"))
        .copied()
        .unwrap_or(index)
}

impl SymbolSource for SymbolDb {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame> {
        SymbolDb::resolve(self, partition, id)
    }
}

const GO_SHAPE_PREFIX: &str = "[go.shape.";

fn drop_go_type_parameters(input: &str) -> Cow<'_, str> {
    if !input.contains(GO_SHAPE_PREFIX) {
        return Cow::Borrowed(input);
    }

    let mut result = String::with_capacity(input.len());
    let mut cursor = 0;
    while let Some(relative_start) = input[cursor..].find(GO_SHAPE_PREFIX) {
        let start = cursor + relative_start;
        result.push_str(&input[cursor..start]);

        let mut depth = 0_i32;
        let mut end = None;
        for (offset, byte) in input[start..].bytes().enumerate() {
            match byte {
                b'[' => depth += 1,
                b']' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(start + offset + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(next) = end else {
            result.push_str(&input[start..]);
            return Cow::Owned(result);
        };
        cursor = next;
    }
    result.push_str(&input[cursor..]);
    Cow::Owned(result)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

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
    fn new_reserves_empty_string_slot() {
        let mut db = SymbolDb::new();
        let name = db.intern_string("name");

        check!(db.string(0) == "");
        check!(name == 1);
        check!(db.string(name) == "name");
    }

    #[test]
    fn identical_stacks_dedup_to_same_leaf() {
        let (mut db, [a, b, c]) = db_with_abc();
        let id1 = db.intern_stacktrace(0, &[a, b, c]);
        let id2 = db.intern_stacktrace(0, &[a, b, c]);
        assert!(id1 == id2);
    }

    #[test]
    fn intern_stacktrace_roots_first_node_at_sentinel_parent() {
        let (mut db, [a, b, _c]) = db_with_abc();
        let id = db.intern_stacktrace(0, &[a, b]);
        let part = db.partitions.get(&0).unwrap();

        check!(id == 1);
        check!(part.nodes[0].parent == -1);
        check!(part.nodes[1].parent == 0);
    }

    #[test]
    fn resolve_stops_at_corrupt_parent_cycle() {
        let (mut db, [a, b, _c]) = db_with_abc();
        let id = db.intern_stacktrace(0, &[a, b]);
        db.partitions.get_mut(&0).unwrap().nodes[0].parent = 1;

        let frames = db.resolve(0, id);
        let names: Vec<&str> = frames.iter().map(|frame| frame.function.as_str()).collect();
        assert!(names == vec!["a", "b"]);
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
    fn empty_stack_interns_to_sentinel_and_resolves_to_no_frames() {
        let (mut db, [a, _b, _c]) = db_with_abc();
        // The first real stacktrace owns node 0; an empty stack must not collide
        // with it (which would borrow node 0's root frame) — it gets the sentinel.
        let first = db.intern_stacktrace(0, &[a]);
        let empty = db.intern_stacktrace(0, &[]);
        check!(first == 0);
        check!(empty == EMPTY_STACKTRACE_ID);
        check!(empty != first);
        check!(db.resolve(0, empty).is_empty());
        check!(db.raw_locations(0, empty).is_empty());
        // The real stack still resolves to its single frame.
        check!(db.resolve(0, first).len() == 1);
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
    fn invalid_large_stacktrace_ids_resolve_to_empty() {
        let (mut db, [a, b, c]) = db_with_abc();
        let _ = db.intern_stacktrace(0, &[a, b, c]);
        let invalid = u32::try_from(i64::from(i32::MAX) + 1).unwrap();

        assert!(db.resolve(0, invalid).is_empty());

        let mut raw_db = SymbolDb::new();
        let filename = raw_db.intern_string("/bin/app");
        let build_id = raw_db.intern_string("build");
        let mapping = raw_db.intern_mapping(MappingRec {
            memory_start: 0,
            memory_limit: 0x1000,
            file_offset: 0,
            filename,
            build_id,
            symbolization: MappingSymbolization::default(),
        });
        let loc_a = raw_db.intern_location(LocationRec {
            address: 0x10,
            mapping_id: mapping,
            lines: Vec::new(),
        });
        let loc_b = raw_db.intern_location(LocationRec {
            address: 0x20,
            mapping_id: mapping,
            lines: Vec::new(),
        });
        let _ = raw_db.intern_stacktrace(0, &[loc_a, loc_b]);

        assert!(raw_db.raw_locations(0, invalid).is_empty());
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
    fn resolve_drops_go_shape_type_parameters_like_pyroscope() {
        let mut db = SymbolDb::new();
        let name = db.intern_string(
            "github.com/dgraph-io/ristretto/v2.(*Cache[go.shape.string,go.shape.bool]).processItems",
        );
        let file = db.intern_string("cache.go");
        let function = db.intern_function(FunctionRec {
            name,
            system_name: name,
            filename: file,
            start_line: 1,
        });
        let location = db.intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id: function,
                line: 42,
            }],
        });
        let id = db.intern_stacktrace(0, &[location]);

        let frames = db.resolve(0, id);

        assert!(frames[0].function == "github.com/dgraph-io/ristretto/v2.(*Cache).processItems");
    }

    #[test]
    fn drop_go_type_parameters_handles_multiple_nested_and_unclosed_shapes() {
        assert!(
            drop_go_type_parameters("pkg.(*Cache[go.shape.string]).Get[go.shape.int]").as_ref()
                == "pkg.(*Cache).Get"
        );
        assert!(
            drop_go_type_parameters("pkg.F[go.shape.struct{Field [go.shape.int]}].G").as_ref()
                == "pkg.F.G"
        );
        let unclosed = "pkg.F[go.shape.string";
        assert!(drop_go_type_parameters(unclosed).as_ref() == unclosed);
        let ordinary_generic = "pkg.F[int]";
        assert!(drop_go_type_parameters(ordinary_generic).as_ref() == ordinary_generic);
    }

    #[test]
    fn encode_decode_round_trips() {
        let (mut db, [a, b, c]) = db_with_abc();
        let id = db.intern_stacktrace(0, &[a, b, c]);
        let bytes = db.encode();
        let mut back = SymbolDb::decode(&bytes).unwrap();
        check!(back.resolve(0, id) == db.resolve(0, id));
        check!(back.intern_string("a") == db.intern_string("a"));
        check!(back.intern_stacktrace(0, &[a, b, c]) == id);
    }

    #[test]
    fn symbol_source_impl_delegates_to_resolve() {
        let (mut db, [a, b, c]) = db_with_abc();
        let id = db.intern_stacktrace(0, &[a, b, c]);
        let source: &dyn SymbolSource = &db;
        assert!(source.resolve(0, id) == db.resolve(0, id));
    }

    #[test]
    fn copy_partition_preserves_stacktrace_ids_with_remapped_symbols() {
        let (mut source, [a, b, _]) = db_with_abc();
        let id = source.intern_stacktrace(0, &[a, b]);
        let mut dest = SymbolDb::new();
        let pre_name = dest.intern_string("preexisting");
        let pre_fn = dest.intern_function(FunctionRec {
            name: pre_name,
            system_name: pre_name,
            filename: 0,
            start_line: 1,
        });
        let _ = dest.intern_location(LocationRec {
            address: 0xff,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id: pre_fn,
                line: 1,
            }],
        });

        dest.copy_partition_from(&source, 0, 17).unwrap();

        assert!(dest.resolve(17, id) == source.resolve(0, id));
    }

    #[test]
    fn copy_partition_rebuilds_children_and_rejects_nonempty_destination() {
        let (mut source, [a, b, _]) = db_with_abc();
        let id = source.intern_stacktrace(0, &[a, b]);
        let mut dest = SymbolDb::new();

        dest.copy_partition_from(&source, 0, 17).unwrap();

        assert!(dest.intern_stacktrace(17, &[a, b]) == id);
        assert!(dest.copy_partition_from(&source, 0, 17).is_err());
    }
}
