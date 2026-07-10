//! Resolved stack frames and symbol resolution boundary.

/// One resolved stack frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub function: String,
    pub file: String,
    pub line: i32,
}

/// Resolves a raw `(partition, stacktrace_id)` into frames.
pub trait SymbolSource: Send + Sync {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(Vec<Frame>);

    impl SymbolSource for Fixed {
        fn resolve(&self, _partition: u64, _id: u32) -> Vec<Frame> {
            self.0.clone()
        }
    }

    #[test]
    fn symbol_source_is_object_safe_and_returns_frames() {
        let src: Box<dyn SymbolSource> = Box::new(Fixed(vec![Frame {
            function: "main".to_string(),
            file: "main.go".to_string(),
            line: 10,
        }]));
        let frames = src.resolve(0, 1);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].function.as_str(), "main");
    }
}
