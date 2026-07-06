//! Error taxonomy for object-store construction and access.

use object_store::path::Path as ObjectPath;

/// Errors raised by the object-store substrate.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreError {
    /// The backend builder rejected the config (bad bucket/region/endpoint/credentials).
    #[error("invalid object store config: {0}")]
    InvalidConfig(String),
    /// A specific object was not found (structured so consumers can upgrade it to
    /// their own domain error without string-matching).
    #[error("object not found: {0}")]
    NotFound(ObjectPath),
    /// Any other backend error, stringified so the public surface stays stable.
    #[error("object store backend error: {0}")]
    Backend(String),
}

impl From<object_store::Error> for ObjectStoreError {
    fn from(err: object_store::Error) -> Self {
        match err {
            object_store::Error::NotFound { path, .. } => Self::NotFound(ObjectPath::from(path)),
            other => Self::Backend(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn not_found_maps_to_structured_variant() {
        let err = object_store::Error::NotFound {
            path: "tenant/block".to_string(),
            source: "missing".into(),
        };
        let mapped = ObjectStoreError::from(err);
        assert!(
            matches!(&mapped, ObjectStoreError::NotFound(p) if p.to_string() == "tenant/block")
        );
    }

    #[test]
    fn other_errors_map_to_backend() {
        let err = object_store::Error::Generic {
            store: "s",
            source: "boom".into(),
        };
        assert!(matches!(
            ObjectStoreError::from(err),
            ObjectStoreError::Backend(_)
        ));
    }
}
