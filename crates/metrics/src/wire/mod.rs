//! `remote_write` wire surface: prost-generated message types, content
//! negotiation, snappy decode, and decode to the shared `DecodedSeries`.

mod decoded;
mod histogram;
mod remote_read;
mod v1;
mod v2;

pub use decoded::{
    DecodedExemplar, DecodedMetadata, DecodedSample, DecodedSeries, WireError, WireFormat,
    negotiate, snappy_block_decode,
};
pub use histogram::{v1_histogram_to_native, v2_histogram_to_native};
pub use remote_read::{
    DEFAULT_MAX_READ_DECOMPRESSED, RemoteReadError, decode_read_request, encode_read_response,
    matchers_to_selectors, series_to_timeseries,
};
pub use v1::decode_v1;
pub use v2::{WrittenCounts, decode_v2};

/// prost-generated message types from the vendored protos.
pub mod pb {
    /// `remote_write` v1 (`prometheus.WriteRequest`).
    pub mod v1 {
        #![allow(clippy::pedantic, clippy::useless_borrows_in_formatting)]
        include!(concat!(env!("OUT_DIR"), "/prometheus.rs"));
    }

    /// `remote_write` v2 (`io.prometheus.write.v2.Request`).
    pub mod v2 {
        #![allow(clippy::pedantic, clippy::useless_borrows_in_formatting)]
        include!(concat!(env!("OUT_DIR"), "/io.prometheus.write.v2.rs"));
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use prost::Message;

    use super::pb;

    #[test]
    fn v1_write_request_round_trips_via_prost() {
        let req = pb::v1::WriteRequest {
            timeseries: vec![pb::v1::TimeSeries {
                labels: vec![pb::v1::Label {
                    name: "__name__".into(),
                    value: "up".into(),
                }],
                samples: vec![pb::v1::Sample {
                    value: 1.0,
                    timestamp: 42,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let bytes = req.encode_to_vec();
        let back = pb::v1::WriteRequest::decode(bytes.as_slice()).unwrap();

        assert!(back.timeseries.len() == 1);
        assert!(back.timeseries[0].samples[0].timestamp == 42);
    }

    #[test]
    fn v2_request_has_symbols_and_label_refs() {
        let req = pb::v2::Request {
            symbols: vec![String::new(), "__name__".into(), "up".into()],
            timeseries: vec![pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![pb::v2::Sample {
                    value: 3.0,
                    timestamp: 7,
                    start_timestamp: 0,
                }],
                ..Default::default()
            }],
        };

        let bytes = req.encode_to_vec();
        let back = pb::v2::Request::decode(bytes.as_slice()).unwrap();

        assert!(back.symbols[0].is_empty());
        assert!(back.timeseries[0].labels_refs == vec![1, 2]);
    }
}
