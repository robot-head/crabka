//! Typed Processor API and the type-erased execution graph used by the runtime.

pub mod api;
pub mod erased;
pub(crate) mod factory;
pub mod fixed_key;
pub(crate) mod graph;
pub(crate) mod node;
pub mod punctuation;
pub mod record;
pub mod serde;

pub mod schema_serde;

pub use api::{Processor, ProcessorContext, ProcessorSupplier};
pub use erased::ProcessorError;
pub use fixed_key::{
    FixedKeyProcessor, FixedKeyProcessorContext, FixedKeyProcessorSupplier, FixedKeyRecord,
};
pub use punctuation::{Cancellable, PunctuationType, Punctuator};
pub use record::{Record, RecordContext};
pub use serde::{
    BytesSerde, Changed, Consumed, DefaultSerde, I64Serde, Produced, Serde, SerdeError, SerdeRole,
    StringSerde,
};

/// Implement [`Processor`] with a compact `(input_key, input_value) ->
/// (output_key, output_value)` declaration.
///
/// ```
/// use crabka_client_streams::{Record, impl_processor};
///
/// struct Upper;
/// impl_processor! {
///     impl Upper: (String, String) -> (String, String) {
///         async fn process(&mut self, ctx, r) {
///             ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
///         }
///     }
/// }
/// ```
///
/// [`Processor`]: crate::processor::api::Processor
#[macro_export]
macro_rules! impl_processor {
    (
        impl $processor:ty: ($kin:ty, $vin:ty) -> ($kout:ty, $vout:ty) {
            $(
                async fn init(&mut self, $init_ctx:ident) $init_body:block
            )?
            async fn process(&mut self, $ctx:ident, $record:ident) $process_body:block
            $(
                async fn close(&mut self) $close_body:block
            )?
        }
    ) => {
        #[$crate::__async_trait]
        impl $crate::processor::api::Processor<$kin, $vin, $kout, $vout> for $processor {
            $(
                async fn init(
                    &mut self,
                    $init_ctx: &mut $crate::processor::api::ProcessorContext<'_, '_, $kout, $vout>,
                ) $init_body
            )?

            async fn process(
                &mut self,
                $ctx: &mut $crate::processor::api::ProcessorContext<'_, '_, $kout, $vout>,
                $record: $crate::processor::record::Record<$kin, $vin>,
            ) $process_body

            $(
                async fn close(&mut self) $close_body
            )?
        }
    };
}

/// Implement [`FixedKeyProcessor`] with a compact `(key, input_value) ->
/// output_value` declaration.
///
/// ```
/// use crabka_client_streams::impl_fixed_key_processor;
///
/// struct UpperValue;
/// impl_fixed_key_processor! {
///     impl UpperValue: (String, String) -> String {
///         async fn process(&mut self, ctx, r) {
///             let v = r.value.clone();
///             ctx.forward(r.with_value(v.to_uppercase()));
///         }
///     }
/// }
/// ```
///
/// [`FixedKeyProcessor`]: crate::processor::fixed_key::FixedKeyProcessor
#[macro_export]
macro_rules! impl_fixed_key_processor {
    (
        impl $processor:ty: ($kin:ty, $vin:ty) -> $vout:ty {
            async fn process(&mut self, $ctx:ident, $record:ident) $process_body:block
        }
    ) => {
        #[$crate::__async_trait]
        impl $crate::processor::fixed_key::FixedKeyProcessor<$kin, $vin, $vout> for $processor {
            async fn process(
                &mut self,
                $ctx: &mut $crate::processor::fixed_key::FixedKeyProcessorContext<
                    '_,
                    '_,
                    '_,
                    $kin,
                    $vout,
                >,
                $record: $crate::processor::fixed_key::FixedKeyRecord<$kin, $vin>,
            ) $process_body
        }
    };
}
