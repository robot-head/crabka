//! `KStream<K,V>` handle. Stateless ops are added in Task 4.
use std::cell::RefCell;
use std::rc::Rc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::graph::NodeId;

pub struct KStream<K, V> {
    #[allow(dead_code)]
    pub(crate) builder: Rc<RefCell<InternalStreamsBuilder>>,
    #[allow(dead_code)]
    pub(crate) node: NodeId,
    pub(crate) _pd: std::marker::PhantomData<fn() -> (K, V)>,
}

impl<K, V> KStream<K, V> {
    pub(crate) fn new(builder: Rc<RefCell<InternalStreamsBuilder>>, node: NodeId) -> Self {
        Self {
            builder,
            node,
            _pd: std::marker::PhantomData,
        }
    }
}
