use super::security_layer::{SecurityLayer, decode_choice, encode_offer};
use super::{AcceptStep, GssAcceptor, GssError};

/// Result of feeding one client token to the exchange.
#[derive(Debug)]
pub enum ServerStep {
    /// Send this token as the `SaslAuthenticate` response `auth_bytes`; expect more.
    Challenge(Vec<u8>),
    /// Authentication complete; `principal` is the raw Kerberos source principal
    /// (apply `auth_to_local` at the call site).
    Done { principal: String },
}

#[derive(Debug, thiserror::Error)]
pub enum ServerExchangeError {
    #[error(transparent)]
    Gss(#[from] GssError),
    #[error(transparent)]
    Layer(#[from] super::security_layer::LayerError),
    #[error("unexpected token in state {0}")]
    State(&'static str),
}

#[derive(Debug)]
enum State {
    AcceptingContext,
    OfferingLayer, // context done, next step emits the offer
    AwaitingChoice,
    Done,
}

pub struct GssapiServerExchange {
    acceptor: Box<dyn GssAcceptor>,
    state: State,
    max_recv_size: u32,
}

// `acceptor` is a trait object with no `Debug` bound; print the observable
// negotiation state instead so `SaslExchange`/`ConnectionAuth` can derive `Debug`.
impl std::fmt::Debug for GssapiServerExchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GssapiServerExchange")
            .field("state", &self.state)
            .field("max_recv_size", &self.max_recv_size)
            .finish_non_exhaustive()
    }
}

impl GssapiServerExchange {
    #[must_use]
    pub fn new(acceptor: Box<dyn GssAcceptor>, max_recv_size: u32) -> Self {
        Self {
            acceptor,
            state: State::AcceptingContext,
            max_recv_size,
        }
    }

    /// Feed one client token and advance the negotiation.
    ///
    /// # Errors
    /// Returns an error if a GSS context/wrap/unwrap operation fails, the
    /// client selects an unsupported security layer, or a token arrives after
    /// the exchange has completed.
    // Per-step GSSAPI accept driver. skip_all keeps the raw `client_token`
    // (opaque GSS/Kerberos context bytes) out of span fields; only the
    // non-sensitive mechanism is recorded. `err` surfaces the failure (Debug).
    #[tracing::instrument(level = "debug", skip_all, fields(mechanism = "GSSAPI"), err)]
    pub fn step(&mut self, client_token: &[u8]) -> Result<ServerStep, ServerExchangeError> {
        match self.state {
            State::AcceptingContext => match self.acceptor.accept(client_token)? {
                AcceptStep::Continue(t) => Ok(ServerStep::Challenge(t)),
                AcceptStep::Established(t) => {
                    // If the final context token exists (AP-REP), send it now and
                    // emit the layer offer on the next round. Otherwise emit the
                    // offer immediately.
                    if let Some(token) = t {
                        self.state = State::OfferingLayer;
                        Ok(ServerStep::Challenge(token))
                    } else {
                        self.state = State::AwaitingChoice;
                        let offer = encode_offer(SecurityLayer::AUTH, self.max_recv_size);
                        Ok(ServerStep::Challenge(self.acceptor.wrap(&offer, false)?))
                    }
                }
            },
            State::OfferingLayer => {
                self.state = State::AwaitingChoice;
                let offer = encode_offer(SecurityLayer::AUTH, self.max_recv_size);
                Ok(ServerStep::Challenge(self.acceptor.wrap(&offer, false)?))
            }
            State::AwaitingChoice => {
                let plaintext = self.acceptor.unwrap(client_token)?;
                let _choice = decode_choice(&plaintext)?; // errors if not auth-only
                let principal = self.acceptor.src_principal()?;
                self.state = State::Done;
                Ok(ServerStep::Done { principal })
            }
            State::Done => Err(ServerExchangeError::State("Done")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gssapi::{AcceptStep, GssAcceptor, GssError};
    use assert2::assert;

    /// Fake that establishes after one token and echoes wrap/unwrap as identity.
    struct FakeAcceptor {
        established: bool,
    }
    impl GssAcceptor for FakeAcceptor {
        fn accept(&mut self, _t: &[u8]) -> Result<AcceptStep, GssError> {
            self.established = true;
            Ok(AcceptStep::Established(Some(b"AP-REP".to_vec())))
        }
        fn wrap(&self, p: &[u8], _c: bool) -> Result<Vec<u8>, GssError> {
            Ok(p.to_vec())
        }
        fn unwrap(&self, t: &[u8]) -> Result<Vec<u8>, GssError> {
            Ok(t.to_vec())
        }
        fn src_principal(&self) -> Result<String, GssError> {
            Ok("alice@REALM".into())
        }
    }

    #[test]
    fn establishes_then_offers_layer_then_completes() {
        let mut ex =
            GssapiServerExchange::new(Box::new(FakeAcceptor { established: false }), 0x1_0000);

        // Round 1: client AP-REQ -> server returns AP-REP, still negotiating.
        let r1 = ex.step(b"AP-REQ").unwrap();
        assert!(matches!(r1, ServerStep::Challenge(_)));

        // Round 2: client empty -> server sends wrapped security-layer offer.
        let r2 = ex.step(b"").unwrap();
        let offer = match r2 {
            ServerStep::Challenge(t) => t,
            ServerStep::Done { .. } => panic!("expected offer"),
        };
        // offer is wrapped (identity here): bitmask 0x01 + 3-byte size
        assert!(offer[0] == 0x01);

        // Round 3: client choice (auth, size, authzid "alice") -> done.
        let mut choice = vec![0x01u8, 0x00, 0x10, 0x00];
        choice.extend_from_slice(b"alice");
        let r3 = ex.step(&choice).unwrap();
        match r3 {
            ServerStep::Done { principal } => assert!(principal == "alice@REALM"),
            ServerStep::Challenge(_) => panic!("expected Done"),
        }
    }

    #[test]
    fn rejects_non_auth_layer_choice() {
        let mut ex =
            GssapiServerExchange::new(Box::new(FakeAcceptor { established: false }), 0x1_0000);
        ex.step(b"AP-REQ").unwrap();
        ex.step(b"").unwrap();
        let bad = vec![0x04u8, 0x00, 0x10, 0x00]; // confidentiality
        assert!(ex.step(&bad).is_err());
    }

    #[test]
    fn debug_includes_observable_state_and_max_receive_size() {
        let ex = GssapiServerExchange::new(Box::new(FakeAcceptor { established: false }), 0x1_0000);
        let rendered = format!("{ex:?}");
        for part in ["GssapiServerExchange", "AcceptingContext", "max_recv_size"] {
            assert!(rendered.contains(part), "missing {part:?} in {rendered}");
        }
    }
}
