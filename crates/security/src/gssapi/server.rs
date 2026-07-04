use super::security_layer::{SecurityLayer, decode_choice, encode_offer};
use super::{AcceptStep, GssAcceptor, GssError};

/// Result of feeding one client token to the exchange.
#[derive(Debug)]
pub enum ServerStep {
    /// Send this token as the `SaslAuthenticate` response `auth_bytes`; feed
    /// the next client token to the returned exchange.
    Challenge(Vec<u8>, GssapiServerExchange),
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
}

struct AcceptingContext {
    acceptor: Box<dyn GssAcceptor>,
    max_recv_size: u32,
}

struct OfferingLayer {
    acceptor: Box<dyn GssAcceptor>,
    max_recv_size: u32,
}

struct AwaitingChoice {
    acceptor: Box<dyn GssAcceptor>,
}

impl std::fmt::Debug for AcceptingContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceptingContext")
            .field("max_recv_size", &self.max_recv_size)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for OfferingLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OfferingLayer")
            .field("max_recv_size", &self.max_recv_size)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AwaitingChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwaitingChoice").finish_non_exhaustive()
    }
}

enum AcceptOutcome {
    /// Acceptor needs another client token.
    Continue(Vec<u8>, AcceptingContext),
    /// Context established with a trailing AP-REP; offer sent on next round.
    Offer(Vec<u8>, OfferingLayer),
    /// Context established; the layer offer was sent inline.
    Choice(Vec<u8>, AwaitingChoice),
}

impl AcceptingContext {
    fn accept(mut self, client_token: &[u8]) -> Result<AcceptOutcome, ServerExchangeError> {
        match self.acceptor.accept(client_token)? {
            AcceptStep::Continue(t) => Ok(AcceptOutcome::Continue(t, self)),
            AcceptStep::Established(t) => {
                // If the final context token exists (AP-REP), send it now and
                // emit the layer offer on the next round. Otherwise emit the
                // offer immediately.
                if let Some(token) = t {
                    Ok(AcceptOutcome::Offer(
                        token,
                        OfferingLayer {
                            acceptor: self.acceptor,
                            max_recv_size: self.max_recv_size,
                        },
                    ))
                } else {
                    let offer = encode_offer(SecurityLayer::AUTH, self.max_recv_size);
                    let wrapped = self.acceptor.wrap(&offer, false)?;
                    Ok(AcceptOutcome::Choice(
                        wrapped,
                        AwaitingChoice {
                            acceptor: self.acceptor,
                        },
                    ))
                }
            }
        }
    }
}

impl OfferingLayer {
    fn offer(self) -> Result<(Vec<u8>, AwaitingChoice), ServerExchangeError> {
        let offer = encode_offer(SecurityLayer::AUTH, self.max_recv_size);
        let wrapped = self.acceptor.wrap(&offer, false)?;
        Ok((
            wrapped,
            AwaitingChoice {
                acceptor: self.acceptor,
            },
        ))
    }
}

impl AwaitingChoice {
    fn finish(self, client_token: &[u8]) -> Result<String, ServerExchangeError> {
        let plaintext = self.acceptor.unwrap(client_token)?;
        let _choice = decode_choice(&plaintext)?; // errors if not auth-only
        let principal = self.acceptor.src_principal()?;
        Ok(principal)
    }
}

/// SASL/GSSAPI server-side handshake, one type per negotiation phase.
///
/// `Send + Sync` so a live `GssapiServerExchange` can live inside the
/// per-connection `ConnectionAuth` state, which the broker's request-handling
/// future holds across `.await` points (the `wrap`/`unwrap`/`src_principal`
/// methods take `&self` and serialise interior mutability behind a mutex).
///
/// The variant payload types are intentionally not exported: the phase is
/// driven entirely through `step`/`ServerStep`, so callers never need to
/// name `AcceptingContext`/`OfferingLayer`/`AwaitingChoice` directly.
#[allow(private_interfaces)]
pub enum GssapiServerExchange {
    AcceptingContext(AcceptingContext),
    OfferingLayer(OfferingLayer),
    AwaitingChoice(AwaitingChoice),
}

// `acceptor` is a trait object with no `Debug` bound; print the observable
// negotiation phase + fields instead so `SaslExchange`/`ConnectionAuth` can
// derive `Debug`.
impl std::fmt::Debug for GssapiServerExchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AcceptingContext(inner) => f
                .debug_tuple("GssapiServerExchange::AcceptingContext")
                .field(inner)
                .finish(),
            Self::OfferingLayer(inner) => f
                .debug_tuple("GssapiServerExchange::OfferingLayer")
                .field(inner)
                .finish(),
            Self::AwaitingChoice(inner) => f
                .debug_tuple("GssapiServerExchange::AwaitingChoice")
                .field(inner)
                .finish(),
        }
    }
}

impl GssapiServerExchange {
    #[must_use]
    pub fn new(acceptor: Box<dyn GssAcceptor>, max_recv_size: u32) -> Self {
        Self::AcceptingContext(AcceptingContext {
            acceptor,
            max_recv_size,
        })
    }

    /// Feed one client token and advance the negotiation.
    ///
    /// # Errors
    /// Returns an error if a GSS context/wrap/unwrap operation fails, or the
    /// client selects an unsupported security layer.
    // Per-step GSSAPI accept driver. skip_all keeps the raw `client_token`
    // (opaque GSS/Kerberos context bytes) out of span fields; only the
    // non-sensitive mechanism is recorded. `err` surfaces the failure (Debug).
    #[tracing::instrument(level = "debug", skip_all, fields(mechanism = "GSSAPI"), err)]
    pub fn step(self, client_token: &[u8]) -> Result<ServerStep, ServerExchangeError> {
        match self {
            Self::AcceptingContext(ctx) => match ctx.accept(client_token)? {
                AcceptOutcome::Continue(t, next) => {
                    Ok(ServerStep::Challenge(t, Self::AcceptingContext(next)))
                }
                AcceptOutcome::Offer(t, next) => {
                    Ok(ServerStep::Challenge(t, Self::OfferingLayer(next)))
                }
                AcceptOutcome::Choice(t, next) => {
                    Ok(ServerStep::Challenge(t, Self::AwaitingChoice(next)))
                }
            },
            Self::OfferingLayer(off) => {
                let (t, next) = off.offer()?;
                Ok(ServerStep::Challenge(t, Self::AwaitingChoice(next)))
            }
            Self::AwaitingChoice(ch) => Ok(ServerStep::Done {
                principal: ch.finish(client_token)?,
            }),
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
        let ex = GssapiServerExchange::new(Box::new(FakeAcceptor { established: false }), 0x1_0000);

        // Round 1: client AP-REQ -> server returns AP-REP, still negotiating.
        let r1 = ex.step(b"AP-REQ").unwrap();
        let ex = match r1 {
            ServerStep::Challenge(_, next) => next,
            ServerStep::Done { .. } => panic!("expected challenge"),
        };

        // Round 2: client empty -> server sends wrapped security-layer offer.
        let r2 = ex.step(b"").unwrap();
        let (offer, ex) = match r2 {
            ServerStep::Challenge(t, next) => (t, next),
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
            ServerStep::Challenge(..) => panic!("expected Done"),
        }
    }

    #[test]
    fn rejects_non_auth_layer_choice() {
        let ex = GssapiServerExchange::new(Box::new(FakeAcceptor { established: false }), 0x1_0000);
        let ex = match ex.step(b"AP-REQ").unwrap() {
            ServerStep::Challenge(_, next) => next,
            ServerStep::Done { .. } => panic!("expected challenge"),
        };
        let ex = match ex.step(b"").unwrap() {
            ServerStep::Challenge(_, next) => next,
            ServerStep::Done { .. } => panic!("expected challenge"),
        };
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

    #[test]
    fn debug_includes_offering_layer_and_awaiting_choice_phases() {
        let ex = GssapiServerExchange::new(Box::new(FakeAcceptor { established: false }), 0x1_0000);

        // Round 1: FakeAcceptor establishes with a trailing AP-REP -> OfferingLayer.
        let ex = match ex.step(b"AP-REQ").unwrap() {
            ServerStep::Challenge(_, next) => next,
            ServerStep::Done { .. } => panic!("expected challenge"),
        };
        let rendered = format!("{ex:?}");
        for part in [
            "GssapiServerExchange::OfferingLayer",
            "OfferingLayer",
            "max_recv_size",
        ] {
            assert!(rendered.contains(part), "missing {part:?} in {rendered}");
        }

        // Round 2: layer offer sent -> AwaitingChoice.
        let ex = match ex.step(b"").unwrap() {
            ServerStep::Challenge(_, next) => next,
            ServerStep::Done { .. } => panic!("expected challenge"),
        };
        let rendered = format!("{ex:?}");
        // `AwaitingChoice` has no fields, so its own `Debug` impl only ever
        // contributes the `finish_non_exhaustive()` marker (`{ .. }`) — assert
        // on that exact text rather than the bare name, which the *enclosing*
        // `GssapiServerExchange::AwaitingChoice(...)` wrapper would already
        // contain even if the inner impl wrote nothing at all.
        assert!(
            rendered.contains("AwaitingChoice { .. }"),
            "missing \"AwaitingChoice {{ .. }}\" in {rendered}"
        );
    }
}
