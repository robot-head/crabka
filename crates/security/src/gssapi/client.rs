use super::{
    GssError, GssInitiator, InitStep,
    security_layer::{SecurityLayer, decode_offer_layers},
};

/// Result of feeding one server token to the initiate exchange.
#[derive(Debug)]
pub enum ClientStep {
    /// Send this token to the server as `SaslAuthenticate` `auth_bytes`; feed
    /// the server's reply to the returned exchange.
    Token(Vec<u8>, GssapiClientExchange),
    /// Handshake complete: send this final token to the server, then check
    /// the `SaslAuthenticate` response's `error_code` — no further `step()`
    /// call is needed.
    Final(Vec<u8>),
}

#[derive(Debug, thiserror::Error)]
pub enum ClientExchangeError {
    #[error(transparent)]
    Gss(#[from] GssError),
    #[error(transparent)]
    Layer(#[from] super::security_layer::LayerError),
    #[error("server offered no supported security layer")]
    NoCommonLayer,
}

struct Establishing {
    initiator: Box<dyn GssInitiator>,
    max_recv_size: u32,
    authzid: Option<String>,
}

struct AwaitingOffer {
    initiator: Box<dyn GssInitiator>,
    max_recv_size: u32,
    authzid: Option<String>,
}

impl std::fmt::Debug for Establishing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Establishing")
            .field("max_recv_size", &self.max_recv_size)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AwaitingOffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwaitingOffer")
            .field("max_recv_size", &self.max_recv_size)
            .finish_non_exhaustive()
    }
}

enum EstablishOutcome {
    Continue(Vec<u8>, Establishing),
    Offer(Vec<u8>, AwaitingOffer),
}

impl Establishing {
    fn step(
        mut self,
        server_token: Option<&[u8]>,
    ) -> Result<EstablishOutcome, ClientExchangeError> {
        match self.initiator.step(server_token)? {
            InitStep::Continue(t) => Ok(EstablishOutcome::Continue(t, self)),
            InitStep::Established(t) => {
                // If there's a trailing token send it; else wait for the offer.
                Ok(EstablishOutcome::Offer(
                    t.unwrap_or_default(),
                    AwaitingOffer {
                        initiator: self.initiator,
                        max_recv_size: self.max_recv_size,
                        authzid: self.authzid,
                    },
                ))
            }
        }
    }
}

impl AwaitingOffer {
    // Terminal: no further exchange state to hold.
    fn choose(self, server_token: &[u8]) -> Result<Vec<u8>, ClientExchangeError> {
        let offered = decode_offer_layers(&self.initiator.unwrap(server_token)?)?;
        if offered.0 & SecurityLayer::AUTH.0 == 0 {
            return Err(ClientExchangeError::NoCommonLayer);
        }
        // Reply: select auth, our max recv size, optional authzid.
        let s = self.max_recv_size.to_be_bytes();
        let mut reply = vec![SecurityLayer::AUTH.0, s[1], s[2], s[3]];
        if let Some(z) = &self.authzid {
            reply.extend_from_slice(z.as_bytes());
        }
        let wrapped = self.initiator.wrap(&reply, false)?;
        Ok(wrapped)
    }
}

/// SASL/GSSAPI client-side handshake, one type per negotiation phase.
///
/// The variant payload types are intentionally not exported: the phase is
/// driven entirely through `step`/`ClientStep`, so callers never need to
/// name `Establishing`/`AwaitingOffer` directly.
#[allow(private_interfaces)]
pub enum GssapiClientExchange {
    Establishing(Establishing),
    AwaitingOffer(AwaitingOffer),
}

impl std::fmt::Debug for GssapiClientExchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Establishing(inner) => f
                .debug_tuple("GssapiClientExchange::Establishing")
                .field(inner)
                .finish(),
            Self::AwaitingOffer(inner) => f
                .debug_tuple("GssapiClientExchange::AwaitingOffer")
                .field(inner)
                .finish(),
        }
    }
}

impl GssapiClientExchange {
    #[must_use]
    pub fn new(
        initiator: Box<dyn GssInitiator>,
        max_recv_size: u32,
        authzid: Option<String>,
    ) -> Self {
        Self::Establishing(Establishing {
            initiator,
            max_recv_size,
            authzid,
        })
    }

    /// Feed one server token (or `None` for the initial step) and advance the
    /// negotiation.
    ///
    /// # Errors
    /// Returns an error if a GSS context/wrap/unwrap operation fails, the
    /// server's offer is malformed, or the server offers no security layer we
    /// support.
    // Per-step GSSAPI initiate driver. skip_all keeps the opaque `server_token`
    // (GSS/Kerberos context bytes) out of span fields; only the non-sensitive
    // mechanism is recorded. `err` surfaces the failure (Debug).
    #[tracing::instrument(level = "debug", skip_all, fields(mechanism = "GSSAPI"), err)]
    pub fn step(self, server_token: Option<&[u8]>) -> Result<ClientStep, ClientExchangeError> {
        match self {
            Self::Establishing(e) => match e.step(server_token)? {
                EstablishOutcome::Continue(t, next) => {
                    Ok(ClientStep::Token(t, Self::Establishing(next)))
                }
                EstablishOutcome::Offer(t, next) => {
                    Ok(ClientStep::Token(t, Self::AwaitingOffer(next)))
                }
            },
            Self::AwaitingOffer(a) => {
                let server_token = server_token.ok_or(ClientExchangeError::NoCommonLayer)?;
                Ok(ClientStep::Final(a.choose(server_token)?))
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::gssapi::{GssError, GssInitiator, InitStep};

    struct FakeInitiator {
        done: bool,
    }
    impl GssInitiator for FakeInitiator {
        fn step(&mut self, _server_token: Option<&[u8]>) -> Result<InitStep, GssError> {
            if self.done {
                Ok(InitStep::Established(None))
            } else {
                self.done = true;
                Ok(InitStep::Continue(b"AP-REQ".to_vec()))
            }
        }
        fn wrap(&self, p: &[u8], _c: bool) -> Result<Vec<u8>, GssError> {
            Ok(p.to_vec())
        }
        fn unwrap(&self, t: &[u8]) -> Result<Vec<u8>, GssError> {
            Ok(t.to_vec())
        }
    }

    #[test]
    fn produces_first_token_then_replies_to_offer() {
        let ex = GssapiClientExchange::new(Box::new(FakeInitiator { done: false }), 0x1_0000, None);

        // First call: no server token yet -> client AP-REQ.
        let first = ex.step(None).unwrap();
        let ex = match first {
            ClientStep::Token(_, next) => next,
            ClientStep::Final(_) => panic!("expected token"),
        };

        // Server sends AP-REP -> client context completes, still expects offer.
        let ex = match ex.step(Some(b"AP-REP")).unwrap() {
            ClientStep::Token(_, next) => next,
            ClientStep::Final(_) => panic!("expected token"),
        };

        // Server sends wrapped layer offer -> client replies with wrapped choice.
        let offer = vec![0x01u8, 0x00, 0x10, 0x00];
        let reply = match ex.step(Some(&offer)).unwrap() {
            ClientStep::Final(t) => t,
            ClientStep::Token(..) => panic!("expected final reply token"),
        };
        // reply = wrapped (identity) choice: selected 0x01 auth + 3-byte size
        assert2::assert!(reply[0] == 0x01);
    }

    #[test]
    fn debug_includes_observable_phase_and_max_receive_size() {
        let establishing =
            GssapiClientExchange::new(Box::new(FakeInitiator { done: false }), 0x1_0000, None);
        let rendered = format!("{establishing:?}");
        for part in [
            "GssapiClientExchange::Establishing",
            "Establishing",
            "max_recv_size",
        ] {
            assert2::assert!(rendered.contains(part));
        }

        // First step: still negotiating -> stays `Establishing`.
        let establishing = match establishing.step(None).unwrap() {
            ClientStep::Token(_, next) => next,
            ClientStep::Final(_) => panic!("expected token"),
        };
        // Second step: context completes -> `AwaitingOffer`.
        let awaiting_offer = match establishing.step(Some(b"AP-REP")).unwrap() {
            ClientStep::Token(_, next) => next,
            ClientStep::Final(_) => panic!("expected token"),
        };
        let rendered = format!("{awaiting_offer:?}");
        for part in [
            "GssapiClientExchange::AwaitingOffer",
            "AwaitingOffer",
            "max_recv_size",
        ] {
            assert2::assert!(rendered.contains(part));
        }
    }

    #[test]
    fn rejects_offer_without_auth_layer_even_when_other_layers_present() {
        let ex = GssapiClientExchange::new(Box::new(FakeInitiator { done: false }), 0x1_0000, None);

        let ex = match ex.step(None).unwrap() {
            ClientStep::Token(_, next) => next,
            ClientStep::Final(_) => panic!("expected token"),
        };
        let ex = match ex.step(Some(b"AP-REP")).unwrap() {
            ClientStep::Token(_, next) => next,
            ClientStep::Final(_) => panic!("expected token"),
        };

        let integrity_only_offer = vec![0x02u8, 0x00, 0x10, 0x00];
        assert2::assert!(matches!(
            ex.step(Some(&integrity_only_offer)),
            Err(ClientExchangeError::NoCommonLayer)
        ));
    }
}
