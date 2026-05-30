use super::security_layer::{SecurityLayer, decode_offer_layers};
use super::{GssError, GssInitiator, InitStep};

/// Result of feeding one server token to the initiate exchange.
#[derive(Debug)]
pub enum ClientStep {
    /// Send this token to the server as `SaslAuthenticate` `auth_bytes`.
    Token(Vec<u8>),
    /// Handshake complete; the stream is authenticated.
    Done,
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

enum State {
    Establishing,
    AwaitingOffer,
    Done,
}

pub struct GssapiClientExchange {
    initiator: Box<dyn GssInitiator>,
    state: State,
    max_recv_size: u32,
    authzid: Option<String>,
}

impl GssapiClientExchange {
    #[must_use]
    pub fn new(
        initiator: Box<dyn GssInitiator>,
        max_recv_size: u32,
        authzid: Option<String>,
    ) -> Self {
        Self {
            initiator,
            state: State::Establishing,
            max_recv_size,
            authzid,
        }
    }

    /// Feed one server token (or `None` for the initial step) and advance the
    /// negotiation.
    ///
    /// # Errors
    /// Returns an error if a GSS context/wrap/unwrap operation fails, the
    /// server's offer is malformed, or the server offers no security layer we
    /// support.
    pub fn step(&mut self, server_token: Option<&[u8]>) -> Result<ClientStep, ClientExchangeError> {
        match self.state {
            State::Establishing => match self.initiator.step(server_token)? {
                InitStep::Continue(t) => Ok(ClientStep::Token(t)),
                InitStep::Established(t) => {
                    self.state = State::AwaitingOffer;
                    // If there's a trailing token send it; else wait for the offer.
                    Ok(ClientStep::Token(t.unwrap_or_default()))
                }
            },
            State::AwaitingOffer => {
                let token = server_token.ok_or(ClientExchangeError::NoCommonLayer)?;
                let offered = decode_offer_layers(&self.initiator.unwrap(token)?)?;
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
                self.state = State::Done;
                Ok(ClientStep::Token(wrapped))
            }
            State::Done => Ok(ClientStep::Done),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gssapi::{GssError, GssInitiator, InitStep};
    use assert2::assert;

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
        let mut ex =
            GssapiClientExchange::new(Box::new(FakeInitiator { done: false }), 0x1_0000, None);

        // First call: no server token yet -> client AP-REQ.
        let first = ex.step(None).unwrap();
        assert!(matches!(first, ClientStep::Token(_)));

        // Server sends AP-REP -> client context completes, still expects offer.
        let _ = ex.step(Some(b"AP-REP")).unwrap();

        // Server sends wrapped layer offer -> client replies with wrapped choice.
        let offer = vec![0x01u8, 0x00, 0x10, 0x00];
        let reply = match ex.step(Some(&offer)).unwrap() {
            ClientStep::Token(t) => t,
            ClientStep::Done => panic!("expected reply token"),
        };
        // reply = wrapped (identity) choice: selected 0x01 auth + 3-byte size
        assert!(reply[0] == 0x01);
    }
}
