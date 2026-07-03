---
name: api-typestate
description: Use the typestate pattern to encode state-machine invariants in Rust's type system instead of runtime checks. Apply when designing or reviewing any Rust type that tracks a lifecycle/mode (connection, transaction, builder, session, handshake, subscription) and currently guards operations with an enum field plus runtime `if`/`match` checks. Also use when reviewing PRs that add a new "invalid state" error variant to an existing struct.
---

# api-typestate

> Use the typestate pattern to encode state machine invariants in the type system

## Why It Matters

State machines with runtime state checks ("are we connected?", "is the transaction started?") can have invalid transitions. The typestate pattern uses different types for each state, making invalid state transitions compile errors. The compiler enforces your state machine.

Signals that a type in this codebase should be reconsidered as a typestate:
- A struct field like `state: XState` (an enum) that is checked with `if self.state != ...` or `match self.state { ... => return Err(...) }` before doing real work.
- An error variant named `NotConnected`, `NotAuthenticated`, `NotStarted`, `AlreadyClosed`, `InvalidState`, or similar, whose only purpose is to reject a call made in the wrong phase of an object's lifecycle.
- A builder that returns `Result`/panics from `build()` because a required field wasn't set, when the set of required fields is known at compile time.

## Bad

```rust
struct Connection {
    state: ConnectionState,
    socket: Option<TcpStream>,
}

enum ConnectionState {
    Disconnected,
    Connected,
    Authenticated,
}

impl Connection {
    fn send(&mut self, data: &[u8]) -> Result<(), Error> {
        // Runtime check - can fail if called in wrong state
        if self.state != ConnectionState::Authenticated {
            return Err(Error::NotAuthenticated);
        }
        self.socket.as_mut().unwrap().write_all(data)?;
        Ok(())
    }

    fn authenticate(&mut self, password: &str) -> Result<(), Error> {
        // Runtime check - can fail
        if self.state != ConnectionState::Connected {
            return Err(Error::NotConnected);
        }
        // ...
    }
}

// Bug: forgot to authenticate
let mut conn = Connection::new();
conn.connect()?;
conn.send(b"data")?;  // Runtime error: NotAuthenticated
```

## Good

```rust
// Different types for each state
struct Disconnected;
struct Connected { socket: TcpStream }
struct Authenticated { socket: TcpStream, session: Session }

struct Connection<State> {
    state: State,
}

impl Connection<Disconnected> {
    fn new() -> Self {
        Connection { state: Disconnected }
    }

    fn connect(self, addr: &str) -> Result<Connection<Connected>, Error> {
        let socket = TcpStream::connect(addr)?;
        Ok(Connection { state: Connected { socket } })
    }
}

impl Connection<Connected> {
    fn authenticate(self, password: &str) -> Result<Connection<Authenticated>, Error> {
        let session = do_auth(&self.state.socket, password)?;
        Ok(Connection {
            state: Authenticated { socket: self.state.socket, session }
        })
    }
}

impl Connection<Authenticated> {
    fn send(&mut self, data: &[u8]) -> Result<(), Error> {
        // No runtime check needed - type guarantees we're authenticated
        self.state.socket.write_all(data)?;
        Ok(())
    }
}

// Bug: forgot to authenticate
let conn = Connection::new();
let conn = conn.connect("server:8080")?;
conn.send(b"data");  // Compile error! send() not available on Connection<Connected>

// Correct usage
let conn = Connection::new();
let conn = conn.connect("server:8080")?;
let mut conn = conn.authenticate("secret")?;
conn.send(b"data")?;  // Works - type is Connection<Authenticated>
```

## Builder Typestate

```rust
// Enforce required fields via typestate
struct BuilderNoUrl;
struct BuilderWithUrl { url: String }

struct RequestBuilder<State> {
    state: State,
    timeout: Option<Duration>,
}

impl RequestBuilder<BuilderNoUrl> {
    fn new() -> Self {
        RequestBuilder {
            state: BuilderNoUrl,
            timeout: None,
        }
    }

    fn url(self, url: &str) -> RequestBuilder<BuilderWithUrl> {
        RequestBuilder {
            state: BuilderWithUrl { url: url.to_string() },
            timeout: self.timeout,
        }
    }
}

impl RequestBuilder<BuilderWithUrl> {
    fn timeout(mut self, t: Duration) -> Self {
        self.timeout = Some(t);
        self
    }

    // Only available once URL is set
    fn build(self) -> Request {
        Request {
            url: self.state.url,
            timeout: self.timeout,
        }
    }
}

// Compile error: build() not available
let bad = RequestBuilder::new().build();

// Correct: must set URL first
let good = RequestBuilder::new()
    .url("https://example.com")
    .timeout(Duration::from_secs(30))
    .build();
```

## Transaction Example

```rust
struct NotStarted;
struct InProgress { tx_id: u64 }
struct Committed;

struct Transaction<State> {
    conn: Connection,
    state: State,
}

impl Transaction<NotStarted> {
    fn begin(conn: Connection) -> Result<Transaction<InProgress>, Error> {
        let tx_id = conn.execute("BEGIN")?;
        Ok(Transaction {
            conn,
            state: InProgress { tx_id },
        })
    }
}

impl Transaction<InProgress> {
    fn execute(&mut self, sql: &str) -> Result<(), Error> {
        self.conn.execute(sql)
    }

    fn commit(self) -> Result<Transaction<Committed>, Error> {
        self.conn.execute("COMMIT")?;
        Ok(Transaction {
            conn: self.conn,
            state: Committed,
        })
    }

    fn rollback(self) -> Connection {
        let _ = self.conn.execute("ROLLBACK");
        self.conn
    }
}
```

## Applying This in Crabka

- Prefer typestate over adding a new `InvalidState`/`NotReady`-style error variant to an existing type. If a call is only ever valid in one phase of an object's life, make it a method that only exists on the type representing that phase.
- Typestate is a fit for connection/session lifecycles, multi-phase protocol handshakes, and builders with required fields — not for things that are genuinely dynamic at runtime (e.g. cluster membership, partition leadership) where the "state" is external fact learned over the network rather than a local sequencing rule.
- Per this repo's compatibility policy (see root `CLAUDE.md`), Crabka is greenfield: when a typestate refactor changes a public API shape, just change it — don't keep the old runtime-checked API around behind a flag or as a deprecated alias.
- Kafka wire-protocol types (request/response structs mirroring the Kafka spec) are the exception: their shape is dictated by protocol compatibility, not by our internal lifecycle rules, so don't force typestate onto them.

## See Also

- [api-builder-pattern](https://github.com/leonardomso/rust-skills/blob/master/rules/api-builder-pattern.md) - Basic builder pattern
- [api-parse-dont-validate](https://github.com/leonardomso/rust-skills/blob/master/rules/api-parse-dont-validate.md) - Type-driven invariants
- [api-sealed-trait](https://github.com/leonardomso/rust-skills/blob/master/rules/api-sealed-trait.md) - Restricting trait implementations
