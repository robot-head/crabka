# crabka-pgexec

PostgreSQL execution engine for Crabka Gres.

`crabka-pgexec` turns parsed SQL into catalog, MVCC, and KV-store operations,
then exposes the result through the `crabka-pgwire` engine/session traits. It
comes from `crabgresql` executor commit `93f3d17168d056a28b4abe60af3b489d4bf62f1d`.
