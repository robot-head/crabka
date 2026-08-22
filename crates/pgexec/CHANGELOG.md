# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
## [0.4.1] — 2026-08-22


### <!-- 1 -->🐛 Bug Fixes


- Repair the deterministic main failures and root-cause the flakes ([#1044](https://github.com/robot-head/crabka/pull/1044))


### <!-- 10 -->💼 Other


- PostgreSQL compatibility — 122,505 changed lines, 33/231 pg_regress files exact ([#985](https://github.com/robot-head/crabka/pull/985))

## [0.4.0] — 2026-08-12


### <!-- 0 -->🚀 Features


- Two-mode timestamp source for horizontal write scaling ([#842](https://github.com/robot-head/crabka/pull/842))

- Scalability + chaos harness for crabka-gres in both timestamp modes ([#863](https://github.com/robot-head/crabka/pull/863))

- Add jsonb, arrays, ON CONFLICT, and LISTEN/NOTIFY ([#923](https://github.com/robot-head/crabka/pull/923))

- Raise PostgreSQL 18.4 conformance to 63.1% and fix six correctness defects ([#930](https://github.com/robot-head/crabka/pull/930))

- Enforce foreign keys and give relations real schemas, including pg_temp ([#934](https://github.com/robot-head/crabka/pull/934)) (**breaking**)

- Expose runtime configuration policy ([#904](https://github.com/robot-head/crabka/pull/904)) (**breaking**)

- Emit rich distributed traces for the query path ([#961](https://github.com/robot-head/crabka/pull/961))


### <!-- 1 -->🐛 Bug Fixes


- Run explicit transactions concurrently ([#891](https://github.com/robot-head/crabka/pull/891))


### <!-- 10 -->💼 Other


- Remove the scaling walls — wire fixes, O(data) costs, write concurrency, 10x data volume ([#813](https://github.com/robot-head/crabka/pull/813))

- Soak-test fix round — vacuum pacing, fjall read-amp, driver compat, pgbench milestone ([#830](https://github.com/robot-head/crabka/pull/830))


### <!-- 3 -->📚 Documentation


- Rewrite all prose to ASD-STE100 Simplified Technical English ([#982](https://github.com/robot-head/crabka/pull/982))


### <!-- 4 -->⚡ Performance


- Persistent range-0 barrier sampler — 2x peak write throughput ([#899](https://github.com/robot-head/crabka/pull/899))
