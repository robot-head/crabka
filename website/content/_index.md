+++
title = "Crabka"
sort_by = "weight"

[extra]
lead = "A Rust reimplementation of Apache Kafka — wire-protocol exact, KRaft metadata, Kubernetes operator."
url = "/guide/introduction/"
url_button = "Get started"
repo_url = "https://github.com/robot-head/crabka"
repo_license = "Apache 2.0"
repo_version = "0.1"

[[extra.menu.main]]
name = "Guide"
url = "/guide/"
section = "guide"
weight = 10

[[extra.menu.main]]
name = "Reference"
url = "/reference/"
section = "reference"
weight = 20
+++

A Rust reimplementation of Apache Kafka. Speaks the Kafka wire protocol
byte-for-byte, runs its metadata quorum on KRaft, and ships a Kubernetes
operator and a Cruise-Control-equivalent rebalancer.
