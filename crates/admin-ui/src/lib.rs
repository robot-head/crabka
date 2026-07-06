//! Standalone Dioxus administration UI for one Crabka cluster.

pub mod admin;
pub mod auth;
pub mod config;
pub mod dto;
pub mod error;
pub mod permissions;
pub mod server;
pub mod server_fns;
pub mod session;
pub mod views;

#[allow(non_snake_case)]
pub fn App() -> Element {
    views::overview::overview_view()
}

pub fn app() -> Element {
    App()
}

use dioxus::dioxus_core::Element;
