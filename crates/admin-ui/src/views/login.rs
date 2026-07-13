use dioxus::dioxus_core::Element;

use super::{RoutePage, render_page_element};

/// # Errors
/// Returns an error when the request is invalid, authentication or session validation fails, or the broker admin operation reports a failure.
pub fn login_view() -> Element {
    render_page_element(&RoutePage::login())
}
