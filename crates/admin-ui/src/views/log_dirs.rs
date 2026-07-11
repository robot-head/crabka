use dioxus::dioxus_core::Element;

use super::{Route, RoutePage, render_page_element};

/// # Errors
/// Returns an error when the request is invalid, authentication or session validation fails, or the broker admin operation reports a failure.
pub fn log_dirs_view() -> Element {
    render_page_element(&RoutePage::for_unauthenticated_route(Route::LogDirs))
}
