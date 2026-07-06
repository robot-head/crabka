use dioxus::dioxus_core::Element;

use super::{Route, RoutePage, render_page_element};

pub fn users_view() -> Element {
    render_page_element(&RoutePage::for_unauthenticated_route(Route::Users))
}
