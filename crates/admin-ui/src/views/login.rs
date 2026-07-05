use dioxus::dioxus_core::Element;

use super::{RoutePage, render_page_element};

pub fn login_view() -> Element {
    render_page_element(&RoutePage::login())
}
