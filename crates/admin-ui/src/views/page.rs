use super::Route;
use crate::dto::{AclRow, GroupRow, LogDirRow, QuotaRow, TopicRow, UserRow};

#[derive(Clone, PartialEq, Eq)]
pub enum ReadRouteState<T> {
    Loading,
    AuthenticationRequired,
    LoadFailed,
    Rows(Vec<T>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LoginRouteState {
    Form,
    AuthenticationFailed,
}

#[derive(Clone, PartialEq, Eq)]
pub enum RoutePage {
    Overview { message: Option<&'static str> },
    Login { state: LoginRouteState },
    Topics { state: ReadRouteState<TopicRow> },
    Groups { state: ReadRouteState<GroupRow> },
    Acls { state: ReadRouteState<AclRow> },
    Users { state: ReadRouteState<UserRow> },
    Quotas { state: ReadRouteState<QuotaRow> },
    LogDirs { state: ReadRouteState<LogDirRow> },
}

impl RoutePage {
    #[must_use]
    pub const fn overview() -> Self {
        Self::Overview { message: None }
    }

    #[must_use]
    pub const fn signed_in() -> Self {
        Self::Overview {
            message: Some("Signed in."),
        }
    }

    #[must_use]
    pub const fn login() -> Self {
        Self::Login {
            state: LoginRouteState::Form,
        }
    }

    #[must_use]
    pub const fn login_failed() -> Self {
        Self::Login {
            state: LoginRouteState::AuthenticationFailed,
        }
    }

    #[must_use]
    pub const fn for_unauthenticated_route(route: Route) -> Self {
        Self::for_guarded_route(route.guard_for_authentication(false))
    }

    #[must_use]
    pub const fn for_authenticated_route(route: Route) -> Self {
        Self::for_guarded_route(route.guard_for_authentication(true))
    }

    const fn for_guarded_route(route: Route) -> Self {
        match route {
            Route::Overview => Self::overview(),
            Route::Login => Self::login(),
            Route::Topics => Self::topics(ReadRouteState::Loading),
            Route::Groups => Self::groups(ReadRouteState::Loading),
            Route::Acls => Self::acls(ReadRouteState::Loading),
            Route::Users => Self::users(ReadRouteState::Loading),
            Route::Quotas => Self::quotas(ReadRouteState::Loading),
            Route::LogDirs => Self::log_dirs(ReadRouteState::Loading),
        }
    }

    #[must_use]
    pub const fn topics(state: ReadRouteState<TopicRow>) -> Self {
        Self::Topics { state }
    }

    #[must_use]
    pub const fn groups(state: ReadRouteState<GroupRow>) -> Self {
        Self::Groups { state }
    }

    #[must_use]
    pub const fn acls(state: ReadRouteState<AclRow>) -> Self {
        Self::Acls { state }
    }

    #[must_use]
    pub const fn users(state: ReadRouteState<UserRow>) -> Self {
        Self::Users { state }
    }

    #[must_use]
    pub const fn quotas(state: ReadRouteState<QuotaRow>) -> Self {
        Self::Quotas { state }
    }

    #[must_use]
    pub const fn log_dirs(state: ReadRouteState<LogDirRow>) -> Self {
        Self::LogDirs { state }
    }

    const fn title(&self) -> &'static str {
        match self {
            Self::Overview { .. } => "Crabka Admin",
            Self::Login { .. } => "Sign in to Crabka",
            Self::Topics { .. } => "Topics",
            Self::Groups { .. } => "Consumer Groups",
            Self::Acls { .. } => "ACLs",
            Self::Users { .. } => "SCRAM Users",
            Self::Quotas { .. } => "Quotas",
            Self::LogDirs { .. } => "Log Dirs",
        }
    }
}

#[must_use]
pub fn render_page(page: &RoutePage) -> String {
    let mut html = String::new();
    html.push_str(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>"#,
    );
    push_escaped(&mut html, page.title());
    html.push_str("</title></head><body>");
    html.push_str(&render_page_body_html(page));
    html.push_str("</body></html>");
    html
}

#[must_use]
pub fn render_page_body_html(page: &RoutePage) -> String {
    dioxus_ssr::render_element(super::render_page_element(page))
}

fn push_escaped(html: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '&' => html.push_str("&amp;"),
            '<' => html.push_str("&lt;"),
            '>' => html.push_str("&gt;"),
            '"' => html.push_str("&quot;"),
            '\'' => html.push_str("&#39;"),
            _ => html.push(character),
        }
    }
}
