pub mod acls;
pub mod groups;
pub mod layout;
pub mod log_dirs;
pub mod login;
pub mod overview;
pub mod page;
pub mod quotas;
pub mod topics;
pub mod users;

use dioxus::prelude::*;
use page::LoginRouteState;
pub use page::{ReadRouteState, RoutePage, render_page, render_page_body_html};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Route {
    Overview,
    Login,
    Topics,
    Groups,
    Acls,
    Users,
    Quotas,
    LogDirs,
}

impl Route {
    #[must_use]
    pub const fn path(self) -> &'static str {
        match self {
            Self::Overview => "/",
            Self::Login => "/login",
            Self::Topics => "/topics",
            Self::Groups => "/groups",
            Self::Acls => "/acls",
            Self::Users => "/users",
            Self::Quotas => "/quotas",
            Self::LogDirs => "/log-dirs",
        }
    }

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Login => "Login",
            Self::Topics => "Topics",
            Self::Groups => "Groups",
            Self::Acls => "ACLs",
            Self::Users => "Users",
            Self::Quotas => "Quotas",
            Self::LogDirs => "Log Dirs",
        }
    }

    #[must_use]
    pub const fn guard_for_authentication(self, is_authenticated: bool) -> Self {
        if is_authenticated || matches!(self, Self::Login) {
            return self;
        }

        Self::Login
    }
}

#[must_use]
pub fn render_route_html(route: Route) -> String {
    render_page(&RoutePage::for_unauthenticated_route(route))
}

/// # Errors
/// Returns an error when the request is invalid, authentication or session validation fails, or the broker admin operation reports a failure.
pub fn render_route(route: Route) -> Element {
    render_page_element(&RoutePage::for_unauthenticated_route(route))
}

/// # Errors
/// Returns an error when the request is invalid, authentication or session validation fails, or the broker admin operation reports a failure.
pub fn render_page_element(page: &RoutePage) -> Element {
    match page.clone() {
        RoutePage::Login { state } => render_login_element(state),
        page => render_operations_shell_element(page),
    }
}

fn render_login_element(state: LoginRouteState) -> Element {
    match state {
        LoginRouteState::Form => rsx! {
            div {
                section { class: "login-shell",
                    h1 { "Sign in to Crabka Admin" }
                    p { "Authentication is required before broker operations are shown." }
                    form { method: "post", action: "/login",
                        label {
                            "Username "
                            input { name: "username", autocomplete: "username" }
                        }
                        label {
                            "Password "
                            input { name: "password", r#type: "password", autocomplete: "current-password" }
                        }
                        button { r#type: "submit", "Sign in" }
                    }
                }
            }
        },
        LoginRouteState::AuthenticationFailed => rsx! {
            div {
                section { class: "login-shell",
                    h1 { "Sign in to Crabka Admin" }
                    p { "Authentication is required before broker operations are shown." }
                    p { "Authentication failed." }
                }
            }
        },
    }
}

fn render_operations_shell_element(page: RoutePage) -> Element {
    let content = match page {
        RoutePage::Overview { message } => render_overview_element(message),
        RoutePage::Topics { state } => render_topics_element(state),
        RoutePage::Groups { state } => render_groups_element(state),
        RoutePage::Acls { state } => render_acls_element(state),
        RoutePage::Users { state } => render_users_element(state),
        RoutePage::Quotas { state } => render_quotas_element(state),
        RoutePage::LogDirs { state } => render_log_dirs_element(state),
        RoutePage::Login { .. } => unreachable!("login pages render outside the operations shell"),
    }?;

    rsx! {
        div {
            section { class: "operations-shell",
                aside {
                    h1 { "Crabka Operations" }
                    nav { "Overview · Topics · Groups · ACLs · Users · Quotas · Log Dirs" }
                }
                main { {content} }
            }
        }
    }
}

fn render_overview_element(message: Option<&'static str>) -> Element {
    rsx! {
        section {
            h2 { "Cluster Overview" }
            p { "Broker administration shell is ready." }
            if let Some(message) = message {
                p { "{message}" }
            }
        }
    }
}

fn render_topics_element(state: ReadRouteState<crate::dto::TopicRow>) -> Element {
    match state {
        ReadRouteState::Loading => rsx! {
            section { class: "admin-section topics-section",
                h2 { "Topics" }
                button { "Create Topic" }
                p { "Loading topics…" }
            }
        },
        ReadRouteState::AuthenticationRequired => rsx! {
            section { class: "admin-section topics-section",
                h2 { "Topics" }
                button { "Create Topic" }
                p { "Authentication required." }
            }
        },
        ReadRouteState::LoadFailed => rsx! {
            section { class: "admin-section topics-section",
                h2 { "Topics" }
                button { "Create Topic" }
                p { "Unable to load topics." }
            }
        },
        ReadRouteState::Rows(rows) if rows.is_empty() => rsx! {
            section { class: "admin-section topics-section",
                h2 { "Topics" }
                button { "Create Topic" }
                p { "No topics loaded yet." }
            }
        },
        ReadRouteState::Rows(rows) => rsx! {
            section { class: "admin-section topics-section",
                h2 { "Topics" }
                button { "Create Topic" }
                ul {
                    for row in rows {
                        li { "{row.name}" }
                    }
                }
            }
        },
    }
}

fn render_groups_element(state: ReadRouteState<crate::dto::GroupRow>) -> Element {
    match state {
        ReadRouteState::Loading => rsx! {
            section { class: "admin-section groups-section",
                h2 { "Consumer Groups" }
                p { "Loading consumer groups…" }
            }
        },
        ReadRouteState::AuthenticationRequired => rsx! {
            section { class: "admin-section groups-section",
                h2 { "Consumer Groups" }
                p { "Authentication required." }
            }
        },
        ReadRouteState::LoadFailed => rsx! {
            section { class: "admin-section groups-section",
                h2 { "Consumer Groups" }
                p { "Unable to load consumer groups." }
            }
        },
        ReadRouteState::Rows(rows) if rows.is_empty() => rsx! {
            section { class: "admin-section groups-section",
                h2 { "Consumer Groups" }
                p { "No consumer groups loaded yet." }
            }
        },
        ReadRouteState::Rows(rows) => rsx! {
            section { class: "admin-section groups-section",
                h2 { "Consumer Groups" }
                ul {
                    for row in rows {
                        li { "{row.group_id}" }
                    }
                }
            }
        },
    }
}

fn render_acls_element(state: ReadRouteState<crate::dto::AclRow>) -> Element {
    match state {
        ReadRouteState::Loading => rsx! {
            section { class: "admin-section acls-section",
                h2 { "ACLs" }
                button { "Create ACL" }
                p { "Loading ACLs…" }
            }
        },
        ReadRouteState::AuthenticationRequired => rsx! {
            section { class: "admin-section acls-section",
                h2 { "ACLs" }
                button { "Create ACL" }
                p { "Authentication required." }
            }
        },
        ReadRouteState::LoadFailed => rsx! {
            section { class: "admin-section acls-section",
                h2 { "ACLs" }
                button { "Create ACL" }
                p { "Unable to load ACLs." }
            }
        },
        ReadRouteState::Rows(rows) if rows.is_empty() => rsx! {
            section { class: "admin-section acls-section",
                h2 { "ACLs" }
                button { "Create ACL" }
                p { "No ACLs loaded yet." }
            }
        },
        ReadRouteState::Rows(rows) => rsx! {
            section { class: "admin-section acls-section",
                h2 { "ACLs" }
                button { "Create ACL" }
                ul {
                    for row in rows {
                        li { "{row.resource} {row.principal} {row.operation} {row.permission}" }
                    }
                }
            }
        },
    }
}

fn render_users_element(state: ReadRouteState<crate::dto::UserRow>) -> Element {
    match state {
        ReadRouteState::Loading => rsx! {
            section { class: "admin-section users-section",
                h2 { "SCRAM Users" }
                button { "Upsert SCRAM-SHA-512" }
                p { "Loading SCRAM users…" }
            }
        },
        ReadRouteState::AuthenticationRequired => rsx! {
            section { class: "admin-section users-section",
                h2 { "SCRAM Users" }
                button { "Upsert SCRAM-SHA-512" }
                p { "Authentication required." }
            }
        },
        ReadRouteState::LoadFailed => rsx! {
            section { class: "admin-section users-section",
                h2 { "SCRAM Users" }
                button { "Upsert SCRAM-SHA-512" }
                p { "Unable to load SCRAM users." }
            }
        },
        ReadRouteState::Rows(rows) if rows.is_empty() => rsx! {
            section { class: "admin-section users-section",
                h2 { "SCRAM Users" }
                button { "Upsert SCRAM-SHA-512" }
                p { "No SCRAM users loaded yet." }
            }
        },
        ReadRouteState::Rows(rows) => rsx! {
            section { class: "admin-section users-section",
                h2 { "SCRAM Users" }
                button { "Upsert SCRAM-SHA-512" }
                ul {
                    for row in rows {
                        li { "{row.username} {row.principal}" }
                    }
                }
            }
        },
    }
}

fn render_quotas_element(state: ReadRouteState<crate::dto::QuotaRow>) -> Element {
    match state {
        ReadRouteState::Loading => rsx! {
            section { class: "admin-section quotas-section",
                h2 { "Quotas" }
                p { "Loading quotas…" }
            }
        },
        ReadRouteState::AuthenticationRequired => rsx! {
            section { class: "admin-section quotas-section",
                h2 { "Quotas" }
                p { "Authentication required." }
            }
        },
        ReadRouteState::LoadFailed => rsx! {
            section { class: "admin-section quotas-section",
                h2 { "Quotas" }
                p { "Unable to load quotas." }
            }
        },
        ReadRouteState::Rows(rows) if rows.is_empty() => rsx! {
            section { class: "admin-section quotas-section",
                h2 { "Quotas" }
                p { "No quotas loaded yet." }
            }
        },
        ReadRouteState::Rows(rows) => rsx! {
            section { class: "admin-section quotas-section",
                h2 { "Quotas" }
                ul {
                    for row in rows {
                        li { "{row.entity} {row.quota_type} {row.value}" }
                    }
                }
            }
        },
    }
}

fn render_log_dirs_element(state: ReadRouteState<crate::dto::LogDirRow>) -> Element {
    match state {
        ReadRouteState::Loading => rsx! {
            section { class: "admin-section log-dirs-section",
                h2 { "Log Dirs" }
                p { "Loading log-dir data…" }
            }
        },
        ReadRouteState::AuthenticationRequired => rsx! {
            section { class: "admin-section log-dirs-section",
                h2 { "Log Dirs" }
                p { "Authentication required." }
            }
        },
        ReadRouteState::LoadFailed => rsx! {
            section { class: "admin-section log-dirs-section",
                h2 { "Log Dirs" }
                p { "Unable to load log-dir data." }
            }
        },
        ReadRouteState::Rows(rows) if rows.is_empty() => rsx! {
            section { class: "admin-section log-dirs-section",
                h2 { "Log Dirs" }
                p { "No log-dir data loaded yet." }
            }
        },
        ReadRouteState::Rows(rows) => rsx! {
            section { class: "admin-section log-dirs-section",
                h2 { "Log Dirs" }
                ul {
                    for row in rows {
                        li { "{row.log_dir} {row.topic}/{row.partition}-{row.partition_size}" }
                    }
                }
            }
        },
    }
}
