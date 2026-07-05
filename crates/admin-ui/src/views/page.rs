use std::fmt::Write as _;

use crate::dto::{AclRow, GroupRow, LogDirRow, QuotaRow, TopicRow, UserRow};

use super::Route;

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
    push_page_body(page, &mut html);
    html.push_str("</body></html>");
    html
}

#[must_use]
pub fn render_page_body_html(page: &RoutePage) -> String {
    let mut html = String::new();
    push_page_body(page, &mut html);
    html
}

fn push_page_body(page: &RoutePage, html: &mut String) {
    if let RoutePage::Login { state } = page {
        render_login(*state, html);
        return;
    }

    render_operations_shell(page, html);
}

fn render_login(state: LoginRouteState, html: &mut String) {
    html.push_str(
        r#"<section class="login-shell"><h1>Sign in to Crabka Admin</h1><p>Authentication is required before broker operations are shown.</p>"#,
    );

    match state {
        LoginRouteState::Form => html.push_str(login_form()),
        LoginRouteState::AuthenticationFailed => render_paragraph("Authentication failed.", html),
    }

    html.push_str("</section>");
}

fn render_operations_shell(page: &RoutePage, html: &mut String) {
    html.push_str(
        r#"<section class="operations-shell"><aside><h1>Crabka Operations</h1><nav>Overview · Topics · Groups · ACLs · Users · Quotas · Log Dirs</nav></aside><main>"#,
    );

    match page {
        RoutePage::Overview { message } => render_overview(*message, html),
        RoutePage::Topics { state } => render_topics(state, html),
        RoutePage::Groups { state } => render_groups(state, html),
        RoutePage::Acls { state } => render_acls(state, html),
        RoutePage::Users { state } => render_users(state, html),
        RoutePage::Quotas { state } => render_quotas(state, html),
        RoutePage::LogDirs { state } => render_log_dirs(state, html),
        RoutePage::Login { .. } => unreachable!("login pages render outside the operations shell"),
    }

    html.push_str("</main></section>");
}

fn render_overview(message: Option<&str>, html: &mut String) {
    html.push_str("<section><h2>Cluster Overview</h2><p>Broker administration shell is ready.</p>");

    if let Some(message) = message {
        render_paragraph(message, html);
    }

    html.push_str("</section>");
}

fn render_topics(state: &ReadRouteState<TopicRow>, html: &mut String) {
    html.push_str(r#"<section class="admin-section topics-section"><h2>Topics</h2><button>Create Topic</button>"#);

    match state {
        ReadRouteState::Loading => render_paragraph("Loading topics…", html),
        ReadRouteState::AuthenticationRequired => {
            render_paragraph("Authentication required.", html);
        }
        ReadRouteState::LoadFailed => render_paragraph("Unable to load topics.", html),
        ReadRouteState::Rows(rows) => render_topic_rows(rows, html),
    }

    html.push_str("</section>");
}

fn render_topic_rows(rows: &[TopicRow], html: &mut String) {
    if rows.is_empty() {
        render_paragraph("No topics loaded yet.", html);
        return;
    }

    html.push_str("<ul>");
    for row in rows {
        html.push_str("<li>");
        push_escaped(html, &row.name);
        html.push_str("</li>");
    }
    html.push_str("</ul>");
}

fn render_groups(state: &ReadRouteState<GroupRow>, html: &mut String) {
    html.push_str(r#"<section class="admin-section groups-section"><h2>Consumer Groups</h2>"#);

    match state {
        ReadRouteState::Loading => render_paragraph("Loading consumer groups…", html),
        ReadRouteState::AuthenticationRequired => {
            render_paragraph("Authentication required.", html);
        }
        ReadRouteState::LoadFailed => render_paragraph("Unable to load consumer groups.", html),
        ReadRouteState::Rows(rows) => render_group_rows(rows, html),
    }

    html.push_str("</section>");
}

fn render_group_rows(rows: &[GroupRow], html: &mut String) {
    if rows.is_empty() {
        render_paragraph("No consumer groups loaded yet.", html);
        return;
    }

    html.push_str("<ul>");
    for row in rows {
        html.push_str("<li>");
        push_escaped(html, &row.group_id);
        html.push_str("</li>");
    }
    html.push_str("</ul>");
}

fn render_acls(state: &ReadRouteState<AclRow>, html: &mut String) {
    html.push_str(
        r#"<section class="admin-section acls-section"><h2>ACLs</h2><button>Create ACL</button>"#,
    );

    match state {
        ReadRouteState::Loading => render_paragraph("Loading ACLs…", html),
        ReadRouteState::AuthenticationRequired => {
            render_paragraph("Authentication required.", html);
        }
        ReadRouteState::LoadFailed => render_paragraph("Unable to load ACLs.", html),
        ReadRouteState::Rows(rows) => render_acl_rows(rows, html),
    }

    html.push_str("</section>");
}

fn render_acl_rows(rows: &[AclRow], html: &mut String) {
    if rows.is_empty() {
        render_paragraph("No ACLs loaded yet.", html);
        return;
    }

    html.push_str("<ul>");
    for row in rows {
        html.push_str("<li>");
        push_escaped(html, &row.resource);
        html.push(' ');
        push_escaped(html, &row.principal);
        html.push(' ');
        push_escaped(html, &row.operation);
        html.push(' ');
        push_escaped(html, &row.permission);
        html.push_str("</li>");
    }
    html.push_str("</ul>");
}

fn render_users(state: &ReadRouteState<UserRow>, html: &mut String) {
    html.push_str(r#"<section class="admin-section users-section"><h2>SCRAM Users</h2><button>Upsert SCRAM-SHA-512</button>"#);

    match state {
        ReadRouteState::Loading => render_paragraph("Loading SCRAM users…", html),
        ReadRouteState::AuthenticationRequired => {
            render_paragraph("Authentication required.", html);
        }
        ReadRouteState::LoadFailed => render_paragraph("Unable to load SCRAM users.", html),
        ReadRouteState::Rows(rows) => render_user_rows(rows, html),
    }

    html.push_str("</section>");
}

fn render_user_rows(rows: &[UserRow], html: &mut String) {
    if rows.is_empty() {
        render_paragraph("No SCRAM users loaded yet.", html);
        return;
    }

    html.push_str("<ul>");
    for row in rows {
        html.push_str("<li>");
        push_escaped(html, &row.username);
        html.push(' ');
        push_escaped(html, &row.principal);
        html.push_str("</li>");
    }
    html.push_str("</ul>");
}

fn render_quotas(state: &ReadRouteState<QuotaRow>, html: &mut String) {
    html.push_str(r#"<section class="admin-section quotas-section"><h2>Quotas</h2>"#);

    match state {
        ReadRouteState::Loading => render_paragraph("Loading quotas…", html),
        ReadRouteState::AuthenticationRequired => {
            render_paragraph("Authentication required.", html);
        }
        ReadRouteState::LoadFailed => render_paragraph("Unable to load quotas.", html),
        ReadRouteState::Rows(rows) => render_quota_rows(rows, html),
    }

    html.push_str("</section>");
}

fn render_quota_rows(rows: &[QuotaRow], html: &mut String) {
    if rows.is_empty() {
        render_paragraph("No quotas loaded yet.", html);
        return;
    }

    html.push_str("<ul>");
    for row in rows {
        html.push_str("<li>");
        push_escaped(html, &row.entity);
        html.push(' ');
        push_escaped(html, &row.quota_type);
        html.push(' ');
        push_escaped(html, &row.value);
        html.push_str("</li>");
    }
    html.push_str("</ul>");
}

fn render_log_dirs(state: &ReadRouteState<LogDirRow>, html: &mut String) {
    html.push_str(r#"<section class="admin-section log-dirs-section"><h2>Log Dirs</h2>"#);

    match state {
        ReadRouteState::Loading => render_paragraph("Loading log-dir data…", html),
        ReadRouteState::AuthenticationRequired => {
            render_paragraph("Authentication required.", html);
        }
        ReadRouteState::LoadFailed => render_paragraph("Unable to load log-dir data.", html),
        ReadRouteState::Rows(rows) => render_log_dir_rows(rows, html),
    }

    html.push_str("</section>");
}

fn render_log_dir_rows(rows: &[LogDirRow], html: &mut String) {
    if rows.is_empty() {
        render_paragraph("No log-dir data loaded yet.", html);
        return;
    }

    html.push_str("<ul>");
    for row in rows {
        html.push_str("<li>");
        push_escaped(html, &row.log_dir);
        html.push(' ');
        push_escaped(html, &row.topic);
        write!(html, "/{}-{}</li>", row.partition, row.partition_size)
            .expect("writing to String cannot fail");
    }
    html.push_str("</ul>");
}

fn render_paragraph(message: &str, html: &mut String) {
    html.push_str("<p>");
    push_escaped(html, message);
    html.push_str("</p>");
}

fn login_form() -> &'static str {
    r#"<form method="post" action="/login"><label>Username <input name="username" autocomplete="username"/></label><label>Password <input name="password" type="password" autocomplete="current-password"/></label><button type="submit">Sign in</button></form>"#
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
