use crabka_admin_ui::views::{
    ReadRouteState, Route, RoutePage, acls, groups, layout::sidebar_links, log_dirs, quotas,
    render_page, render_route, topics, users,
};
use crabka_admin_ui::{dto::TopicRow, server_fns::AclRow};
use dioxus::dioxus_core::{Element, TemplateNode, VNode};

fn rendered_text(view: Element) -> String {
    let vnode = view.expect("static view should render without runtime errors");

    collect_vnode_text(&vnode)
}

fn collect_vnode_text(vnode: &VNode) -> String {
    let mut text = String::new();

    collect_template_text(vnode.template.roots, &mut text);

    text
}

fn collect_template_text(nodes: &[TemplateNode], text: &mut String) {
    for node in nodes {
        match node {
            TemplateNode::Element { children, .. } => collect_template_text(children, text),
            TemplateNode::Text { text: node_text } => text.push_str(node_text),
            TemplateNode::Dynamic { .. } => {}
        }
    }
}

fn assert_view_contains(view: Element, expected_text: &[&str]) {
    let actual_text = rendered_text(view);

    for expected in expected_text {
        assert!(
            actual_text.contains(expected),
            "expected rendered text to contain {expected:?}, got {actual_text:?}"
        );
    }
}

#[test]
fn route_exposes_first_slice_paths_and_labels() {
    let routes = [
        (Route::Overview, "/", "Overview"),
        (Route::Login, "/login", "Login"),
        (Route::Topics, "/topics", "Topics"),
        (Route::Groups, "/groups", "Groups"),
        (Route::Acls, "/acls", "ACLs"),
        (Route::Users, "/users", "Users"),
        (Route::Quotas, "/quotas", "Quotas"),
        (Route::LogDirs, "/log-dirs", "Log Dirs"),
    ];

    for (route, expected_path, expected_label) in routes {
        assert_eq!(route.path(), expected_path);
        assert_eq!(route.label(), expected_label);
    }
}

#[test]
fn sidebar_links_include_operations_routes_in_order() {
    let links = sidebar_links();

    let labels: Vec<_> = links.iter().map(|link| link.label).collect();
    let paths: Vec<_> = links.iter().map(|link| link.path).collect();

    assert_eq!(
        labels,
        vec![
            "Overview", "Topics", "Groups", "ACLs", "Users", "Quotas", "Log Dirs"
        ]
    );
    assert_eq!(
        paths,
        vec![
            "/",
            "/topics",
            "/groups",
            "/acls",
            "/users",
            "/quotas",
            "/log-dirs"
        ]
    );
}

#[test]
fn login_route_is_outside_operations_sidebar() {
    assert_eq!(Route::Login.path(), "/login");
    assert_eq!(Route::Login.label(), "Login");
    assert!(
        sidebar_links()
            .iter()
            .all(|link| link.route != Route::Login)
    );
}

#[test]
fn unauthenticated_route_guard_selects_login() {
    assert_eq!(
        Route::Overview.guard_for_authentication(false),
        Route::Login
    );
    assert_eq!(Route::Topics.guard_for_authentication(false), Route::Login);
    assert_eq!(Route::Login.guard_for_authentication(false), Route::Login);
    assert_eq!(
        Route::Overview.guard_for_authentication(true),
        Route::Overview
    );
}

#[test]
fn app_remains_callable() {
    assert!(crabka_admin_ui::app().is_ok());
}

#[test]
fn topic_view_renders_read_only_empty_state() {
    assert_view_contains(
        topics::topics_view(),
        &["Topics", "Create Topic", "No topics loaded yet."],
    );
}

#[test]
fn consumer_groups_view_renders_read_only_empty_state() {
    assert_view_contains(
        groups::groups_view(),
        &["Consumer Groups", "No groups loaded yet."],
    );
}

#[test]
fn acls_view_renders_read_only_empty_state() {
    assert_view_contains(
        acls::acls_view(),
        &["ACLs", "Create ACL", "No ACLs loaded yet."],
    );
}

#[test]
fn users_view_renders_scram_empty_state() {
    assert_view_contains(
        users::users_view(),
        &[
            "SCRAM Users",
            "Upsert SCRAM-SHA-512",
            "No user operation selected.",
        ],
    );
}

#[test]
fn quotas_view_renders_search_empty_state() {
    assert_view_contains(
        quotas::quotas_view(),
        &["Quotas", "Search for a user to describe quotas."],
    );
}

#[test]
fn log_dirs_view_renders_empty_state() {
    assert_view_contains(
        log_dirs::log_dirs_view(),
        &["Log Dirs", "No log-dir data loaded yet."],
    );
}

#[test]
fn every_route_renders_a_page() {
    let routes = [
        Route::Overview,
        Route::Login,
        Route::Topics,
        Route::Groups,
        Route::Acls,
        Route::Users,
        Route::Quotas,
        Route::LogDirs,
    ];

    for route in routes {
        assert!(
            render_route(route).is_ok(),
            "route {route:?} should render a page"
        );
    }
}

#[test]
fn shared_page_renderer_renders_dynamic_topics_and_acls() {
    let topics_html = render_page(&RoutePage::topics(ReadRouteState::Rows(vec![TopicRow {
        name: "orders<east>".to_string(),
        topic_id: None,
        partition_count: 3,
        replication_factor: 1,
        error: None,
    }])));
    let acls_html = render_page(&RoutePage::acls(ReadRouteState::Rows(vec![AclRow {
        resource: "Topic:orders".to_string(),
        principal: "User:alice".to_string(),
        operation: "Read".to_string(),
        permission: "Allow".to_string(),
    }])));

    assert!(topics_html.contains("admin-section topics-section"));
    assert!(topics_html.contains("orders&lt;east&gt;"));
    assert!(!topics_html.contains("orders<east>"));
    assert!(acls_html.contains("admin-section acls-section"));
    assert!(acls_html.contains("Topic:orders User:alice Read Allow"));
}
