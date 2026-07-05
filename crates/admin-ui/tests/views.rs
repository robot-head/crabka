use crabka_admin_ui::views::{
    ReadRouteState, Route, RoutePage, acls, groups, layout::sidebar_links, log_dirs, quotas,
    render_page, render_page_body_html, render_route, render_route_html, topics, users,
};
use crabka_admin_ui::{dto::TopicRow, server_fns::AclRow};

fn ssr_route_body(page: &RoutePage) -> String {
    format!("<div>{}</div>", render_page_body_html(page))
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
fn protected_view_modules_use_shared_unauthenticated_page_by_default() {
    let expected_body = ssr_route_body(&RoutePage::login());

    for view in [
        topics::topics_view,
        groups::groups_view,
        acls::acls_view,
        users::users_view,
        quotas::quotas_view,
        log_dirs::log_dirs_view,
    ] {
        assert_eq!(dioxus_ssr::render_element(view()), expected_body);
    }
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
fn protected_render_routes_use_shared_unauthenticated_page_by_default() {
    let expected_html = render_page(&RoutePage::login());
    let expected_body = ssr_route_body(&RoutePage::login());

    for route in [
        Route::Overview,
        Route::Topics,
        Route::Groups,
        Route::Acls,
        Route::Users,
        Route::Quotas,
        Route::LogDirs,
    ] {
        let route_html = render_route_html(route);

        assert_eq!(route_html, expected_html, "{route:?} shared HTML");
        assert_eq!(
            dioxus_ssr::render_element(render_route(route)),
            expected_body
        );
        assert!(route_html.contains("Sign in to Crabka Admin"));
        assert!(!route_html.contains("operations-shell"));
    }
}

#[test]
fn route_element_ssr_embeds_shared_page_body_html() {
    let page = RoutePage::for_unauthenticated_route(Route::Acls);

    assert_eq!(
        dioxus_ssr::render_element(render_route(Route::Acls)),
        ssr_route_body(&page)
    );
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
