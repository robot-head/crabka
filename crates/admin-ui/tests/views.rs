use crabka_admin_ui::{
    dto::TopicRow,
    server_fns::AclRow,
    views::{
        ReadRouteState, Route, RoutePage, acls, groups, layout::sidebar_links, log_dirs, quotas,
        render_page, render_page_body_html, render_route, render_route_html, topics, users,
    },
};

fn ssr_route_body(page: &RoutePage) -> String {
    render_page_body_html(page)
}

fn ssr_full_page(page: &RoutePage) -> String {
    format!(
        "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>{}</title></head><body>{}</body></html>",
        expected_title(page),
        dioxus_ssr::render_element(crabka_admin_ui::views::render_page_element(page))
    )
}

fn expected_title(page: &RoutePage) -> &'static str {
    match page {
        RoutePage::Overview { .. } => "Crabka Admin",
        RoutePage::Login { .. } => "Sign in to Crabka",
        RoutePage::Topics { .. } => "Topics",
        RoutePage::Groups { .. } => "Consumer Groups",
        RoutePage::Acls { .. } => "ACLs",
        RoutePage::Users { .. } => "SCRAM Users",
        RoutePage::Quotas { .. } => "Quotas",
        RoutePage::LogDirs { .. } => "Log Dirs",
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
        assert2::assert!(route.path() == expected_path);
        assert2::assert!(route.label() == expected_label);
    }
}

#[test]
fn sidebar_links_include_operations_routes_in_order() {
    let links = sidebar_links();

    assert2::assert!(
        links
            .iter()
            .map(|link| (link.label, link.path))
            .collect::<Vec<_>>()
            == [
                ("Overview", "/"),
                ("Topics", "/topics"),
                ("Groups", "/groups"),
                ("ACLs", "/acls"),
                ("Users", "/users"),
                ("Quotas", "/quotas"),
                ("Log Dirs", "/log-dirs"),
            ]
    );
}

#[test]
fn login_route_is_outside_operations_sidebar() {
    assert2::assert!(Route::Login.path() == "/login");
    assert2::assert!(Route::Login.label() == "Login");
    assert2::assert!(
        sidebar_links()
            .iter()
            .all(|link| link.route != Route::Login)
    );
}

#[test]
fn unauthenticated_route_guard_selects_login() {
    for (_name, route, authenticated, expected) in [
        (
            "overview unauthenticated",
            Route::Overview,
            false,
            Route::Login,
        ),
        ("topics unauthenticated", Route::Topics, false, Route::Login),
        ("login unauthenticated", Route::Login, false, Route::Login),
        (
            "overview authenticated",
            Route::Overview,
            true,
            Route::Overview,
        ),
    ] {
        assert2::assert!(route.guard_for_authentication(authenticated) == expected);
    }
}

#[test]
fn app_remains_callable() {
    assert2::assert!(crabka_admin_ui::app().is_ok());
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
        assert2::assert!(dioxus_ssr::render_element(view()) == expected_body);
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
        assert2::assert!(render_route(route).is_ok());
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

        assert2::assert!(route_html == expected_html);
        assert2::assert!(dioxus_ssr::render_element(render_route(route)) == expected_body);
        assert2::assert!(route_html.contains("Sign in to Crabka Admin"));
        assert2::assert!(!route_html.contains("operations-shell"));
    }
}

#[test]
fn route_element_ssr_embeds_shared_page_body_html() {
    let page = RoutePage::for_unauthenticated_route(Route::Acls);

    assert2::assert!(
        dioxus_ssr::render_element(render_route(Route::Acls)) == ssr_route_body(&page)
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

    assert2::assert!(topics_html.contains("admin-section topics-section"));
    assert2::assert!(topics_html.contains("orders&#60;east&#62;"));
    assert2::assert!(!topics_html.contains("orders<east>"));
    assert2::assert!(acls_html.contains("admin-section acls-section"));
    assert2::assert!(acls_html.contains("Topic:orders User:alice Read Allow"));
}

#[test]
fn full_page_renderer_uses_dioxus_ssr_for_dynamic_and_protected_pages() {
    let dynamic_topic_page = RoutePage::topics(ReadRouteState::Rows(vec![TopicRow {
        name: "orders<east>".to_string(),
        topic_id: None,
        partition_count: 3,
        replication_factor: 1,
        error: None,
    }]));
    let protected_page = RoutePage::for_unauthenticated_route(Route::Topics);

    for page in [&dynamic_topic_page, &protected_page] {
        let rendered_page = render_page(page);

        assert2::assert!(rendered_page == ssr_full_page(page));
        assert2::assert!(rendered_page.contains("<body><div>"));
    }
}

#[test]
fn shared_page_renderer_renders_empty_table_states() {
    let cases = [
        (
            RoutePage::topics(ReadRouteState::Rows(Vec::new())),
            "No topics loaded yet.",
        ),
        (
            RoutePage::groups(ReadRouteState::Rows(Vec::new())),
            "No consumer groups loaded yet.",
        ),
        (
            RoutePage::acls(ReadRouteState::Rows(Vec::new())),
            "No ACLs loaded yet.",
        ),
        (
            RoutePage::users(ReadRouteState::Rows(Vec::new())),
            "No SCRAM users loaded yet.",
        ),
        (
            RoutePage::quotas(ReadRouteState::Rows(Vec::new())),
            "No quotas loaded yet.",
        ),
        (
            RoutePage::log_dirs(ReadRouteState::Rows(Vec::new())),
            "No log-dir data loaded yet.",
        ),
    ];

    for (page, empty_message) in cases {
        let rendered = render_page(&page);

        assert2::assert!(rendered.contains(empty_message));
        assert2::assert!(!rendered.contains("<ul>"));
    }
}

#[test]
fn shared_page_renderer_escapes_all_html_metacharacters() {
    let rendered = render_page(&RoutePage::topics(ReadRouteState::Rows(vec![TopicRow {
        name: "<&>\"'".to_string(),
        topic_id: None,
        partition_count: 1,
        replication_factor: 1,
        error: None,
    }])));

    assert2::assert!(rendered.contains("&#38;"));
    assert2::assert!(rendered.contains("&#60;"));
    assert2::assert!(rendered.contains("&#62;"));
    assert2::assert!(rendered.contains("&#34;"));
    assert2::assert!(rendered.contains("&#39;"));
    assert2::assert!(!rendered.contains("<&>\"'"));
}
