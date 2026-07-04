use crabka_admin_ui::views::{Route, layout::sidebar_links};

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
