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

use dioxus::dioxus_core::{Element, Template, VNode};

pub use page::{ReadRouteState, RoutePage, render_page};

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

pub fn render_route(route: Route) -> Element {
    match route {
        Route::Overview => overview::overview_view(),
        Route::Login => login::login_view(),
        Route::Topics => topics::topics_view(),
        Route::Groups => groups::groups_view(),
        Route::Acls => acls::acls_view(),
        Route::Users => users::users_view(),
        Route::Quotas => quotas::quotas_view(),
        Route::LogDirs => log_dirs::log_dirs_view(),
    }
}

fn static_vnode(template: Template) -> VNode {
    VNode::new(None, template, Box::new([]), Box::new([]))
}
