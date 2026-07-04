pub mod layout;
pub mod login;
pub mod overview;

use dioxus::dioxus_core::{Template, VNode};

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

fn static_vnode(template: Template) -> VNode {
    VNode::new(None, template, Box::new([]), Box::new([]))
}
