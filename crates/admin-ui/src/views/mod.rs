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

use dioxus::dioxus_core::{Attribute, Element, Template, TemplateAttribute, TemplateNode, VNode};

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

pub fn render_route(route: Route) -> Element {
    render_page_element(&RoutePage::for_unauthenticated_route(route))
}

pub fn render_page_element(page: &RoutePage) -> Element {
    Ok(render_html_fragment_vnode(render_page_body_html(page)))
}

fn render_html_fragment_vnode(html: String) -> VNode {
    const ROOT_ATTRS: &[TemplateAttribute] = &[TemplateAttribute::Dynamic { id: 0 }];
    const ROOTS: &[TemplateNode] = &[TemplateNode::Element {
        tag: "div",
        namespace: None,
        attrs: ROOT_ATTRS,
        children: &[],
    }];
    const ATTR_PATHS: &[&[u8]] = &[&[0]];
    const TEMPLATE: Template = Template {
        roots: ROOTS,
        node_paths: &[],
        attr_paths: ATTR_PATHS,
    };

    VNode::new(
        None,
        TEMPLATE,
        Box::new([]),
        Box::new([Box::new([Attribute::new(
            "dangerous_inner_html",
            html,
            None,
            false,
        )])]),
    )
}
