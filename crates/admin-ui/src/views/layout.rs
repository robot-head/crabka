use dioxus::dioxus_core::{Element, Template, TemplateAttribute, TemplateNode};

use super::{Route, static_vnode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SidebarLink {
    pub route: Route,
    pub path: &'static str,
    pub label: &'static str,
}

impl SidebarLink {
    const fn new(route: Route) -> Self {
        Self {
            route,
            path: route.path(),
            label: route.label(),
        }
    }
}

const SIDEBAR_LINKS: &[SidebarLink] = &[
    SidebarLink::new(Route::Overview),
    SidebarLink::new(Route::Topics),
    SidebarLink::new(Route::Groups),
    SidebarLink::new(Route::Acls),
    SidebarLink::new(Route::Users),
    SidebarLink::new(Route::Quotas),
    SidebarLink::new(Route::LogDirs),
];

#[must_use]
pub const fn sidebar_links() -> &'static [SidebarLink] {
    SIDEBAR_LINKS
}

pub fn operations_shell() -> Element {
    const SHELL_ATTRS: &[TemplateAttribute] = &[TemplateAttribute::Static {
        name: "class",
        value: "operations-shell",
        namespace: None,
    }];
    const BRAND_TEXT: &[TemplateNode] = &[TemplateNode::Text {
        text: "Crabka Operations",
    }];
    const SIDEBAR_TEXT: &[TemplateNode] = &[TemplateNode::Text {
        text: "Overview · Topics · Groups · ACLs · Users · Quotas · Log Dirs",
    }];
    const CONTENT_TITLE: &[TemplateNode] = &[TemplateNode::Text {
        text: "Cluster Overview",
    }];
    const CONTENT_BODY: &[TemplateNode] = &[TemplateNode::Text {
        text: "Broker administration shell is ready.",
    }];
    const ASIDE_CHILDREN: &[TemplateNode] = &[
        TemplateNode::Element {
            tag: "h1",
            namespace: None,
            attrs: &[],
            children: BRAND_TEXT,
        },
        TemplateNode::Element {
            tag: "nav",
            namespace: None,
            attrs: &[],
            children: SIDEBAR_TEXT,
        },
    ];
    const MAIN_CHILDREN: &[TemplateNode] = &[
        TemplateNode::Element {
            tag: "h2",
            namespace: None,
            attrs: &[],
            children: CONTENT_TITLE,
        },
        TemplateNode::Element {
            tag: "p",
            namespace: None,
            attrs: &[],
            children: CONTENT_BODY,
        },
    ];
    const SHELL_CHILDREN: &[TemplateNode] = &[
        TemplateNode::Element {
            tag: "aside",
            namespace: None,
            attrs: &[],
            children: ASIDE_CHILDREN,
        },
        TemplateNode::Element {
            tag: "main",
            namespace: None,
            attrs: &[],
            children: MAIN_CHILDREN,
        },
    ];
    const ROOTS: &[TemplateNode] = &[TemplateNode::Element {
        tag: "section",
        namespace: None,
        attrs: SHELL_ATTRS,
        children: SHELL_CHILDREN,
    }];
    const TEMPLATE: Template = Template {
        roots: ROOTS,
        node_paths: &[],
        attr_paths: &[],
    };

    Ok(static_vnode(TEMPLATE))
}
