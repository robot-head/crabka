//! Standalone Dioxus administration UI for one Crabka cluster.

pub mod server;

use dioxus::dioxus_core::{Element, Template, TemplateAttribute, TemplateNode, VNode};

#[allow(non_snake_case)]
pub fn App() -> Element {
    const MAIN_ATTRS: &[TemplateAttribute] = &[TemplateAttribute::Static {
        name: "class",
        value: "admin-shell",
        namespace: None,
    }];
    const H1_CHILDREN: &[TemplateNode] = &[TemplateNode::Text {
        text: "Crabka Admin",
    }];
    const P_CHILDREN: &[TemplateNode] = &[TemplateNode::Text {
        text: "Admin UI server is running.",
    }];
    const MAIN_CHILDREN: &[TemplateNode] = &[
        TemplateNode::Element {
            tag: "h1",
            namespace: None,
            attrs: &[],
            children: H1_CHILDREN,
        },
        TemplateNode::Element {
            tag: "p",
            namespace: None,
            attrs: &[],
            children: P_CHILDREN,
        },
    ];
    const ROOTS: &[TemplateNode] = &[TemplateNode::Element {
        tag: "main",
        namespace: None,
        attrs: MAIN_ATTRS,
        children: MAIN_CHILDREN,
    }];
    const TEMPLATE: Template = Template {
        roots: ROOTS,
        node_paths: &[],
        attr_paths: &[],
    };

    Ok(VNode::new(None, TEMPLATE, Box::new([]), Box::new([])))
}

pub fn app() -> Element {
    App()
}
