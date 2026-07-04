use dioxus::dioxus_core::{Element, Template, TemplateAttribute, TemplateNode};

use super::static_vnode;

pub fn users_view() -> Element {
    const SECTION_ATTRS: &[TemplateAttribute] = &[TemplateAttribute::Static {
        name: "class",
        value: "admin-section users-section",
        namespace: None,
    }];
    const TITLE_CHILDREN: &[TemplateNode] = &[TemplateNode::Text {
        text: "SCRAM Users",
    }];
    const ACTION_CHILDREN: &[TemplateNode] = &[TemplateNode::Text {
        text: "Upsert SCRAM-SHA-512",
    }];
    const EMPTY_CHILDREN: &[TemplateNode] = &[TemplateNode::Text {
        text: "No user operation selected.",
    }];
    const SECTION_CHILDREN: &[TemplateNode] = &[
        TemplateNode::Element {
            tag: "h2",
            namespace: None,
            attrs: &[],
            children: TITLE_CHILDREN,
        },
        TemplateNode::Element {
            tag: "button",
            namespace: None,
            attrs: &[],
            children: ACTION_CHILDREN,
        },
        TemplateNode::Element {
            tag: "p",
            namespace: None,
            attrs: &[],
            children: EMPTY_CHILDREN,
        },
    ];
    const ROOTS: &[TemplateNode] = &[TemplateNode::Element {
        tag: "section",
        namespace: None,
        attrs: SECTION_ATTRS,
        children: SECTION_CHILDREN,
    }];
    const TEMPLATE: Template = Template {
        roots: ROOTS,
        node_paths: &[],
        attr_paths: &[],
    };

    Ok(static_vnode(TEMPLATE))
}
