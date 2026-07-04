use dioxus::dioxus_core::{Element, Template, TemplateAttribute, TemplateNode};

use super::static_vnode;

pub fn topics_view() -> Element {
    const SECTION_ATTRS: &[TemplateAttribute] = &[TemplateAttribute::Static {
        name: "class",
        value: "admin-section topics-section",
        namespace: None,
    }];
    const TITLE_CHILDREN: &[TemplateNode] = &[TemplateNode::Text { text: "Topics" }];
    const ACTION_CHILDREN: &[TemplateNode] = &[TemplateNode::Text {
        text: "Create Topic",
    }];
    const EMPTY_CHILDREN: &[TemplateNode] = &[TemplateNode::Text {
        text: "No topics loaded yet.",
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
