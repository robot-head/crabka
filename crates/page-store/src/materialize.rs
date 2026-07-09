//! Descendant-aware materialization and garbage-collection planning seams.

use std::collections::BTreeMap;

use crabka_postgres_wal::Lsn;

use crate::{LayerDesc, LayerKind, TimelineGraph, TimelineId};

/// Pure GC decision output for one timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcPlan {
    /// Layers that can be removed without changing any descendant read.
    pub removable: Vec<LayerDesc>,
    /// Layers retained because a descendant still depends on ancestor history.
    pub pinned: Vec<LayerDesc>,
}

/// Plans removal of layers at or below `horizon` while honoring branch ancestry.
#[must_use]
pub fn plan_descendant_aware_gc(
    graph: &TimelineGraph,
    timeline_id: &TimelineId,
    horizon: Lsn,
    candidate_layers: &[LayerDesc],
    layers_by_timeline: &BTreeMap<TimelineId, Vec<LayerDesc>>,
) -> GcPlan {
    let mut removable = Vec::new();
    let mut pinned = Vec::new();
    let branch_points = graph.descendant_branch_points_of(timeline_id);

    for layer in candidate_layers {
        if layer.lsn_end > horizon {
            pinned.push(layer.clone());
            continue;
        }
        if descendant_branch_points_pin_layer(layer, &branch_points, layers_by_timeline) {
            pinned.push(layer.clone());
            continue;
        }
        removable.push(layer.clone());
    }

    GcPlan { removable, pinned }
}

fn descendant_branch_points_pin_layer(
    layer: &LayerDesc,
    branch_points: &[(TimelineId, Lsn)],
    layers_by_timeline: &BTreeMap<TimelineId, Vec<LayerDesc>>,
) -> bool {
    for (descendant_id, branch_lsn) in branch_points {
        if layer.lsn_start > *branch_lsn {
            continue;
        }
        if image_coverage_releases_pin(layer, *branch_lsn, descendant_id, layers_by_timeline) {
            continue;
        }
        return true;
    }
    false
}

fn image_coverage_releases_pin(
    layer: &LayerDesc,
    branch_lsn: Lsn,
    descendant_id: &TimelineId,
    layers_by_timeline: &BTreeMap<TimelineId, Vec<LayerDesc>>,
) -> bool {
    layers_by_timeline
        .get(&layer.timeline.timeline_id)
        .into_iter()
        .chain(layers_by_timeline.get(descendant_id))
        .flat_map(|layers| layers.iter())
        .any(|image| image_covers_layer_at_branch(image, layer, branch_lsn))
}

fn image_covers_layer_at_branch(image: &LayerDesc, layer: &LayerDesc, branch_lsn: Lsn) -> bool {
    let layer_branch_visible_lsn_end = layer.lsn_end.min(branch_lsn);

    image.kind == LayerKind::Image
        && image.lsn_end <= branch_lsn
        && image.lsn_end >= layer_branch_visible_lsn_end
        && image.key_start <= layer.key_start
        && layer.key_end <= image.key_end
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{PageKey, TenantId, TimelineMeta, TimelinePath};

    fn timeline_id(raw: &str) -> TimelineId {
        TimelineId::parse(raw).expect("test timeline id is valid")
    }

    fn timeline(raw: &str) -> TimelinePath {
        TimelinePath::new(
            TenantId::parse("tenant").expect("test tenant id is valid"),
            timeline_id(raw),
        )
    }

    fn key(block_number: u32) -> PageKey {
        PageKey::new(1663, 5, 16_384, 0, block_number)
    }

    fn layer(timeline: &str, kind: LayerKind, lsn_start: u64, lsn_end: u64) -> LayerDesc {
        LayerDesc::new(
            self::timeline(timeline),
            kind,
            key(0),
            key(9),
            Lsn(lsn_start),
            Lsn(lsn_end),
        )
        .expect("test layer descriptor is valid")
    }

    #[test]
    fn descendant_branch_points_pin_parent_layers() {
        let root_id = timeline_id("root");
        let child_id = timeline_id("child");
        let graph = TimelineGraph::new([
            TimelineMeta::root(root_id.clone(), Lsn(100)),
            TimelineMeta::branch(child_id.clone(), root_id.clone(), Lsn(50), Lsn(50)),
        ])
        .expect("graph is valid");
        let parent_layer = layer("root", LayerKind::Delta, 10, 40);
        let layers_by_timeline = BTreeMap::from([(root_id.clone(), vec![parent_layer.clone()])]);

        let plan = plan_descendant_aware_gc(
            &graph,
            &root_id,
            Lsn(100),
            std::slice::from_ref(&parent_layer),
            &layers_by_timeline,
        );

        assert!(plan.removable.is_empty());
        assert!(plan.pinned == vec![parent_layer]);
    }

    #[test]
    fn child_image_coverage_unpins_parent_layer() {
        let root_id = timeline_id("root");
        let child_id = timeline_id("child");
        let graph = TimelineGraph::new([
            TimelineMeta::root(root_id.clone(), Lsn(100)),
            TimelineMeta::branch(child_id.clone(), root_id.clone(), Lsn(50), Lsn(50)),
        ])
        .expect("graph is valid");
        let parent_layer = layer("root", LayerKind::Delta, 10, 40);
        let child_image = layer("child", LayerKind::Image, 50, 50);
        let layers_by_timeline = BTreeMap::from([
            (root_id.clone(), vec![parent_layer.clone()]),
            (child_id, vec![child_image]),
        ]);

        let plan = plan_descendant_aware_gc(
            &graph,
            &root_id,
            Lsn(100),
            std::slice::from_ref(&parent_layer),
            &layers_by_timeline,
        );

        assert!(plan.removable == vec![parent_layer]);
        assert!(plan.pinned.is_empty());
    }

    #[test]
    fn old_child_image_does_not_unpin_newer_parent_delta() {
        let root_id = timeline_id("root");
        let child_id = timeline_id("child");
        let graph = TimelineGraph::new([
            TimelineMeta::root(root_id.clone(), Lsn(100)),
            TimelineMeta::branch(child_id.clone(), root_id.clone(), Lsn(50), Lsn(50)),
        ])
        .expect("graph is valid");
        let parent_layer = layer("root", LayerKind::Delta, 10, 40);
        let old_child_image = layer("child", LayerKind::Image, 20, 20);
        let layers_by_timeline = BTreeMap::from([
            (root_id.clone(), vec![parent_layer.clone()]),
            (child_id, vec![old_child_image]),
        ]);

        let plan = plan_descendant_aware_gc(
            &graph,
            &root_id,
            Lsn(100),
            std::slice::from_ref(&parent_layer),
            &layers_by_timeline,
        );

        assert!(plan.removable.is_empty());
        assert!(plan.pinned == vec![parent_layer]);
    }

    #[test]
    fn child_image_at_parent_delta_end_unpins_parent_layer() {
        let root_id = timeline_id("root");
        let child_id = timeline_id("child");
        let graph = TimelineGraph::new([
            TimelineMeta::root(root_id.clone(), Lsn(100)),
            TimelineMeta::branch(child_id.clone(), root_id.clone(), Lsn(50), Lsn(50)),
        ])
        .expect("graph is valid");
        let parent_layer = layer("root", LayerKind::Delta, 10, 40);
        let child_image = layer("child", LayerKind::Image, 40, 40);
        let layers_by_timeline = BTreeMap::from([
            (root_id.clone(), vec![parent_layer.clone()]),
            (child_id, vec![child_image]),
        ]);

        let plan = plan_descendant_aware_gc(
            &graph,
            &root_id,
            Lsn(100),
            std::slice::from_ref(&parent_layer),
            &layers_by_timeline,
        );

        assert!(plan.removable == vec![parent_layer]);
        assert!(plan.pinned.is_empty());
    }

    #[test]
    fn child_image_must_reach_parent_delta_end_to_unpin_parent_layer() {
        for (image_lsn, releases_pin) in [(20, false), (40, true), (50, true)] {
            let root_id = timeline_id("root");
            let child_id = timeline_id("child");
            let graph = TimelineGraph::new([
                TimelineMeta::root(root_id.clone(), Lsn(100)),
                TimelineMeta::branch(child_id.clone(), root_id.clone(), Lsn(50), Lsn(50)),
            ])
            .expect("graph is valid");
            let parent_layer = layer("root", LayerKind::Delta, 10, 40);
            let child_image = layer("child", LayerKind::Image, image_lsn, image_lsn);
            let layers_by_timeline = BTreeMap::from([
                (root_id.clone(), vec![parent_layer.clone()]),
                (child_id, vec![child_image]),
            ]);

            let plan = plan_descendant_aware_gc(
                &graph,
                &root_id,
                Lsn(100),
                std::slice::from_ref(&parent_layer),
                &layers_by_timeline,
            );

            if releases_pin {
                assert!(plan.removable == vec![parent_layer]);
                assert!(plan.pinned.is_empty());
                continue;
            }

            assert!(plan.removable.is_empty());
            assert!(plan.pinned == vec![parent_layer]);
        }
    }

    #[test]
    fn child_image_at_branch_point_unpins_branch_visible_parent_delta() {
        let root_id = timeline_id("root");
        let child_id = timeline_id("child");
        let graph = TimelineGraph::new([
            TimelineMeta::root(root_id.clone(), Lsn(100)),
            TimelineMeta::branch(child_id.clone(), root_id.clone(), Lsn(50), Lsn(50)),
        ])
        .expect("graph is valid");
        let parent_layer = layer("root", LayerKind::Delta, 10, 60);
        let child_image = layer("child", LayerKind::Image, 50, 50);
        let layers_by_timeline = BTreeMap::from([
            (root_id.clone(), vec![parent_layer.clone()]),
            (child_id, vec![child_image]),
        ]);

        let plan = plan_descendant_aware_gc(
            &graph,
            &root_id,
            Lsn(100),
            std::slice::from_ref(&parent_layer),
            &layers_by_timeline,
        );

        assert!(plan.removable == vec![parent_layer]);
        assert!(plan.pinned.is_empty());
    }

    #[test]
    fn grandchild_branch_points_pin_ancestor_layers() {
        let root_id = timeline_id("root");
        let child_id = timeline_id("child");
        let grandchild_id = timeline_id("grandchild");
        let graph = TimelineGraph::new([
            TimelineMeta::root(root_id.clone(), Lsn(100)),
            TimelineMeta::branch(child_id.clone(), root_id.clone(), Lsn(60), Lsn(80)),
            TimelineMeta::branch(grandchild_id, child_id, Lsn(70), Lsn(70)),
        ])
        .expect("graph is valid");
        let parent_layer = layer("root", LayerKind::Delta, 10, 50);
        let layers_by_timeline = BTreeMap::from([(root_id.clone(), vec![parent_layer.clone()])]);

        let plan = plan_descendant_aware_gc(
            &graph,
            &root_id,
            Lsn(100),
            std::slice::from_ref(&parent_layer),
            &layers_by_timeline,
        );

        assert!(plan.pinned == vec![parent_layer]);
    }
}
