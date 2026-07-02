//! Render Crabka's own deterministic consensus simulator into a Mermaid
//! sequence-diagram "slideshow" markdown page.
//!
//! The diagrams here are not hand-drawn: [`failure_scenarios_md`] runs
//! [`crabka_raft::scenarios::scenarios`] in process and turns each recorded
//! [`ScenarioTrace`](crabka_raft::scenarios::ScenarioTrace) into a Zola
//! `{% mermaid() %}` paired shortcode, so what the reader sees is exactly what
//! the real KIP-595/996 algorithm did.

use std::fmt::Write as _;

use crabka_raft::scenarios::{ScenarioTrace, TraceAction};

/// The intro paragraph that opens the page.
const INTRO: &str = "\
These diagrams are **generated**, not drawn. Each one is produced by running \
Crabka's own deterministic `KRaft` consensus simulator — the same pure, \
no-IO state machine the broker runs in production — and recording every \
message, timeout, partition, and leader election it takes. Because the \
simulator is deterministic, the diagrams below reflect the *real* algorithm, \
step for step, rather than an idealized cartoon of it.\n";

/// Render the complete failure-scenarios slideshow page body (no front matter).
#[must_use]
pub fn failure_scenarios_md() -> String {
    let mut out = String::new();
    out.push_str(INTRO);
    out.push('\n');
    for trace in crabka_raft::scenarios::scenarios() {
        render_scenario(&mut out, &trace);
    }
    out
}

/// Append one scenario (heading, summary, invariant, diagram(s), outcome).
fn render_scenario(out: &mut String, trace: &ScenarioTrace) {
    let _ = writeln!(out, "## {}\n", trace.title);
    let _ = writeln!(out, "{}\n", trace.summary);
    let _ = writeln!(out, "**Invariant:** {}\n", trace.invariant);

    // For the split-brain scenario, lead with the hand-authored "what goes
    // wrong without quorum" contrast, then the honest generated diagram.
    if trace.id == "split_brain_prevented" {
        out.push_str(SPLIT_BRAIN_CONTRAST);
        out.push('\n');
        let _ = writeln!(
            out,
            "**✓ With Crabka's quorum + pre-vote** — the generated trace from the simulator:\n"
        );
    }

    render_diagram(out, trace);

    let _ = writeln!(out, "**Outcome:** {}\n", trace.outcome);
}

/// Render the generated Mermaid sequence diagram for `trace`.
fn render_diagram(out: &mut String, trace: &ScenarioTrace) {
    out.push_str("{% mermaid() %}\n");
    out.push_str("sequenceDiagram\n");
    for id in &trace.nodes {
        let _ = writeln!(out, "    participant N{id}");
    }
    for step in &trace.steps {
        match &step.action {
            TraceAction::Deliver { src, dst, event } => {
                let _ = writeln!(out, "    N{src}->>N{dst}: {event}");
            }
            TraceAction::Partition { node } => {
                let _ = writeln!(out, "    Note over N{node}: ✂ partitioned");
            }
            TraceAction::Heal { node } => {
                let _ = writeln!(out, "    Note over N{node}: 🔗 healed");
            }
            TraceAction::Elected { node, epoch } => {
                let _ = writeln!(out, "    Note over N{node}: 👑 leader epoch {epoch}");
            }
            TraceAction::Timeout { node, kind } => {
                let _ = writeln!(out, "    Note over N{node}: ⏰ {kind} timeout");
            }
            TraceAction::Append { node, count } => {
                let _ = writeln!(out, "    Note over N{node}: ✏ append {count} record(s)");
            }
            TraceAction::Drop { src, dst, event } => {
                let _ = writeln!(out, "    N{src}--xN{dst}: {event} (dropped)");
            }
        }
    }
    out.push_str("{% end %}\n\n");
}

/// Hand-authored illustrative contrast for the split-brain scenario: what a
/// *naive* leader election WITHOUT a quorum requirement would look like — two
/// nodes both believing they are leader and their logs diverging. This is the
/// only non-generated diagram on the page; it exists to make the generated,
/// code-backed diagram below it legible by contrast.
const SPLIT_BRAIN_CONTRAST: &str = "\
**❌ Without quorum — what split-brain looks like:**

{% mermaid() %}
sequenceDiagram
    participant N1
    participant N2
    participant N3
    Note over N1: 👑 leader epoch 1
    Note over N1,N3: ✂ network splits {N1} | {N2,N3}
    Note over N1: still thinks it is leader
    N1->>N1: accepts write A (epoch 1)
    Note over N2: ⏰ election timeout
    Note over N2: 👑 leader epoch 1 (no quorum check!)
    N2->>N3: accepts write B (epoch 1)
    Note over N1,N3: 💥 two leaders, logs diverge (A vs B)
{% end %}

Crabka prevents this: a candidate must win a **majority** of votes before it \
can lead, and KIP-996 **pre-vote** stops a partitioned node from disrupting a \
healthy leader. With three voters, the minority side (one node) can never \
reach the two-vote majority, so it cannot elect itself.
";

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::{assert, check};

    #[test]
    fn renders_sequence_diagrams_and_split_brain() {
        let md = failure_scenarios_md();
        check!(md.contains("sequenceDiagram"));
        check!(md.contains("{% mermaid() %}"));
        check!(md.contains("{% end %}"));
        check!(md.contains("split-brain") || md.contains("Split-brain"));
    }

    #[test]
    fn renders_all_three_scenarios() {
        let md = failure_scenarios_md();
        for needle in [
            "Reordered message delivery",
            "Duplicate message delivery",
            "**Invariant:**",
            "**Outcome:**",
        ] {
            assert!(md.contains(needle), "missing {needle:?}");
        }
    }

    #[test]
    fn emits_a_generated_diagram_per_scenario() {
        // Each of the three scenarios renders one generated `{% mermaid() %}`
        // sequence diagram, plus the split-brain scenario leads with one
        // hand-authored contrast diagram — four mermaid blocks in total. This
        // pins `render_diagram`'s output specifically: if it stopped emitting,
        // only the single hand-authored contrast block would remain.
        let md = failure_scenarios_md();
        let mermaid_blocks = md.matches("{% mermaid() %}").count();
        check!(
            mermaid_blocks == 4,
            "expected 3 generated diagrams + 1 contrast, got {mermaid_blocks}"
        );
        // A generated diagram declares its participants and draws message
        // arrows — content the hand-authored contrast for a single scenario
        // cannot account for on its own (e.g. the reordered/duplicate scenarios).
        check!(md.matches("participant N").count() >= 6);
        check!(md.contains("->>"));
    }
}
