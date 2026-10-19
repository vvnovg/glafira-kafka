//! Consumer groups: брошенные группы, ребаланс в момент проверки, участники без партиций,
//! рост лага с прошлой проверки. И служебная `check.partial`.

use serde_json::json;

use super::Findings;
use crate::health::model::Target;
use crate::health::snapshot::ClusterSnapshot;

/// `group.lagGrowing`: прошлая проверка не раньше минуты и не позже суток назад.
const LAG_MIN_AGE_MS: i64 = 60_000;
const LAG_MAX_AGE_MS: i64 = 24 * 60 * 60 * 1000;
/// Рост лага не меньше `max(1000, половина прошлого лага)`.
const LAG_MIN_GROWTH: i64 = 1000;

pub(super) fn check(s: &ClusterSnapshot, out: &mut Findings) {
    let previous = s
        .previous
        .as_ref()
        .filter(|p| (LAG_MIN_AGE_MS..=LAG_MAX_AGE_MS).contains(&p.age_ms));
    for g in &s.groups {
        let target = || Target::group(&g.group_id);
        if g.state == "Empty"
            && let Some(lag) = g.total_lag
            && lag > 0
        {
            out.emit("group.abandoned", target(), None, json!({ "lag": lag }));
        }
        if matches!(
            g.state.as_str(),
            "PreparingRebalance" | "CompletingRebalance"
        ) {
            out.emit(
                "group.rebalancing",
                target(),
                None,
                json!({ "state": g.state }),
            );
        }
        if g.group_type == "classic" && g.state == "Stable" {
            let idle = g.member_partitions.iter().filter(|&&n| n == 0).count();
            if idle > 0 {
                out.emit(
                    "group.idleMembers",
                    target(),
                    None,
                    json!({
                        "idle": idle,
                        "members": g.member_partitions.len(),
                        "partitions": g.member_partitions.iter().sum::<usize>(),
                    }),
                );
            }
        }
        if let (Some(p), Some(now)) = (previous, g.total_lag)
            && let Some(&before) = p.lag.get(&g.group_id)
            && 2 * (now - before) >= (2 * LAG_MIN_GROWTH).max(before)
        {
            out.emit(
                "group.lagGrowing",
                target(),
                None,
                json!({ "lag": now, "previousLag": before, "minutesAgo": p.age_ms / 60_000 }),
            );
        }
    }
}

/// Проверка неполная: что не проверено и почему.
pub(super) fn partial(s: &ClusterSnapshot, out: &mut Findings) {
    let c = &s.coverage;
    if c.partial() {
        out.emit(
            "check.partial",
            Target::cluster(),
            None,
            json!({
                "topicsChecked": c.topics_checked,
                "topicsTotal": c.topics_total,
                "topicConfigErrors": c.topic_config_errors,
                "brokerConfigErrors": c.broker_config_errors,
                "groupsChecked": c.groups_checked,
                "groupsTotal": c.groups_total,
                "groupErrors": c.group_errors,
                "timedOut": c.timed_out,
            }),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::evaluate;
    use crate::health::model::Severity;
    use crate::health::snapshot::{ClusterSnapshot, Coverage, GroupSnapshot, PreviousLag};
    use crate::storage::connections::Environment;

    fn group(
        id: &str,
        state: &str,
        kind: &str,
        members: &[usize],
        lag: Option<i64>,
    ) -> GroupSnapshot {
        GroupSnapshot {
            group_id: id.into(),
            state: state.into(),
            group_type: kind.into(),
            member_partitions: members.to_vec(),
            total_lag: lag,
        }
    }

    fn snap(groups: Vec<GroupSnapshot>) -> ClusterSnapshot {
        ClusterSnapshot {
            controller_id: Some(1),
            brokers: super::super::fixtures::brokers(3),
            groups,
            ..Default::default()
        }
    }

    fn rules(s: &ClusterSnapshot, env: Environment) -> Vec<(String, Severity)> {
        evaluate(s, env)
            .into_iter()
            .map(|f| (f.rule_id, f.severity))
            .collect()
    }

    #[test]
    fn abandoned_group_needs_empty_state_and_lag() {
        let s = snap(vec![
            group("a", "Empty", "classic", &[], Some(5)),
            group("b", "Empty", "classic", &[], Some(0)),
            group("c", "Empty", "classic", &[], None),
        ]);
        assert_eq!(
            rules(&s, Environment::Prod),
            [("group.abandoned".into(), Severity::Warning)]
        );
        assert_eq!(
            rules(&s, Environment::Local),
            [("group.abandoned".into(), Severity::Info)]
        );
    }

    #[test]
    fn rebalancing_and_idle_members() {
        let s = snap(vec![
            group("r", "PreparingRebalance", "classic", &[1], Some(0)),
            group("i", "Stable", "classic", &[2, 0, 0], Some(0)),
            group("k", "Stable", "consumer", &[2, 0], Some(0)),
        ]);
        let f = evaluate(&s, Environment::Dev);
        let ids: Vec<_> = f
            .iter()
            .map(|x| (x.rule_id.as_str(), x.target.name.as_deref()))
            .collect();
        assert_eq!(
            ids,
            [
                ("group.rebalancing", Some("r")),
                ("group.idleMembers", Some("i"))
            ]
        );
        assert_eq!(f[1].params["idle"], 2);
        assert_eq!(f[1].params["members"], 3);
        assert_eq!(f[1].params["partitions"], 2);
    }

    fn with_previous(age_ms: i64, before: i64, now: i64) -> ClusterSnapshot {
        let mut s = snap(vec![group("g", "Stable", "classic", &[1], Some(now))]);
        s.previous = Some(PreviousLag {
            age_ms,
            lag: BTreeMap::from([("g".to_string(), before)]),
        });
        s
    }

    #[test]
    fn lag_growing_thresholds() {
        let growing = |s: &ClusterSnapshot| {
            rules(s, Environment::Prod)
                .iter()
                .any(|(r, _)| r == "group.lagGrowing")
        };
        // Рост ≥ max(1000, prev/2).
        assert!(growing(&with_previous(120_000, 0, 1000)));
        assert!(!growing(&with_previous(120_000, 0, 999)));
        assert!(growing(&with_previous(120_000, 2000, 3000)));
        assert!(!growing(&with_previous(120_000, 4000, 5000)));
        // Половина нечётного прошлого лага не округляется вниз: 3001 → +1500 — мало, +1501 — да.
        assert!(!growing(&with_previous(120_000, 3001, 4501)));
        assert!(growing(&with_previous(120_000, 3001, 4502)));
        // Ветка «половина прошлого лага» срабатывает: 4000 → 6000.
        assert!(growing(&with_previous(120_000, 4000, 6000)));
        // Прошлая проверка: от минуты до суток.
        assert!(!growing(&with_previous(59_000, 0, 5000)));
        assert!(growing(&with_previous(60_000, 0, 5000)));
        assert!(growing(&with_previous(86_400_000, 0, 5000)));
        assert!(!growing(&with_previous(86_401_000, 0, 5000)));
        let f = evaluate(&with_previous(180_000, 0, 1500), Environment::Prod);
        assert_eq!(f[0].params["minutesAgo"], 3);
        assert_eq!(f[0].params["previousLag"], 0);
    }

    #[test]
    fn partial_check_lists_coverage() {
        let mut s = snap(vec![]);
        s.coverage = Coverage {
            topics_total: 2500,
            topics_checked: 2000,
            ..Default::default()
        };
        let f = evaluate(&s, Environment::Prod);
        assert_eq!(f[0].rule_id, "check.partial");
        assert_eq!(f[0].params["topicsChecked"], 2000);
        assert_eq!(f[0].params["topicsTotal"], 2500);
        s.coverage = Coverage::default();
        assert!(evaluate(&s, Environment::Prod).is_empty());
    }
}
