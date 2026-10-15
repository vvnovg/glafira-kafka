mod common;

use std::sync::Arc;

use common::TestMsg;
use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::group_ops::{
    delete_group, describe_group, group_offsets, groups_lag, list_groups, preview_reset,
    reset_offsets, topic_groups,
};
use kafka_explorer_lib::kafka::groups::{
    OffsetTarget, ResetRequest, ResetScope, ResetStrategy, check_deletable,
};
use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::secrets::MemoryStore;
use rdkafka::consumer::Consumer;

fn handles(bs: &str) -> Arc<KafkaHandles> {
    Arc::new(KafkaHandles::create(&common::plaintext_profile(bs), &MemoryStore::default()).unwrap())
}

/// Топик с 2 партициями: 5 сообщений в p0, 3 в p1; группа закоммитила (p0: 2, p1: 3) → лаг 3.
async fn seeded(bs: &str) -> (common::TempTopic, common::TempGroup) {
    let topic = common::TempTopic::create(bs, "it-grp", 2).await;
    let msgs: Vec<TestMsg> = (0..8)
        .map(|i| TestMsg {
            partition: if i < 5 { 0 } else { 1 },
            key: None,
            value: "v",
            headers: &[],
        })
        .collect();
    common::produce(bs, &topic, &msgs).await;
    let group = common::TempGroup::new(bs, "it-grp");
    common::commit_group_offsets(bs, &group, &topic, &[(0, 2), (1, 3)]);
    (topic, group)
}

fn request(group: &str, strategy: ResetStrategy) -> ResetRequest {
    ResetRequest {
        group_id: group.into(),
        strategy,
        scope: ResetScope::All,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn offsets_lag_and_topic_groups() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let (topic, group) = seeded(&bs).await;
    let h = handles(&bs);
    let (t, g) = (topic.to_string(), group.to_string());
    tokio::task::spawn_blocking(move || {
        let list = list_groups(&h).unwrap();
        assert!(
            list.groups
                .iter()
                .any(|s| s.group_id == g && s.state == "Empty")
        );

        let rows = group_offsets(&h, &g).unwrap();
        let view: Vec<_> = rows
            .iter()
            .map(|r| (r.partition, r.committed, r.log_end, r.lag))
            .collect();
        assert_eq!(
            view,
            [
                (0, Some(2), Some(5), Some(3)),
                (1, Some(3), Some(3), Some(0))
            ]
        );

        let lag = groups_lag(&h, std::slice::from_ref(&g)).unwrap();
        assert_eq!(lag[0].total_lag, Some(3));
        assert_eq!(lag[0].topics, std::slice::from_ref(&t));

        let tg = topic_groups(&h, &t).unwrap();
        assert!(!tg.incomplete);
        let mine = tg
            .groups
            .iter()
            .find(|x| x.group_id == g)
            .expect("группа топика");
        assert_eq!((mine.lag, mine.partitions), (Some(3), 2));
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn preview_and_reset() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let (topic, group) = seeded(&bs).await;
    let h = handles(&bs);
    let (t, g) = (topic.to_string(), group.to_string());
    tokio::task::spawn_blocking(move || {
        let targets = |s: ResetStrategy| -> Vec<Option<i64>> {
            preview_reset(&h, &request(&g, s))
                .unwrap()
                .iter()
                .map(|r| r.target)
                .collect()
        };
        assert_eq!(targets(ResetStrategy::Earliest), [Some(0), Some(0)]);
        assert_eq!(targets(ResetStrategy::Latest), [Some(5), Some(3)]);
        assert_eq!(
            targets(ResetStrategy::Shift { delta: -1 }),
            [Some(1), Some(2)]
        );
        assert_eq!(
            targets(ResetStrategy::Offset { offset: 4 }),
            [Some(4), Some(3)]
        );
        assert_eq!(
            targets(ResetStrategy::Timestamp {
                timestamp_ms: 32_503_680_000_000
            }),
            [Some(5), Some(3)]
        );
        assert_eq!(
            targets(ResetStrategy::Timestamp { timestamp_ms: 0 }),
            [Some(0), Some(0)]
        );

        // Область «топики» включает и партиции без коммитов.
        let scoped = preview_reset(
            &h,
            &ResetRequest {
                group_id: g.clone(),
                strategy: ResetStrategy::Earliest,
                scope: ResetScope::Topics {
                    topics: vec![t.clone()],
                },
            },
        )
        .unwrap();
        assert_eq!(scoped.len(), 2);

        let plan = preview_reset(&h, &request(&g, ResetStrategy::Earliest)).unwrap();
        let apply: Vec<OffsetTarget> = plan
            .iter()
            .map(|r| OffsetTarget {
                topic: r.topic.clone(),
                partition: r.partition,
                offset: r.target.unwrap(),
            })
            .collect();
        let res = reset_offsets(&h, &g, &apply).unwrap();
        assert!(res.iter().all(|r| r.error.is_none()), "{res:?}");
        let committed: Vec<_> = group_offsets(&h, &g)
            .unwrap()
            .iter()
            .map(|r| r.committed)
            .collect();
        assert_eq!(committed, [Some(0), Some(0)]);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn active_group_cannot_be_reset_or_deleted() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let (topic, group) = seeded(&bs).await;
    let h = handles(&bs);
    let (t, g, bs2) = (topic.to_string(), group.to_string(), bs.clone());
    let h2 = h.clone();
    tokio::task::spawn_blocking(move || {
        let member = common::join_group(&bs2, &g, &t);
        let target = [OffsetTarget {
            topic: t.clone(),
            partition: 0,
            offset: 0,
        }];
        let e = reset_offsets(&h2, &g, &target).unwrap_err();
        assert_eq!(e.code, ErrorCode::GroupNotEmpty);
        assert!(e.details.unwrap().contains("it-member"));
        let d = describe_group(&h2, &g).unwrap();
        assert_eq!(
            check_deletable(&d).unwrap_err().code,
            ErrorCode::GroupNotEmpty
        );
        member.unsubscribe();
        drop(member);
    })
    .await
    .unwrap();

    // Участник ушёл — группа Empty, удаляется.
    let g = group.to_string();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let (h1, g1) = (h.clone(), g.clone());
        let d = tokio::task::spawn_blocking(move || describe_group(&h1, &g1))
            .await
            .unwrap()
            .unwrap();
        if d.state == "Empty" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "группа не опустела: {}",
            d.state
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    delete_group(&h, &g).await.unwrap();
    let (h1, g1) = (h.clone(), g.clone());
    let list = tokio::task::spawn_blocking(move || list_groups(&h1))
        .await
        .unwrap()
        .unwrap();
    assert!(!list.groups.iter().any(|s| s.group_id == g1));
}
