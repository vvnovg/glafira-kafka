mod common;

use std::sync::Arc;

use kafka_explorer_lib::kafka::admin_ext::{
    alter_consumer_group_offsets, describe_consumer_groups, incremental_alter_topic_configs,
    list_consumer_group_offsets, list_consumer_groups,
};
use kafka_explorer_lib::kafka::configs::describe_configs;
use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::secrets::MemoryStore;
use rdkafka::admin::ResourceSpecifier;
use rdkafka::consumer::Consumer;
use rdkafka::types::RDKafkaErrorCode;

fn handles(bs: &str) -> Arc<KafkaHandles> {
    Arc::new(KafkaHandles::create(&common::plaintext_profile(bs), &MemoryStore::default()).unwrap())
}

fn alter(
    h: &Arc<KafkaHandles>,
    topic: &str,
    changes: &[(&str, Option<&str>)],
) -> Result<(), RDKafkaErrorCode> {
    let changes: Vec<(String, Option<String>)> = changes
        .iter()
        .map(|(k, v)| (k.to_string(), v.map(String::from)))
        .collect();
    incremental_alter_topic_configs(&h.admin, h.timeout, topic, &changes)
        .expect("запрос выполнен")
        .map_err(|e| e.code)
}

async fn config(h: &Arc<KafkaHandles>, topic: &str, name: &str) -> (Option<String>, bool) {
    let entries = describe_configs(h, ResourceSpecifier::Topic(topic))
        .await
        .unwrap();
    let e = entries.iter().find(|e| e.name == name).unwrap();
    (e.value.clone(), e.is_default)
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_alter_sets_and_deletes_without_touching_other_configs() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-alter", 1).await;
    let h = handles(&bs);

    let (h1, t1) = (h.clone(), topic.to_string());
    tokio::task::spawn_blocking(move || {
        alter(
            &h1,
            &t1,
            &[
                ("retention.ms", Some("123456")),
                ("cleanup.policy", Some("compact")),
            ],
        )
        .unwrap();
    })
    .await
    .unwrap();
    assert_eq!(
        config(&h, &topic, "retention.ms").await,
        (Some("123456".into()), false)
    );
    assert_eq!(
        config(&h, &topic, "cleanup.policy").await.0.as_deref(),
        Some("compact")
    );

    // DELETE возвращает только retention.ms к default; cleanup.policy остаётся (в отличие от AlterConfigs).
    let (h1, t1) = (h.clone(), topic.to_string());
    tokio::task::spawn_blocking(move || alter(&h1, &t1, &[("retention.ms", None)]).unwrap())
        .await
        .unwrap();
    assert!(
        config(&h, &topic, "retention.ms").await.1,
        "retention.ms снова default"
    );
    assert_eq!(
        config(&h, &topic, "cleanup.policy").await.0.as_deref(),
        Some("compact")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_alter_reports_item_errors() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-alter-bad", 1).await;
    let h = handles(&bs);
    let t1 = topic.to_string();
    let (bad_value, missing) = tokio::task::spawn_blocking(move || {
        (
            alter(&h, &t1, &[("retention.ms", Some("abc"))]),
            alter(
                &h,
                "does-not-exist-topic",
                &[("retention.ms", Some("1000"))],
            ),
        )
    })
    .await
    .unwrap();
    assert_eq!(bad_value, Err(RDKafkaErrorCode::InvalidConfig));
    assert_eq!(missing, Err(RDKafkaErrorCode::UnknownTopicOrPartition));
}

#[tokio::test(flavor = "multi_thread")]
async fn groups_list_describe_offsets_alter() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-groups-ffi", 2).await;
    let group = common::TempGroup::new(&bs, "it-group-ffi");
    common::commit_group_offsets(&bs, &group, &topic, &[(0, 3), (1, 5)]);
    let h = handles(&bs);
    let (t, g, bs2) = (topic.to_string(), group.to_string(), bs.clone());

    tokio::task::spawn_blocking(move || {
        let (groups, errors) = list_consumer_groups(&h.admin, h.timeout).unwrap();
        assert!(errors.is_empty(), "{errors:?}");
        let listing = groups
            .iter()
            .find(|l| l.group_id == g)
            .expect("группа в списке");
        assert_eq!(listing.state, "Empty");

        let d = describe_consumer_groups(&h.admin, h.timeout, std::slice::from_ref(&g)).unwrap();
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].state, "Empty");
        assert!(d[0].members.is_empty());
        assert!(d[0].error.is_none());

        let mut offsets = list_consumer_group_offsets(&h.admin, h.timeout, &g)
            .unwrap()
            .unwrap();
        offsets.sort_by_key(|p| p.partition);
        let pairs: Vec<(i32, i64)> = offsets.iter().map(|p| (p.partition, p.offset)).collect();
        assert_eq!(pairs, [(0, 3), (1, 5)]);
        assert!(offsets.iter().all(|p| p.topic == t && p.error.is_none()));

        let altered = alter_consumer_group_offsets(
            &h.admin,
            h.timeout,
            &g,
            &[(t.clone(), 0, 1), (t.clone(), 1, 0)],
        )
        .unwrap()
        .unwrap();
        assert!(altered.iter().all(|p| p.error.is_none()), "{altered:?}");
        let mut after = list_consumer_group_offsets(&h.admin, h.timeout, &g)
            .unwrap()
            .unwrap();
        after.sort_by_key(|p| p.partition);
        assert_eq!(after.iter().map(|p| p.offset).collect::<Vec<_>>(), [1, 0]);

        // Активная группа: участник и его назначение видны.
        let member = common::join_group(&bs2, &g, &t);
        let d = describe_consumer_groups(&h.admin, h.timeout, std::slice::from_ref(&g)).unwrap();
        assert_eq!(d[0].state, "Stable");
        assert_eq!(d[0].members.len(), 1);
        assert_eq!(d[0].members[0].client_id, "it-member");
        let mut assigned = d[0].members[0].assignment.clone();
        assigned.sort();
        assert_eq!(assigned, [(t.clone(), 0), (t.clone(), 1)]);
        assert!(!d[0].assignor.is_empty());
        member.unsubscribe();
        drop(member);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_unknown_group_is_dead() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let h = handles(&bs);
    tokio::task::spawn_blocking(move || {
        let d =
            describe_consumer_groups(&h.admin, h.timeout, &["it-never-existed-group".to_string()])
                .unwrap();
        assert_eq!(d[0].state, "Dead");
        assert!(d[0].members.is_empty());
    })
    .await
    .unwrap();
}
