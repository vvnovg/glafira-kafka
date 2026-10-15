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

/// Ждёт, пока топик исчезнет из метаданных брокера.
fn wait_topic_gone(bs: &str, name: &str) {
    use rdkafka::ClientConfig;
    use rdkafka::consumer::BaseConsumer;
    use std::time::{Duration, Instant};
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bs)
        .create()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let md = c.fetch_metadata(None, Duration::from_secs(2)).unwrap();
        if !md.topics().iter().any(|t| t.name() == name) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "топик {name} не удалился за 15 с"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Удалённый топик с коммитами группы не роняет весь просмотр: строки/группы
/// с живыми топиками получают данные, удалённые помечаются ошибкой.
/// cp-kafka 7.6 при удалении топика сам убирает коммиты групп по нему, так что строка
/// удалённого топика обычно исчезает; окно до очистки покрывает
/// bounds_for_missing_topic_degrade_per_partition.
#[tokio::test(flavor = "multi_thread")]
async fn deleted_topic_degrades_per_row() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let (healthy, healthy_group) = seeded(&bs).await;
    let doomed = common::TempTopic::create(&bs, "it-grp-doomed", 1).await;
    let mixed = common::TempGroup::new(&bs, "it-grp-mixed");
    common::commit_group_offsets(&bs, &mixed, &doomed, &[(0, 0)]);
    common::commit_group_offsets(&bs, &mixed, &healthy, &[(0, 1)]);
    let h = handles(&bs);
    let (hg, mg, ht, dt) = (
        healthy_group.to_string(),
        mixed.to_string(),
        healthy.to_string(),
        doomed.to_string(),
    );
    // Прогрев: клиент видел удаляемый топик в метаданных (как долгоживущий пул).
    {
        let (h, mg) = (h.clone(), mg.clone());
        tokio::task::spawn_blocking(move || assert_eq!(group_offsets(&h, &mg).unwrap().len(), 2))
            .await
            .unwrap();
    }
    common::delete_topics_sync(&bs, std::slice::from_ref(&dt));
    {
        let (bs, dt) = (bs.clone(), dt.clone());
        tokio::task::spawn_blocking(move || wait_topic_gone(&bs, &dt))
            .await
            .unwrap();
    }
    tokio::task::spawn_blocking(move || {
        let limit = h.timeout;
        let started = std::time::Instant::now();
        let rows = group_offsets(&h, &mg);
        eprintln!("group_offsets: {:?} за {:?}", rows, started.elapsed());
        let rows = rows.expect("group_offsets не должен падать целиком");
        assert!(started.elapsed() < limit, "group_offsets сжёг дедлайн");
        let live = rows
            .iter()
            .find(|r| r.topic == ht)
            .expect("строка живого топика");
        assert_eq!(
            (live.committed, live.log_end, live.lag),
            (Some(1), Some(5), Some(4))
        );
        assert!(live.error.is_none(), "{:?}", live.error);
        if let Some(dead) = rows.iter().find(|r| r.topic == dt) {
            assert_eq!(dead.lag, None);
            assert!(dead.error.is_some());
        }

        let started = std::time::Instant::now();
        let lag = groups_lag(&h, &[hg.clone(), mg.clone()]);
        eprintln!("groups_lag: {:?} за {:?}", lag, started.elapsed());
        let lag = lag.expect("groups_lag не должен падать целиком");
        assert!(started.elapsed() < limit, "groups_lag сжёг дедлайн");
        let healthy_lag = lag.iter().find(|l| l.group_id == hg).unwrap();
        assert_eq!(healthy_lag.total_lag, Some(3));
        assert!(healthy_lag.error.is_none(), "{:?}", healthy_lag.error);

        let started = std::time::Instant::now();
        let tg = topic_groups(&h, &ht);
        eprintln!("topic_groups: {:?} за {:?}", tg, started.elapsed());
        let tg = tg.expect("topic_groups не должен падать целиком");
        assert!(started.elapsed() < limit, "topic_groups сжёг дедлайн");
        let g = tg.groups.iter().find(|x| x.group_id == hg).unwrap();
        assert_eq!(g.lag, Some(3));
        let m = tg.groups.iter().find(|x| x.group_id == mg).unwrap();
        assert_eq!(m.lag, Some(4));
    })
    .await
    .unwrap();
    drop(doomed);
}

/// Партиция несуществующего топика (окно между удалением топика и очисткой коммитов
/// координатором) получает ошибку только в своей строке и не тянет дедлайн.
#[tokio::test(flavor = "multi_thread")]
async fn bounds_for_missing_topic_degrade_per_partition() {
    let Some(bs) = common::bootstrap() else {
        return;
    };
    let topic = common::TempTopic::create(&bs, "it-grp", 1).await;
    let h = handles(&bs);
    let (live, ghost) = (topic.to_string(), common::unique_topic("it-ghost"));
    tokio::task::spawn_blocking(move || {
        let keys = vec![(live.clone(), 0), (ghost.clone(), 0)];
        let started = std::time::Instant::now();
        let b = kafka_explorer_lib::kafka::topics::fetch_bounds(&h.consumer, &keys, h.timeout)
            .expect("ошибка одной партиции не роняет запрос");
        assert!(started.elapsed() < h.timeout);
        assert_eq!(b[&(live, 0)], Ok((0, 0)));
        assert!(b[&(ghost, 0)].is_err());
    })
    .await
    .unwrap();
}
