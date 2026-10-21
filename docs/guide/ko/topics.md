# 클러스터와 토픽

이 장에서는 클러스터 개요, 토픽 목록, 토픽 만들기와 삭제, 토픽 화면, 토픽 구성을 설명합니다. [[ui:sidebar.connections]] 패널의 각 연결 아래에는 [[ui:sidebar.cluster]] 링크와 [[ui:sidebar.topics]] 링크가 있습니다. 각 링크는 별도의 탭에서 열립니다.

## 클러스터 개요 {#cluster-overview}

[[ui:cluster.overview.title]]는 불러온 시점의 클러스터 상태를 보여 줍니다. 다시 불러오려면 [[ui:cluster.overview.refresh]]을 클릭하세요.

- `Cluster ID` — 클러스터 식별자.
- [[ui:cluster.overview.controller]] — 클러스터가 컨트롤러로 보고하는 브로커의 ID. KRaft 클러스터에서는 클라이언트가 컨트롤러와 직접 통신하지 않습니다. 클러스터는 여기에 활성 컨트롤러가 아닌 임의의 브로커를 보고하며, 이 값은 새로 고칠 때마다 바뀔 수 있습니다.
- [[ui:cluster.overview.brokers]] — 브로커 수.
- [[ui:cluster.overview.topicsInternal]] — 전체 토픽 수. 괄호 안은 내부 토픽 수입니다.
- [[ui:cluster.overview.partitions]] — 모든 토픽의 파티션 수.
- `Under-replicated` — 일부 복제본이 동기화되지 않은(ISR에 없는) 파티션. 보통 브로커가 중지되었거나 따라잡지 못하는 경우입니다.
- `Offline` — 리더가 없는 파티션. 아무도 이 파티션에 쓰거나 읽을 수 없습니다.

`Under-replicated` 또는 `Offline` 값이 0이 아니면 강조 표시됩니다.

아래에는 `ID`와 [[ui:cluster.overview.address]] 열이 있는 브로커 표가 있습니다. [[ui:cluster.overview.controller]] 필드의 브로커에는 [[ui:cluster.overview.controllerBadge]] 표시가 붙습니다. KRaft 클러스터에서는 이 브로커도 활성 컨트롤러가 아닌 같은 임의의 브로커입니다. 브로커를 클릭하면 표 아래에 그 구성이 표시됩니다. 브로커 구성은 보기만 할 수 있습니다. 이름으로 매개변수를 찾으려면 [[ui:common.config.searchParam]] 필드를, 기본값인 매개변수를 숨기려면 [[ui:common.config.onlyChanged]] 옵션을 사용하세요. 자물쇠 아이콘은 읽기 전용 매개변수를 나타냅니다.

## 토픽 목록 {#topic-list}

[[ui:sidebar.topics]] 화면에는 클러스터의 토픽이 나열됩니다. 행을 클릭하면 토픽이 별도의 탭에서 열립니다.

열은 [[ui:topics.list.colName]], [[ui:topics.list.colPartitions]], `RF`(복제 계수), [[ui:topics.list.colMessages]], `cleanup.policy`, [[ui:topics.list.colUnderRepl]](복제 부족 파티션 수)입니다. 메시지 수는 근삿값으로, 파티션의 끝 오프셋과 시작 오프셋의 차이입니다. 컴팩션과 트랜잭션 마커 때문에 실제 수는 더 적습니다. 메시지 수와 `cleanup.policy`는 스크롤하여 표시된 행에 대해서만 불러옵니다.

도구 모음의 컨트롤:

- [[ui:topics.list.search]] — 이름의 일부를 입력하여 토픽을 찾습니다. 대소문자는 구분하지 않습니다. 정규식으로 검색하려면 슬래시 사이에 넣으세요. 예: `/^orders\..*/`.
- [[ui:topics.list.internal]] — `__consumer_offsets` 같은 내부 토픽을 표시합니다. 내부 토픽에는 [[ui:topics.list.internalBadge]] 표시가 붙습니다.
- 자동 새로 고침 목록 — [[ui:topics.list.autoRefreshOff]], [[ui:topics.list.every10]] 또는 [[ui:topics.list.every30]].
- [[ui:topics.list.refresh]] — 목록과 메시지 수를 지금 다시 불러옵니다.

도구 모음 옆에는 전체 토픽 중 검색과 일치하는 토픽 수가 표시됩니다. [[ui:common.readOnly]] 연결에서는 [[ui:topics.list.create]] 버튼과 [[ui:topics.list.select]] 버튼이 숨겨집니다.

## 토픽 만들기 {#create-topic}

1. 토픽 목록에서 [[ui:topics.list.create]]를 클릭합니다. 목록 위에 [[ui:topics.create.title]] 양식이 열립니다.
2. [[ui:topics.create.name]]을 입력합니다. 라틴 문자, 숫자, `.`, `_`, `-`를 최대 249자까지 사용할 수 있습니다. 이름에 `.`와 `_`가 모두 있으면 양식이 경고합니다. 이런 토픽은 Kafka 메트릭 이름이 다른 토픽과 겹칠 수 있습니다.
3. [[ui:topics.create.partitions]] 수를 설정합니다. 파티션은 병렬로 읽히며, 순서는 한 파티션 안에서만 보장됩니다. 파티션 수는 나중에 늘릴 수 있지만 줄일 수는 없습니다.
4. [[ui:topics.create.rf]]를 설정합니다. 브로커가 각 파티션의 사본을 몇 개 보관할지 정하는 값입니다. 브로커 수보다 클 수 없으며, 브로커 수는 필드 아래에 표시됩니다. 기본값은 3이며, 브로커가 3개보다 적으면 브로커 수입니다. `-1`은 브로커 기본값을 사용합니다.
5. [[ui:topics.create.preset]]을 선택합니다:
   - [[ui:topics.create.presetDelete]] — 오래된 메시지가 시간에 따라 삭제됩니다.
   - `compact` — 키마다 최신 값만 보관됩니다.
   - `compact + delete` — 둘 다 적용됩니다.
   - [[ui:topics.create.presetTest]] — 메시지가 1시간 동안 보관됩니다.
6. 필요하면 [[ui:topics.create.extraConfigs]] 필드에 한 줄에 하나씩 구성을 추가합니다. 예: `retention.ms=86400000`. 이 값은 프리셋보다 우선합니다. `#`로 시작하는 줄은 무시됩니다.
7. [[ui:topics.create.submit]]를 클릭합니다.

양식이 올바르면 같은 작업을 하는 `kafka-topics.sh` 명령이 표시됩니다. 이 명령을 복사하여 예를 들어 다른 클러스터에 같은 토픽을 만들 수 있습니다.

운영 환경 연결에서는 연결 이름을 입력하여 만들기를 확인합니다. 토픽이 만들어지면 그 탭이 열립니다.

## 토픽 삭제 {#delete-topics}

토픽 하나를 삭제하려면 토픽을 열고 [[ui:topics.actions.deleteTopic]]를 클릭하세요. 토픽 이름을 입력하여 확인하고 [[ui:topics.delete.confirm]]를 클릭합니다. 토픽 탭이 닫힙니다.

토픽 여러 개를 한 번에 삭제하려면:

1. 토픽 목록에서 [[ui:topics.list.select]]을 클릭합니다. 이름 옆에 확인란이 나타납니다.
2. 토픽을 선택합니다. 내부 토픽은 선택할 수 없습니다.
3. 삭제 버튼을 클릭합니다. 버튼에는 선택한 토픽 수가 표시됩니다.
4. 이름 목록을 확인하고, 토픽 수를 입력하여 확인한 다음 [[ui:topics.delete.confirm]]를 클릭합니다.

[[ui:topics.list.cancelSelect]]를 클릭하면 확인란이 숨겨집니다.

운영 환경 연결에서는 두 방법 모두 연결 이름도 입력해야 합니다.

삭제는 되돌릴 수 없으며 데이터가 사라집니다. 권한이 없는 등의 이유로 일부 토픽이 삭제되지 않으면 앱은 그 토픽과 이유를 표시합니다. 나머지 토픽은 삭제됩니다.

## 토픽 화면 {#topic-details}

토픽 탭에는 파티션 수, 복제 계수, 대략적인 메시지 수가 표시됩니다. 위쪽의 버튼:

- [[ui:topics.details.refresh]] — 토픽 데이터를 다시 불러옵니다.
- [[ui:topics.details.produce]] — 이 토픽으로 [[ui:sidebar.producer]] 화면을 엽니다. [Producer](producer.md)를 참고하세요.
- [[ui:topics.details.consume]] — 이 토픽에 대한 컨슈머 세션을 시작합니다. [컨슈머 세션](consumer-sessions.md)을 참고하세요.

[[ui:common.readOnly]] 연결에서는 두 버튼이 모두 비활성화됩니다.

두 번째 줄에는 토픽을 변경하는 작업이 있습니다. 읽기 전용 연결과 내부 토픽에서는 이 줄이 숨겨집니다.

- [[ui:topics.actions.addPartitions]] — 새 전체 파티션 수를 입력하고 [[ui:topics.actions.addSubmit]]를 클릭합니다. 파티션 수는 줄일 수 없습니다. 파티션 수를 늘리면 같은 키의 메시지가 다른 파티션으로 갈 수 있으므로 키 기준 순서가 깨집니다.
- [[ui:topics.actions.purge]] — 선택한 파티션에서 현재 끝까지의 모든 메시지를 삭제합니다. 토픽 자체는 남습니다. 파티션을 선택하고 토픽 이름을 입력한 다음 [[ui:topics.actions.purgeConfirm]]를 클릭합니다. 비우기는 `cleanup.policy`에 `delete`가 포함된 경우에만 동작합니다. 그렇지 않으면 버튼이 비활성화되고 이유가 표시됩니다. `compact,delete` 토픽에서는 비우기가 키의 최신 값도 삭제합니다.
- [[ui:topics.actions.deleteTopic]] — [토픽 삭제](topics.md#delete-topics)를 참고하세요.

운영 환경 연결에서는 이러한 작업마다 연결 이름도 입력해야 합니다.

아래쪽의 탭:

- [[ui:topics.details.tabMessages]] — 메시지를 조회합니다. [메시지 조회](browse.md)를 참고하세요.
- [[ui:topics.details.tabPartitions]] — 파티션별 표: [[ui:topics.details.colLeader]], [[ui:topics.details.colReplicas]], `ISR`(동기화된 복제본), 시작(`Low`) 오프셋과 끝(`High`) 오프셋, 메시지 수. 리더가 없는 파티션에는 [[ui:topics.details.noLeader]]이 표시됩니다.
- [[ui:topics.details.tabConfigs]] — 토픽 매개변수. [토픽 구성](topics.md#topic-config)을 참고하세요.
- [[ui:topics.details.tabGroups]] — 이 토픽에 커밋된 오프셋이 있는 컨슈머 그룹이 [[ui:topics.groups.colState]], [[ui:topics.groups.colPartitions]], [[ui:topics.groups.colLag]] 열과 함께 표시됩니다. 그룹을 클릭하면 그룹이 열립니다. [컨슈머 그룹](groups.md)을 참고하세요.

## 토픽 구성 {#topic-config}

[[ui:topics.details.tabConfigs]] 탭에는 모든 토픽 매개변수가 세 개의 열로 나열됩니다:

- [[ui:common.config.colParam]] — 매개변수 이름. 굵게 표시된 이름은 토픽이나 브로커 구성에서 명시적으로 설정된 것입니다.
- [[ui:common.config.colValue]] — 현재 값. 민감한 값은 `••••••`로 숨겨집니다.
- [[ui:common.config.colSource]] — 값의 출처. 예를 들어 `DynamicTopic`은 이 토픽에 설정된 값, `StaticBroker`와 `DynamicBroker`는 브로커 구성에서 온 값, `Default`는 Kafka 기본값입니다.

이름으로 매개변수를 찾으려면 [[ui:common.config.searchParam]] 필드를, 굵게 표시된 매개변수만 보려면 [[ui:common.config.onlyChanged]] 옵션을 사용하세요. 자물쇠 아이콘은 읽기 전용 매개변수를 나타냅니다. 이름 옆의 `?` 아이콘은 매개변수를 설명합니다.

## 구성 변경 {#edit-config}

1. [[ui:topics.details.tabConfigs]] 탭에서 [[ui:topics.details.edit]]을 클릭합니다. [[ui:common.readOnly]] 연결에서는 이 버튼이 숨겨집니다.
2. [[ui:common.config.colValue]] 열에서 값을 변경합니다. 읽기 전용 매개변수와 민감한 매개변수는 편집할 수 없습니다. 토픽 자체의 값을 제거하려면 [[ui:common.config.reset]]을 클릭하세요. 그러면 브로커 값이 적용됩니다. 빈 값은 허용되지 않습니다.
3. [[ui:common.config.review]]를 클릭합니다. 앱은 각 변경을 이전 값 → 새 값으로 표시합니다. [[ui:common.config.defaultValue]]은 값이 재설정된다는 뜻입니다.
4. [[ui:common.config.apply]]을 클릭합니다. 계속 편집하려면 [[ui:common.config.back]]를 클릭합니다.

적용하기 전에는 클러스터에서 아무것도 변경되지 않습니다. [[ui:common.config.discard]]는 모든 편집을 취소합니다. 저장하지 않은 편집이 있는 동안에는 [[ui:topics.details.closeEditor]]가 비활성화됩니다. 운영 환경 연결에서는 연결 이름을 입력하여 변경을 확인합니다.

변경한 매개변수만 전송됩니다. 다른 토픽 매개변수는 그대로 유지됩니다. 브로커는 값을 검사하고 잘못된 값은 오류와 함께 거부합니다.

## 토픽 매개변수 {#config-parameters}

Kafka 토픽에는 매개변수가 수십 개 있습니다. 구성 표의 `?` 아이콘이 각 매개변수를 설명합니다. 가장 자주 필요한 매개변수는 다음과 같습니다:

- `cleanup.policy` — 오래된 데이터를 어떻게 처리할지 정합니다. `delete`는 시간이나 크기에 따라 오래된 세그먼트를 제거합니다. `compact`는 키마다 최신 값만 보관합니다. `compact,delete`는 둘 다 수행합니다.
- `retention.ms` — 메시지 보존 기간(밀리초). 기본값은 `604800000`(7일)이며, `-1`이면 영구 보관합니다. `cleanup.policy`에 `delete`가 포함된 경우에 동작합니다.
- `retention.bytes` — 오래된 세그먼트가 삭제되기 전 각 파티션의 최대 크기. 기본값 `-1`은 크기 제한이 없다는 뜻입니다.
- `segment.ms`와 `segment.bytes` — 브로커가 새 로그 세그먼트를 시작하는 시점. 보존과 컴팩션은 세그먼트 단위로 동작하므로, 메시지는 세그먼트가 닫힐 때까지 `retention.ms`보다 오래 남을 수 있습니다.
- `min.insync.replicas` — `acks=all`일 때 쓰기를 확인해야 하는 복제본 수. 복제 계수가 3이면 보통 2입니다.
- `max.message.bytes` — 브로커가 이 토픽에 대해 받아들이는 가장 큰 배치. 기본값은 약 1 MB입니다. 프로듀서도 같은 크기를 허용해야 합니다(`max.request.size`).
- `compression.type` — 브로커가 메시지를 저장하는 방식. `producer`는 프로듀서의 압축을 그대로 유지합니다.
- `message.timestamp.type` — `CreateTime`(프로듀서가 설정한 시간) 또는 `LogAppendTime`(브로커가 메시지를 기록한 시간). 메시지를 조회할 때 표시되는 시간입니다.
- `delete.retention.ms` — 컴팩션되는 토픽에서 삭제 마커(값이 null인 메시지)가 남아 있는 기간. 기본값은 1일입니다.

일부 매개변수는 최신 Kafka 버전에만 있고, 일부는 더 이상 사용되지 않습니다. 필요한 경우 `?` 아이콘에 그 내용이 표시됩니다. `remote.storage.enable` 같은 계층형 스토리지 매개변수는 클러스터가 지원하는 경우에만 동작합니다.
