# LLM 어시스턴트(MCP)

Glafira는 LLM 어시스턴트의 게이트웨이 역할을 할 수 있습니다. 대상은 Claude Desktop, Claude Code, Cursor 또는 MCP(Model Context Protocol)를 지원하는 다른 클라이언트입니다. 어시스턴트는 열어 준 클러스터의 메타데이터를 읽고 문제를 찾도록 돕습니다. 예를 들어 복제 부족 파티션, 랙, 계속되는 리밸런싱, 부적절한 설정이 있습니다. Glafira를 통해서는 클러스터에서 아무것도 변경할 수 없습니다. 변경은 직접 인터페이스에서 합니다.

## 작동 방식 {#how-it-works}

클라이언트는 `glafira-kafka mcp`를 실행합니다. 이것은 앱과 같은 실행 파일을 창이 없는 모드로 실행하는 것입니다. 이 명령은 로컬 소켓(Windows에서는 명명된 파이프)을 통해 실행 중인 Glafira에 연결하고, Glafira는 자체 연결로 어시스턴트의 요청에 응답합니다. 비밀번호와 인증서는 Glafira 안에 남아 있으며 어시스턴트는 이를 받지 않습니다.

Glafira가 실행 중이 아니면 클라이언트가 직접 실행하고 최대 30초 동안 기다립니다.

## 사용 설정 {#enable}

1. [[ui:sidebar.settings]]을 열고 [[ui:settings.mcp.section]] 섹션으로 이동합니다.
2. [[ui:settings.mcp.enable]]을 선택합니다. 상태에 [[ui:settings.mcp.state.running]]라고 표시됩니다.
3. 연결 양식의 [[ui:connections.form.llmAccess]] 항목에서 수준을 선택하고 저장합니다: [[ui:connections.form.llmLevel.metadata]] 또는 [[ui:connections.form.llmLevel.messages]] 수준([메시지 읽기](llm-mcp.md#messages) 참조). [[ui:connections.form.llmLevel.off]] 수준인 연결은 어시스턴트에게 보이지 않습니다. 연결 목록에서 열려 있는 연결에는 `LLM` 표시가 붙고, 메시지를 읽을 수 있는 연결에서는 표시가 채워집니다.

관리자가 MCP를 사용하지 못하게 했다면 [[ui:settings.mcp.enable]] 확인란을 사용할 수 없으며 옆에 [[ui:settings.mcp.policy]]라고 표시됩니다.

접근 수준과 마스킹 목록은 연결을 내보내고 가져올 때 옮겨지지 않습니다. 다른 컴퓨터에서는 다시 설정하세요.

## 클라이언트 연결 {#connect-client}

[[ui:settings.mcp.connectTitle]]에는 바로 사용할 수 있는 스니펫과 [[ui:settings.mcp.copy]] 버튼이 있습니다:

- [[ui:settings.mcp.claudeDesktop]] — 스니펫을 Claude Desktop 구성 파일의 `mcpServers` 섹션에 붙여 넣고 Claude Desktop을 다시 시작하세요.
- [[ui:settings.mcp.claudeCode]] — `claude mcp add` 명령입니다.
- [[ui:settings.mcp.otherClient]] — 명령으로 MCP 서버를 시작하는 모든 클라이언트용입니다(stdio).

스니펫에는 설치된 앱의 경로가 들어 있습니다. Glafira를 다른 위치에 다시 설치했다면 스니펫을 다시 복사하세요.

## 클라이언트 승인 {#approve}

클라이언트가 처음 연결되면 Glafira가 클라이언트 이름이 표시된 대화 상자를 엽니다. [[ui:mcp.approval.allow]] 또는 [[ui:mcp.approval.reject]]를 클릭합니다. 답하기 전까지 어시스턴트는 클라이언트가 아직 승인되지 않았다는 메시지를 받고, Glafira 창에서 답해 달라고 요청합니다.

허용된 클라이언트는 이름으로 기억되며 [[ui:settings.mcp.clients]] 목록에 표시됩니다. 거부는 기억되지 않습니다. 다음에 연결하면 Glafira가 다시 묻습니다. 클라이언트가 이름을 보내지 않으면 허용은 해당 세션이 끝날 때까지만 유효합니다.

## 어시스턴트가 볼 수 있는 것 {#what-llm-sees}

어시스턴트가 볼 수 있는 항목은 다음과 같습니다:

- 접근 수준이 [[ui:connections.form.llmLevel.metadata]] 또는 [[ui:connections.form.llmLevel.messages]] 수준인 연결: 이름, 환경, 폴더, 접근 수준.
- 호스트와 포트가 있는 브로커, 컨트롤러.
- 토픽, 파티션, 복제본, ISR, 오프셋, 메시지의 대략적인 개수.
- 브로커와 토픽의 구성. 비밀번호와 그 밖의 비밀 값은 숨겨집니다.
- 컨슈머 그룹: 상태, 프로토콜, 코디네이터, client id·호스트·`group.instance.id`가 있는 멤버, 할당, 커밋된 오프셋, 랙.
- 자신의 제안과 그에 대한 사용자의 결정: 적용됨(그대로 또는 수정하여), 사유와 함께 거부됨, 철회됨.

[[ui:connections.form.llmLevel.messages]] 수준이 아니면 어시스턴트는 토픽의 메시지를 볼 수 없습니다. 연결의 부트스트랩 서버와 보안 설정, 비밀번호, 인증서, librdkafka 추가 속성, 전송 기록과 템플릿은 어떤 경우에도 볼 수 없습니다.

리밸런싱이 계속되는 원인을 조사하기 위해 어시스턴트는 그룹을 최대 50초 동안 관찰할 수 있습니다. 누가 나가고 들어오는지, 얼마나 자주인지, 어느 호스트인지 확인합니다. 컨슈머 자체의 구성과 로그는 Glafira를 통해 가져올 수 없으므로 어시스턴트가 사용자에게 요청합니다.

토픽, 그룹, client id의 이름은 클러스터에 접근할 수 있는 누구나 정할 수 있습니다. Glafira는 이 데이터가 어시스턴트에 대한 지시가 아니라고 어시스턴트에게 알려 줍니다.

## 메시지 읽기 {#messages}

[[ui:connections.form.llmLevel.messages]] 수준에서는 어시스턴트가 메시지 내용도 읽습니다. 예를 들어 서비스가 로그에 오류를 기록하던 시점에 토픽에 무엇이 기록되었고 컨슈머 그룹이 어디까지 처리했는지 알아볼 수 있습니다.

- `summarize_messages` — 내용 없이 시간 범위를 개괄합니다: 파티션별·시간별 메시지 수, 자주 나오는 키와 반복되는 키, 순서가 어긋난 타임스탬프.
- `read_messages` — 시간 범위 또는 오프셋 범위의 메시지를 모든 파티션에 걸쳐 시간순으로 보여 줍니다. [[ui:topics.details.tabMessages]] 탭과 같은 필터를 쓸 수 있습니다.
- `get_group_commit_history` — 컨슈머 그룹이 언제 어떤 오프셋을 커밋했는지 내부 토픽 `__consumer_offsets`에서 읽습니다.

예를 들어 이렇게 물어볼 수 있습니다: '14:32에 주문 서비스가 주문 123의 오류를 로그에 기록했어. 그때 orders 토픽에 무엇이 있었고 order-service 그룹이 어디까지 처리했는지 알아봐 줘.' 로그의 시간대를 어시스턴트에게 알려 주세요. 시간대가 없는 시각은 Glafira가 받지 않습니다.

데이터가 어시스턴트에게 전달되기 전에 Glafira가 마스킹합니다. 마스킹 목록에 있는 필드의 값은 JSON, 헤더, `name=value` 쌍에서 `***`로 바뀝니다. 이 목록은 연결 양식의 [[ui:connections.form.mask]] 섹션에서 연결마다 지정하며, 이 섹션은 [[ui:connections.form.llmLevel.messages]] 수준에서 표시됩니다. 표준 목록에는 비밀번호, 토큰, 이메일, 전화번호, 카드 번호, 여권 번호 등이 포함됩니다. 이메일, 카드 번호, JWT, Bearer 토큰은 어떤 텍스트에서든 바뀝니다. 마스킹은 위험을 줄이지만 사람 이름, 주소, 낯선 이름의 필드에 있는 데이터는 인식하지 못하며, Avro나 Protobuf 같은 바이너리 형식은 일부만 마스킹됩니다. 필터는 마스킹하지 않은 데이터를 보므로 어시스턴트는 로그에 있는 값으로 메시지를 찾을 수 있습니다. 운영 환경 연결에서는 양식에 경고가 표시됩니다.

커밋 기록을 읽으려면 `__consumer_offsets`에 대한 Read 권한이 필요합니다. 컴팩션은 오래된 커밋을 삭제하지만 최근 커밋은 보통 남아 있습니다.

읽기에는 제한이 있습니다. 세션마다 오래 걸리는 작업은 하나만 할 수 있으며, 호출 한 번에 최대 40초, 1,000,000개 레코드까지 읽습니다. 한도에 도달하면 어시스턴트가 시간 범위를 좁힙니다. Glafira는 컨슈머 그룹 없이 읽으며 아무것도 커밋하지 않습니다. `client.id`는 연결에서 직접 지정하지 않으면 `glafira-kafka-mcp`입니다.

관리자는 정책 키 `mcpMessagesEnabled`로 메시지 읽기를 금지할 수 있습니다. 그러면 [[ui:connections.form.llmLevel.messages]] 수준을 사용할 수 없고, 이 수준의 연결은 [[ui:connections.form.llmLevel.metadata]] 수준으로 동작합니다.

## 세션, 승인 취소, 로그 {#sessions-log}

클라이언트가 연결되어 있는 동안 왼쪽 패널 아래쪽에 클라이언트 이름이 있는 `LLM` 표시기가 나타나며, 요청 중에는 켜집니다. 이 표시기를 클릭하면 MCP 섹션이 열립니다.

- [[ui:settings.mcp.sessions]]: [[ui:settings.mcp.disconnect]]는 세션 하나를, [[ui:settings.mcp.disconnectAll]]는 모든 세션을 닫습니다.
- [[ui:settings.mcp.clients]]: [[ui:settings.mcp.revoke]]는 클라이언트의 세션을 닫으며, 클라이언트는 다음에 연결할 때 다시 승인을 요청합니다.
- [[ui:connections.form.llmAccess]] 항목에서 수준을 바꾸면 바로 적용되며 다시 연결할 필요가 없습니다.
- [[ui:settings.mcp.audit.title]] — 모든 요청: 시간, 클라이언트, 도구, 연결, 결과. 응답 내용은 기록되지 않습니다. [[ui:settings.mcp.audit.reveal]]를 클릭하면 `mcp-audit.jsonl` 파일이 열립니다.
- 메시지 읽기의 경우 로그에는 토픽 또는 그룹, 스캔한 레코드 중 반환한 개수, 마스킹한 값의 개수도 표시됩니다. 성공한 `read_messages` 및 `summarize_messages` 호출에서 [[ui:settings.mcp.audit.openInBrowse]] 버튼을 클릭하면 토픽의 [[ui:topics.details.tabMessages]] 탭에 있는 양식이 호출의 매개변수로 채워집니다. [[ui:browse.panel.read]] 버튼을 클릭하면 메시지를 마스킹 없이 다시 읽어 어시스턴트가 받은 내용과 비교할 수 있습니다. 필터나 제한을 정확히 옮기지 못한 경우 양식에 표시되며, 버튼이 비활성화되어 있으면 버튼에 포인터를 올려 이유를 볼 수 있습니다.

MCP를 완전히 끄려면 [[ui:settings.mcp.enable]] 확인란의 선택을 해제하세요. 클라이언트의 연결이 끊어집니다.

## 클러스터 점검, 메모, 제안 {#health}

어시스턴트는 Glafira 기본 제공 클러스터 점검(`health_check` 도구)을 실행하고 그 발견 사항을 근거로 삼을 수 있습니다. 개체에 대한 메모도 남길 수 있습니다. 메모는 [[ui:sidebar.findings]] 탭과 토픽·그룹 화면에 표시되며 클러스터는 바뀌지 않습니다. [클러스터 점검과 메모](health-check.md#with-llm)를 참고하세요.

어시스턴트는 `propose_change` 도구로 변경을 제안할 수도 있습니다. 토픽 설정, 새 토픽, 파티션 추가, 오프셋 재설정입니다. 제안은 개체 옆에 표시되며, 미리 채워진 양식에서 직접 적용하거나 거부하는 것은 사용자의 몫입니다. 어시스턴트는 그 결정을 `list_proposals`에서 확인합니다. 삭제와 비우기는 제안할 수 없습니다. [LLM 제안](health-check.md#proposals)을 참고하세요.

## 작동하지 않을 때 {#troubleshooting}

클라이언트에 Glafira의 메시지가 표시됩니다:

- `MCP is disabled in Glafira` — [[ui:settings.mcp.enable]] 확인란을 선택하고 클라이언트에서 MCP 서버를 다시 시작하세요.
- `MCP is disabled by your organization` — 조직 정책으로 MCP가 비활성화되어 있습니다.
- `Glafira did not start the MCP server within 30 s` — Glafira를 직접 실행하고 [[ui:settings.mcp.section]] 섹션에서 상태를 확인하세요.
- `Invalid MCP token` — 클라이언트에서 MCP 서버를 다시 시작하세요. Glafira는 실행할 때마다 새 토큰을 발급합니다.

macOS에서는 앱이 서명 및 공증되어 있으므로 첫 실행 전에 추가 단계가 필요하지 않습니다.
