# 라이선스 및 업데이트

이 장에서는 Pro에 추가되는 기능, 라이선스 키 활성화 방법, 라이선스가 만료되면 어떻게 되는지, 앱 업데이트 방법, [[ui:sidebar.about]] 화면, 진단 아카이브를 설명합니다.

## Free와 Pro {#free-and-pro}

Free로 Kafka의 일상적인 작업을 모두 할 수 있습니다. mTLS를 포함한 모든 보안 프로토콜, 클러스터 개요와 토픽, 메시지 조회, 일괄 전송을 포함한 Producer, 컨슈머 그룹, 오프셋 재설정, 클러스터 점검이 포함됩니다. 모든 표시 언어도 포함됩니다. [표시 언어](getting-started.md#language)를 참고하세요.

Pro에서 추가되는 기능:

- 컨슈머 세션: 그룹 멤버로 읽기, 오프셋 커밋, 파티션 일시 중지. [컨슈머 세션](consumer-sessions.md#what-is)을 참고하세요.
- 운영 환경 모드: 운영 환경 연결 만들기, 연결을 운영 환경으로 바꾸기, 운영 환경 연결에서 [[ui:common.readOnly]] 해제. [환경과 운영 환경 연결](connections.md#environments)을 참고하세요.
- LLM 메모: MCP로 연결된 어시스턴트가 클러스터, 토픽, 그룹에 대한 메모를 남깁니다. [클러스터 점검과 메모](health-check.md#notes)를 참고하세요.

Free에서는 이러한 기능이 보이지만 사용할 수 없습니다. 컨슈머 세션 명령은 비활성화되고 자물쇠와 `Pro` 표시가 붙습니다. 운영 환경 옵션은 [[ui:connections.form.prodPro]] 항목으로 표시됩니다. Pro가 필요한 작업을 하면 [[ui:common.openLicense]] 버튼과 함께 다음 오류가 표시됩니다: [[ui:errors.license.proRequired]].

[[ui:tabs.license]] 화면의 [[ui:license.whatProGives]] 아래에 컨슈머 세션, 운영 환경 모드, LLM 메모가 나열됩니다.

## 키 활성화 {#activate}

라이선스 키는 개인 또는 조직에 발급되는 한 줄의 텍스트입니다. 앱은 컴퓨터에서 키를 확인하며 어떤 서버에도 접속하지 않습니다.

1. 왼쪽 패널 아래쪽에서 [[ui:tabs.license]]를 클릭합니다.
2. [[ui:license.keyLabel]] 필드에 키 전체를 붙여 넣습니다.
3. [[ui:license.activate]]를 클릭합니다.

Pro 기능을 즉시 사용할 수 있습니다. 화면에는 [[ui:license.tier]](Pro), [[ui:license.licensee]], [[ui:license.kind]]([[ui:license.kindPersonal]] 또는 [[ui:license.kindOrganization]]), [[ui:license.expires]](날짜 또는 [[ui:license.perpetual]])가 표시됩니다. 키는 앱 데이터 폴더에 저장되므로 다시 시작하거나 업데이트한 후에도 유지됩니다. 새 키는 이전 키를 대체합니다.

키가 받아들여지지 않으면 필드 아래에 이유가 표시됩니다:

- [[ui:errors.license.malformed]] — 키의 일부가 빠졌거나 변경되었습니다. 추가 문자 없이 한 줄로 키를 다시 복사하세요.
- [[ui:errors.license.badSignature]] — 키가 Glafira용으로 발급되지 않았거나 편집되었습니다.
- 키가 만료됨 — 메시지에 만료 날짜가 표시됩니다. 새 키를 요청하세요.
- 키에 더 새로운 버전의 앱이 필요함 — 메시지에 키 버전이 표시됩니다. 먼저 앱을 업데이트하세요. [업데이트](license-updates.md#updates)를 참고하세요.

키를 제거하려면 [[ui:license.removeKey]]를 클릭하세요. 앱이 다음과 같이 묻습니다: [[ui:license.removeQuestion]] 확인하려면 [[ui:license.removeYes]]를 클릭하세요. 데이터는 그대로 유지됩니다.

조직은 정책을 통해 모든 사용자의 Pro를 활성화할 수 있습니다. 이 경우 키 필드와 버튼이 숨겨지고 화면에 다음 문구가 표시됩니다: [[ui:license.managed]] 정책 키가 설정되어 있는 동안에는 직접 입력한 키가 사용되지 않습니다. [[ui:license.policyTitle]] 섹션에는 정책의 출처와 오류 목록이 표시됩니다. 조직 키가 올바르지 않으면 [[ui:tabs.license]] 화면에 이유가 표시됩니다. 이 경우 관리자에게 문의하세요. 관리자는 Glafira 소스 코드의 `docs/admin.md`에서 정책 설정을 확인할 수 있습니다.

## 만료 {#expiry}

키에는 만료 날짜가 있을 수 있으며 [[ui:license.expires]]에 표시됩니다. 라이선스는 그날이 끝날 때까지 유효합니다.

남은 기간이 14일 미만이면 탭 위의 알림에 남은 일수가 표시되고, [[ui:tabs.license]] 화면에 만료 날짜가 표시됩니다. 알림의 [[ui:license.notice.more]]를 클릭하면 [[ui:tabs.license]] 화면이 열립니다. `×` 버튼은 앱을 다시 시작할 때까지 알림을 숨깁니다.

만료 날짜가 지나면 플랜이 Free로 바뀝니다. 알림과 [[ui:tabs.license]] 화면에 라이선스가 만료되어 Pro 기능이 비활성화되었다고 표시됩니다. 바뀌는 점은 다음과 같습니다:

- 새 컨슈머 세션을 시작할 수 없습니다. 이미 실행 중인 세션에서는 커밋과 파티션 일시 중지가 만료 날짜를 알려 주는 오류와 함께 거부됩니다.
- 운영 환경 연결은 같은 확인 절차와 함께 계속 동작합니다. 새 운영 환경 연결을 만들 수 없으며, 운영 환경 연결에서 [[ui:common.readOnly]] 설정을 해제할 수 없습니다.

연결, 비밀번호, 템플릿, 전송 기록은 유지됩니다. Pro를 계속 사용하려면 [키 활성화](license-updates.md#activate)에 설명된 대로 새 키를 활성화하세요.

## 업데이트 {#updates}

앱은 시작할 때 새 버전이 있는지 한 번 확인합니다. 새 버전이 있으면 탭 위의 알림에 그 버전이 표시됩니다. [[ui:license.notice.more]]를 클릭하면 [[ui:sidebar.about]] 화면이 열립니다. 인터넷에 연결되어 있지 않은 경우처럼 확인에 실패하면 아무 메시지도 표시되지 않습니다.

직접 확인하려면:

1. 왼쪽 패널 아래쪽에서 [[ui:sidebar.about]]를 엽니다.
2. [[ui:about.updates.title]] 섹션에서 [[ui:about.updates.check]]을 클릭합니다.
3. 새 버전이 있으면 화면에 버전 번호와 릴리스 노트가 표시됩니다. [[ui:about.updates.install]]을 클릭합니다.

앱은 업데이트를 다운로드하고 서명을 확인한 다음 설치하고 다시 시작합니다. 네트워크 문제로 다운로드가 중단되면 자동으로 다시 시도하며, 시도는 모두 세 번까지입니다. 세 번 모두 실패하면 앱이 다음 오류를 표시합니다: [[ui:errors.updates.downloadInterrupted]]. 서명 확인에 실패하면 앱이 다음 오류를 표시합니다: [[ui:errors.updates.badSignature]].

앱은 컴퓨터의 모든 사용자를 위해 설치되므로 설치할 때 관리자 비밀번호를 물을 수 있습니다. macOS에서는 `.pkg` 설치 프로그램, Windows에서는 `.msi` 설치 프로그램, Linux에서는 `.deb` 또는 `.rpm` 패키지로 설치한 경우입니다. 관리자 권한이 없는 사용자는 이런 설치를 업데이트할 수 없습니다.

메시지를 읽는 중이거나, 컨슈머 세션이 실행 중이거나, 일괄 전송이 진행 중이면 앱은 먼저 확인을 요청하고 활성 세션 수를 알려 줍니다. 세션을 중지하고 계속하려면 [[ui:about.updates.stopAndUpdate]]를, 그렇지 않으면 [[ui:common.cancel]]를 클릭하세요. 연결, 설정, 라이선스 키 및 기타 데이터는 업데이트 후에도 유지됩니다.

업데이트는 GitHub의 릴리스 페이지에서 다운로드됩니다. '[[ui:errors.updates.checkFailed]]' 오류로 확인에 실패하면 네트워크나 프록시를 확인하세요. '[[ui:errors.updates.notConfigured]]' 메시지는 이 빌드가 업데이트 키 없이 만들어졌다는 뜻입니다. 예를 들어 개발용 또는 테스트용 빌드입니다. 이 경우 릴리스 페이지에서 새 버전을 다운로드하세요: https://github.com/vvnovg/glafira-kafka-releases/releases

조직은 정책을 통해 업데이트 확인을 끄거나 내부 미러로 지정할 수 있습니다. 확인이 꺼져 있으면 [[ui:about.updates.check]]을 클릭한 후 [[ui:about.updates.title]] 섹션에 '[[ui:errors.updates.managed]]'라는 메시지가 표시됩니다. '[[ui:errors.updates.notConfigured]]' 메시지와 마찬가지로 이 메시지는 클릭한 후에만 나타나며, 이런 경우 시작할 때의 확인에서는 아무것도 표시되지 않습니다.

## Glafira 정보 {#about}

왼쪽 패널 아래쪽에서 [[ui:sidebar.about]]를 여세요. macOS에서는 '도움말' 메뉴의 [[ui:menu.about]] 항목으로도 열 수 있습니다.

화면에는 앱에 대한 짧은 설명과 다음 항목이 표시됩니다:

- [[ui:about.version]] — 버전 번호. [[ui:about.build]] — 앱을 빌드한 소스 코드의 식별자. 지원 요청에는 둘 다 포함하세요.
- [[ui:about.system]] — 운영 체제와 프로세서 아키텍처.
- [[ui:about.tier]] — Free 또는 Pro.
- [[ui:about.policy]] — [[ui:about.policySet]] 또는 [[ui:about.policyNone]]. 정책의 오류는 아래에 나열됩니다.
- [[ui:about.dataDir]] 연결 프로필, 템플릿, 기록, 설정, 라이선스 키가 있는 폴더.
- [[ui:about.logDir]] 로그 파일이 있는 폴더. [로그](troubleshooting.md#logs)를 참고하세요.

이 화면에는 [[ui:about.updates.title]] 섹션과 [[ui:about.diagnostics.title]] 섹션도 있습니다.

## 진단 아카이브 {#diagnostics}

진단 아카이브는 지원 담당자가 문제를 파악하는 데 필요한 정보를 모읍니다. 아카이브를 만들려면:

1. [[ui:sidebar.about]]를 엽니다.
2. [[ui:about.diagnostics.title]] 섹션에서 [[ui:about.diagnostics.collect]]을 클릭합니다. macOS에서는 '도움말' 메뉴의 [[ui:menu.diagnostics]] 항목을 선택해도 됩니다.
3. 파일을 저장할 위치를 선택합니다. 제안되는 이름은 `diagnostics-<date>.zip`입니다.

아카이브가 저장되면 화면에 그 경로가 표시됩니다. ZIP 파일에는 다음이 들어 있습니다:

- `info.json` — 앱 버전과 빌드, 운영 체제, 데이터 및 로그 폴더 경로, 정책 상태, 라이선스 상태(플랜, 라이선스 소유자, 유형, 만료 날짜).
- `connections.json` — 연결 프로필: 이름, 환경, 부트스트랩 서버, 프로토콜, SASL 사용자 이름, 인증서 파일 경로, 추가 속성, 제한 시간.
- `logs/` — 앱 로그 파일.

아카이브에는 비밀번호가 없습니다. 비밀번호는 시스템 보안 저장소에 보관되며, 비밀번호와 비밀 정보가 들어 있는 추가 속성은 연결을 저장할 때 거부됩니다. 라이선스 키와 인증서 파일도 포함되지 않습니다. 호스트 이름, 사용자 이름, 파일 경로는 일반 텍스트로 저장되며, 로그에는 Kafka 오류 메시지의 브로커 주소가 들어 있습니다. 회사 밖으로 보내기 전에 아카이브를 살펴보세요.

앱은 스스로 아무것도 보내지 않습니다. 지원 요청에 아카이브를 직접 첨부하세요. [지원 문의](troubleshooting.md#support)를 참고하세요.
