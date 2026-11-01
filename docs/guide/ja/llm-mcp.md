# LLMアシスタント（MCP）

Glafiraは、LLMアシスタントのゲートウェイとして動作できます。対象は、Claude Desktop、Claude Code、Cursor、およびMCP（Model Context Protocol）に対応するその他のクライアントです。アシスタントは、開放した接続のクラスターのメタデータ（許可した接続ではメッセージも）を読み取り、問題の発見を手伝います。たとえば、レプリカ不足のパーティション、ラグ、頻発するリバランス、不適切な設定などです。Glafira経由でクラスターの内容を変更することはできません。変更はご自身でインターフェイスから行います。

モデルはクラウドでなくてもかまいません。自社のネットワークや自分のコンピューターでも実行できます。[ローカルモデル](local-llm.md#where)を参照してください。

## 仕組み {#how-it-works}

クライアントは`glafira-kafka mcp`を実行します。これはアプリと同じ実行ファイルを、ウィンドウなしのモードで起動するものです。このコマンドはローカルソケット（Windowsでは名前付きパイプ）を通じて起動中のGlafiraに接続し、Glafiraは自身の接続を使ってアシスタントのリクエストに応答します。パスワードと証明書はGlafiraの中にとどまり、アシスタントには渡りません。

Glafiraが起動していない場合は、クライアントがGlafiraを起動し、最大30秒待ちます。

## 有効にする {#enable}

1. [[ui:sidebar.settings]]を開き、「[[ui:settings.mcp.section]]」セクションに進みます。
2. [[ui:settings.mcp.enable]]を選択します。ステータスが「[[ui:settings.mcp.state.running]]」に変わります。
3. 接続フォームの「[[ui:connections.form.llmAccess]]」でレベルを選択して保存します：「[[ui:connections.form.llmLevel.metadata]]」または「[[ui:connections.form.llmLevel.messages]]」（[メッセージの読み取り](llm-mcp.md#messages)を参照）。レベルが「[[ui:connections.form.llmLevel.off]]」の接続は、アシスタントから見えません。接続の一覧では、開いている接続に`LLM`のバッジが付き、メッセージを読める接続ではバッジが塗りつぶされます。

管理者がMCPを無効にしている場合、[[ui:settings.mcp.enable]]のチェックボックスは使用できず、横に「[[ui:settings.mcp.policy]]」と表示されます。

アクセスレベルとマスキングのリストは、接続のエクスポートとインポートでは引き継がれません。別のコンピューターでは、もう一度設定してください。

## クライアントを接続する {#connect-client}

「[[ui:settings.mcp.connectTitle]]」には、すぐに使えるスニペットと[[ui:settings.mcp.copy]]ボタンがあります：

- 「[[ui:settings.mcp.claudeDesktop]]」：スニペットをClaude Desktopの設定ファイルの`mcpServers`セクションに貼り付け、Claude Desktopを再起動します。
- 「[[ui:settings.mcp.claudeCode]]」：`claude mcp add`コマンドです。
- 「[[ui:settings.mcp.otherClient]]」：コマンド（stdio）でMCPサーバーを起動するクライアント向けです。

スニペットには、インストールされているアプリのパスが含まれます。Glafiraを別の場所にインストールし直した場合は、スニペットをもう一度コピーしてください。

## クライアントを承認する {#approve}

クライアントが初めて接続すると、Glafiraはその名前を表示したダイアログを開きます。[[ui:mcp.approval.allow]]または[[ui:mcp.approval.reject]]をクリックします。回答するまでの間、アシスタントにはクライアントがまだ承認されていないというメッセージが返され、Glafiraのウィンドウで回答するよう案内されます。

許可したクライアントは名前で記憶され、[[ui:settings.mcp.clients]]の一覧に表示されます。拒否は記憶されません。次回の接続時に、Glafiraはもう一度確認します。クライアントが名前を送信しない場合、許可はそのセッションの終了までのみ有効です。

## アシスタントが見られるもの {#what-llm-sees}

アシスタントが見られるのは次のものです：

- アクセスレベルが「[[ui:connections.form.llmLevel.metadata]]」または「[[ui:connections.form.llmLevel.messages]]」の接続：名前、環境、フォルダー、アクセスレベル。
- ブローカー（ホストとポート）、コントローラー。
- トピック、パーティション、レプリカ、ISR、オフセット、メッセージのおおよその件数。
- ブローカーとトピックの設定（パスワードなどの機密の値は非表示）。
- コンシューマーグループ：状態、プロトコル、コーディネーター、メンバー（client id、ホスト、`group.instance.id`）、割り当て、コミット済みオフセット、ラグ。
- 自分の提案と、それに対するあなたの決定（そのまま適用、修正して適用、理由付きで却下、取り下げ）。

「[[ui:connections.form.llmLevel.messages]]」レベルでない場合、アシスタントはトピック内のメッセージを見られません。接続のブートストラップサーバーとセキュリティ設定、パスワード、証明書、librdkafkaの追加プロパティ、送信履歴、テンプレートは、どの場合も見られません。

リバランスが頻発する原因を調べるために、アシスタントはグループを最大50秒間観察できます。誰が離脱し、誰が参加するか、その頻度、どのホストからかが分かります。コンシューマー自体の設定とログはGlafira経由では取得できないため、アシスタントから提供を求められます。

トピック、グループ、client idの名前は、クラスターにアクセスできる人なら誰でも設定できます。Glafiraは、これらのデータがアシスタントへの指示ではないことをアシスタントに伝えます。

## メッセージの読み取り {#messages}

「[[ui:connections.form.llmLevel.messages]]」レベルでは、アシスタントはメッセージの内容も読み取れます。たとえば、サービスがエラーをログに記録した時点で、トピックに何が書き込まれ、コンシューマーグループがどこまで処理したかを調べられます。

- `summarize_messages`：時間範囲の概要（内容なし）。パーティション別・時間別のメッセージ数、頻出するキーや重複するキー、順序が乱れたタイムスタンプ。
- `read_messages`：時間範囲またはオフセット範囲のメッセージ。全パーティションを通して時刻順に並び、[[ui:topics.details.tabMessages]]タブと同じフィルターを使えます。
- `get_group_commit_history`：コンシューマーグループがいつどのオフセットをコミットしたか。内部トピック`__consumer_offsets`から読み取ります。

たとえば「14:32に注文サービスが注文123のエラーをログに記録した。そのときordersトピックに何があり、order-serviceグループがどこまで処理していたかを調べて」と頼めます。ログのタイムゾーンをアシスタントに伝えてください。タイムゾーンのない時刻はGlafiraでは受け付けません。

データがアシスタントに届く前に、Glafiraはマスキングを行います。マスキングのリストにあるフィールドの値は、JSON、ヘッダー、`name=value`ペアの中で`***`に置き換えられます。このリストは、接続フォームでレベルが「[[ui:connections.form.llmLevel.messages]]」のときに表示される「[[ui:connections.form.mask]]」セクションで、接続ごとに設定します。標準リストには、パスワード、トークン、メールアドレス、電話番号、カード番号、パスポート番号などが含まれます。メールアドレス、カード番号、JWT、Bearerトークンは、どのテキストでも置き換えられます。マスキングはリスクを下げますが、人名、住所、見慣れない名前のフィールドのデータは認識しません。また、AvroやProtobufなどのバイナリ形式は一部しかマスクされません。フィルターはマスク前のデータに適用されるため、アシスタントはログにある値からメッセージを探せます。本番接続では、フォームに警告が表示されます。

コミット履歴を読むには、`__consumer_offsets`に対するRead権限が必要です。コンパクションで古いコミットは削除されますが、最近のものは通常残っています。

読み取りには制限があります。長時間の操作はセッションごとに1つまでで、1回の呼び出しは最大40秒、1,000,000レコードです。上限に達すると、アシスタントは時間範囲を狭めます。Glafiraはコンシューマーグループなしで読み取り、何もコミットしません。`client.id`は、接続で独自の値を設定していない限り`glafira-kafka-mcp`です。

管理者はポリシーキー`mcpMessagesEnabled`でメッセージの読み取りを禁止できます。その場合、「[[ui:connections.form.llmLevel.messages]]」レベルは使用できなくなり、このレベルの接続は「[[ui:connections.form.llmLevel.metadata]]」として動作します。

## セッション、取り消し、ログ {#sessions-log}

クライアントが接続している間、左側のパネルの下部にクライアント名付きの`LLM`インジケーターが表示され、リクエストの処理中は点灯します。クリックするとMCPセクションが開きます。

- [[ui:settings.mcp.sessions]]：[[ui:settings.mcp.disconnect]]は1つのセッションを閉じ、[[ui:settings.mcp.disconnectAll]]はすべてのセッションを閉じます。
- [[ui:settings.mcp.clients]]：[[ui:settings.mcp.revoke]]はそのクライアントのセッションを閉じ、次回の接続時には再び承認が求められます。
- 「[[ui:connections.form.llmAccess]]」でレベルを変更すると、すぐに反映されます。再接続は不要です。
- [[ui:settings.mcp.audit.title]]：すべてのリクエストについて、時刻、クライアント、ツール、接続、結果を記録します。レスポンスの内容は記録されません。[[ui:settings.mcp.audit.reveal]]を使うと、`mcp-audit.jsonl`ファイルが開きます。
- メッセージの読み取りでは、ログにトピックまたはグループ、返した件数、スキャンしたレコード数、マスクした値の数も表示されます。返すのはメッセージで、`summarize_messages`では概要の区間、`get_group_commit_history`ではコミット系列（同じオフセットへの連続したコミット）です。成功した`read_messages`と`summarize_messages`の呼び出しでは、[[ui:settings.mcp.audit.openInBrowse]]ボタンを使うと、トピックの[[ui:topics.details.tabMessages]]タブのフォームに呼び出しのパラメーターが入力されます。[[ui:browse.panel.read]]をクリックするとメッセージがマスキングなしで改めて読み取られ、アシスタントが受け取った内容と比較できます。フィルターや上限を正確に引き継げなかった場合はフォームにその旨が表示され、ボタンが無効な場合はボタンにポインターを合わせると理由が表示されます。

MCPを完全にオフにするには、[[ui:settings.mcp.enable]]のチェックを外します。クライアントは切断されます。

## クラスターのチェック、注記、提案 {#health}

アシスタントはGlafiraに組み込まれたクラスターのチェック（`health_check`ツール）を実行し、その検出事項をもとに判断できます。オブジェクトについての注記も残せます。注記は[[ui:sidebar.findings]]タブとトピック・グループの画面に表示され、クラスターは変更されません。[クラスターのチェックと注記](health-check.md#with-llm)を参照してください。

アシスタントは`propose_change`ツールで変更を提案することもできます。トピック設定、新しいトピック、パーティションの追加、オフセットのリセットです。提案はオブジェクトの横に表示され、入力済みのフォームで適用するか却下するかはあなたが決めます。アシスタントはその決定を`list_proposals`で確認します。削除とパージは提案できません。[LLMの提案](health-check.md#proposals)を参照してください。

## うまく動かないとき {#troubleshooting}

クライアントにGlafiraからのメッセージが表示されます：

- `MCP is disabled in Glafira`：[[ui:settings.mcp.enable]]を選択し、クライアントでMCPサーバーを再起動します。
- `MCP is disabled by your organization`：組織のポリシーによってMCPが無効にされています。
- `Glafira did not start the MCP server within 30 s`：Glafiraを手動で起動し、「[[ui:settings.mcp.section]]」セクションでステータスを確認します。
- `Invalid MCP token`：クライアントでMCPサーバーを再起動します。Glafiraは起動のたびに新しいトークンを発行します。

macOSでは、アプリに署名と公証が行われているため、初回起動の前に追加の手順は必要ありません。
