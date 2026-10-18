# トラブルシューティング

この章では、よくある接続エラーと、それぞれで確認すべき点を説明します。ログの場所と、サポートへの問い合わせに含める内容も紹介します。macOSでは、「ヘルプ」メニューの[[ui:menu.troubleshooting]]でこの章を開けます。

エラーメッセージには短いテキストがあり、ほとんどの画面ではかっこ内にエラーコードも表示されます。例：`TIMEOUT`。[[ui:common.details]]を展開すると、Kafkaクライアント（librdkafka）の元のメッセージが表示されます。多くの場合、ブローカーの正確なアドレスと原因が示されています。

## 接続できない {#cannot-connect}

まず、接続のフォームで[[ui:connections.form.test]]を実行します。[接続をテストする](connections.md#test)を参照してください。テストは最初に失敗した段階で止まり、その段階から確認すべき箇所がわかります：

- [[ui:connections.form.step.config]]：フォーム自体に問題があります。ポートのないアドレス、証明書ファイルがない、または読み込めない、証明書のパスワードが間違っている、などです。エラーにはフィールド名が示されます。証明書ファイルについては、[TLSのエラー](troubleshooting.md#tls-errors)を参照してください。
- [[ui:connections.form.step.tcp]]：コンピューターからブートストラップサーバーに到達できません。[DNSとTCP](troubleshooting.md#dns-tcp)を参照してください。
- [[ui:connections.form.step.kafka]]：ネットワークは機能していますが、TLSハンドシェイク、SASLログイン、またはメタデータのリクエストが失敗しました。[TLSのエラー](troubleshooting.md#tls-errors)、[SASLのエラー](troubleshooting.md#sasl-errors)、[タイムアウト](troubleshooting.md#timeouts)を参照してください。

最もよくある原因は、プロトコルがポートと一致していないことです。ブローカーのリスナーごとに、独自のポートとプロトコルがあります。どのポートがどのプロトコルに対応するかは、Kafkaの管理者に確認してください。[セキュリティプロトコル](connections.md#security)を参照してください。

テストは成功するのに読み取りや送信が失敗する場合、原因はブローカーのアドレスであることがよくあります。クライアントはブートストラップサーバーからブローカーの一覧を取得し、各ブローカーが公開するアドレス（`advertised.listeners`）で各ブローカーに接続します。これらのアドレスがDockerやKubernetesのホスト名などの内部アドレスの場合、お使いのコンピューターからは到達できません。アドレスはクラスターの概要のブローカーのテーブルに一覧表示されます。[クラスターの概要](topics.md#cluster-overview)を参照してください。外部から到達できるリスナーについて、管理者に問い合わせてください。

## DNSとTCP {#dns-tcp}

「[[ui:errors.kafka.tcpUnreachable]]」は、[[ui:connections.form.bootstrap]]のどのアドレスにもTCP接続を開けなかったことを意味します。各アドレスの待ち時間は最大5秒です。詳細には、最後に試したアドレスとその理由が表示されます。詳細に`DNS`とある場合は、ホスト名を解決できなかったことを意味します。

確認すること：

- ホスト名とポート。`https://`などのプロトコルのプレフィックスは付けません。
- クラスターがプライベートネットワーク内にある場合は、VPNがオンになっていること。
- ファイアウォールがポートをブロックしていないこと。
- Docker内のブローカーの場合は、ポートがホストに公開されていること。例：`-p 9092:9092`。

アプリの外でポートを確認するには、macOSまたはLinuxでは`nc -vz <host> <port>`を、WindowsではPowerShellで`Test-NetConnection <host> -Port <port>`を実行します。

TCPの段階が成功したあとの「[[ui:errors.kafka.connectionFailed]]」は、通常、ブローカーが接続を閉じたか、その後ブローカーに到達できなくなったことを意味します。ブローカーのアドレスも確認してください。[接続できない](troubleshooting.md#cannot-connect)を参照してください。

## TLSのエラー {#tls-errors}

TLSハンドシェイク中に「[[ui:connections.form.step.kafka]]」の段階で発生するエラー：

- [[ui:errors.kafka.plaintextListener]]。
- [[ui:errors.kafka.caUntrusted]]。[[ui:connections.tls.ca]]に、ブローカーの証明書に署名したCAのファイルを指定します。このファイルは管理者から入手できます。フィールドが空の場合は、システムのルート証明書だけが信頼されます。
- [[ui:errors.kafka.hostnameMismatch]]。IPアドレスや別名ではなく、証明書の発行対象の名前で接続してください。テスト環境に限り、[[ui:connections.tls.verifyHostname]]をオフにできます。
- [[ui:errors.kafka.clientCertRejected]]。ブローカーがクライアント証明書を要求しているか、使用中の証明書を信頼していません。「[[ui:connections.tls.mtls]]」がオンになっていること、証明書がこのクラスター用に発行されたものであることを確認してください。[クライアント証明書（mTLS）](connections.md#mtls)を参照してください。
- [[ui:errors.kafka.tlsError]]：その他のTLSの問題。原因を確認するには[[ui:common.details]]を展開します。

SSLを使わないプロトコルを選んだのにTLSのエラーが表示される場合、そのポートはおそらくTLSのリスナーです。その場合、詳細に`connecting to a SSL listener?`と表示されます。`SSL`または`SASL_SSL`を選ぶか、TLSを使わないリスナーのポートを使ってください。

証明書ファイルの問題は、アプリが接続する前に「[[ui:connections.form.step.config]]」の段階で報告されます。メッセージはフィールド名（信頼されたCA、クライアント証明書、クライアント秘密鍵）で始まります。よくあるケース：

- パスが絶対パスではないか、ファイルを読み込めない。フルパスを入力するか、「[[ui:connections.tls.pick]]」でファイルを選んでください。
- [[ui:errors.tls.keyEncrypted]]、[[ui:errors.tls.keyBadPassword]]、[[ui:errors.tls.clientPkcs12BadPassword]]、[[ui:errors.tls.jksBadStorePassword]]：フォームに正しいパスワードを入力してください。
- [[ui:errors.tls.keyMismatch]]：鍵ファイルが別の証明書のものです。
- クライアント証明書の有効期限が切れている、またはまだ有効になっていない：メッセージに日付が表示されます。新しい証明書を依頼してください。
- PKCS#12ストアが古いアルゴリズムを使っている、またはストアがJCEKS形式である。[証明書の形式](connections.md#certificate-formats)の説明に従ってファイルを変換してください。

## SASLのエラー {#sasl-errors}

「[[ui:errors.kafka.authFailed]]」は、ブローカーがSASLログインを受け付けなかったことを意味します。確認すること：

- ユーザー名とパスワード。保存済みの接続を編集するとき、パスワードのフィールドが空なら保存済みのパスワードが使われます。正しいかどうか自信がない場合は、パスワードを入力し直してください。
- [[ui:connections.form.saslMechanism]]。ユーザーが作成されたメカニズムと一致している必要があります。SCRAMの資格情報は`SCRAM-SHA-256`と`SCRAM-SHA-512`で別々に作成され、`PLAIN`のユーザーはさらに別に設定されます。
- プロトコル。通常、`SASL_SSL`と`SASL_PLAINTEXT`は異なるポートを使います。

[[ui:errors.connections.saslPasswordMissing]]：インポート後などで、この接続にパスワードが保存されていません。接続を開いてパスワードを入力し、保存してください。[パスワード](connections.md#passwords)を参照してください。

[[ui:errors.secrets.keychain]]：アプリがシステムの資格情報ストアからパスワードを読み取れません。macOSでは、キーチェーンから求められたらアクセスを許可してください。Linuxでは、GNOME Keyring、KWallet、またはほかのSecret Serviceプロバイダーがセッションで動作していることを確認してください。

「[[ui:errors.kafka.notAuthorized]]」は別の問題です。ログインには成功していますが、Kafkaのユーザーにこの操作（トピックやグループの読み取りなど）の権限（ACL）がありません。管理者に権限を依頼してください。なお、Kafkaは詳細の取得が許可されていないトピックについてエラーを報告しません。そのようなトピックは単にトピック一覧に表示されません。

## タイムアウト {#timeouts}

「[[ui:errors.kafka.timeout]]」は、ブローカーが時間内に応答しなかったことを意味します。主な原因は、低速なネットワークやVPN、過負荷のクラスター、到達できないブローカーのアドレスです。[接続できない](troubleshooting.md#cannot-connect)を参照してください。

TLSやSASLの問題がタイムアウトのように見えることがあります。アプリは本当の原因を探して代わりに表示しようとしますが、常にできるとは限りません。[[ui:common.details]]を展開してください。`SSL handshake failed`や`SASL authentication error`などのメッセージが原因を示しています。

ネットワークが単に遅い場合は、接続のフォームで「[[ui:connections.form.timeout]]」の値を大きくします。デフォルトは`30000` msです。この値は、ブローカーへのリクエスト、メッセージの送信、読み取りに適用されます。接続のテストには独自の制限（TCPの各アドレスに5秒、Kafkaの段階に10秒）があるため、値を大きくしてもテストは変わりません。

## 操作が利用できない、または拒否される {#read-only-blocked}

ボタンが非表示または無効になっている場合や、操作が拒否される場合は、次の原因を確認してください：

- 接続が読み取り専用である。名前の横に鍵のアイコンが表示されます。クラスターを変更するボタンは非表示または無効になります。画面を開いている間にモードがオンになった場合など、そのようなリクエストがアプリのコアに届いた場合は、接続が読み取り専用であることを示すエラーが表示されます。変更するには、接続を開いて[[ui:common.readOnly]]のチェックを外し、保存します。Freeでは、本番接続のこのチェックは外せません。[読み取り専用](connections.md#read-only)を参照してください。
- 本番接続である。変更時に接続名の入力を求められます。名前が一致するまで操作は実行されません。[環境と本番](connections.md#environments)を参照してください。
- 機能にProが必要である。「[[ui:errors.license.proRequired]]」というエラーが表示されるか、ライセンスの期限が切れている場合は有効期限の日付が表示されます。[FreeとPro](license-updates.md#free-and-pro)を参照してください。
- コンシューマーグループにアクティブなメンバーがいる。オフセットのリセットとグループの削除は、メンバーがいない場合にだけ行えます。[オフセットのリセット](groups.md#reset-offsets)を参照してください。
- 設定が組織によって管理されている。ポリシーでは表示言語、ライセンスキー、アップデートの確認を設定でき、これらはアプリ内では変更できません。[組織が設定した項目](getting-started.md#managed-settings)を参照してください。
- Kafkaが「[[ui:errors.kafka.notAuthorized]]」というエラーで操作を拒否した。これはGlafiraの設定ではなく、クラスターの権限の問題です。[SASLのエラー](troubleshooting.md#sasl-errors)を参照してください。

## ログ {#logs}

アプリは、動作とKafkaクライアントのエラーをログに書き込みます。正確なフォルダーは、[[ui:sidebar.about]]画面の「[[ui:about.logDir]]」に表示されます。デフォルトの場所：

- macOS：`~/Library/Logs/io.github.vvnovg.glafira.kafka`
- Windows：`%LOCALAPPDATA%\io.github.vvnovg.glafira.kafka\logs`
- Linux：`~/.local/share/io.github.vvnovg.glafira.kafka/logs`

現在のファイルは`app.log`です。5 MBに達すると`app.1.log`に名前が変わり、それより古いファイルは`app.2.log`のように順に移ります。アプリは最大5つのファイルを保持し、最も古いものを削除します。

パスワードはログに書き込まれません。ブローカーのアドレスとKafkaのエラーのテキストは書き込まれます。ファイルを手作業で集める必要はありません。診断アーカイブに含まれます。[診断アーカイブ](license-updates.md#diagnostics)を参照してください。

## サポートへの問い合わせ {#support}

サポートに問い合わせる前に、[[ui:connections.form.test]]を実行し、どの段階で失敗するかを控えておいてください。次に、管理者など、Glafiraのサポートを担当している人に問い合わせを送ります。次の内容を含めてください：

- 診断アーカイブ。[診断アーカイブ](license-updates.md#diagnostics)を参照してください。バージョン、ビルド、ログ、パスワードを含まない接続プロファイルが含まれます。
- 行った操作、期待した結果、実際に起きたことを順を追って。
- [[ui:common.details]]の内容を含む、エラーの全文。このテキストは選択してコピーできます。
- インターフェイスの問題の場合は、スクリーンショット。
- ログで探せるように、エラーが発生した時刻。

アーカイブを作成できない場合は、[[ui:sidebar.about]]画面の[[ui:about.version]]と[[ui:about.build]]の値を伝え、ログフォルダーのログファイルを添付してください。
