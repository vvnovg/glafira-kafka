# はじめに

Glafira for Apache Kafkaは、Kafkaクラスター用のデスクトップクライアントです。トピックやコンシューマーグループの閲覧、メッセージの読み取りと送信、オフセットのリセットができます。この章では、インストール、初回起動、練習用のローカルKafka、表示言語について説明します。

## macOSへのインストール {#install-macos}

GlafiraはmacOS 11以降で動作します。リリースページからインストーラーをダウンロードしてください：https://github.com/vvnovg/glafira-kafka-releases/releases

お使いのMacに合ったファイルを選びます。Appleシリコン（M1以降）は`aarch64`、Intelは`x64`です。

1. `.dmg`ファイルを開きます。
2. Glafira for Apache Kafkaを「アプリケーション」フォルダーにドラッグします。
3. 「アプリケーション」またはLaunchpadからアプリを起動します。

`.pkg`インストーラーも使えます。この場合、アプリはすべてのユーザー向けに`/Applications`にインストールされます。

BetaビルドはまだAppleの署名を受けていないため、macOSは初回起動をブロックします。起動を許可するには：

1. 一度アプリを開こうとして、警告を閉じます。
2. 「システム設定」→「プライバシーとセキュリティ」を開きます。
3. 下にスクロールしてGlafira for Apache Kafkaに関するメッセージを探し、「このまま開く」をクリックします。パスワードを求められたら入力して確定します。

macOS 14以前では、代わりにアプリを右クリックして「開く」を選び、もう一度「開く」をクリックする方法もあります。

それでも開けない場合は、「ターミナル」で一度だけ隔離フラグを削除します：

```bash
xattr -dr com.apple.quarantine "/Applications/Glafira for Apache Kafka.app"
```

ダウンロードした`.dmg`自体が開かない場合は、先にそのファイルからフラグを削除します：

```bash
xattr -d com.apple.quarantine ~/Downloads/Glafira_for_Apache_Kafka_*.dmg
```

アプリ内からインストールしたアップデートでは、これらの手順は不要です。

## Windowsへのインストール {#install-windows}

リリースページから`.msi`インストーラー（`x64`）をダウンロードします：https://github.com/vvnovg/glafira-kafka-releases/releases

1. `.msi`ファイルを実行します。
2. Windowsの確認画面でインストールを許可します。アプリはコンピューターのすべてのユーザー向けにインストールされるため、管理者権限が必要です。
3. スタートメニューからGlafira for Apache Kafkaを起動します。

Betaビルドはまだ署名されていません。SmartScreenがインストーラーをブロックした場合は、「詳細情報」をクリックし、「実行」をクリックします。実行前にPowerShellでファイルのブロックを解除することもできます：

```powershell
Unblock-File -Path "$env:USERPROFILE\Downloads\Glafira_for_Apache_Kafka_*_x64_en-US.msi"
```

## Linuxへのインストール {#install-linux}

`x86_64`向けのビルドが3つの形式で提供されています。お使いのディストリビューションに合うものをリリースページからダウンロードしてください：https://github.com/vvnovg/glafira-kafka-releases/releases

- Debian、Ubuntuとその派生：`sudo apt install ./<file>.deb`
- Fedora、RHELとその派生：`sudo dnf install ./<file>.rpm`
- すべてのディストリビューション：`chmod +x <file>.AppImage`でAppImageに実行権限を付けて実行します。

パッケージをインストールしたら、アプリケーションメニューからアプリを起動するか、ターミナルで`glafira-kafka`を実行します。

GlafiraはパスワードをSecret Service（GNOME KeyringやKWalletなど）に保存します。セッションでいずれかが動作していることを確認してください。動作していないと、アプリは接続のパスワードを保存できません。

## 初回起動 {#first-run}

ウィンドウは2つの部分に分かれています。左側は[[ui:sidebar.connections]]パネルです。右側にはタブが並び、トピック一覧やProducerなど、開いた画面ごとにタブが作られます。

まだ接続がない場合、パネルには短いヒントが表示され、タブ領域には接続の追加が案内されます。始めるには：

1. [[ui:sidebar.connections]]パネルのヘッダーにある`+`（[[ui:sidebar.newConnection]]）をクリックします。
2. [[ui:connections.form.name]]と[[ui:connections.form.bootstrap]]を入力します。例：`localhost:9092`。
3. [[ui:connections.form.test]]をクリックして設定を確認し、[[ui:common.save]]をクリックします。

保存すると、クラスターの概要が開きます。パネルの各接続の下には、[[ui:sidebar.cluster]]、[[ui:sidebar.topics]]、[[ui:sidebar.groups]]、[[ui:sidebar.producer]]、[[ui:sidebar.consumer]]へのリンクがあります。接続の設定の詳細は[接続](connections.md#create)を参照してください。

パネルの下部には、[[ui:sidebar.settings]]、[[ui:support.title]]、[[ui:sidebar.help]]、[[ui:sidebar.about]]へのリンクがあります。フィールドの横の`?`にはそのフィールドの短いヒントが表示されます。ウィンドウ内のどこでも`F1`を押すと、現在の画面に対応するガイドの章が開きます。macOSでは「ヘルプ」メニューからもガイドを開けます。

Kafka Explorer（このアプリの以前の名前）を使っていた場合、Glafiraは初回起動時にその接続、テンプレート、送信履歴、ライセンスをコピーします。パスワードもシステムの資格情報ストアからコピーされます。既存のデータは上書きされず、古いデータも削除されません。macOSでは、古いキーチェーン項目へのアクセスを一度だけ求められることがあります。

## 練習用のローカルKafka {#local-cluster}

共有クラスターを使わずにアプリを試すには、お使いのコンピューターのDockerでKafkaを起動します。ブローカーは1つで十分です：

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
```

次に、ブートストラップサーバー`localhost:9092`、プロトコル`PLAINTEXT`、環境`local`で接続を作成します。ブローカーが不要になったら、`docker rm -f kafka`で削除します。

空の[[ui:sidebar.connections]]パネルのヒントには`docker compose -f dev/docker-compose.yml up -d`が記載されています。このファイルはGlafiraのソースコードに含まれています。ソースコードがあれば、このコマンドで3つのリスナーを持つブローカーが起動します：

- `localhost:9092`：`PLAINTEXT`。
- `localhost:9094`：`PLAIN`（ユーザー`tester` / `tester-secret`と`admin` / `admin-secret`）およびSCRAM（`SCRAM-SHA-256`と`SCRAM-SHA-512`用の`scram-user` / `scram-secret`）による`SASL_PLAINTEXT`。
- `localhost:9095`：クライアント証明書（mTLS）による`SSL`。テスト用の証明書は`dev/certs/out`にあります。

これらのユーザーと証明書はローカルでのテスト専用です。

## 表示言語 {#language}

インターフェイスは英語、ロシア語、ドイツ語、フランス語、スペイン語、イタリア語、ポーランド語、ポルトガル語（ブラジル）、中国語（簡体字・繁体字）、日本語、韓国語で利用できます。すべての言語を無料で使えます。

言語を変更するには：

1. 左側のパネルの下部にある[[ui:sidebar.settings]]をクリックします。
2. [[ui:settings.language.label]]で言語を選びます。

変更はすぐに反映され、このガイドとmacOSのメニューにも適用されます。[[ui:settings.language.systemUnknown]]（デフォルト）を選ぶと、オペレーティングシステムの言語に従います。その言語が一覧にない場合、インターフェイスは英語で表示されます。

## 組織が設定した項目 {#managed-settings}

企業では、管理者がポリシーを使って一部の設定をすべてのユーザーに対して指定できます。これらの設定はアプリ内では変更できません。ポリシーでできること：

- 表示言語を指定する。この場合、[[ui:sidebar.settings]]の言語一覧は使用できなくなり、言語が組織によって設定されていることが表示されます。
- アップデートの確認をオフにする、または社内ミラーに向ける。

ポリシーが適用されているかどうかは、[[ui:sidebar.about]]を開いて[[ui:about.policy]]を確認します。ポリシーにエラーがある場合は、そこに表示されます。ポリシーについての質問は管理者にお問い合わせください。アップデートについては[アップデートと診断](updates.md)を参照してください。
