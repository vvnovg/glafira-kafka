# クラスターとトピック

この章では、クラスターの概要、トピック一覧、トピックの作成と削除、トピック画面、トピックの設定について説明します。[[ui:sidebar.connections]]パネルの各接続の下には、[[ui:sidebar.cluster]]と[[ui:sidebar.topics]]へのリンクがあります。それぞれ別のタブで開きます。

## クラスターの概要 {#cluster-overview}

[[ui:cluster.overview.title]]には、読み込んだ時点のクラスターの状態が表示されます。もう一度読み込むには、[[ui:cluster.overview.refresh]]をクリックします。

- `Cluster ID`：クラスターの識別子。
- [[ui:cluster.overview.controller]]：クラスターがコントローラーとして報告するブローカーのID。KRaftクラスターでは、クライアントはコントローラーと直接通信しません。クラスターはここにアクティブなコントローラーではなくランダムなブローカーを報告し、それは更新のたびに変わることがあります。
- [[ui:cluster.overview.brokers]]：ブローカーの数。
- [[ui:cluster.overview.topicsInternal]]：すべてのトピックの数。かっこ内は内部トピックの数です。
- [[ui:cluster.overview.partitions]]：すべてのトピックのパーティションの数。
- `Under-replicated`：すべてのレプリカが同期している（ISRに含まれている）わけではないパーティション。通常は、ブローカーが停止しているか、処理が追いついていません。
- `Offline`：リーダーのいないパーティション。書き込みも読み取りもできません。

`Under-replicated`や`Offline`が0以外の場合は強調表示されます。

その下には、`ID`と[[ui:cluster.overview.address]]を含むブローカーのテーブルがあります。[[ui:cluster.overview.controller]]フィールドのブローカーには[[ui:cluster.overview.controllerBadge]]のマークが付きます。KRaftクラスターでは、これは同じランダムなブローカーであり、アクティブなコントローラーではありません。ブローカーをクリックすると、テーブルの下にその設定が表示されます。ブローカーの設定は閲覧のみ可能です。[[ui:common.config.searchParam]]で名前からパラメーターを探し、[[ui:common.config.onlyChanged]]でデフォルト値のパラメーターを非表示にできます。読み取り専用のパラメーターには鍵のアイコンが表示されます。

## トピック一覧 {#topic-list}

[[ui:sidebar.topics]]には、クラスターのトピックが一覧表示されます。行をクリックすると、トピックが別のタブで開きます。

列は、[[ui:topics.list.colName]]、[[ui:topics.list.colPartitions]]、`RF`（レプリケーションファクター）、[[ui:topics.list.colMessages]]、`cleanup.policy`、[[ui:topics.list.colUnderRepl]]（レプリカ不足のパーティションの数）です。メッセージ数は概算で、パーティションの末尾と先頭のオフセットの差です。コンパクションとトランザクションのマーカーにより、実際の数はこれより少なくなります。メッセージ数と`cleanup.policy`は、スクロールして表示した行についてだけ読み込まれます。

ツールバーの操作：

- [[ui:topics.list.search]]：名前の一部を入力してトピックを探します。大文字と小文字は区別されません。正規表現で検索するには、スラッシュで囲みます。例：`/^orders\..*/`。
- [[ui:topics.list.internal]]：`__consumer_offsets`などの内部トピックを表示します。内部トピックには「[[ui:topics.list.internalBadge]]」と表示されます。
- 自動更新の一覧：[[ui:topics.list.autoRefreshOff]]、[[ui:topics.list.every10]]、[[ui:topics.list.every30]]のいずれか。
- [[ui:topics.list.refresh]]：一覧とメッセージ数をすぐに再読み込みします。

ツールバーの横には、全体のうち検索に一致したトピックの数が表示されます。[[ui:common.readOnly]]の接続では、[[ui:topics.list.create]]と[[ui:topics.list.select]]のボタンは表示されません。

## トピックを作成する {#create-topic}

1. トピック一覧で[[ui:topics.list.create]]をクリックします。一覧の上に[[ui:topics.create.title]]のフォームが開きます。
2. [[ui:topics.create.name]]を入力します。使える文字は半角英字、数字、`.`、`_`、`-`で、最大249文字です。名前に`.`と`_`の両方が含まれている場合は警告が表示されます。そのようなトピックのKafkaメトリクス名が、ほかのトピックと衝突する可能性があるためです。
3. [[ui:topics.create.partitions]]の数を指定します。パーティションは並行して読み取られ、順序が保証されるのは1つのパーティション内だけです。数はあとから増やせますが、減らすことはできません。
4. [[ui:topics.create.rf]]を指定します。これは、ブローカーが各パーティションのコピーをいくつ保持するかです。ブローカー数を超えることはできず、ブローカー数はフィールドの下に表示されます。デフォルトは3で、ブローカーがそれより少ない場合はブローカー数です。`-1`を指定するとブローカーのデフォルトが使われます。
5. [[ui:topics.create.preset]]を選びます：
   - [[ui:topics.create.presetDelete]]：古いメッセージを時間で削除します。
   - `compact`：キーごとに最新の値だけを保持します。
   - `compact + delete`：両方。
   - [[ui:topics.create.presetTest]]：メッセージを1時間保持します。
6. 必要に応じて、「[[ui:topics.create.extraConfigs]]」に1行に1つずつ入力します。例：`retention.ms=86400000`。これらはプリセットより優先されます。`#`で始まる行は無視されます。
7. [[ui:topics.create.submit]]をクリックします。

フォームの内容が有効な場合、同等の`kafka-topics.sh`コマンドが表示されます。コピーして、たとえば別のクラスターに同じトピックを作成するときに使えます。

本番接続では、接続名を入力して作成を確定します。トピックが作成されると、そのタブが開きます。

## トピックを削除する {#delete-topics}

1つのトピックを削除するには、トピックを開いて[[ui:topics.actions.deleteTopic]]をクリックします。確認のためにトピック名を入力し、[[ui:topics.delete.confirm]]をクリックします。トピックのタブが閉じます。

複数のトピックをまとめて削除するには：

1. トピック一覧で[[ui:topics.list.select]]をクリックします。名前の横にチェックボックスが表示されます。
2. トピックを選択します。内部トピックは選択できません。
3. 削除ボタンをクリックします。ボタンには選択したトピックの数が表示されます。
4. 名前の一覧を確認し、確認のためにその数を入力して、[[ui:topics.delete.confirm]]をクリックします。

[[ui:topics.list.cancelSelect]]をクリックすると、チェックボックスが非表示になります。

本番接続では、どちらの削除方法でも接続名の入力も求められます。

削除は元に戻せず、データは失われます。権限がないなどの理由で削除されなかったトピックがある場合は、理由とともに一覧表示されます。ほかのトピックは削除されます。

## トピック画面 {#topic-details}

トピックのタブには、パーティション数、レプリケーションファクター、メッセージ数の概算が表示されます。上部のボタン：

- [[ui:topics.details.refresh]]：トピックのデータを再読み込みします。
- [[ui:topics.details.produce]]：このトピックで[[ui:sidebar.producer]]を開きます。[Producer](producer.md)を参照してください。
- [[ui:topics.details.consume]]：このトピックでコンシューマーセッションを開始します。Proが必要です。[コンシューマーセッション](consumer-sessions.md)を参照してください。

[[ui:common.readOnly]]の接続では、どちらのボタンも無効になります。

2行目には、トピックを変更する操作があります。読み取り専用の接続と内部トピックでは、この行は表示されません。

- [[ui:topics.actions.addPartitions]]：新しい合計数を入力し、[[ui:topics.actions.addSubmit]]をクリックします。パーティション数は減らせません。増やしたあとは、同じキーのメッセージが別のパーティションに送られることがあるため、キーごとの順序が崩れます。
- [[ui:topics.actions.purge]]：選択したパーティションの現在の末尾までのメッセージをすべて削除します。トピック自体は残ります。パーティションを選択し、トピック名を入力して、[[ui:topics.actions.purgeConfirm]]をクリックします。パージは`cleanup.policy`に`delete`が含まれる場合にだけ機能します。それ以外の場合はボタンが無効になり、理由が表示されます。`compact,delete`のトピックでは、パージによってキーの最新の値も削除されます。
- [[ui:topics.actions.deleteTopic]]：[トピックを削除する](topics.md#delete-topics)を参照してください。

本番接続では、これらの操作のたびに接続名の入力も求められます。

下部のタブ：

- [[ui:topics.details.tabMessages]]：メッセージを閲覧します。[メッセージの閲覧](browse.md)を参照してください。
- [[ui:topics.details.tabPartitions]]：各パーティションのテーブル。[[ui:topics.details.colLeader]]、[[ui:topics.details.colReplicas]]、`ISR`（同期レプリカ）、先頭（`Low`）と末尾（`High`）のオフセット、メッセージ数が表示されます。リーダーのいないパーティションには[[ui:topics.details.noLeader]]と表示されます。
- [[ui:topics.details.tabConfigs]]：トピックのパラメーター。[トピックの設定](topics.md#topic-config)を参照してください。
- [[ui:topics.details.tabGroups]]：このトピックのオフセットをコミットしたコンシューマーグループ。[[ui:topics.groups.colState]]、[[ui:topics.groups.colPartitions]]、[[ui:topics.groups.colLag]]が表示されます。グループをクリックすると開きます。[コンシューマーグループ](groups.md)を参照してください。

## トピックの設定 {#topic-config}

[[ui:topics.details.tabConfigs]]タブには、トピックのすべてのパラメーターが3つの列で一覧表示されます：

- [[ui:common.config.colParam]]：パラメーター名。太字の名前は、トピックまたはブローカーの設定で明示的に設定されています。
- [[ui:common.config.colValue]]：現在の値。機密性の高い値は`••••••`として隠されます。
- [[ui:common.config.colSource]]：値の出どころ。たとえば、`DynamicTopic`はこのトピックで設定された値、`StaticBroker`と`DynamicBroker`はブローカーの設定による値、`Default`はKafkaのデフォルトです。

[[ui:common.config.searchParam]]で名前からパラメーターを探し、[[ui:common.config.onlyChanged]]で太字のパラメーターだけを表示できます。読み取り専用のパラメーターには鍵のアイコンが表示されます。名前の横の`?`アイコンには、パラメーターの説明が表示されます。

## 設定を変更する {#edit-config}

1. [[ui:topics.details.tabConfigs]]タブで[[ui:topics.details.edit]]をクリックします。[[ui:common.readOnly]]の接続では、このボタンは表示されません。
2. [[ui:common.config.colValue]]列の値を変更します。読み取り専用のパラメーターと機密性の高いパラメーターは編集できません。トピック独自の値を削除するには、[[ui:common.config.reset]]をクリックします。その場合はブローカーの値が適用されます。空の値は使えません。
3. [[ui:common.config.review]]をクリックします。各変更が「古い値 → 新しい値」の形式で表示されます。[[ui:common.config.defaultValue]]は、値がリセットされることを意味します。
4. [[ui:common.config.apply]]をクリックします。編集を続けるには[[ui:common.config.back]]をクリックします。

適用するまで、クラスターは何も変更されません。[[ui:common.config.discard]]をクリックすると、すべての編集が取り消されます。未保存の編集がある間は、[[ui:topics.details.closeEditor]]は無効です。本番接続では、接続名を入力して変更を確定します。

送信されるのは変更したパラメーターだけです。トピックのほかのパラメーターはそのまま残ります。ブローカーが値を確認し、無効な値はエラーで拒否されます。

## トピックのパラメーター {#config-parameters}

Kafkaのトピックには数十のパラメーターがあります。設定テーブルの`?`アイコンで、それぞれの説明を確認できます。よく必要になるのは次のパラメーターです：

- `cleanup.policy`：古いデータの扱い。`delete`は時間またはサイズで古いセグメントを削除します。`compact`はキーごとに最新の値だけを保持します。`compact,delete`は両方を行います。
- `retention.ms`：メッセージを保持する期間（ミリ秒）。デフォルトは`604800000`（7日）で、`-1`を指定すると無期限に保持します。`cleanup.policy`に`delete`が含まれる場合に機能します。
- `retention.bytes`：古いセグメントが削除されるまでの、各パーティションの最大サイズ。デフォルトの`-1`はサイズの上限なしを意味します。
- `segment.ms`と`segment.bytes`：ブローカーが新しいログセグメントを始めるタイミング。保持とコンパクションはセグメント単位で行われるため、メッセージはそのセグメントが閉じられるまで`retention.ms`より長く残ることがあります。
- `min.insync.replicas`：`acks=all`の書き込みを確認する必要があるレプリカの数。レプリケーションファクターが3の場合、通常は2です。
- `max.message.bytes`：ブローカーがこのトピックで受け付ける最大のバッチ。デフォルトは約1 MBです。プロデューサーも同じサイズを許可する必要があります（`max.request.size`）。
- `compression.type`：ブローカーがメッセージを保存する方法。`producer`は、プロデューサーの圧縮をそのまま保持します。
- `message.timestamp.type`：`CreateTime`（プロデューサーが設定した時刻）または`LogAppendTime`（ブローカーがメッセージを書き込んだ時刻）。メッセージの閲覧時に表示されるのはこの時刻です。
- `delete.retention.ms`：削除マーカー（値がnullのメッセージ）がコンパクションされたトピックに残る期間。デフォルトは1日です。

一部のパラメーターは新しいバージョンのKafkaにしかなく、一部は非推奨です。重要な場合は、`?`アイコンにその旨が表示されます。`remote.storage.enable`など階層型ストレージのパラメーターは、クラスターが階層型ストレージをサポートしている場合にだけ機能します。
