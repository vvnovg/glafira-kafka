# メッセージの閲覧

メッセージは、トピックの[[ui:topics.details.tabMessages]]タブで閲覧します。閲覧ではコンシューマーグループに参加せず、何もコミットしないため、クラスターは変更されません。[[ui:common.readOnly]]の接続でも使えます。グループのメンバーとして読み取るには、[コンシューマーセッション](consumer-sessions.md)を使います。

## 読み取りを開始する {#start-reading}

1. [[ui:sidebar.topics]]の一覧からトピックを開きます。最初に[[ui:topics.details.tabMessages]]タブが開きます。
2. [[ui:browse.form.from]]で読み取りの開始位置を選びます。[開始位置](browse.md#start-position)を参照してください。
3. 必要に応じて、形式とフィルターを設定します。
4. [[ui:browse.panel.read]]をクリックします。

メッセージは届いた順にテーブルに表示されます。ボタンの横の行には、スキャンしたメッセージ数、フィルターに一致した数、テーブルにある数が表示されます。状態も表示されます：「[[ui:browse.panel.reading]]」、[[ui:browse.panel.finishLimit]]、[[ui:browse.panel.finishEnd]]、[[ui:browse.panel.finishCancelled]]。

`Live tail`がオフの場合、読み取りは[[ui:browse.panel.read]]をクリックした時点のトピックの末尾で止まります。[[ui:browse.form.limit]]に達した場合はそれより前に止まります。上限はフィルターに一致したメッセージを数えます。1から100000まで指定でき、大きな値にすると読み取りが遅くなります。

`Live tail`をオンにすると、読み取りは末尾で止まらず、新しいメッセージが書き込まれるたびに追加されます。古いメッセージをすべて読み終えると、状態は「[[ui:browse.panel.waiting]]」になります。テーブルには上限までの最新のメッセージが残り、最も古いものから削除されます。

[[ui:browse.panel.stop]]は読み取りを止め、テーブルはそのまま残します。読み取り中は、[[ui:browse.panel.read]]ボタンが[[ui:browse.panel.restart]]に変わります。このボタンは現在の設定で読み取りをやり直し、テーブルをクリアします。ブローカーへの接続が切れた場合、アプリは自動的に再接続し、そのことを状態の行に表示します。

テーブルでは、異なるパーティションのメッセージが混在します。Kafkaが順序を保証するのは1つのパーティション内だけです。`P`列はパーティションを、`Offset`列はパーティション内の位置を示します。時刻をローカル時刻ではなくUTCで表示するには、`UTC`を選択します。

## 開始位置 {#start-position}

[[ui:browse.form.startPosition]]は、各パーティションのどこから読み取りを始めるかを指定します：

- [[ui:browse.form.startLatest]]：最新のN件のメッセージ。Nはパーティション間で均等に分けられます。たとえば100件で4つのパーティションの場合、各パーティションから最大25件を取得します。
- [[ui:browse.form.startEarliest]]：まだ保存されている最も古いメッセージから。
- [[ui:browse.form.startOffset]]：各パーティションの同じオフセットから。パーティションにそのオフセットがない場合は、パーティションの先頭または末尾から読み取ります。このモードはパーティションが1つの場合に最も役立ちます。
- [[ui:browse.form.startTimestamp]]：指定したローカル時刻以降のタイムスタンプを持つ最初のメッセージから。それより新しいメッセージがないパーティションでは、末尾から読み取ります。
- [[ui:browse.form.startLastMinutes]]：同様に、現在から遡って数えます。

[[ui:browse.form.partitionsPlaceholder]]と表示されたフィールドでは、読み取るパーティションを限定できます。パーティション番号をコンマ区切りで入力します。例：`0,2`。すべてのパーティションを読み取るには空のままにします。

## フィルター {#filters}

フィルターを使うと、一致したメッセージだけが表示されます。アプリは開始位置からすべてのメッセージを読み取り、1件ずつ確認するため、フィルターで読み取りが速くなることはありません。入力したフィールドすべてに一致する必要があります。

- [[ui:browse.filters.keyContains]]と[[ui:browse.filters.valueContains]]：キーまたは値に含まれるテキスト。
- `regex`：これらのフィールドとヘッダーの値を正規表現として扱います。
- `Aa`：大文字と小文字を区別します。デフォルトでは区別しません。
- [[ui:browse.filters.header]]：メッセージにこの名前と完全に一致するヘッダーがあること。[[ui:browse.filters.headerValue]]を入力すると、その値も確認します。
- `JSONPath`：JSONの値に対する条件。例：`$.order.status == "FAILED"`。`==`や`!=`がない場合は、パスが存在するだけで一致します。例：`$.error`。値が有効なJSONでないメッセージは一致しません。
- [[ui:browse.filters.timeFrom]]と[[ui:browse.filters.timeTo]]：メッセージのタイムスタンプがこの範囲（ローカル時刻）にあること。両端を含みます。

フィルターは、デコード後にテーブルに表示されるテキストを確認します。値の形式が`hex`の場合、値のフィルターは16進数のテキストを検索します。フィルターは[[ui:browse.panel.read]]をクリックしたときに適用されます。変更したら、もう一度読み取ってください。

フィルターは開始位置を変えません。たとえば、[[ui:browse.form.startLatest]]とフィルターを組み合わせると、最新のN件の中からだけ一致するものを探します。トピック全体を検索するには、[[ui:browse.form.startEarliest]]を選んで開始します。

## 形式 {#formats}

[[ui:browse.form.format]]は、キーと値のデコード方法を指定します。[[ui:browse.form.key]]と[[ui:browse.form.value]]で別々に設定できます。変わるのは表示だけで、データは変わりません。

- `auto`：デフォルト。有効なJSONオブジェクトと配列はJSONとして、その他のUTF-8テキストは文字列として、バイナリデータは16進数として表示されます。
- `string`：UTF-8のテキスト。有効なUTF-8でないバイトは`�`に置き換えられます。
- `json`：メッセージの詳細で、インデント付きのJSONとして表示されます。値が有効なJSONでない場合は、詳細にエラーが表示されます。
- `hex`：各バイトを2桁の16進数で表示します。
- `base64`：バイトをBase64で表示します。

詳細には、実際に使われた形式が表示されます。たとえば、`auto`がJSONを検出した場合は`json`です。ヘッダーの値はテキストとして表示され、バイナリのヘッダー値は`0x`に続く16進数で表示されます。キーや値がない場合は`null`と表示されます。

非常に大きなキーと値は、テキストの先頭約1 MBだけが表示されます。これは`hex`と`base64`にも当てはまります。その場合、メッセージの詳細に警告が表示されます。

## メッセージの詳細 {#message-details}

行をクリックすると、テーブルの横にメッセージの詳細が開きます。上部にはパーティション、オフセット、時刻が表示されます。その下には：

- [[ui:browse.details.value]]と[[ui:browse.details.key]]：全文と、形式およびバイト単位のサイズ。JSONはインデント付きで表示されます。[[ui:browse.details.copy]]でテキストをコピーできます。
- [[ui:browse.table.colHeaders]]：ヘッダー名と値のテーブル。

テーブルの列は、`P`、`Offset`、[[ui:browse.table.colTime]]、[[ui:browse.table.colKey]]、[[ui:browse.table.colValue]]、[[ui:browse.table.colHeaders]]（ヘッダーの数）、[[ui:browse.table.colSize]]（キーと値のバイト単位のサイズ）です。時刻はメッセージのタイムスタンプです。トピックの設定`message.timestamp.type`に応じて、プロデューサーまたはブローカーが設定します。

## メッセージをコピーする {#export-messages}

Glafiraは閲覧したメッセージをファイルに保存しません。メッセージをアプリの外に取り出すには、コピーします：

- [[ui:browse.details.copyJson]]は、メッセージ全体をJSONとしてコピーします。トピック、パーティション、オフセット、タイムスタンプ、キー、値、ヘッダー、サイズが含まれます。キーと値はデコードされた状態で、その形式とともに含まれます。
- [[ui:browse.details.value]]または[[ui:browse.details.key]]の横の[[ui:browse.details.copy]]は、そのテキストだけをコピーします。

コピーされるテキストには、読み取り時に選んだ形式が使われます。バイナリデータを正確にコピーするには、`hex`または`base64`で読み取ってください。非常に大きなキーと値は、`hex`と`base64`でも、表示されている部分だけがコピーされます。[形式](browse.md#formats)を参照してください。

## Producerで開く {#open-in-producer}

[[ui:browse.details.resend]]は、同じ接続の[[ui:sidebar.producer]]でメッセージを開きます。フォームには、メッセージのトピック、キー、値、ヘッダー、形式が入力されます。パーティションはKafkaに任されます。メッセージのテキスト内のプレースホルダーは置き換えられず、読み取ったとおりのコピーが送信されます。

すぐには何も送信されません。フォームを確認し、必要なら変更して、Producerから送信してください。[Producer](producer.md)を参照してください。

ボタンは次の2つの場合に無効になり、理由が表示されます：

- キーまたは値が大きすぎて、一部しか表示されていない場合。
- テキストが有効なUTF-8でないため、デコード時に情報が失われた場合。正確なコピーを送信するには、`hex`または`base64`で読み取ってください。

[[ui:common.readOnly]]の接続では、このボタンは表示されません。

## グループをメッセージの位置にリセットする {#reset-group-here}

「[[ui:browse.details.resetHere]]」は、コンシューマーグループがこのメッセージから読み取りを始めるように、オフセットのリセットを準備します。

1. メッセージの詳細で「[[ui:browse.details.resetHere]]」をクリックします。
2. 一覧からグループを選びます。一覧には既存のグループだけが表示されます。
3. [[ui:browse.details.goToReset]]をクリックします。

グループのタブが開き、リセットのフォームに値が入力されています。[[ui:groups.reset.strategy]]は[[ui:groups.reset.strategyOffset]]で、メッセージのオフセットが指定されます。[[ui:groups.reset.scope]]は[[ui:groups.reset.scopePartitions]]で、メッセージのトピックとパーティションが指定されます。この時点ではまだ何も変更されません。[[ui:groups.reset.preview]]をクリックして新しいオフセットを確認し、そこでリセットを適用します。グループにアクティブなメンバーがいない必要があります。[コンシューマーグループ](groups.md)を参照してください。

[[ui:common.readOnly]]の接続では、このボタンは表示されません。
