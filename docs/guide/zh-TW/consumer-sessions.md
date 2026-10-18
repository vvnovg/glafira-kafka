# 消費者工作階段

消費者工作階段會以消費者群組真正成員的身分讀取主題。消費者工作階段是 Pro 功能。

## 什麼是工作階段 {#what-is}

[瀏覽](browse.md)會在不加入群組的情況下讀取訊息，不會變更叢集中的任何內容。工作階段則不同：它會以指定的 `group.id` 加入群組，從 Kafka 取得分區，並且可以提交偏移量。你可以用它讀取群組會讀到的內容、透過提交移動群組的偏移量，或觀察群組如何重新平衡。

工作階段會影響叢集。它加入或離開時，Kafka 會重新分配群組的分區，其他成員都會經歷一次重新平衡。提交會變更群組的偏移量，而群組的應用程式會使用這些偏移量。

在 Free 中，[[ui:sidebar.consumer]]、[[ui:topics.details.consume]]和[[ui:groups.details.join]]會停用，並標示鎖頭和 `Pro`。如果在沒有有效 Pro 授權的情況下打開工作階段分頁，分頁會顯示訊息「[[ui:consumer.proOnly]]」，並提供連結：[[ui:common.openLicense]]。請參閱[啟用金鑰](license-updates.md#activate)。

[[ui:common.readOnly]]連線無法使用工作階段，分頁會顯示「[[ui:consumer.readOnlyBlocked]]」如果連線在工作階段執行期間變成唯讀，工作階段會離開群組。

## 啟動工作階段 {#start-session}

有三種方式可以打開工作階段：

- [[ui:sidebar.connections]]面板中連線下方的[[ui:sidebar.consumer]]：空白表單；
- 主題分頁上的[[ui:topics.details.consume]]：已填入主題；
- 群組分頁上的[[ui:groups.details.join]]：已填入群組及其有提交的主題。

每次都會打開新的分頁，因此可以同時執行多個工作階段。

在[[ui:consumer.form.groupTopics]]中：

- `group.id`：要加入的群組。如果沒有輸入，應用程式會建議一個新名稱，例如 `glafira-kafka-anna-1a2b3c`。新群組沒有任何提交。
- [[ui:consumer.form.topicsLabel]]：以逗號或空格分隔的主題名稱。也可以勾選[[ui:consumer.form.useRegex]]，並輸入[[ui:consumer.form.patternLabel]]，例如 `orders\..*`。運算式會從名稱開頭比對：如果缺少 `^`，會自動加上。之後建立的符合主題會在幾分鐘內（中繼資料重新整理時）加入，這會引發重新平衡。
- `auto.offset.reset`：在群組沒有已提交偏移量的分區中，從哪裡開始讀取：[[ui:consumer.form.resetLatest]]（預設）或[[ui:consumer.form.resetEarliest]]。在群組有已提交偏移量的分區中，會從該偏移量繼續讀取。
- [[ui:consumer.form.autoCommit]]：自動提交已讀取的偏移量。預設為關閉。請參閱[提交](consumer-sessions.md#commits)。
- `isolation.level`：`read_committed`（預設）只顯示已提交交易的訊息。`read_uncommitted` 也會顯示進行中和已中止交易的訊息。

在[[ui:consumer.form.formatLegend]]中選擇[[ui:consumer.form.keyFormat]]和[[ui:consumer.form.valueFormat]]，方式與瀏覽時相同。請參閱[格式](browse.md#formats)。[[ui:consumer.form.buffer]]：設定表格最多保留多少則訊息，範圍為 1 到 100000，預設為 5000。

篩選條件的用法與瀏覽時相同。請參閱[篩選條件](browse.md#filters)。

按一下[[ui:consumer.actions.connect]]。在正式環境連線上，應用程式會要求你輸入連線名稱，並按[[ui:consumer.dialog.confirm]]確認。工作階段執行期間無法變更表單。

## 讀取 {#reading}

按鈕旁的狀態列會先顯示[[ui:consumer.status.connecting]]，然後顯示[[ui:consumer.status.inGroup]]。狀態列也會顯示已讀取多少訊息、有多少符合篩選條件，以及表格中有多少訊息。如果與 Broker 的連線中斷，工作階段會自行重新連線，並在狀態列中顯示。

訊息一到達就會顯示在表格中，工作階段會持續讀取新訊息，直到你停止為止。表格滿了之後，最舊的訊息會被移除。按一下某一列即可查看訊息詳細資訊，方式與瀏覽時相同。請參閱[訊息詳細資訊](browse.md#message-details)。勾選 `UTC` 可改以 UTC 顯示時間。

工作階段只會讀取 Kafka 分配給它的分區。如果群組還有其他成員，其他分區由它們讀取。如果群組的成員多於分區，工作階段可能完全分配不到分區。

[[ui:consumer.disconnect]]會離開群組，其分區會交給其他成員。關閉工作階段分頁也會離開群組。工作階段結束後，狀態列會顯示[[ui:consumer.status.disconnected]]。

如果主題不存在，工作階段會以錯誤停止，並顯示需要檢查的名稱。

## 提交 {#commits}

提交會為群組儲存偏移量。下一個讀取該分區的成員，或重新啟動後的同一個應用程式，都會從該偏移量開始。

開啟[[ui:consumer.form.autoCommit]]時，工作階段每隔幾秒就會在背景提交已讀取的偏移量，離開群組時也會再提交一次。被篩選條件隱藏的訊息也算已讀取。

未開啟自動提交時，工作階段不會自行提交任何內容。請使用以下按鈕：

- [[ui:consumer.actions.commitSelected]]：提交所選訊息之後的偏移量，只限該訊息所在的分區。群組會從下一則訊息繼續。如果所選訊息比目前的提交更舊，偏移量會往回移動。
- [[ui:consumer.actions.commitAll]]：提交每個已分配分區中的目前位置。這包括被篩選條件隱藏的訊息，以及已從表格中移除的訊息。

提交後，會有一行以 `topic:partition → offset` 格式顯示已提交的偏移量。在正式環境連線上，每次提交都會要求輸入連線名稱。

如果工作階段未及時回應，應用程式會警告無法得知提交是否已套用。請在群組分頁上檢查偏移量。請參閱[群組詳細資訊](groups.md#group-details)。

## 暫停分區 {#pause-partitions}

表格右側的面板會以 `topic:partition` 格式列出分配給工作階段的分區。按一下分區旁的[[ui:consumer.assignment.pause]]可停止讀取該分區，按[[ui:consumer.assignment.resume]]則可繼續。已暫停的分區會標示為[[ui:consumer.assignment.paused]]。

暫停不會離開群組，因此不會重新平衡。分區仍分配給工作階段，暫停期間其他成員不會讀取它。暫停不會變更叢集中的任何內容，也不需要確認。

重新平衡後，新分配的分區一開始不會暫停。工作階段離開群組時，所有暫停都會結束。

## 重新平衡日誌 {#rebalance-log}

分區清單下方的[[ui:consumer.assignment.logTitle]]會記錄每次分配變更，最新的在最前面。每個項目都有時間，並屬於以下三種之一：分配給工作階段的分區、從工作階段撤銷的分區，或重新平衡錯誤及其文字。

當成員加入或離開群組、訂閱的主題變更，或成員未及時回應時，就會發生重新平衡。若要觀察重新平衡，請在另一個分頁中以相同的 `group.id` 啟動第二個工作階段：分區會由兩個工作階段分攤，兩邊的日誌都會顯示這一點。

日誌會保留最近 200 個項目。在第一次重新平衡之前，日誌會顯示[[ui:consumer.assignment.logEmpty]]

## 限制 {#limits}

- 工作階段需要 Pro。如果工作階段執行期間 Pro 不再有效，工作階段會繼續讀取，但提交、暫停和繼續都會被拒絕，也無法啟動新的工作階段。
- [[ui:common.readOnly]]連線無法使用工作階段。
- 工作階段無法從選定的偏移量開始。若要設定起點，請先重設群組的偏移量：請參閱[重設偏移量](groups.md#reset-offsets)。重設需要群組沒有成員，因此重設前請先中斷此群組的工作階段。
- 表格最多保留 100000 則訊息。較舊的訊息會被移除，但仍算已讀取。
- 非常大的鍵和值只會顯示一部分，與瀏覽時相同。
- 除非你確實有此意圖，否則不要加入正在執行的應用程式的群組。工作階段會從應用程式取走分區，在工作階段持有這些分區期間，應用程式不會處理它們。工作階段的提交（包括自動提交）會移動應用程式的偏移量。
