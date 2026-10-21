# 消費者群組

消費者群組是一組共同分擔主題讀取工作的消費者。Kafka 會將每個分區分配給群組中的一個成員，並為每個分區儲存群組已提交的偏移量。應用程式重新啟動後，會從該偏移量繼續讀取。

請在[[ui:sidebar.connections]]面板中的連線下方開啟[[ui:sidebar.groups]]。單一主題的群組也會顯示在該主題的[[ui:topics.details.tabGroups]]分頁上。請參閱[主題畫面](topics.md#topic-details)。

## 群組清單 {#group-list}

清單會顯示叢集中的所有消費者群組。欄位如下：

- [[ui:groups.list.colGroup]]：群組 ID（`group.id`）。
- [[ui:groups.list.colState]]：Kafka 回報的狀態，例如 `Stable`（成員正在讀取）、`Empty`（沒有成員）、`PreparingRebalance` 或 `CompletingRebalance`（正在重新分配分區），以及 `Dead`（群組已不存在）。
- [[ui:groups.list.colMembers]]：作用中的成員數。
- `Assignor`：分配分區所用的策略，例如 `range`。
- [[ui:groups.list.colLag]]：群組的總延遲。請參閱[延遲](groups.md#lag)。
- [[ui:groups.list.colTopics]]：群組已提交偏移量的主題。

在[[ui:groups.list.search]]中輸入文字，即可顯示 ID 中包含該文字的群組，不區分大小寫。旁邊的狀態清單可只顯示處於某一狀態的群組；選擇[[ui:groups.list.allStates]]可移除此篩選條件。計數器會顯示目前顯示的群組數和總數。

成員、Assignor 和延遲只會在捲動時針對看得到的列載入。資料到達之前，儲存格會顯示 `…`。如果無法載入，儲存格會顯示 `—`；將指標停在上面即可查看原因。

[[ui:groups.list.refresh]]會重新載入清單及其中的資料。如果有些 Broker 沒有回應，會出現警告，說明清單可能不完整。

## 延遲 {#lag}

延遲是群組尚未讀取的訊息數。對每個分區而言，延遲等於日誌結尾減去已提交的偏移量。延遲絕不會顯示為小於零。

延遲只取決於已提交的偏移量。作用中的消費者讀取的位置可能比它提交的更遠，因此實際的延遲可能較小。群組再次提交後，延遲就會下降。

沒有已提交偏移量的分區會顯示[[ui:groups.lag.noCommit]]，且不計入總數。群組完全沒有提交（「[[ui:groups.lag.noCommits]]」），或無法讀取某個分區的偏移量（[[ui:groups.lag.unknown]]）時，群組的總延遲會顯示 `—`。

延遲計算的是偏移量，而不是訊息。在使用 Compaction 或交易的主題中，有些偏移量沒有可見的訊息，因此群組剩下要讀取的訊息可能比延遲顯示的少。

這些值是載入當下的快照。若要持續追蹤延遲，請開啟群組並開啟自動重新整理。

## 群組詳細資訊 {#group-details}

在清單中按一下群組即可開啟其分頁。上方的摘要會顯示狀態、成員數和 Assignor。

- [[ui:groups.details.autoRefresh]]：[[ui:groups.details.refreshOff]]、[[ui:groups.details.refresh5]]或[[ui:groups.details.refresh10]]。
- [[ui:groups.details.refresh]]：立即重新載入。
- [[ui:groups.details.join]]：在這個群組中啟動[消費者工作階段](consumer-sessions.md)。工作階段表單會填入群組 ID 和群組有提交的主題。在[[ui:common.readOnly]]連線上按鈕會停用；將指標停在按鈕上即可查看原因。

[[ui:groups.details.partitions]]會列出群組有提交的分區，以及分配給其成員的分區。群組的總延遲顯示在表格上方。欄位包括[[ui:groups.details.colTopic]]、[[ui:groups.details.colPartition]]、`Committed`（已提交的偏移量）、`Log end`（分區的結尾偏移量）、[[ui:groups.details.colLag]]和[[ui:groups.details.colMember]]（讀取該分區的成員的用戶端 ID）。

[[ui:groups.details.members]]會列出作用中的成員：`Client ID`、[[ui:groups.details.colHost]]、`Member ID` 和[[ui:groups.details.colAssignment]]（成員的分區，格式為 `topic:partition`）。沒有成員的群組會顯示[[ui:groups.details.noMembers]]

第二列有[[ui:groups.details.resetOffsets]]和[[ui:groups.details.deleteGroup]]。在唯讀連線上會隱藏。

## 重設偏移量 {#reset-offsets}

重設偏移量會移動群組已提交的偏移量，之後群組會重新讀取或略過訊息。只有沒有作用中成員的群組（狀態為 `Empty` 或 `Dead`）才能重設偏移量，否則成員會以自己的提交覆寫新的偏移量。請先停止該群組的應用程式。

1. 在群組分頁上按一下[[ui:groups.details.resetOffsets]]。群組有成員時，按鈕會停用；將指標停在按鈕上即可查看原因。
2. 選擇[[ui:groups.reset.strategy]]。請參閱[重設策略](groups.md#reset-strategies)。
3. 選擇[[ui:groups.reset.scope]]。請參閱[重設範圍](groups.md#reset-scope)。
4. 按一下[[ui:groups.reset.preview]]。
5. 檢查表格後按一下套用按鈕。按鈕上會顯示將變更的分區數。

預覽不會變更任何內容。它會為每個分區顯示主題、分區、[[ui:groups.reset.colCurrent]]（已提交的偏移量）、[[ui:groups.reset.colNew]]（目標偏移量）以及 `Low / High`（分區的第一個和結尾偏移量）。無法計算目標的分區會標示為[[ui:groups.reset.noTarget]]，並附上原因。這類分區會被略過。

只會套用預覽中顯示的計畫。表單中的任何變更都會隱藏預覽，你需要再按一次[[ui:groups.reset.preview]]。在正式環境連線上，套用時會要求輸入連線名稱；在其他連線上則會立即套用。

套用之前，應用程式會再次檢查群組沒有成員。套用後，[[ui:groups.reset.result]]會列出每個分區及其新的偏移量或錯誤。

也可以從訊息準備重設：請參閱[將群組重設到某則訊息](browse.md#reset-group-here)。[[ui:common.close]]會關閉重設面板。

## 重設策略 {#reset-strategies}

[[ui:groups.reset.strategy]]會為範圍內的每個分區設定目標偏移量：

- [[ui:groups.reset.strategyEarliest]]：仍保存的第一個偏移量。群組會重新讀取主題保留的所有內容。
- [[ui:groups.reset.strategyLatest]]：分區的結尾。群組會略過累積的所有內容，只讀取新訊息。
- [[ui:groups.reset.strategyTimestamp]]：在「[[ui:groups.reset.timestamp]]」當下或之後寫入的第一則訊息。在沒有較新訊息的分區中，目標為結尾。
- [[ui:groups.reset.strategyOffset]]：在每個分區中使用指定的[[ui:groups.reset.offset]]，必須是大於或等於 0 的整數。如果某個分區沒有這個偏移量，目標為它的第一個或結尾偏移量。
- [[ui:groups.reset.strategyShift]]：將已提交的偏移量移動「[[ui:groups.reset.shift]]」指定的數量。負數會往回重新讀取 N 則訊息，正數會略過 N 則訊息。結果會保持在第一個和結尾偏移量之間。沒有已提交偏移量的分區無法位移，會被略過。

時間為你的本機時間，包含秒。Kafka 會依訊息時間戳記尋找偏移量。

## 重設範圍 {#reset-scope}

[[ui:groups.reset.scope]]用於設定要重設哪些分區。其他分區會保留其偏移量。

- [[ui:groups.reset.scopeAll]]：群組已提交偏移量的所有分區。如果群組還沒有任何提交，請改為選擇主題。
- [[ui:groups.reset.scopeTopics]]：所列主題的所有分區，包括沒有提交的分區。名稱之間以逗號、空格或換行分隔。可用來讓群組從選定的位置開始讀取新主題。
- [[ui:groups.reset.scopePartitions]]：只重設所列的分區。請以 `topic:partition` 格式輸入分區，以逗號或空格分隔，例如 `orders:0, orders:3`。輸入欄位為[[ui:groups.reset.partitionsLabel]]。

## 刪除群組 {#delete-group}

刪除群組會移除其所有已提交的偏移量。只有沒有成員、處於 `Empty` 狀態的群組才能刪除。

1. 在群組分頁上按一下[[ui:groups.details.deleteGroup]]。群組有成員時，按鈕會停用；將指標停在按鈕上即可查看原因。
2. 輸入群組 ID 確認。在正式環境連線上，還要輸入連線名稱。
3. 按一下[[ui:common.delete]]。

刪除後，群組分頁會關閉。如果使用此 `group.id` 的應用程式再次啟動，Kafka 會重新建立群組。由於沒有已提交的偏移量，應用程式會依其 `auto.offset.reset` 設定開始讀取：從頭開始或從結尾開始。

在[[ui:common.readOnly]]連線上，這個按鈕會隱藏。
