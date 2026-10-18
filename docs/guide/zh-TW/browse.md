# 瀏覽訊息

訊息是在主題的[[ui:topics.details.tabMessages]]分頁上瀏覽。瀏覽不會加入消費者群組，也不會提交任何內容，因此叢集不會有任何變更。在[[ui:common.readOnly]]連線上也可以瀏覽。若要以群組成員身分讀取，請使用[消費者工作階段](consumer-sessions.md)。

## 開始讀取 {#start-reading}

1. 從[[ui:sidebar.topics]]清單中開啟一個主題。首先開啟的是[[ui:topics.details.tabMessages]]分頁。
2. 在[[ui:browse.form.from]]中選擇從哪裡開始。請參閱[起始位置](browse.md#start-position)。
3. 視需要設定格式和篩選條件。
4. 按一下[[ui:browse.panel.read]]。

訊息一到達就會顯示在表格中。按鈕旁的狀態列會顯示已掃描多少訊息、有多少符合篩選條件，以及表格中有多少訊息，也會顯示狀態：[[ui:browse.panel.reading]]、[[ui:browse.panel.finishLimit]]、[[ui:browse.panel.finishEnd]]或[[ui:browse.panel.finishCancelled]]。

未開啟 `Live tail` 時，讀取會在按下[[ui:browse.panel.read]]那一刻的主題結尾停止；如果先達到[[ui:browse.form.limit]]，則會提早停止。上限計算的是符合篩選條件的訊息，範圍為 1 到 100000；值越大，讀取越慢。

開啟 `Live tail` 時，讀取到結尾也不會停止：新訊息一寫入就會加入表格。所有舊訊息讀完後，狀態會顯示[[ui:browse.panel.waiting]]。表格最多保留上限數量的最新訊息，並捨棄最舊的訊息。

[[ui:browse.panel.stop]]會停止讀取並保留表格。讀取進行時，[[ui:browse.panel.read]]按鈕會變成[[ui:browse.panel.restart]]：它會以目前的設定重新開始，並清除表格。如果與 Broker 的連線中斷，應用程式會自行重新連線，並在狀態列中顯示。

不同分區的訊息會混合顯示在表格中。Kafka 只在單一分區內保證順序。`P` 欄顯示分區，`Offset` 顯示在分區中的位置。勾選 `UTC` 可改以 UTC 而非本機時間顯示時間。

## 起始位置 {#start-position}

[[ui:browse.form.startPosition]]用於設定在每個分區中從哪裡開始讀取：

- [[ui:browse.form.startLatest]]：最後 N 則訊息。N 會平均分配給各分區，因此在 100 則訊息、4 個分區的情況下，每個分區最多讀取 25 則。
- [[ui:browse.form.startEarliest]]：從仍保存的最舊訊息開始。
- [[ui:browse.form.startOffset]]：在每個分區中從相同的偏移量開始。如果某個分區沒有這個偏移量，則從該分區的開頭或結尾開始讀取。這個模式最適合只有一個分區的情況。
- [[ui:browse.form.startTimestamp]]：從時間戳記等於或晚於指定本機時間的第一則訊息開始。在沒有較新訊息的分區中，從結尾開始讀取。
- [[ui:browse.form.startLastMinutes]]：同上，但從現在往回計算。

顯示[[ui:browse.form.partitionsPlaceholder]]的欄位可將讀取範圍限制在部分分區。請輸入以逗號分隔的分區編號，例如 `0,2`。留空則讀取所有分區。

## 篩選條件 {#filters}

篩選條件只會顯示符合的訊息。應用程式仍會從起始位置讀取所有訊息並逐一檢查，因此篩選條件不會加快讀取。所有已填寫的欄位都必須符合。

- [[ui:browse.filters.keyContains]]和[[ui:browse.filters.valueContains]]：鍵或值中的文字。
- `regex`：將這些欄位以及標頭值當作規則運算式。
- `Aa`：區分大小寫。預設不區分大小寫。
- [[ui:browse.filters.header]]：訊息含有名稱完全相同的標頭。[[ui:browse.filters.headerValue]]還會檢查它的值。
- `JSONPath`：針對 JSON 值的條件，例如 `$.order.status == "FAILED"`。如果沒有 `==` 或 `!=`，則路徑只要存在即可，例如 `$.error`。值不是有效 JSON 的訊息不符合條件。
- [[ui:browse.filters.timeFrom]]和[[ui:browse.filters.timeTo]]：訊息時間戳記在此範圍內，以本機時間計算，包含兩端。

篩選條件檢查的是表格中顯示的文字，也就是解碼後的文字。如果值格式為 `hex`，值篩選條件會搜尋十六進位文字。篩選條件在按下[[ui:browse.panel.read]]時套用；變更後請重新讀取。

篩選條件不會移動起始位置。例如，搭配篩選條件使用[[ui:browse.form.startLatest]]時，只會在最後 N 則訊息中尋找符合的訊息。若要搜尋整個主題，請選擇[[ui:browse.form.startEarliest]]。

## 格式 {#formats}

[[ui:browse.form.format]]用於設定鍵和值的解碼方式，[[ui:browse.form.key]]和[[ui:browse.form.value]]分別設定。只會改變顯示方式，不會改變資料。

- `auto`：預設值。有效的 JSON 物件和陣列以 JSON 顯示，其他 UTF-8 文字以字串顯示，二進位資料以十六進位顯示。
- `string`：UTF-8 文字。不是有效 UTF-8 的位元組會被替換為 `�`。
- `json`：在訊息詳細資訊中以縮排顯示 JSON。如果值不是有效的 JSON，詳細資訊會顯示錯誤。
- `hex`：每個位元組以兩個十六進位數字表示。
- `base64`：以 Base64 表示的位元組。

詳細資訊會顯示實際使用的格式，例如 `auto` 偵測到 JSON 時會顯示 `json`。標頭值以文字顯示；二進位的標頭值會以 `0x` 加上十六進位顯示。缺少的鍵或值會顯示為 `null`。

非常大的鍵和值只會顯示一部分，大約是文字的前 1 MB。這也適用於 `hex` 和 `base64`。此時訊息詳細資訊會顯示警告。

## 訊息詳細資訊 {#message-details}

按一下某一列，即可在表格旁開啟訊息詳細資訊。上方是分區、偏移量和時間。下方：

- [[ui:browse.details.value]]和[[ui:browse.details.key]]：完整文字，以及格式和以位元組計算的大小。JSON 會以縮排顯示。[[ui:browse.details.copy]]會複製文字。
- [[ui:browse.table.colHeaders]]：標頭名稱和值的表格。

表格欄位為 `P`、`Offset`、[[ui:browse.table.colTime]]、[[ui:browse.table.colKey]]、[[ui:browse.table.colValue]]、[[ui:browse.table.colHeaders]]（標頭數量）和[[ui:browse.table.colSize]]（鍵和值的大小，以位元組計算）。時間是訊息的時間戳記，依主題設定 `message.timestamp.type` 而定，由生產者或 Broker 設定。

## 複製訊息 {#export-messages}

Glafira 不會將瀏覽的訊息儲存成檔案。若要將訊息帶出應用程式，請複製它：

- [[ui:browse.details.copyJson]] 會將整則訊息複製為 JSON：主題、分區、偏移量、時間戳記、鍵、值、標頭和大小。鍵和值以解碼後的內容包含在內，並附上格式。
- [[ui:browse.details.value]]或[[ui:browse.details.key]]旁的[[ui:browse.details.copy]]只會複製該段文字。

複製的文字使用讀取時選擇的格式。若要精確複製二進位資料，請以 `hex` 或 `base64` 讀取。非常大的鍵和值只會依顯示的內容複製一部分，即使是 `hex` 和 `base64` 也一樣。請參閱[格式](browse.md#formats)。

## 在 Producer 中開啟 {#open-in-producer}

[[ui:browse.details.resend]]會在同一個連線的 [[ui:sidebar.producer]] 中開啟這則訊息。表單會填入訊息的主題、鍵、值、標頭和格式。分區交由 Kafka 決定。訊息文字中的預留位置不會被替換：副本會依讀取時的原樣傳送。

不會立即傳送任何內容。請檢查表單，視需要修改，然後在 Producer 中傳送。請參閱 [Producer](producer.md)。

在兩種情況下按鈕會停用，並顯示原因：

- 鍵或值太大，只顯示了一部分；
- 文字不是有效的 UTF-8，解碼時有遺失。請以 `hex` 或 `base64` 讀取，以傳送精確的副本。

在[[ui:common.readOnly]]連線上，這個按鈕會隱藏。

## 將群組重設到某則訊息 {#reset-group-here}

使用「[[ui:browse.details.resetHere]]」可以準備偏移量重設，讓消費者群組從這則訊息開始讀取。

1. 在訊息詳細資訊中按一下[[ui:browse.details.resetHere]]。
2. 在清單中選擇群組。清單只會列出已存在的群組。
3. 按一下[[ui:browse.details.goToReset]]。

群組分頁會開啟，並已填好重設表單。[[ui:groups.reset.strategy]]為[[ui:groups.reset.strategyOffset]]，並填入這則訊息的偏移量。[[ui:groups.reset.scope]]為[[ui:groups.reset.scopePartitions]]，並填入這則訊息的主題和分區。此時還不會有任何變更：按一下[[ui:groups.reset.preview]]查看新的偏移量，再在那裡套用重設。群組不能有作用中的成員。請參閱[消費者群組](groups.md)。

在[[ui:common.readOnly]]連線上，這個按鈕會隱藏。
