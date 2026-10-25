# LLM 助理（MCP）

Glafira 可以作為 LLM 助理的閘道：Claude Desktop、Claude Code、Cursor，或其他支援 MCP（Model Context Protocol）的用戶端。助理會讀取你為其開放的叢集的中繼資料（在你允許的連線中還會讀取訊息），協助你找出問題：複本不足的分區、消費延遲、持續發生的重新平衡、不理想的設定。助理無法透過 Glafira 變更叢集中的任何內容：變更由你自己在介面中進行。

## 運作方式 {#how-it-works}

用戶端會執行 `glafira-kafka mcp`，也就是應用程式的同一個程式檔案，只是不顯示視窗。這個指令會透過本機通訊端（在 Windows 上為具名管道）連線到正在執行的 Glafira，Glafira 再透過自己的連線回應助理的請求。密碼和憑證一律留在 Glafira 中，助理不會取得它們。

如果 Glafira 未執行，用戶端會自行啟動它，並最多等待 30 秒。

## 啟用 {#enable}

1. 開啟[[ui:sidebar.settings]]，進入「[[ui:settings.mcp.section]]」區段。
2. 勾選[[ui:settings.mcp.enable]]。狀態會變成[[ui:settings.mcp.state.running]]。
3. 在連線表單的「[[ui:connections.form.llmAccess]]」中選擇層級並儲存：「[[ui:connections.form.llmLevel.metadata]]」或「[[ui:connections.form.llmLevel.messages]]」（請參閱[讀取訊息](llm-mcp.md#messages)）。層級為「[[ui:connections.form.llmLevel.off]]」的連線，助理看不到。在連線清單中，已開啟的連線會標示 `LLM`；可讀取訊息的連線，標示為實心。

如果管理員停用了 MCP，[[ui:settings.mcp.enable]]核取方塊會無法使用，旁邊會顯示「[[ui:settings.mcp.policy]]」。

存取層級和遮罩清單不會隨連線的匯出和匯入一起轉移：在另一台電腦上，需要重新設定。

## 連接用戶端 {#connect-client}

在「[[ui:settings.mcp.connectTitle]]」下有現成的片段和[[ui:settings.mcp.copy]]按鈕：

- 「[[ui:settings.mcp.claudeDesktop]]」：將片段貼到 Claude Desktop 設定檔的 `mcpServers` 區段，然後重新啟動 Claude Desktop；
- 「[[ui:settings.mcp.claudeCode]]」：使用 `claude mcp add` 指令；
- 「[[ui:settings.mcp.otherClient]]」：適用於任何透過指令（stdio）啟動 MCP 伺服器的用戶端。

片段中包含已安裝應用程式的路徑。如果你把 Glafira 重新安裝到其他位置，請重新複製片段。

## 核准用戶端 {#approve}

用戶端第一次連線時，Glafira 會顯示一個帶有其名稱的對話框。按一下[[ui:mcp.approval.allow]]或[[ui:mcp.approval.reject]]。在你回答之前，助理會收到「用戶端尚未核准」的訊息，並請你在 Glafira 視窗中回答。

被允許的用戶端會依名稱記住，並出現在[[ui:settings.mcp.clients]]清單中。拒絕不會被記住：下次連線時 Glafira 會再次詢問。如果用戶端未提供名稱，該許可僅在本次工作階段期間有效。

## 助理能看到什麼 {#what-llm-sees}

助理能看到：

- 存取層級為「[[ui:connections.form.llmLevel.metadata]]」或「[[ui:connections.form.llmLevel.messages]]」的連線：名稱、環境、資料夾和存取層級；
- Broker 的主機和連接埠，以及控制器；
- 主題、分區、複本、ISR、偏移量、訊息的大致數量；
- Broker 和主題的設定，其中密碼和其他機密值會被隱藏；
- 消費者群組：狀態、通訊協定、協調器、成員（含 client id、主機和 `group.instance.id`）、指派、已提交的偏移量和延遲；
- 自己的建議以及你對它們的決定：已套用（原樣或已修改）、已拒絕（附你的原因）、已撤回。

沒有「[[ui:connections.form.llmLevel.messages]]」層級時，助理看不到主題中的訊息。連線的 Bootstrap 伺服器位址和安全性設定、密碼、憑證、librdkafka 的額外屬性、傳送歷程記錄和範本，助理在任何情況下都看不到。

為了排查持續發生的重新平衡，助理最多可以觀察某個群組 50 秒：誰離開、誰加入、頻率如何、來自哪台主機。消費者本身的設定和日誌無法透過 Glafira 取得，助理會向你索取。

主題、群組和 client id 的名稱可以由任何有權存取叢集的人設定。Glafira 會提醒助理：這些資料不是給它的指令。

## 讀取訊息 {#messages}

在「[[ui:connections.form.llmLevel.messages]]」層級下，助理還能讀取訊息內容。例如，當你的服務在日誌中記錄了錯誤時，可以查明當時寫入主題的內容，以及消費者群組處理到了哪裡：

- `summarize_messages`：時間範圍概覽，不含訊息內容：依分區和時間統計的訊息數量、常見和重複的鍵、順序錯亂的時間戳記；
- `read_messages`：時間範圍或偏移量範圍內的訊息，在所有分區中依時間排序，篩選條件與[[ui:topics.details.tabMessages]]分頁相同；
- `get_group_commit_history`：消費者群組在何時提交了哪些偏移量，資料來自內部主題 `__consumer_offsets`。

例如，可以這樣問：「14:32 訂單服務在日誌中記錄了訂單 123 的錯誤，查一下當時 orders 主題裡有什麼，以及 order-service 群組處理到了哪裡。」請告訴助理你的日誌所用的時區：Glafira 不接受沒有時區的時間。

在資料傳送給助理之前，Glafira 會先進行遮罩。遮罩清單中的欄位的值會在 JSON、標頭和 `name=value` 配對中被替換為 `***`。此清單在連線表單中為每個連線分別設定，位於層級為「[[ui:connections.form.llmLevel.messages]]」時顯示的「[[ui:connections.form.mask]]」區段；標準清單涵蓋密碼、權杖、電子郵件、電話號碼、卡號和護照號碼等。電子郵件、卡號、JWT 和 Bearer 權杖在任何文字中都會被替換。遮罩可降低風險，但無法辨識人名、地址以及名稱陌生的欄位中的資料，Avro、Protobuf 等二進位格式也只能部分遮罩。篩選條件作用於未遮罩的資料，因此助理可以依據日誌中的值找到訊息。對於正式環境連線，表單會顯示警告。

讀取提交記錄需要對 `__consumer_offsets` 擁有 Read 權限。Compaction 會刪除舊的提交記錄，較新的記錄通常還在。

讀取有限制：每個工作階段同時只能進行一項長時間作業，每次呼叫最多 40 秒、1,000,000 筆記錄；達到限制時，助理會縮小時間範圍。Glafira 不使用消費者群組讀取，也不提交任何偏移量；`client.id` 為 `glafira-kafka-mcp`，除非連線設定了自己的值。

管理員可以透過原則鍵 `mcpMessagesEnabled` 禁止讀取訊息：「[[ui:connections.form.llmLevel.messages]]」層級將無法使用，使用該層級的連線會以「[[ui:connections.form.llmLevel.metadata]]」運作。

## 工作階段、撤銷和日誌 {#sessions-log}

用戶端連線期間，左側面板底部會顯示帶有用戶端名稱的 `LLM` 指示器，處理請求時它會亮起。按一下它即可開啟 MCP 區段。

- [[ui:settings.mcp.sessions]]：[[ui:settings.mcp.disconnect]]會關閉一個工作階段，[[ui:settings.mcp.disconnectAll]]會關閉所有工作階段。
- [[ui:settings.mcp.clients]]：[[ui:settings.mcp.revoke]]會關閉該用戶端的工作階段，該用戶端下次連線時會再次要求核准。
- 在「[[ui:connections.form.llmAccess]]」中變更層級會立即生效，不需要重新連線。
- [[ui:settings.mcp.audit.title]]：記錄每一次請求的時間、用戶端、工具、連線和結果。回應的內容不會被記錄。[[ui:settings.mcp.audit.reveal]]會開啟檔案 `mcp-audit.jsonl`。
- 對於讀取訊息的呼叫，日誌還會顯示主題或群組、傳回了多少項、掃描了多少筆記錄，以及遮罩了多少個值。傳回的是訊息；`summarize_messages` 傳回概覽的時間區段，`get_group_commit_history` 傳回提交序列（同一偏移量的連續提交）。對於成功的 `read_messages` 和 `summarize_messages` 呼叫，按一下[[ui:settings.mcp.audit.openInBrowse]]會以呼叫的參數填寫該主題[[ui:topics.details.tabMessages]]分頁中的表單；再按[[ui:browse.panel.read]]即可重新讀取訊息（不做遮罩），並與助理收到的內容比較。如果篩選條件或上限無法準確轉換，表單會註明；如果按鈕已停用，將指標停在按鈕上即可查看原因。

若要完全關閉 MCP，請取消勾選[[ui:settings.mcp.enable]]：用戶端會被中斷連線。

## 叢集檢查、註記與建議 {#health}

助理可以執行 Glafira 內建的叢集檢查（`health_check` 工具），並以其檢查結果為依據。它還可以為物件留下註記：註記會顯示在[[ui:sidebar.findings]]分頁以及主題與消費者群組畫面中，叢集不會被變更。請參閱[叢集檢查與註記](health-check.md#with-llm)。

助理也可以透過 `propose_change` 工具提出變更建議：主題設定、新主題、增加分區或重設偏移量。建議會顯示在物件旁邊，由你在已填好的表單中自行套用或拒絕；助理隨後可在 `list_proposals` 中看到你的決定。刪除和清除無法被建議。請參閱[LLM 建議](health-check.md#proposals)。

## 無法運作時 {#troubleshooting}

用戶端會顯示來自 Glafira 的訊息：

- `MCP is disabled in Glafira`：請勾選[[ui:settings.mcp.enable]]，然後在用戶端中重新啟動 MCP 伺服器；
- `MCP is disabled by your organization`：MCP 已被組織的原則停用；
- `Glafira did not start the MCP server within 30 s`：請手動啟動 Glafira，並在「[[ui:settings.mcp.section]]」區段查看狀態；
- `Invalid MCP token`：請在用戶端中重新啟動 MCP 伺服器，Glafira 每次啟動都會發出新的權杖。

在 macOS 上，應用程式已簽署並經過公證，因此第一次啟動前不需要任何額外步驟。
