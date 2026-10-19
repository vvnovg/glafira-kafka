# 叢集檢查與註記

Glafira 使用一組內建規則檢查叢集，並在物件旁顯示發現的問題：連線樹、主題與消費者群組清單，以及主題和消費者群組的頁面。使用 Glafira Pro 時，LLM 助理可以為這些檢查結果補充自己的註記。檢查只會讀取叢集，不會變更其中的任何內容。

## 執行檢查 {#run}

開啟連線的[[ui:sidebar.cluster]]或[[ui:sidebar.findings]]，然後按一下[[ui:health.check]]。一次檢查最多需要 45 秒：Glafira 會讀取 Broker、分區、主題設定（最多 2000 個主題）與消費者群組（最多 500 個）。在大型叢集上，結果會標示為[[ui:health.partial]]，檢查結果[[ui:health.ruleNames.check.partial]]會說明哪些內容沒有讀取。

Glafira 不會自行輪詢叢集：只有在你按下按鈕或 LLM 助理要求時才會執行檢查。結果會保留到下一次檢查。

## 檢查內容 {#rules}

- Broker 與分區：沒有控制器、無法使用與複本不足的分區、單一 Broker 叢集、Leader 分布不均、Leader 不在偏好複本上。
- 主題的持久性：複寫係數為 1，`min.insync.replicas` 大於或等於複寫係數或等於 1，不乾淨的 Leader 選舉。
- Broker 設定：自動建立主題、預設複寫係數與 `min.insync.replicas`、各 Broker 之間不一致的重要設定。
- 消費者群組：有延遲的空群組、正在重新平衡的群組、沒有分區的成員、自上次檢查以來增加的延遲。

## 嚴重程度與環境 {#severity}

每個檢查結果都有嚴重程度：[[ui:health.severity.critical]]、[[ui:health.severity.warning]]或[[ui:health.severity.info]]。它取決於連線的環境。例如，複寫係數為 1 在 prod 環境中屬於嚴重，在 stage 環境中屬於警告，在 dev、test 與 local 環境中不會顯示。環境在連線表單中設定。

## 「檢查結果」分頁 {#tab}

在[[ui:sidebar.findings]]分頁中，檢查結果依規則分組，每一筆都會說明問題與處理方式。可以依[[ui:health.filterSeverity]]、[[ui:health.filterSource]]與[[ui:health.filterKind]]篩選，也可以依名稱搜尋。按一下物件名稱會開啟它的頁面。

連線樹以及主題和消費者群組清單中帶數字的標記，會顯示與該物件相關的嚴重問題與警告數量。

## 隱藏檢查結果 {#hide}

如果某個物件的檢查結果在預期之內，例如 prod 環境中複寫係數為 1 的測試主題，請按一下[[ui:health.hide]]。隱藏的檢查結果不會計入標記，之後的檢查中也會保持隱藏。若要查看已隱藏的結果，請勾選「顯示已隱藏」，在那裡可以按一下[[ui:health.restore]]。

## LLM 註記 {#notes}

使用 Glafira Pro 時，透過 MCP 連線的 LLM 助理（請參閱 [LLM 助理（MCP）](llm-mcp.md#how-it-works)）可以為叢集、Broker、主題或消費者群組留下註記。註記會顯示在「[[ui:health.notes]]」與物件頁面中，並署名用戶端名稱。文字會原樣顯示，不含格式與連結。

- [[ui:health.resolve]]與[[ui:health.dismiss]]會關閉註記，已關閉的註記保留在摺疊的區段中。
- [[ui:common.delete]]會刪除一則註記，[[ui:health.deleteAll]]會刪除此連線的所有註記。
- 註記會在建立 7 天後自動刪除。一個連線最多可以有 200 則未關閉的註記。

沒有 Pro 時，助理仍可執行檢查並讀取檢查結果，但無法留下註記。

## 與 LLM 助理搭配 {#with-llm}

助理會使用 `health_check` 工具執行同樣的檢查，並看到同樣的檢查結果，包括帶有標示的已隱藏結果。如果上次檢查不到 5 分鐘，助理會直接取得其結果，不會再次查詢叢集。例如，可以這樣要求：「檢查叢集的健康狀況，並針對最重要的問題留下註記。」
