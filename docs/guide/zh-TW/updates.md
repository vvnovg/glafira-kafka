# 更新與診斷

本章說明如何更新應用程式、[[ui:sidebar.about]]畫面、診斷壓縮檔以及如何支持本專案。

## 更新 {#updates}

應用程式啟動時，會檢查一次是否有新版本。如果有，分頁上方的通知會顯示其版本。[[ui:about.notice.more]]會開啟[[ui:sidebar.about]]畫面。如果檢查失敗，例如沒有網際網路連線，則不會顯示任何訊息。

手動檢查：

1. 開啟左側面板底部的[[ui:sidebar.about]]。
2. 在[[ui:about.updates.title]]中按一下[[ui:about.updates.check]]。
3. 如果有新版本，畫面會顯示版本號碼和版本資訊。按一下[[ui:about.updates.install]]。

應用程式會下載更新、檢查簽章、安裝，然後重新啟動。因網路問題而中斷的下載會自動重試，總共最多三次。如果三次都失敗，應用程式會回報「[[ui:errors.updates.downloadInterrupted]]」。如果簽章檢查失敗，應用程式會回報「[[ui:errors.updates.badSignature]]」。

安裝時可能會要求輸入系統管理員密碼，因為應用程式是安裝給這台電腦的所有使用者：在 macOS 上透過 `.pkg` 安裝程式，在 Windows 上透過 `.msi` 安裝程式，在 Linux 上透過 `.deb` 或 `.rpm` 套件。沒有系統管理員權限的使用者無法更新這類安裝。

如果正在讀取訊息、有消費者工作階段在執行或正在批次傳送，應用程式會先詢問，並顯示有多少工作階段在執行中。按一下[[ui:about.updates.stopAndUpdate]]可停止它們並繼續，或按[[ui:common.cancel]]。更新後，連線、設定和其他資料都會保留。

更新會從 GitHub 上的發行頁面下載。如果檢查失敗並顯示「[[ui:errors.updates.checkFailed]]」，請檢查你的網路或 Proxy。「[[ui:errors.updates.notConfigured]]」表示這個 build 在建置時沒有更新程式金鑰，例如開發或測試用的 build。這種情況請從發行頁面下載新版本：https://github.com/vvnovg/glafira-kafka-releases/releases

組織可以透過原則關閉更新檢查，或將它指向內部鏡像站。關閉檢查時，按一下[[ui:about.updates.check]]後，[[ui:about.updates.title]]區段會顯示「[[ui:errors.updates.managed]]」。與「[[ui:errors.updates.notConfigured]]」一樣，這則訊息只會在按下按鈕後出現；在這些情況下，啟動時的檢查不會顯示任何內容。

## 關於 {#about}

開啟左側面板底部的[[ui:sidebar.about]]。在 macOS 上，也可以在「輔助說明」選單中找到[[ui:menu.about]]。

畫面會顯示應用程式的簡短說明，以及：

- [[ui:about.version]]：版本號碼；[[ui:about.build]]：此 build 所用原始碼的識別碼。提出支援要求時請附上這兩項。
- [[ui:about.system]]：作業系統和處理器架構。
- [[ui:about.license]]：Apache 2.0，連結會在瀏覽器中開啟授權全文。
- [[ui:about.policy]]：[[ui:about.policySet]]或[[ui:about.policyNone]]。原則中的錯誤會列在下方。
- 「[[ui:about.dataDir]]」後顯示的是存放連線設定檔、範本、歷程記錄、設定的資料夾。
- 「[[ui:about.logDir]]」後顯示的是存放日誌檔案的資料夾。請參閱[日誌](troubleshooting.md#logs)。

這個畫面上還有[[ui:about.updates.title]]和[[ui:about.diagnostics.title]]區段。

## 診斷壓縮檔 {#diagnostics}

診斷壓縮檔會收集支援人員了解問題所需的資訊。建立方式：

1. 開啟[[ui:sidebar.about]]。
2. 在[[ui:about.diagnostics.title]]中按一下[[ui:about.diagnostics.collect]]。在 macOS 上，也可以在「輔助說明」選單中選擇[[ui:menu.diagnostics]]。
3. 選擇檔案的儲存位置。建議的名稱為 `diagnostics-<date>.zip`。

壓縮檔儲存後，畫面會顯示其路徑。ZIP 檔案包含：

- `info.json`：應用程式版本和 build、作業系統、資料和日誌資料夾的路徑、原則狀態；
- `connections.json`：連線設定檔，包括名稱、環境、Bootstrap 伺服器、通訊協定、SASL 使用者名稱、憑證檔案路徑、額外屬性和逾時；
- `logs/`：應用程式的日誌檔案。

壓縮檔中不含密碼：密碼保存在系統金鑰儲存區中，而含有密碼和機密的額外屬性在儲存連線時就會被拒絕。憑證檔案也不會包含在內。主機名稱、使用者名稱和檔案路徑會以純文字儲存，而日誌中包含 Kafka 錯誤訊息裡的 Broker 位址。將壓縮檔傳送到公司外部之前，請先檢查其內容。

應用程式不會自行傳送任何內容。請自行將壓縮檔附加到你的支援要求中，請參閱[聯絡支援人員](troubleshooting.md#support)。

## 支持本專案 {#support}

Glafira 免費且開源，採用 Apache License 2.0。捐助讓它持續發展。左側面板底部、[[ui:sidebar.about]]畫面以及 macOS 上「輔助說明」選單中的[[ui:support.title]]會在瀏覽器中開啟支持頁面：https://github.com/vvnovg/glafira-kafka-releases/blob/main/SUPPORT.md
