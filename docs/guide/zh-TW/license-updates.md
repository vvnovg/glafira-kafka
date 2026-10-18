# 授權與更新

本章說明 Pro 增加了哪些功能、如何啟用授權金鑰、授權到期後會發生什麼、如何更新應用程式、[[ui:sidebar.about]]畫面以及診斷壓縮檔。

## Free 與 Pro {#free-and-pro}

Free 涵蓋 Kafka 的日常工作：所有安全性通訊協定（包括 mTLS）、叢集概觀和主題、瀏覽訊息、支援批次傳送的 Producer、消費者群組以及偏移量重設。所有介面語言也都包含在內。請參閱[介面語言](getting-started.md#language)。

Pro 增加：

- 消費者工作階段：以群組成員身分讀取、提交偏移量、暫停分區。請參閱[消費者工作階段](consumer-sessions.md#what-is)；
- 正式環境模式：建立正式環境連線、將連線切換為正式環境，以及在正式環境連線上取消勾選[[ui:common.readOnly]]。請參閱[環境與正式環境](connections.md#environments)。

在 Free 中，這些功能看得到但無法使用。消費者工作階段指令會停用，並標示鎖頭和 `Pro`。正式環境選項顯示為 [[ui:connections.form.prodPro]]。如果某個動作需要 Pro，錯誤訊息會顯示「[[ui:errors.license.proRequired]]」，並提供按鈕：[[ui:common.openLicense]]。

[[ui:tabs.license]]畫面會在「[[ui:license.whatProGives]]」下列出消費者工作階段和正式環境模式。

## 啟用金鑰 {#activate}

授權金鑰是一行核發給個人或組織的文字。應用程式會在你的電腦上檢查金鑰，不會連線到任何伺服器。

1. 按一下左側面板底部的[[ui:tabs.license]]。
2. 將完整的金鑰貼到[[ui:license.keyLabel]]中。
3. 按一下[[ui:license.activate]]。

Pro 功能會立即可用。畫面會顯示[[ui:license.tier]]（Pro）、[[ui:license.licensee]]、[[ui:license.kind]]（[[ui:license.kindPersonal]]或[[ui:license.kindOrganization]]）以及[[ui:license.expires]]：日期或[[ui:license.perpetual]]。金鑰會儲存在應用程式資料資料夾中，因此重新啟動或更新後仍會保留。新的金鑰會取代先前的金鑰。

如果金鑰未被接受，欄位下方會顯示原因：

- [[ui:errors.license.malformed]]：金鑰有部分遺失或遭到變更。請重新將金鑰複製為一行，不要包含多餘的字元。
- [[ui:errors.license.badSignature]]：金鑰不是為 Glafira 核發的，或已被編輯。
- 金鑰已過期：訊息會顯示其到期日。請索取新的金鑰。
- 金鑰需要較新版本的應用程式：訊息會顯示金鑰版本。請先更新應用程式，請參閱[更新](license-updates.md#updates)。

若要移除金鑰，請按一下[[ui:license.removeKey]]。應用程式會詢問「[[ui:license.removeQuestion]]」按一下[[ui:license.removeYes]]確認。你的資料會保留。

組織可以透過原則為所有使用者啟用 Pro。此時金鑰欄位和按鈕會隱藏，畫面會顯示「[[ui:license.managed]]」設定了原則金鑰時，不會使用你自己輸入的金鑰。[[ui:license.policyTitle]]區段會顯示原則的來源並列出其錯誤。如果組織金鑰無效，[[ui:tabs.license]]畫面上會顯示原因。這種情況請洽詢你的管理員。管理員可以在 Glafira 原始碼的 `docs/admin.md` 中找到原則設定。

## 到期 {#expiry}

金鑰可以有到期日，顯示在[[ui:license.expires]]中。授權在當天結束前有效。

剩不到 14 天時，分頁上方的通知會顯示剩餘天數，[[ui:tabs.license]]畫面會顯示到期日。按一下通知中的[[ui:license.notice.more]]會開啟[[ui:tabs.license]]畫面。`×` 按鈕會隱藏通知，直到應用程式重新啟動。

到期日之後，方案會切換為 Free。通知和[[ui:tabs.license]]畫面會顯示授權已過期、Pro 功能已停用。變更如下：

- 無法啟動新的消費者工作階段。在已經執行中的工作階段中，提交和暫停分區會被拒絕，錯誤訊息會顯示到期日。
- 正式環境連線仍可繼續使用，確認方式相同。無法建立新的正式環境連線，也無法在正式環境連線上取消勾選[[ui:common.readOnly]]。

連線、密碼、範本和傳送歷程記錄都會保留。若要繼續使用 Pro，請依[啟用金鑰](license-updates.md#activate)的說明啟用新的金鑰。

## 更新 {#updates}

應用程式啟動時，會檢查一次是否有新版本。如果有，分頁上方的通知會顯示其版本。[[ui:license.notice.more]]會開啟[[ui:sidebar.about]]畫面。如果檢查失敗，例如沒有網際網路連線，則不會顯示任何訊息。

手動檢查：

1. 開啟左側面板底部的[[ui:sidebar.about]]。
2. 在[[ui:about.updates.title]]中按一下[[ui:about.updates.check]]。
3. 如果有新版本，畫面會顯示版本號碼和版本資訊。按一下[[ui:about.updates.install]]。

應用程式會下載更新、檢查簽章、安裝，然後重新啟動。因網路問題而中斷的下載會自動重試，總共最多三次。如果三次都失敗，應用程式會回報「[[ui:errors.updates.downloadInterrupted]]」。如果簽章檢查失敗，應用程式會回報「[[ui:errors.updates.badSignature]]」。

安裝時可能會要求輸入系統管理員密碼，因為應用程式是安裝給這台電腦的所有使用者：在 macOS 上透過 `.pkg` 安裝程式，在 Windows 上透過 `.msi` 安裝程式，在 Linux 上透過 `.deb` 或 `.rpm` 套件。沒有系統管理員權限的使用者無法更新這類安裝。

如果正在讀取訊息、有消費者工作階段在執行或正在批次傳送，應用程式會先詢問，並顯示有多少工作階段在執行中。按一下[[ui:about.updates.stopAndUpdate]]可停止它們並繼續，或按[[ui:common.cancel]]。更新後，連線、設定、授權金鑰和其他資料都會保留。

更新會從 GitHub 上的發行頁面下載。如果檢查失敗並顯示「[[ui:errors.updates.checkFailed]]」，請檢查你的網路或 Proxy。「[[ui:errors.updates.notConfigured]]」表示這個 build 在建置時沒有更新程式金鑰，例如開發或測試用的 build。這種情況請從發行頁面下載新版本：https://github.com/vvnovg/glafira-kafka-releases/releases

組織可以透過原則關閉更新檢查，或將它指向內部鏡像站。關閉檢查時，按一下[[ui:about.updates.check]]後，[[ui:about.updates.title]]區段會顯示「[[ui:errors.updates.managed]]」。與「[[ui:errors.updates.notConfigured]]」一樣，這則訊息只會在按下按鈕後出現；在這些情況下，啟動時的檢查不會顯示任何內容。

## 關於 {#about}

開啟左側面板底部的[[ui:sidebar.about]]。在 macOS 上，也可以在「輔助說明」選單中找到[[ui:menu.about]]。

畫面會顯示應用程式的簡短說明，以及：

- [[ui:about.version]]：版本號碼；[[ui:about.build]]：此 build 所用原始碼的識別碼。提出支援要求時請附上這兩項。
- [[ui:about.system]]：作業系統和處理器架構。
- [[ui:about.tier]]：Free 或 Pro。
- [[ui:about.policy]]：[[ui:about.policySet]]或[[ui:about.policyNone]]。原則中的錯誤會列在下方。
- 「[[ui:about.dataDir]]」後顯示的是存放連線設定檔、範本、歷程記錄、設定和授權金鑰的資料夾。
- 「[[ui:about.logDir]]」後顯示的是存放日誌檔案的資料夾。請參閱[日誌](troubleshooting.md#logs)。

這個畫面上還有[[ui:about.updates.title]]和[[ui:about.diagnostics.title]]區段。

## 診斷壓縮檔 {#diagnostics}

診斷壓縮檔會收集支援人員了解問題所需的資訊。建立方式：

1. 開啟[[ui:sidebar.about]]。
2. 在[[ui:about.diagnostics.title]]中按一下[[ui:about.diagnostics.collect]]。在 macOS 上，也可以在「輔助說明」選單中選擇[[ui:menu.diagnostics]]。
3. 選擇檔案的儲存位置。建議的名稱為 `diagnostics-<date>.zip`。

壓縮檔儲存後，畫面會顯示其路徑。ZIP 檔案包含：

- `info.json`：應用程式版本和 build、作業系統、資料和日誌資料夾的路徑、原則狀態以及授權狀態（方案、授權對象、類型、到期日）；
- `connections.json`：連線設定檔，包括名稱、環境、Bootstrap 伺服器、通訊協定、SASL 使用者名稱、憑證檔案路徑、額外屬性和逾時；
- `logs/`：應用程式的日誌檔案。

壓縮檔中不含密碼：密碼保存在系統金鑰儲存區中，而含有密碼和機密的額外屬性在儲存連線時就會被拒絕。授權金鑰和憑證檔案也不會包含在內。主機名稱、使用者名稱和檔案路徑會以純文字儲存，而日誌中包含 Kafka 錯誤訊息裡的 Broker 位址。將壓縮檔傳送到公司外部之前，請先檢查其內容。

應用程式不會自行傳送任何內容。請自行將壓縮檔附加到你的支援要求中，請參閱[聯絡支援人員](troubleshooting.md#support)。
