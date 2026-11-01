# 本機模型

助理不一定要在雲端執行：Glafira 對所有 MCP 用戶端的回應方式都相同，而用戶端可以連接你網路中或你自己電腦上的模型。本章介紹模型可以在哪裡執行、本機模型需要什麼硬體、如何將其連接到 Glafira，以及如何檢查結果。助理如何存取叢集，請參閱[LLM 助理（MCP）](llm-mcp.md#how-it-works)。

## 模型在哪裡執行 {#where}

1. **雲端模型**（Claude、GPT 等）：能力最強，但包含叢集中繼資料的請求會離開你的網路；當層級為「[[ui:connections.form.llmLevel.messages]]」時，訊息內容也會離開。許多公司禁止這麼做。
2. **企業內部模型伺服器**，位於你的網路內：資料留在內部網路，模型和硬體由管理員選擇。如果還沒有這樣的伺服器，這是下一步的方向。
3. **工程師電腦上的模型**：適用於禁止使用雲端，且企業伺服器尚不存在或仍在建置的情況。資料不會離開這台電腦，但模型比雲端模型弱，回應也更慢。

本章其餘部分談的是第三種方式；第二種方式的用戶端設定方法相同，只是伺服器位址不同。

## 本機模型能做什麼 {#capabilities}

測試環境：搭載 M4 Pro 和 48 GB 記憶體的 Mac，上下文為 64k 權杖，測試叢集包含一個 Broker 和三個主題（7 個分區，約 3,000 則訊息）。三個典型任務：檢查叢集並說明發現的問題、查看沒有成員的群組的延遲，以及依服務日誌中的時間尋找訊息。每個模型執行三次。

| 模型（4 位元） | 伺服器 | 正確回答 | 生成速度 | 叢集檢查 | 群組延遲 | 訊息尋找 |
|---|---|---|---|---|---|---|
| Qwen3.6-35B-A3B | Ollama | 9/9 | 70–80 權杖/秒 | 23–29 秒 | 8–31 秒 | 34–43 秒 |
| Qwen3.6-35B-A3B | vllm-metal | 9/9 | 25–30 權杖/秒 | 33–52 秒 | 43–60 秒 | 約 2 分鐘 |
| Qwen3.8-27B | vllm-metal | 9/9 | 8–10 權杖/秒 | 8–16 分鐘 | 10–12 分鐘 | 28–46 分鐘 |

Qwen3.8-27B 的回答更詳細，但它是稠密模型：每生成一個權杖都要用到全部 270 億個參數，而 Qwen3.6-35B-A3B 只用到約 30 億個。在 Mac 上日常使用，請選擇 Ollama 中的 Qwen3.6-35B-A3B：同一模型在 Ollama 上的回應速度是 vllm-metal 上的 1.5 到 3 倍。

對於需要許多步驟的長時間調查，本機模型的表現不如雲端模型：它在工具參數上更容易出錯，也可能根據不完整的資料得出錯誤的結論。請對照 MCP 日誌檢查它的結論。

## 硬體和模型 {#hardware}

在一般硬體上，活躍參數較少（約 30 億，如 Qwen3.6-35B-A3B）的 MoE 模型效果最好：它們比同樣大小的稠密模型更快。上下文至少為 32k 權杖，最好為 64k。

| 平台 | 模型伺服器 | 可執行的模型 |
|---|---|---|
| macOS，Apple Silicon，48 GB 或更多 | vllm-metal 或 Ollama | Qwen3.6-35B-A3B（已測試） |
| macOS，Apple Silicon，32 GB | Ollama | gpt-oss-20b；35B-A3B 已達極限 |
| Linux，顯示記憶體 24 GB 的 NVIDIA | vLLM | 4 位元的 35B-A3B，上下文較小 |
| Windows，NVIDIA | Ollama；vLLM 僅限 WSL2 | 與 Linux 相同 |
| 無 GPU 的 x86，32–64 GB DDR5 | Ollama | 僅限 MoE 模型，速度較慢 |

搭載 Intel 處理器的 Mac 不適合執行本機模型。

## 模型伺服器 {#server}

伺服器必須提供支援工具呼叫的 OpenAI 相容 API。在 Mac 上，[Ollama](https://ollama.com) 是最簡單的選擇：安裝應用程式，下載模型，並建立一個上下文為 64k 權杖的變體——Ollama 預設使用較小的上下文，Glafira 的工具說明放不下：

```bash
ollama pull qwen3.6:35b-a3b
printf 'FROM qwen3.6:35b-a3b\nPARAMETER num_ctx 65536\n' > Modelfile
ollama create qwen3.6-64k:35b-a3b -f Modelfile
```

用戶端需要位址 `http://127.0.0.1:11434/v1`、模型 `qwen3.6-64k:35b-a3b` 和任意非空的 API 金鑰。Ollama 只監聽 `127.0.0.1`，因此其他電腦無法連接到該模型。

閒置 5 分鐘後，Ollama 會把模型從記憶體中卸載，下一個問題要等模型重新載入。若要讓模型在記憶體中保留更久，請設定環境變數 `OLLAMA_KEEP_ALIVE`，例如設為 `1h`。

在配備 NVIDIA 的 Linux 上，通常使用 vLLM。它透過 `--tool-call-parser` 參數解析工具呼叫，該參數依模型系列設定（Qwen3.6 使用 `qwen3_coder`）；在 Ollama 中，由模型範本負責這項工作。

## Glafira 設定 {#glafira-settings}

在「[[ui:settings.mcp.section]]」區段：

- [[ui:settings.mcp.responseLimit]]：一次工具呼叫的最大回應。100 KB 的回應約占 30k 權杖，因此對於 64k 的上下文，請選擇 48 KB 或 24 KB：Glafira 會截斷過長的清單，並提示模型縮小請求範圍。
- 「[[ui:settings.mcp.clients]]」清單中的「[[ui:settings.mcp.colCompact]]」：不含註記和變更建議的工具集，它們的描述約占 1,000 權杖的上下文。用戶端連線並經你核准後才會出現該核取方塊；用戶端重新連線後才能看到新的工具清單。

## 檢查 {#check}

1. 連接用戶端，並在 Glafira 對話框中核准它。
2. 向助理開放一個連線（最好是測試連線），層級為「[[ui:connections.form.llmLevel.metadata]]」。
3. 提三四個典型問題：「檢查叢集並說明發現的問題」「為什麼群組 … 的延遲很大？」「哪些主題沒有複寫？」。
4. 在「[[ui:settings.mcp.audit.title]]」中查看模型呼叫了哪些工具、結果如何，並將它的回答與 Glafira 中的資料比對。
5. 如果你要開啟訊息，請檢查遮罩：按鈕「[[ui:settings.mcp.audit.openInBrowse]]」會顯示未遮罩的同一批訊息。
6. 中斷網路，重複一個問題：沒有網際網路時也必須能收到回答。

## 限制 {#limits}

- 本機模型的回應比雲端模型慢數倍，而在 Mac 上，Qwen3.8-27B 這類稠密模型更慢（請參閱[表格](local-llm.md#capabilities)）。
- 模型執行時，電腦剩餘的記憶體會減少：在 48 GB 的 Mac 上，一個 350 億參數的模型約占 20 GB。
- 模型可能得出錯誤的結論。Glafira 不允許它變更叢集中的任何內容，但決定權在你。
