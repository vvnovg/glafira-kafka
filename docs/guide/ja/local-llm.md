# ローカルモデル

アシスタントは、必ずしもクラウドで動かす必要はありません。GlafiraはどのMCPクライアントに対しても同じように応答し、クライアントはネットワーク内やご自身のコンピューター上のモデルと通信できます。この章では、モデルをどこで動かせるか、ローカルモデルにどのようなハードウェアが必要か、Glafiraへの接続方法、結果の確認方法を説明します。アシスタントがクラスターにアクセスする仕組みは、[LLMアシスタント（MCP）](llm-mcp.md#how-it-works)で説明しています。

## モデルを動かす場所 {#where}

1. **クラウドモデル**（Claude、GPTなど）：最も高性能ですが、クラスターのメタデータを含むリクエストはネットワークの外に出ます。レベルが「[[ui:connections.form.llmLevel.messages]]」の場合は、メッセージの内容も外に出ます。多くの企業がこれを禁止しています。
2. **社内のモデルサーバー**（ネットワーク内）：データは境界の内側にとどまり、モデルとハードウェアは管理者が選びます。そのようなサーバーがまだない場合は、次の段階にあたります。
3. **エンジニアのコンピューター上のモデル**：クラウドが禁止されていて、社内サーバーがまだない、または準備中の場合に向いています。データはコンピューターの外に出ませんが、モデルはクラウドのものより性能が低く、応答も遅くなります。

この章の残りは3番目の方法についてです。2番目の方法のクライアントも同じように設定でき、変わるのはサーバーのアドレスだけです。

## ローカルモデルにできること {#capabilities}

テスト環境は、M4 Proと48 GBのメモリを搭載したMac、64kトークンのコンテキスト、ブローカー1台とトピック3つ（7パーティション、メッセージ約3,000件）のテストクラスターです。代表的な3つのタスクを実施しました。クラスターの点検と指摘事項の説明、メンバーのいないグループのラグの確認、サービスのログにある時刻からのメッセージの検索です。各モデルを3回ずつ実行しました。

| モデル（4ビット） | サーバー | 正答数 | 生成速度 | クラスターの点検 | グループのラグ | メッセージの検索 |
|---|---|---|---|---|---|---|
| Qwen3.6-35B-A3B | Ollama | 9/9 | 70–80トークン/秒 | 23–29秒 | 8–31秒 | 34–43秒 |
| Qwen3.6-35B-A3B | vllm-metal | 9/9 | 25–30トークン/秒 | 33–52秒 | 43–60秒 | 約2分 |
| Qwen3.8-27B | vllm-metal | 9/9 | 8–10トークン/秒 | 8–16分 | 10–12分 | 28–46分 |

Qwen3.8-27Bはより詳細な回答を返しますが、密なモデルです。1トークンごとに270億個のパラメーターをすべて使うのに対し、Qwen3.6-35B-A3Bは約30億個しか使いません。Macで日常的に使うなら、OllamaのQwen3.6-35B-A3Bを選んでください。同じモデルがvllm-metalより1.5〜3倍速く答えます。

多くの手順を要する長い調査は、ローカルモデルのほうがクラウドモデルより苦手です。ツールの引数を間違えることが多く、不完全なデータから誤った結論を導くことがあります。結論はMCPのログと照らして確認してください。

## ハードウェアとモデル {#hardware}

一般的なハードウェアでは、アクティブなパラメーターが少ない（Qwen3.6-35B-A3Bのように約30億の）MoEモデルが最適です。同じサイズの密なモデルより高速です。コンテキストは32kトークン以上、できれば64kにしてください。

| プラットフォーム | モデルサーバー | 動かせるモデル |
|---|---|---|
| macOS、Apple Silicon、48 GB以上 | vllm-metalまたはOllama | Qwen3.6-35B-A3B（テスト済み） |
| macOS、Apple Silicon、32 GB | Ollama | gpt-oss-20b。35B-A3Bは限界ぎりぎり |
| Linux、ビデオメモリ24 GBのNVIDIA | vLLM | 4ビットの35B-A3B（コンテキストは小さめ） |
| Windows、NVIDIA | Ollama。vLLMはWSL2のみ | Linuxと同じ |
| GPUなしのx86、32–64 GBのDDR5 | Ollama | MoEモデルのみ、低速 |

IntelプロセッサーのMacは、ローカルモデルには向きません。

## モデルサーバー {#server}

サーバーは、ツール呼び出しに対応したOpenAI互換APIを提供する必要があります。Macでは[Ollama](https://ollama.com)がもっとも簡単です。アプリをインストールし、モデルをダウンロードして、コンテキストを64kトークンにした派生モデルを作成します。Ollamaは既定ではより小さいコンテキストを使うため、Glafiraのツールの説明が収まらないからです。

```bash
ollama pull qwen3.6:35b-a3b
printf 'FROM qwen3.6:35b-a3b\nPARAMETER num_ctx 65536\n' > Modelfile
ollama create qwen3.6-64k:35b-a3b -f Modelfile
```

クライアントには、アドレス`http://127.0.0.1:11434/v1`、モデル`qwen3.6-64k:35b-a3b`、空でない任意のAPIキーが必要です。Ollamaは`127.0.0.1`のみで待ち受けるため、他のコンピューターからモデルに接続することはできません。

5 分間アイドル状態が続くと、Ollama はモデルをメモリから解放し、次の質問はモデルの再読み込みを待つことになります。モデルをより長くメモリに残すには、環境変数 `OLLAMA_KEEP_ALIVE` を設定します（例：`1h`）。

NVIDIAを搭載したLinuxでは、通常はvLLMを使います。vLLMは、モデルファミリーに合わせて指定する`--tool-call-parser`フラグ（Qwen3.6では`qwen3_coder`）でツール呼び出しを解析します。Ollamaでは、モデルのテンプレートがその役割を担います。

## クライアント {#clients}

コマンドラインのクライアントを2つテストしました。どちらも、組み込みのシェル、ファイル編集、Webのツールをオフにしてください。オフにしないと、モデルがGlafiraを迂回して、自分でクラスターやインターネットにアクセスできてしまいます。Glafiraは、新しいクライアントを承認ダイアログにその名前で表示します。

**Qwen Code**（オープンソース、Apache 2.0）。設定は`~/.qwen/settings.json`にあります。初回起動の前にこのファイルを作成してください。作成しないと、Qwen Codeが自動で更新され、操作の自動承認がオンになります。

```json
{
  "env": { "LOCAL_LLM_API_KEY": "ollama" },
  "modelProviders": { "openai": [ {
    "id": "qwen3.6-64k:35b-a3b",
    "envKey": "LOCAL_LLM_API_KEY",
    "baseUrl": "http://127.0.0.1:11434/v1",
    "generationConfig": { "contextWindowSize": 65536,
      "samplingParams": { "temperature": 0.2, "max_tokens": 8192 } } } ] },
  "security": { "auth": { "selectedType": "openai" } },
  "model": { "name": "qwen3.6-64k:35b-a3b" },
  "privacy": { "usageStatisticsEnabled": false },
  "general": { "enableAutoUpdate": false },
  "telemetry": { "enabled": false },
  "tools": { "approvalMode": "default",
    "disabled": ["run_shell_command", "edit", "write_file", "web_fetch", "web_search", "agent"] },
  "mcpServers": { "glafira-kafka": {
    "command": "/Applications/Glafira for Apache Kafka.app/Contents/MacOS/glafira-kafka",
    "args": ["mcp"], "timeout": 120000, "trust": true, "alwaysLoadTools": true } }
}
```

`envKey`はキーそのものではなく、キーを保持する変数の名前です。`alwaysLoadTools`を指定すると、ツール検索の手順を挟まずに、Glafiraのツールがすぐモデルに渡されます。Glafiraでは、このクライアントは`qwen-cli-mcp-client-glafira-kafka`という名前で表示されます。サーバーをフォルダーの設定（`.qwen/settings.json`）で定義した場合は、`qwen mcp approve glafira-kafka`で一度承認してください。

**Claude Code**はOllamaと直接連携できます。OllamaはAnthropic形式でも応答するためです。環境変数を設定し、「[[ui:settings.mcp.connectTitle]]」ブロックのコマンドでGlafiraを追加します。

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:11434
export ANTHROPIC_AUTH_TOKEN=ollama
export ANTHROPIC_MODEL=qwen3.6-64k:35b-a3b
export ANTHROPIC_DEFAULT_OPUS_MODEL=qwen3.6-64k:35b-a3b
export ANTHROPIC_DEFAULT_SONNET_MODEL=qwen3.6-64k:35b-a3b
export ANTHROPIC_DEFAULT_HAIKU_MODEL=qwen3.6-64k:35b-a3b
export CLAUDE_CODE_SUBAGENT_MODEL=qwen3.6-64k:35b-a3b
export CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1
claude --disallowedTools "Bash Edit Write WebFetch WebSearch Task"
```

`ANTHROPIC_BASE_URL`には`/v1`を付けません。`CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC`は、更新、テレメトリ、エラーレポートをオフにします。Anthropicアカウントへのサインインは不要です。Glafiraでは、このクライアントは`claude-code`という名前で表示されます。

2つのクライアントを、[上の表](local-llm.md#capabilities)と同じ3つのタスクで、Ollamaの`qwen3.6-64k:35b-a3b`モデルで3回ずつテストしました。

| クライアント | 正答数 | クラスターの点検 | グループのラグ | メッセージの検索 |
|---|---|---|---|---|
| Qwen Code 0.26.5 | 9/9 | 30–98秒 | 33–43秒 | 66–108秒 |
| Claude Code 2.1.316 | 9/9 | 43–59秒 | 43–91秒 | 74–98秒 |

どちらのクライアントもGlafiraのツールだけを使いました。インターネットに接続できず、`localhost`への接続だけを許可した状態でも、同じ答えが返ってきました。Claude Codeは、他の言語で質問しても英語で答えることがあります。その場合は、自分の言語で答えるように頼んでください。

## Glafiraの設定 {#glafira-settings}

「[[ui:settings.mcp.section]]」セクションで設定します。

- [[ui:settings.mcp.responseLimit]]：1回のツール呼び出しで返せる回答の最大サイズです。100 KBの回答は約30kトークンを使うため、64kのコンテキストでは48 KBか24 KBを選んでください。Glafiraは長いリストを切り詰め、リクエストを絞り込むようモデルに伝えます。
- 「[[ui:settings.mcp.clients]]」の一覧にある「[[ui:settings.mcp.colCompact]]」：注記と変更提案を除いたツールセットです。それらの説明が、コンテキストを約1,000トークン使います。このチェックボックスは、クライアントが接続して承認した後に表示されます。新しい一覧は、クライアントが再接続すると反映されます。

## 確認 {#check}

1. クライアントを接続し、Glafiraのダイアログで承認します。
2. アシスタントに接続を1つ（できればテスト用）、レベル「[[ui:connections.form.llmLevel.metadata]]」で開放します。
3. 典型的な質問を3、4個します。「クラスターを点検して、指摘事項を説明して」「グループ…のラグが大きいのはなぜ？」「レプリケーションのないトピックはどれ？」
4. 「[[ui:settings.mcp.audit.title]]」で、モデルがどのツールを呼び出し、どのような結果だったかを確認し、回答をGlafira上のデータと照らし合わせます。
5. メッセージを開く場合は、マスキングを確認します。「[[ui:settings.mcp.audit.openInBrowse]]」ボタンでは、同じメッセージがマスクなしで表示されます。
6. ネットワークを切断して、質問を1つやり直します。インターネットがなくても回答が返る必要があります。

## 制限事項 {#limits}

- ローカルモデルの応答はクラウドモデルより数倍遅く、Macでは、Qwen3.8-27Bのような密なモデルはさらに遅くなります（[表](local-llm.md#capabilities)を参照）。
- モデルの動作中は、コンピューターの空きメモリが減ります。48 GBのMacでは、350億パラメーターのモデルが約20 GBを使います。
- モデルが誤った結論を導くことがあります。Glafiraはクラスターの内容をモデルに変更させませんが、判断はご自身で行ってください。
