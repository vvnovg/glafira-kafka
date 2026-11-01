# Local models

The assistant does not have to run in the cloud: Glafira answers every MCP client the same way, and the client can talk to a model in your network or on your computer. This chapter covers where the model can run, what hardware a local model needs, how to connect it to Glafira and how to check the result. How the assistant's access to clusters works is described in [LLM assistants (MCP)](llm-mcp.md#how-it-works).

## Where the model runs {#where}

1. **A cloud model** (Claude, GPT and others): the strongest option, but requests with cluster metadata — and with the [[ui:connections.form.llmLevel.messages]] level, message contents too — leave your network. Many companies forbid this.
2. **A corporate model server** inside your network: data stays inside the perimeter, and administrators choose the model and the hardware. If there is no such server yet, this is the next step.
3. **A model on the engineer's computer**: for when the cloud is forbidden and a corporate server does not exist yet or is still being set up. Data never leaves the computer, but the model is weaker than a cloud one and answers more slowly.

The rest of the chapter is about the third option; a client for the second one is set up the same way, only the server address changes.

## What a local model can do {#capabilities}

Tested on a Mac with an M4 Pro and 48 GB of memory, the vllm-metal server, a 64k-token context and a test cluster with one broker and three topics (7 partitions, about 3,000 messages). Three typical tasks: a cluster check with an explanation of the findings, the lag of a group without members, and finding a message by the time from a service log. Each model ran three times.

| Model (4-bit) | Correct answers | Generation | Cluster check | Group lag | Message search |
|---|---|---|---|---|---|
| Qwen3.6-35B-A3B | 9 of 9 | 25–30 tokens/s | 30–50 s | 45–60 s | about 2 min |
| Qwen3.8-27B | 9 of 9 | 8–10 tokens/s | 8–16 min | 10–12 min | 28–46 min |

Qwen3.8-27B gives more detailed answers, but it is a dense model: it uses all 27 billion parameters for every token, while Qwen3.6-35B-A3B uses about 3 billion. For everyday work on a Mac, choose Qwen3.6-35B-A3B.

A local model handles long investigations of many steps worse than a cloud model: it makes more mistakes in tool arguments and can draw a wrong conclusion from incomplete data. Check its conclusions against the MCP log.

## Hardware and models {#hardware}

On ordinary hardware, MoE models with few active parameters (about 3 billion, such as Qwen3.6-35B-A3B) work best: they are faster than dense models of the same size. The context must be at least 32k tokens, preferably 64k.

| Platform | Model server | What fits |
|---|---|---|
| macOS, Apple Silicon, 48 GB or more | vllm-metal or Ollama | Qwen3.6-35B-A3B (tested) |
| macOS, Apple Silicon, 32 GB | Ollama | gpt-oss-20b; 35B-A3B is at the limit |
| Linux, NVIDIA with 24 GB of video memory | vLLM | 35B-A3B at 4 bits with a smaller context |
| Windows, NVIDIA | Ollama; vLLM only in WSL2 | as on Linux |
| x86 without a GPU, 32–64 GB DDR5 | Ollama | MoE models only, slowly |

A Mac with an Intel processor is not suitable for local models.

## Model server {#server}

The server must provide an OpenAI-compatible API with tool calling. On a Mac, [Ollama](https://ollama.com) is the simplest option: install the app, download the model and create a variant of it with a 64k-token context — by default Ollama uses a smaller context, and Glafira's tool descriptions do not fit into it:

```bash
ollama pull qwen3.6:35b-a3b
printf 'FROM qwen3.6:35b-a3b\nPARAMETER num_ctx 65536\n' > Modelfile
ollama create qwen3.6-64k:35b-a3b -f Modelfile
```

The client needs the address `http://127.0.0.1:11434/v1`, the model `qwen3.6-64k:35b-a3b` and any non-empty API key. Ollama listens only on `127.0.0.1`, so other computers cannot connect to the model.

On Linux with NVIDIA, vLLM is the usual choice. It parses tool calls with the `--tool-call-parser` flag set for the model family (`qwen3_coder` for Qwen3.6); in Ollama, the model template does this.

## Glafira settings {#glafira-settings}

In the [[ui:settings.mcp.section]] section:

- [[ui:settings.mcp.responseLimit]] — the largest answer of one tool call. A 100 KB answer takes about 30k tokens, so for a 64k context choose 48 or 24 KB: Glafira truncates long lists and tells the model to narrow the request.
- [[ui:settings.mcp.colCompact]] in the [[ui:settings.mcp.clients]] list — a tool set without notes and change proposals: their descriptions take about 1,000 tokens of context. The checkbox appears once the client connects and you approve it; the client sees the new list after it reconnects.

## Check {#check}

1. Connect the client and approve it in the Glafira dialog.
2. Open one connection to the assistant, preferably a test one, with the [[ui:connections.form.llmLevel.metadata]] level.
3. Ask three or four typical questions: "Check the cluster and explain the findings", "Why does group … have a large lag?", "Which topics have no replication?".
4. In [[ui:settings.mcp.audit.title]], check which tools the model called and with what result, and compare its answer with the data in Glafira.
5. If you open messages, check the masking: the [[ui:settings.mcp.audit.openInBrowse]] button shows the same messages without masking.
6. Turn off the network and repeat one question: the answer must arrive without internet access.

## Limitations {#limits}

- A local model answers several times more slowly than a cloud one, and dense models such as Qwen3.8-27B are slower still on a Mac (see the [table](local-llm.md#capabilities)).
- While the model runs, the computer has less memory left: on a 48 GB Mac, a 35-billion-parameter model takes about 20 GB.
- The model can draw a wrong conclusion. Glafira does not let it change anything in the cluster, but the decisions are yours.
