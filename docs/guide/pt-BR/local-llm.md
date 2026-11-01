# Modelos locais

O assistente não precisa rodar na nuvem: o Glafira responde a todo cliente MCP da mesma forma, e o cliente pode se comunicar com um modelo na sua rede ou no seu computador. Este capítulo mostra onde o modelo pode rodar, de que hardware um modelo local precisa, como conectá-lo ao Glafira e como verificar o resultado. Como funciona o acesso do assistente aos clusters está descrito em [Assistentes LLM (MCP)](llm-mcp.md#how-it-works).

## Onde o modelo roda {#where}

1. **Um modelo na nuvem** (Claude, GPT e outros): a opção mais forte, mas as solicitações com metadados do cluster — e, no nível [[ui:connections.form.llmLevel.messages]], também com o conteúdo das mensagens — saem da sua rede. Muitas empresas proíbem isso.
2. **Um servidor de modelos corporativo** dentro da rede: os dados ficam dentro do perímetro, e os administradores escolhem o modelo e o hardware. Se ainda não existe esse servidor, este é o próximo passo.
3. **Um modelo no computador do engenheiro**: para quando a nuvem é proibida e um servidor corporativo ainda não existe ou está sendo configurado. Os dados nunca saem do computador, mas o modelo é mais fraco que um da nuvem e responde mais devagar.

O restante do capítulo trata da terceira opção; um cliente para a segunda é configurado da mesma forma, mudando apenas o endereço do servidor.

## O que um modelo local consegue fazer {#capabilities}

O teste foi feito em um Mac com M4 Pro e 48 GB de memória, contexto de 64 mil tokens e um cluster de teste com um broker e três tópicos (7 partições, cerca de 3.000 mensagens). Três tarefas típicas: uma verificação do cluster com explicação dos achados, o lag de um grupo sem membros e a busca de uma mensagem pelo horário de um log de serviço. Cada modelo foi executado três vezes.

| Modelo (4 bits) | Servidor | Respostas corretas | Geração | Verificação do cluster | Lag do grupo | Busca de mensagens |
|---|---|---|---|---|---|---|
| Qwen3.6-35B-A3B | Ollama | 9 de 9 | 70–80 tokens/s | 23–29 s | 8–31 s | 34–43 s |
| Qwen3.6-35B-A3B | vllm-metal | 9 de 9 | 25–30 tokens/s | 33–52 s | 43–60 s | cerca de 2 min |
| Qwen3.8-27B | vllm-metal | 9 de 9 | 8–10 tokens/s | 8–16 min | 10–12 min | 28–46 min |

O Qwen3.8-27B dá respostas mais detalhadas, mas é um modelo denso: usa todos os 27 bilhões de parâmetros para cada token, enquanto o Qwen3.6-35B-A3B usa cerca de 3 bilhões. Para o trabalho diário em um Mac, escolha o Qwen3.6-35B-A3B no Ollama: lá o mesmo modelo responde de 1,5 a 3 vezes mais rápido do que no vllm-metal.

Um modelo local lida pior com investigações longas e de muitas etapas do que um modelo da nuvem: erra mais nos argumentos das ferramentas e pode tirar uma conclusão errada de dados incompletos. Confira as conclusões dele com o log do MCP.

## Hardware e modelos {#hardware}

Em hardware comum, os modelos MoE com poucos parâmetros ativos (cerca de 3 bilhões, como o Qwen3.6-35B-A3B) funcionam melhor: são mais rápidos que modelos densos do mesmo tamanho. O contexto deve ter pelo menos 32 mil tokens, de preferência 64 mil.

| Plataforma | Servidor de modelos | O que cabe |
|---|---|---|
| macOS, Apple Silicon, 48 GB ou mais | vllm-metal ou Ollama | Qwen3.6-35B-A3B (testado) |
| macOS, Apple Silicon, 32 GB | Ollama | gpt-oss-20b; o 35B-A3B está no limite |
| Linux, NVIDIA com 24 GB de memória de vídeo | vLLM | 35B-A3B em 4 bits com contexto menor |
| Windows, NVIDIA | Ollama; vLLM somente no WSL2 | como no Linux |
| x86 sem GPU, 32–64 GB de DDR5 | Ollama | somente modelos MoE, devagar |

Um Mac com processador Intel não serve para modelos locais.

## Servidor de modelos {#server}

O servidor precisa oferecer uma API compatível com a OpenAI com chamada de ferramentas. No Mac, o [Ollama](https://ollama.com) é a opção mais simples: instale o aplicativo, baixe o modelo e crie uma variante dele com contexto de 64 mil tokens — por padrão, o Ollama usa um contexto menor, e as descrições de ferramentas do Glafira não cabem nele:

```bash
ollama pull qwen3.6:35b-a3b
printf 'FROM qwen3.6:35b-a3b\nPARAMETER num_ctx 65536\n' > Modelfile
ollama create qwen3.6-64k:35b-a3b -f Modelfile
```

O cliente precisa do endereço `http://127.0.0.1:11434/v1`, do modelo `qwen3.6-64k:35b-a3b` e de qualquer chave de API não vazia. O Ollama escuta somente em `127.0.0.1`, para que outros computadores não possam se conectar ao modelo.

Após 5 minutos ocioso, o Ollama descarrega o modelo da memória, e a próxima pergunta espera até que ele seja carregado de novo. Para manter o modelo na memória por mais tempo, defina a variável de ambiente `OLLAMA_KEEP_ALIVE`, por exemplo como `1h`.

No Linux com NVIDIA, o vLLM é a escolha usual. Ele interpreta as chamadas de ferramentas com a flag `--tool-call-parser`, definida para a família do modelo (`qwen3_coder` para o Qwen3.6); no Ollama, isso vem do template do modelo.

## Clientes {#clients}

Foram testados dois clientes de linha de comando. Em ambos, desative o shell integrado e as ferramentas de edição de arquivos e de acesso à web: caso contrário, o modelo pode contornar o Glafira e acessar por conta própria o cluster ou a internet. O Glafira mostra um novo cliente na caixa de diálogo de aprovação com o nome dele.

**Qwen Code** (código aberto, Apache 2.0). As configurações ficam em `~/.qwen/settings.json`; crie o arquivo antes da primeira execução, senão o Qwen Code se atualiza sozinho e ativa a aprovação automática das ações:

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

`envKey` é o nome da variável que guarda a chave, não a chave em si. `alwaysLoadTools` entrega ao modelo as ferramentas do Glafira imediatamente, sem uma etapa extra de busca de ferramentas. No Glafira, o cliente se chama `qwen-cli-mcp-client-glafira-kafka`. Se o servidor estiver definido nas configurações de uma pasta (`.qwen/settings.json`), aprove-o uma vez com `qwen mcp approve glafira-kafka`.

**Claude Code** funciona diretamente com o Ollama: o Ollama também responde no formato da Anthropic. Defina as variáveis de ambiente e adicione o Glafira com o comando do bloco [[ui:settings.mcp.connectTitle]]:

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

`ANTHROPIC_BASE_URL` não tem `/v1`. `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC` desativa atualizações, telemetria e relatórios de erros. Não é preciso entrar com uma conta da Anthropic. No Glafira, o cliente se chama `claude-code`.

Os dois clientes foram testados nas mesmas três tarefas da [tabela acima](local-llm.md#capabilities), com o modelo `qwen3.6-64k:35b-a3b` no Ollama, três execuções cada:

| Cliente | Respostas corretas | Verificação do cluster | Lag do grupo | Busca de mensagens |
|---|---|---|---|---|
| Qwen Code 0.26.5 | 9 de 9 | 30–98 s | 33–43 s | 66–108 s |
| Claude Code 2.1.316 | 9 de 9 | 43–59 s | 43–91 s | 74–98 s |

Os dois clientes usaram apenas as ferramentas do Glafira. Sem acesso à internet, com apenas conexões a `localhost` permitidas, eles responderam da mesma forma. O Claude Code às vezes responde em inglês mesmo quando você pergunta em outro idioma: peça a ele que responda no seu.

## Configurações do Glafira {#glafira-settings}

Na seção [[ui:settings.mcp.section]]:

- [[ui:settings.mcp.responseLimit]] — a maior resposta de uma chamada de ferramenta. Uma resposta de 100 KB ocupa cerca de 30 mil tokens; por isso, para um contexto de 64 mil, escolha 48 ou 24 KB: o Glafira trunca listas longas e diz ao modelo para restringir a solicitação.
- [[ui:settings.mcp.colCompact]] na lista [[ui:settings.mcp.clients]] — um conjunto de ferramentas sem notas e sem propostas de alteração: as descrições delas ocupam cerca de 1.000 tokens de contexto. A caixa de seleção aparece depois que o cliente se conecta e você o aprova; o cliente vê a nova lista após se reconectar.

## Verificação {#check}

1. Conecte o cliente e aprove-o na caixa de diálogo do Glafira.
2. Abra para o assistente uma conexão, de preferência de teste, com o nível [[ui:connections.form.llmLevel.metadata]].
3. Faça três ou quatro perguntas típicas: “Verifique o cluster e explique os achados”, “Por que o grupo … tem um lag grande?”, “Quais tópicos não têm replicação?”.
4. Em [[ui:settings.mcp.audit.title]], veja quais ferramentas o modelo chamou e com que resultado, e compare a resposta dele com os dados do Glafira.
5. Se você abrir mensagens, confira o mascaramento: o botão [[ui:settings.mcp.audit.openInBrowse]] mostra as mesmas mensagens sem mascaramento.
6. Desligue a rede e repita uma pergunta: a resposta deve chegar sem acesso à internet.

## Limitações {#limits}

- Um modelo local responde várias vezes mais devagar que um da nuvem, e modelos densos como o Qwen3.8-27B são ainda mais lentos em um Mac (veja a [tabela](local-llm.md#capabilities)).
- Enquanto o modelo roda, sobra menos memória no computador: em um Mac de 48 GB, um modelo de 35 bilhões de parâmetros ocupa cerca de 20 GB.
- O modelo pode tirar uma conclusão errada. O Glafira não permite que ele altere nada no cluster, mas as decisões são suas.
