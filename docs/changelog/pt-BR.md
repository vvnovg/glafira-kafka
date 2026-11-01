# Registro de alterações

Versão em português de [CHANGELOG.md](../../CHANGELOG.md), onde também estão as versões anteriores. A seção de uma versão aparece em “Sobre” como notas da atualização.

## 0.5.3
- Erros de TLS: quando o broker fecha a conexão durante o handshake de TLS, a mensagem deixa de afirmar que a porta não tem TLS. Ela informa que a porta pode não ter TLS ou que o certificado do cliente não foi aceito, que é o caso comum com mTLS.
- O MCP funciona melhor com modelos locais (Qwen, gpt-oss e outros):
  - os esquemas de parâmetros das ferramentas não contêm mais referências `$ref`, que alguns servidores (vLLM com o analisador `qwen3_coder`) passavam às ferramentas como strings;
  - um objeto aninhado enviado como string JSON é aceito, e valores `null` e chaves vazias são ignorados; se os argumentos ainda não servirem, o erro mostra os argumentos esperados;
  - quando uma janela de tempo não tem mensagens, `read_messages` sugere verificar o fuso horário e ampliar a janela; as descrições das ferramentas explicam como passar uma hora com fuso horário e como filtrar por uma única chave exata.
- “Configurações” → “Assistentes LLM (MCP)”:
  - “Limite de tamanho da resposta”: 100 KB (padrão), 48 KB ou 24 KB — para um modelo local com contexto pequeno, escolha menos;
  - “Conjunto compacto” para cada cliente aprovado: sem as ferramentas de observações e propostas de alteração, o que economiza cerca de 1000 tokens de contexto.
- Guia: um novo capítulo, “Modelos locais” — onde o modelo pode rodar, resultados do Qwen3.6 e do Qwen3.8 em um Mac, hardware, Ollama, configuração do Qwen Code e do Claude Code e verificação do resultado.

## 0.5.2
- Mascaramento mais completo das mensagens lidas por um assistente LLM:
  - no texto, os valores de cookies e de cabeçalhos `Authorization` (inclusive `Proxy-Authorization`) são mascarados até o fim da linha, então todos os cookies, os parâmetros Digest e as assinaturas da AWS ficam ocultos;
  - em pares `name=value`, nomes em qualquer alfabeto são reconhecidos, por exemplo um nome de campo da lista própria da conexão escrito em cirílico;
  - números de cartão grudados em letras (`id4111…`) ou escritos em agrupamentos incomuns são encontrados, assim como números de cartão negativos e fracionários em JSON.
- Tópico sem a permissão Describe: a aba “Mensagens” e as ferramentas `read_messages`, `summarize_messages` e `get_group_commit_history` informam “Sem permissão para executar a operação” em vez de “não encontrado”.
- `get_group_commit_history`: se o histórico de commits não couber em 100 KB nem mesmo sem séries de commits, o assistente recebe, em vez de um histórico vazio, o erro “resposta grande demais” com a sugestão de restringir a solicitação. `summarize_messages` usa menos memória com chaves grandes.
- Formulário de conexão:
  - quando dois formulários de conexão estavam abertos, o nível de acesso para LLM escolhido em um deles não aparecia, embora fosse salvo corretamente; agora cada formulário mostra o seu;
  - leitores de tela anunciam o aviso de que o conteúdo das mensagens de um cluster de produção será enviado ao provedor de LLM;
  - se não foi possível carregar o status do MCP, uma observação explica por que “Metadados e mensagens” não pode ser escolhido;
  - uma lista de mascaramento oculta nos níveis “Desativado” e “Somente metadados” não impede mais o salvamento.
- Indicativos `LLM` na árvore de conexões: leitores de tela os leem por completo, por exemplo “Acesso para LLM: metadados e mensagens”, em vez de “LLM”; o texto tem mais contraste.
- Log do MCP: o botão “Abrir em ‘Mensagens’” explica que está indisponível quando não foi possível carregar a lista de conexões.

## 0.5.1
- As notas da atualização em “Sobre” → “Atualizações” aparecem no idioma da interface (em inglês, se não houver tradução) e como texto formatado em vez de Markdown bruto. Isso vale para as atualizações oferecidas à versão 0.5.1 e posteriores; as versões anteriores mostram as notas como texto simples em inglês.
- O registro de alterações agora é mantido em inglês (`CHANGELOG.md`); as versões nos outros idiomas da interface ficam em `docs/changelog/`.

## 0.5.0
- Leitura de mensagens por um assistente LLM. Uma conexão tem três níveis de acesso para LLM: “Desativado”, “Somente metadados”, “Metadados e mensagens”. No terceiro nível, o assistente pode, via MCP:
  - ler mensagens de uma janela de tempo ou por offsets, com os mesmos filtros da aba “Mensagens” (`read_messages`);
  - ver uma visão geral de uma janela sem o conteúdo: volume por partição e no tempo, chaves frequentes, timestamps fora de ordem (`summarize_messages`);
  - ver de quais offsets um consumer group fez commit e quando, a partir de `__consumer_offsets` (`get_group_commit_history`).
- Os horários são passados somente com fuso horário. Uma chamada dura no máximo 40 segundos, percorre no máximo um milhão de registros e retorna no máximo 100 KB.
- Mascaramento antes de os dados chegarem ao assistente:
  - os valores dos campos de uma lista de nomes — a padrão ou a própria da conexão — são substituídos por `***`;
  - e-mails, números de cartão, JWTs e tokens de autorização são mascarados em qualquer texto;
  - os filtros trabalham com os dados sem mascaramento, então uma mensagem pode ser encontrada por um valor tirado de um log.
- O nível de acesso e a lista de mascaramento não são exportados nem importados.
- A chave de política `mcpMessagesEnabled` proíbe a leitura de mensagens: o nível “Metadados e mensagens” passa a funcionar como “Somente metadados”.
- Log do MCP: nas leituras de mensagens, mostra o tópico ou o grupo, quanto foi retornado do que foi percorrido e quantos valores foram mascarados. O botão “Abrir em ‘Mensagens’” preenche o formulário da tela “Mensagens” com a mesma janela e os mesmos filtros; se não for possível transferi-los com exatidão, o formulário avisa.
- MCP: clientes do protocolo MCP 2026-07-28 são compatíveis (por exemplo, Claude Code 2.1.310): o nome do cliente e a aprovação vêm logo na conexão. Uma sessão pendente tem o botão “Aprovar”. Todas as ferramentas estão marcadas explicitamente como somente leitura (`idempotentHint`).
- O servidor de catálogo `glafira-mcp-catalog` para registros de servidores MCP (Glama): uma imagem Docker, a lista real de ferramentas, e as chamadas respondem que é necessário o aplicativo para desktop. O README está em inglês (em russo — `README.ru.md`).
- Redefinição de offsets proposta por um LLM: antes da gravação, a composição do grupo é verificada de novo. Se o broker recusou porque membros entraram no grupo nesse meio-tempo, aparece um erro claro, “o grupo não está vazio”, e o formulário de redefinição mostra na hora o estado atual do grupo.
- Interface:
  - os textos de exemplo nos campos vazios ficam em itálico, para não serem confundidos com valores digitados;
  - os campos de posição inicial e de partições da tela “Mensagens” mostram uma dica ao passar o ponteiro;
  - sobre os indicativos de ambiente e de LLM na árvore de conexões aparece a seta normal.
- Guia: a seção “Leitura de mensagens” no capítulo “Assistentes LLM (MCP)”, em 12 idiomas.
