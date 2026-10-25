# Registro de alterações

Versão em português de [CHANGELOG.md](../../CHANGELOG.md), onde também estão as versões anteriores. A seção de uma versão aparece em “Sobre” como notas da atualização.

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
