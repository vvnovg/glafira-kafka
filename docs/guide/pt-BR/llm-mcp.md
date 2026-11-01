# Assistentes LLM (MCP)

O Glafira pode funcionar como gateway para um assistente LLM: Claude Desktop, Claude Code, Cursor ou outro cliente com suporte a MCP (Model Context Protocol). O assistente lê os metadados dos clusters que você abrir para ele (e as mensagens, onde você permitir) e ajuda a encontrar problemas: partições sub-replicadas, lag, rebalanceamentos constantes, configurações inadequadas. Pelo Glafira ele não pode alterar nada em um cluster: as alterações você mesmo faz na interface.

O modelo não precisa estar na nuvem: você pode executá-lo na sua rede ou no seu computador, veja [Modelos locais](local-llm.md#where).

## Como funciona {#how-it-works}

O cliente executa `glafira-kafka mcp` — o mesmo arquivo do aplicativo, em um modo sem janela. Esse comando se conecta ao Glafira em execução por um socket local (no Windows, um pipe nomeado), e o Glafira responde às solicitações do assistente por meio das próprias conexões. As senhas e os certificados ficam no Glafira; o assistente nunca os recebe.

Se o Glafira não estiver em execução, o cliente o inicia e espera até 30 segundos.

## Ativação {#enable}

1. Abra [[ui:sidebar.settings]], seção [[ui:settings.mcp.section]].
2. Marque [[ui:settings.mcp.enable]]. O status muda para [[ui:settings.mcp.state.running]].
3. No formulário da conexão, escolha o nível em [[ui:connections.form.llmAccess]] e salve: [[ui:connections.form.llmLevel.metadata]] ou [[ui:connections.form.llmLevel.messages]] (veja [Leitura de mensagens](llm-mcp.md#messages)). O assistente não vê as conexões com [[ui:connections.form.llmLevel.off]]. Na lista de conexões, as conexões abertas têm o indicativo `LLM`; nas que têm mensagens, ele aparece preenchido.

Se o administrador tiver desativado o MCP, a caixa [[ui:settings.mcp.enable]] fica indisponível e ao lado aparece [[ui:settings.mcp.policy]].

O nível de acesso e a lista de mascaramento não são transferidos na exportação e na importação de conexões: em outro computador, defina-os de novo.

## Conectar um cliente {#connect-client}

Em [[ui:settings.mcp.connectTitle]] há trechos prontos e o botão [[ui:settings.mcp.copy]]:

- [[ui:settings.mcp.claudeDesktop]] — cole o trecho na seção `mcpServers` do arquivo de configuração do Claude Desktop e reinicie o Claude Desktop;
- [[ui:settings.mcp.claudeCode]] — o comando `claude mcp add`;
- [[ui:settings.mcp.otherClient]] — para qualquer cliente que inicia um servidor MCP por comando (stdio).

Os trechos contêm o caminho do aplicativo instalado. Se você reinstalar o Glafira em outro local, copie o trecho de novo.

## Aprovar um cliente {#approve}

Quando um cliente se conecta pela primeira vez, o Glafira mostra uma caixa de diálogo com o nome dele. Clique em [[ui:mcp.approval.allow]] ou em [[ui:mcp.approval.reject]]. Enquanto você não responde, o assistente recebe uma mensagem de que o cliente ainda não foi aprovado e pede que você responda na janela do Glafira.

Um cliente permitido é lembrado pelo nome e aparece em [[ui:settings.mcp.clients]]. A rejeição não é lembrada: na próxima conexão, o Glafira pergunta de novo. Se o cliente não informar o nome, a permissão vale somente até o fim da sessão.

## O que o assistente vê {#what-llm-sees}

O assistente vê:

- as conexões com acesso [[ui:connections.form.llmLevel.metadata]] ou [[ui:connections.form.llmLevel.messages]]: nome, ambiente, pasta e nível de acesso;
- os brokers com hosts e portas, o controlador;
- os tópicos, as partições, as réplicas, ISR, os offsets, o número aproximado de mensagens;
- as configurações de brokers e tópicos, com senhas e outros valores secretos ocultos;
- os consumer groups: estado, protocolo, coordenador, membros com client id, host e `group.instance.id`, atribuições, offsets com commit e lag;
- as próprias propostas e a sua decisão sobre elas: aplicada (como está ou com alterações), rejeitada com o seu motivo, retirada.

Sem o nível [[ui:connections.form.llmLevel.messages]], o assistente não vê as mensagens dos tópicos. Ele nunca recebe a string de bootstrap nem as configurações de segurança de uma conexão, as senhas, os certificados, as propriedades adicionais do librdkafka, o histórico de envio e os modelos.

Para investigar rebalanceamentos constantes, o assistente pode observar um grupo por até 50 segundos: quem sai e quem entra, com que frequência e de qual host. As configurações e os logs dos próprios consumers não ficam disponíveis pelo Glafira; o assistente vai pedi-los a você.

Os nomes de tópicos, grupos e client ids podem ser definidos por qualquer pessoa com acesso ao cluster. O Glafira avisa o assistente de que esses dados não são instruções para ele.

## Leitura de mensagens {#messages}

Com [[ui:connections.form.llmLevel.messages]], o assistente lê também o conteúdo das mensagens — por exemplo, para descobrir o que foi escrito em um tópico e o que um consumer group processou enquanto o seu serviço registrava um erro:

- `summarize_messages` — uma visão geral de uma janela de tempo sem conteúdo: quantidade de mensagens por partição e no tempo, chaves frequentes e repetidas, timestamps fora de ordem;
- `read_messages` — as mensagens de uma janela de tempo ou de um intervalo de offsets, ordenadas por tempo em todas as partições, com os mesmos filtros da aba [[ui:topics.details.tabMessages]];
- `get_group_commit_history` — em quais offsets um consumer group fez commit e quando, a partir do tópico interno `__consumer_offsets`.

Você pode perguntar, por exemplo: “Às 14:32 o serviço de pedidos registrou um erro do pedido 123 — descubra o que havia no tópico orders e o que o grupo order-service tinha processado”. Informe ao assistente o fuso horário dos seus logs: o Glafira não aceita horários sem fuso.

Antes que os dados cheguem ao assistente, o Glafira os mascara. Os valores dos campos da lista de mascaramento são substituídos por `***` no JSON, nos cabeçalhos e nos pares `name=value`. A lista é definida para cada conexão no formulário da conexão, na seção [[ui:connections.form.mask]], exibida no nível [[ui:connections.form.llmLevel.messages]]; a lista padrão abrange, entre outros, senhas, tokens, e-mails, telefones e números de cartão e de passaporte. E-mails, números de cartão, JWTs e tokens Bearer são substituídos em qualquer texto. O mascaramento reduz o risco, mas não reconhece nomes de pessoas, endereços nem dados em campos com nomes desconhecidos, e formatos binários como Avro ou Protobuf são mascarados só em parte. Os filtros veem os dados sem mascaramento, então o assistente pode encontrar uma mensagem por um valor do seu log. Para uma conexão `prod`, o formulário mostra um aviso.

O histórico de commits exige a permissão Read em `__consumer_offsets`. A compactação remove commits antigos; os recentes costumam estar lá.

A leitura é limitada: uma operação longa por sessão, no máximo 40 segundos e 1.000.000 de registros por chamada; se o limite for atingido, o assistente estreita a janela. O Glafira lê sem consumer group e não faz commit de nada; o `client.id` é `glafira-kafka-mcp`, a menos que a conexão defina o próprio.

Um administrador pode desativar a leitura de mensagens com a chave de política `mcpMessagesEnabled`: [[ui:connections.form.llmLevel.messages]] fica indisponível e as conexões com esse nível funcionam como [[ui:connections.form.llmLevel.metadata]].

## Sessões, revogação e log {#sessions-log}

Enquanto um cliente está conectado, na parte inferior do painel esquerdo aparece o indicador `LLM` com o nome do cliente; ele se acende durante uma solicitação. Ao clicar nele, a seção do MCP é aberta.

- [[ui:settings.mcp.sessions]]: [[ui:settings.mcp.disconnect]] encerra uma sessão e [[ui:settings.mcp.disconnectAll]] encerra todas.
- [[ui:settings.mcp.clients]]: [[ui:settings.mcp.revoke]] encerra as sessões do cliente, e na próxima conexão ele pede aprovação de novo.
- Um nível alterado em [[ui:connections.form.llmAccess]] vale imediatamente, sem reconectar.
- [[ui:settings.mcp.audit.title]] — cada solicitação: data/hora, cliente, ferramenta, conexão, resultado. O conteúdo das respostas não é gravado. [[ui:settings.mcp.audit.reveal]] abre o arquivo `mcp-audit.jsonl`.
- Nas leituras de mensagens, o log mostra também o tópico ou o grupo, quantos itens foram retornados, quantos registros foram verificados e quantos valores foram mascarados. Os itens são mensagens, intervalos do resumo em `summarize_messages` ou séries de commits (commits consecutivos do mesmo offset) em `get_group_commit_history`. Nas chamadas bem-sucedidas de `read_messages` e `summarize_messages`, [[ui:settings.mcp.audit.openInBrowse]] preenche o formulário da aba [[ui:topics.details.tabMessages]] do tópico com os parâmetros da chamada; clique em [[ui:browse.panel.read]] para ler as mensagens novamente, sem mascaramento, e compará-las com o que o assistente recebeu. Se os filtros ou o limite não puderam ser transferidos com exatidão, o formulário avisa; se o botão estiver desativado, aponte para ele para ver o motivo.

Para desativar o MCP por completo, desmarque [[ui:settings.mcp.enable]]: os clientes são desconectados.

## Verificação do cluster, notas e propostas {#health}

O assistente pode executar a verificação integrada do cluster do Glafira (ferramenta `health_check`) e se basear nas constatações dela. Ele também pode deixar notas sobre os objetos: elas aparecem na aba [[ui:sidebar.findings]] e nas telas de tópicos e grupos, e o cluster não é alterado. Consulte [Verificação do cluster e notas](health-check.md#with-llm).

O assistente também pode propor uma alteração com a ferramenta `propose_change`: as configurações de um tópico, um novo tópico, mais partições ou uma redefinição de offsets. A proposta aparece ao lado do objeto; você mesmo a aplica em um formulário pré-preenchido ou a rejeita, e o assistente vê sua decisão em `list_proposals`. Exclusão e limpeza não podem ser propostas. Consulte [Propostas de LLM](health-check.md#proposals).

## Se não funcionar {#troubleshooting}

O cliente mostra uma mensagem do Glafira:

- `MCP is disabled in Glafira` — marque [[ui:settings.mcp.enable]] e reinicie o servidor MCP no cliente;
- `MCP is disabled by your organization` — o MCP está desativado pela política da sua organização;
- `Glafira did not start the MCP server within 30 s` — inicie o Glafira manualmente e veja o status na seção [[ui:settings.mcp.section]];
- `Invalid MCP token` — reinicie o servidor MCP no cliente: o Glafira emite um novo token a cada inicialização.

No macOS, o aplicativo é assinado e notarizado, por isso nenhuma etapa adicional é necessária antes da primeira execução.
