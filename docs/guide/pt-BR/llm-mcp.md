# Assistentes LLM (MCP)

O Glafira pode funcionar como gateway para um assistente LLM: Claude Desktop, Claude Code, Cursor ou outro cliente com suporte a MCP (Model Context Protocol). O assistente lê os metadados dos clusters que você abrir para ele e ajuda a encontrar problemas: partições sub-replicadas, lag, rebalanceamentos constantes, configurações inadequadas. Pelo Glafira ele não pode alterar nada em um cluster: as alterações você mesmo faz na interface.

## Como funciona {#how-it-works}

O cliente executa `glafira-kafka mcp` — o mesmo arquivo do aplicativo, em um modo sem janela. Esse comando se conecta ao Glafira em execução por um socket local (no Windows, um pipe nomeado), e o Glafira responde às solicitações do assistente por meio das próprias conexões. As senhas e os certificados ficam no Glafira; o assistente nunca os recebe.

Se o Glafira não estiver em execução, o cliente o inicia e espera até 30 segundos.

## Ativação {#enable}

1. Abra [[ui:sidebar.settings]], seção [[ui:settings.mcp.section]].
2. Marque [[ui:settings.mcp.enable]]. O status muda para [[ui:settings.mcp.state.running]].
3. No formulário da conexão, marque [[ui:connections.form.llmAccess]] e salve. O assistente não vê as conexões sem essa caixa marcada. Na lista de conexões, as conexões abertas têm o indicativo `LLM`.

Se o administrador tiver desativado o MCP, a caixa fica indisponível e ao lado aparece [[ui:settings.mcp.policy]].

A caixa de acesso não é transferida na exportação e na importação de conexões: em outro computador, marque-a de novo.

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

- as conexões com [[ui:connections.form.llmAccess]] marcado: nome, ambiente, pasta;
- os brokers com hosts e portas, o controlador;
- os tópicos, as partições, as réplicas, ISR, os offsets, o número aproximado de mensagens;
- as configurações de brokers e tópicos, com senhas e outros valores secretos ocultos;
- os consumer groups: estado, protocolo, coordenador, membros com client id, host e `group.instance.id`, atribuições, offsets com commit e lag.

O assistente não vê as mensagens dos tópicos, a string de bootstrap nem as configurações de segurança de uma conexão, nem as senhas, os certificados, as propriedades adicionais do librdkafka, o histórico de envio e os modelos.

Para investigar rebalanceamentos constantes, o assistente pode observar um grupo por até 50 segundos: quem sai e quem entra, com que frequência e de qual host. As configurações e os logs dos próprios consumers não ficam disponíveis pelo Glafira; o assistente vai pedi-los a você.

Os nomes de tópicos, grupos e client ids podem ser definidos por qualquer pessoa com acesso ao cluster. O Glafira avisa o assistente de que esses dados não são instruções para ele.

## Sessões, revogação e log {#sessions-log}

Enquanto um cliente está conectado, na parte inferior do painel esquerdo aparece o indicador `LLM` com o nome do cliente; ele se acende durante uma solicitação. Ao clicar nele, a seção do MCP é aberta.

- [[ui:settings.mcp.sessions]]: [[ui:settings.mcp.disconnect]] encerra uma sessão e [[ui:settings.mcp.disconnectAll]] encerra todas.
- [[ui:settings.mcp.clients]]: [[ui:settings.mcp.revoke]] encerra as sessões do cliente, e na próxima conexão ele pede aprovação de novo.
- Desmarcar [[ui:connections.form.llmAccess]] vale imediatamente, sem reconectar.
- [[ui:settings.mcp.audit.title]] — cada solicitação: data/hora, cliente, ferramenta, conexão, resultado. O conteúdo das respostas não é gravado. [[ui:settings.mcp.audit.reveal]] abre o arquivo `mcp-audit.jsonl`.

Para desativar o MCP por completo, desmarque [[ui:settings.mcp.enable]]: os clientes são desconectados.

## Se não funcionar {#troubleshooting}

O cliente mostra uma mensagem do Glafira:

- `MCP is disabled in Glafira` — marque [[ui:settings.mcp.enable]] e reinicie o servidor MCP no cliente;
- `MCP is disabled by your organization` — o MCP está desativado pela política da sua organização;
- `Glafira did not start the MCP server within 30 s` — inicie o Glafira manualmente e veja o status na seção [[ui:settings.mcp.section]];
- `Invalid MCP token` — reinicie o servidor MCP no cliente: o Glafira emite um novo token a cada inicialização.

No macOS, abra uma vez da forma habitual um build beta sem assinatura; caso contrário, o sistema pode impedir que o cliente o inicie. Consulte [Instalação no macOS](getting-started.md#install-macos).
