# Sessões de consumer

Uma sessão de consumer lê tópicos como um membro real de um consumer group. As sessões de consumer são um recurso do Pro.

## O que é uma sessão {#what-is}

A [navegação](browse.md) lê as mensagens sem entrar em um grupo e não altera nada no cluster. Uma sessão é diferente: ela entra em um grupo com o `group.id` indicado, recebe partições do Kafka e pode fazer commit de offsets. Use-a para ler o que um grupo leria, para mover os offsets de um grupo por meio de commits ou para observar como um grupo faz o rebalanceamento.

Uma sessão afeta o cluster. Quando ela entra ou sai, o Kafka redistribui as partições do grupo, e os outros membros passam por um rebalanceamento. Os commits alteram os offsets do grupo, que os aplicativos dele vão usar.

No Free, os itens [[ui:sidebar.consumer]], [[ui:topics.details.consume]] e [[ui:groups.details.join]] ficam desativados e marcados com um cadeado e `Pro`. Se uma aba de sessão for aberta sem uma licença Pro ativa, ela mostra [[ui:consumer.proOnly]] e o link [[ui:common.openLicense]]. Consulte [Ativar uma chave](license-updates.md#activate).

As sessões não estão disponíveis nas conexões com a opção [[ui:common.readOnly]] ativada: a aba mostra [[ui:consumer.readOnlyBlocked]]. Se uma conexão passar a ser somente leitura durante uma sessão, a sessão sai do grupo.

## Iniciar uma sessão {#start-session}

Abra uma sessão de uma destas três formas:

- [[ui:sidebar.consumer]] abaixo da conexão no painel [[ui:sidebar.connections]] — um formulário vazio;
- [[ui:topics.details.consume]] na aba de um tópico — o tópico vem preenchido;
- [[ui:groups.details.join]] na aba de um grupo — o grupo e os tópicos em que ele tem commits vêm preenchidos.

Cada vez, abre-se uma nova aba, então várias sessões podem rodar ao mesmo tempo.

Na seção [[ui:consumer.form.groupTopics]]:

- `group.id` — o grupo em que entrar. Se você não informar um, o aplicativo sugere um novo nome, como `glafira-kafka-anna-1a2b3c`. Um grupo novo não tem commits.
- [[ui:consumer.form.topicsLabel]] — nomes de tópicos separados por vírgulas ou espaços. Ou marque [[ui:consumer.form.useRegex]] e informe a expressão em [[ui:consumer.form.patternLabel]], por exemplo `orders\..*`. A expressão é comparada a partir do início do nome: `^` é acrescentado se estiver faltando. Os tópicos correspondentes criados depois são adicionados em poucos minutos (na atualização dos metadados); isso causa um rebalanceamento.
- `auto.offset.reset` — onde começar nas partições em que o grupo não tem offset com commit: [[ui:consumer.form.resetLatest]] (o padrão) ou [[ui:consumer.form.resetEarliest]]. Onde o grupo tem um offset com commit, a leitura continua a partir dele.
- [[ui:consumer.form.autoCommit]] — faz o commit dos offsets lidos automaticamente. Desativado por padrão. Consulte [Commits](consumer-sessions.md#commits).
- `isolation.level` — `read_committed` (o padrão) mostra apenas as mensagens de transações confirmadas. `read_uncommitted` também mostra as mensagens de transações abertas e abortadas.

Na seção [[ui:consumer.form.formatLegend]], defina [[ui:consumer.form.keyFormat]] e [[ui:consumer.form.valueFormat]], como na navegação. Consulte [Formatos](browse.md#formats). O campo [[ui:consumer.form.buffer]] define quantas mensagens a tabela mantém, de 1 a 100000; o padrão é 5000.

Os filtros funcionam como na navegação. Consulte [Filtros](browse.md#filters).

Clique em [[ui:consumer.actions.connect]]. Em uma conexão prod, o aplicativo pede que você digite o nome da conexão e confirme com [[ui:consumer.dialog.confirm]]. Enquanto a sessão está em execução, o formulário não pode ser alterado.

## Leitura {#reading}

A linha de estado ao lado dos botões mostra [[ui:consumer.status.connecting]] e depois [[ui:consumer.status.inGroup]]. Ela também mostra quantas mensagens foram lidas, quantas correspondem aos filtros e quantas estão na tabela. Se a conexão com o broker cair, a sessão se reconecta sozinha e indica isso na linha de estado.

As mensagens aparecem na tabela à medida que chegam, e a sessão continua lendo as novas mensagens até você interrompê-la. Quando a tabela fica cheia, as mensagens mais antigas são removidas. Clique em uma linha para ver os detalhes da mensagem, como na navegação. Consulte [Detalhes da mensagem](browse.md#message-details). Selecione `UTC` para mostrar os horários em UTC.

A sessão lê apenas as partições que o Kafka atribuiu a ela. Se o grupo tiver outros membros, eles leem as demais partições. Se o grupo tiver mais membros do que partições, a sessão pode não receber nenhuma partição.

[[ui:consumer.disconnect]] sai do grupo, e os outros membros recebem as partições da sessão. Fechar a aba da sessão também a faz sair do grupo. Quando a sessão termina, a linha de estado mostra [[ui:consumer.status.disconnected]].

Se um tópico não existir, a sessão para com um erro e mostra os nomes a verificar.

## Commits {#commits}

Um commit salva um offset para o grupo. O próximo membro que ler a partição, ou o mesmo aplicativo depois de reiniciar, começa a partir desse offset.

Com a opção [[ui:consumer.form.autoCommit]] ativada, a sessão faz o commit dos offsets lidos em segundo plano a cada poucos segundos e mais uma vez ao sair do grupo. As mensagens ocultadas pelos filtros contam como lidas.

Sem o commit automático, a sessão não faz commit de nada sozinha. Use os botões:

- [[ui:consumer.actions.commitSelected]] — faz o commit do offset seguinte à mensagem selecionada, apenas na partição dela. O grupo continuará a partir da próxima mensagem. Se a mensagem selecionada for mais antiga que o commit atual, o offset volta para trás.
- [[ui:consumer.actions.commitAll]] — faz o commit da posição atual em todas as partições atribuídas. Isso inclui as mensagens ocultadas pelos filtros e as que já foram removidas da tabela.

Depois de um commit, uma linha mostra os offsets com commit como `topic:partition → offset`. Em uma conexão prod, cada commit pede o nome da conexão.

Se a sessão não responder a tempo, o aplicativo avisa que não se sabe se o commit foi aplicado. Verifique os offsets na aba do grupo. Consulte [Detalhes do grupo](groups.md#group-details).

## Pausar partições {#pause-partitions}

O painel à direita da tabela lista as partições atribuídas à sessão como `topic:partition`. Clique em [[ui:consumer.assignment.pause]] ao lado de uma partição para parar de lê-la e em [[ui:consumer.assignment.resume]] para continuar. Uma partição pausada é marcada com [[ui:consumer.assignment.paused]].

Pausar não faz a sessão sair do grupo, então não há rebalanceamento. A partição continua atribuída à sessão, e os outros membros não a leem enquanto ela está pausada. Pausar não altera nada no cluster e não exige confirmação.

Depois de um rebalanceamento, as partições recém-atribuídas começam sem pausa. Quando a sessão sai do grupo, todas as pausas terminam.

## Log de rebalanceamentos {#rebalance-log}

O [[ui:consumer.assignment.logTitle]], abaixo da lista de partições, registra cada mudança na atribuição, das mais recentes para as mais antigas. Cada entrada tem um horário e um de três tipos: partições atribuídas à sessão, partições revogadas dela ou um erro de rebalanceamento com o texto dele.

Um rebalanceamento acontece quando um membro entra no grupo ou sai dele, quando os tópicos assinados mudam ou quando um membro deixa de responder a tempo. Para observá-lo, inicie uma segunda sessão com o mesmo `group.id` em outra aba: as partições são divididas entre as duas sessões, e os dois logs mostram isso.

O log mantém as últimas 200 entradas. Antes do primeiro rebalanceamento, ele mostra [[ui:consumer.assignment.logEmpty]].

## Limitações {#limits}

- As sessões exigem o Pro. Se o Pro deixar de estar ativo durante uma sessão, a sessão continua lendo, mas os commits, as pausas e as retomadas são recusados, e não é possível iniciar uma nova sessão.
- As sessões não estão disponíveis nas conexões com a opção [[ui:common.readOnly]] ativada.
- Uma sessão não pode começar a partir de um offset escolhido. Para definir o início, redefina antes os offsets do grupo: consulte [Redefinir offsets](groups.md#reset-offsets). A redefinição exige um grupo sem membros, então desconecte as sessões desse grupo antes de redefinir.
- A tabela mantém no máximo 100000 mensagens. As mensagens mais antigas são removidas, mas continuam contando como lidas.
- Chaves e valores muito grandes são mostrados apenas em parte, como na navegação.
- Não entre no grupo de um aplicativo em execução, a menos que seja essa a intenção. A sessão toma partições do aplicativo, e o aplicativo não as processa enquanto a sessão as mantém. Os commits da sessão, inclusive os automáticos, movem os offsets do aplicativo.
