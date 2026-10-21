# Consumer groups

Um consumer group é um conjunto de consumidores que dividem entre si a leitura de tópicos. O Kafka atribui cada partição a um membro do grupo e armazena, para cada partição, o offset com commit do grupo. Depois de reiniciar, os aplicativos continuam a leitura a partir desse offset.

Abra [[ui:sidebar.groups]] abaixo da conexão no painel [[ui:sidebar.connections]]. Os grupos de um tópico também aparecem na aba [[ui:topics.details.tabGroups]] do tópico. Consulte [Tela do tópico](topics.md#topic-details).

## Lista de grupos {#group-list}

A lista mostra todos os consumer groups do cluster. As colunas:

- [[ui:groups.list.colGroup]] — o ID do grupo (`group.id`).
- [[ui:groups.list.colState]] — o estado informado pelo Kafka, por exemplo `Stable` (os membros estão lendo), `Empty` (sem membros), `PreparingRebalance` ou `CompletingRebalance` (as partições estão sendo redistribuídas) e `Dead` (o grupo não existe mais).
- [[ui:groups.list.colMembers]] — o número de membros ativos.
- `Assignor` — a estratégia usada para distribuir as partições, por exemplo `range`.
- [[ui:groups.list.colLag]] — o lag total do grupo. Consulte [Lag](groups.md#lag).
- [[ui:groups.list.colTopics]] — os tópicos em que o grupo tem offsets com commit.

Digite no campo [[ui:groups.list.search]] para mostrar os grupos cujo ID contém o texto; maiúsculas e minúsculas não importam. A lista de estados ao lado mostra apenas os grupos em um estado; [[ui:groups.list.allStates]] remove esse filtro. O contador mostra quantos grupos estão visíveis e quantos existem no total.

Os membros, o assignor e o lag são carregados apenas para as linhas visíveis, à medida que você rola. Até que cheguem, a célula mostra `…`. Se não for possível carregá-los, a célula mostra `—`; aponte para ela para ver o motivo.

[[ui:groups.list.refresh]] recarrega a lista e os dados dela. Se alguns brokers não responderem, um aviso informa que a lista pode estar incompleta.

## Lag {#lag}

O lag é quantas mensagens o grupo ainda não leu. Para cada partição, é o fim do log menos o offset com commit. O lag nunca aparece abaixo de zero.

O lag depende apenas dos offsets com commit. Um consumidor ativo pode ter lido além do que confirmou com commit, então o lag real dele pode ser menor. Quando o grupo faz um novo commit, o lag diminui.

Uma partição sem offset com commit mostra [[ui:groups.lag.noCommit]] e não entra no total. O lag total de um grupo mostra `—` quando o grupo não tem nenhum commit ([[ui:groups.lag.noCommits]]) ou quando não foi possível ler os offsets de alguma partição ([[ui:groups.lag.unknown]]).

O lag conta offsets, não mensagens. Em tópicos com compactação ou transações, alguns offsets não contêm mensagens visíveis, então o grupo pode ter menos mensagens a ler do que o lag indica.

Os valores são um retrato do momento do carregamento. Para acompanhar o lag, abra o grupo e ative a atualização automática.

## Detalhes do grupo {#group-details}

Clique em um grupo na lista para abrir a aba dele. Na parte superior, um resumo mostra o estado, o número de membros e o assignor.

- [[ui:groups.details.autoRefresh]] — [[ui:groups.details.refreshOff]], [[ui:groups.details.refresh5]] ou [[ui:groups.details.refresh10]].
- [[ui:groups.details.refresh]] — recarrega na hora.
- [[ui:groups.details.join]] — inicia uma [sessão de consumer](consumer-sessions.md) neste grupo. O formulário da sessão recebe o ID do grupo e os tópicos em que o grupo tem commits. O botão fica desativado nas conexões com a opção [[ui:common.readOnly]] ativada; aponte para o botão para ver o motivo.

A seção [[ui:groups.details.partitions]] lista as partições em que o grupo tem commits e as partições atribuídas aos membros dele. O lag total do grupo aparece acima da tabela. As colunas são [[ui:groups.details.colTopic]], [[ui:groups.details.colPartition]], `Committed` (o offset com commit), `Log end` (o offset final da partição), [[ui:groups.details.colLag]] e [[ui:groups.details.colMember]] (o client ID do membro que lê a partição).

A seção [[ui:groups.details.members]] lista os membros ativos: `Client ID`, [[ui:groups.details.colHost]], `Member ID` e [[ui:groups.details.colAssignment]] (as partições do membro como `topic:partition`). Um grupo sem membros mostra [[ui:groups.details.noMembers]].

A segunda linha tem [[ui:groups.details.resetOffsets]] e [[ui:groups.details.deleteGroup]]. Ela fica oculta nas conexões somente leitura.

## Redefinir offsets {#reset-offsets}

Redefinir offsets move os offsets com commit de um grupo. Com isso, o grupo relê mensagens ou as pula. Só é possível redefinir os offsets de um grupo sem membros ativos (estado `Empty` ou `Dead`). Caso contrário, os membros sobrescreveriam os novos offsets com os próprios commits. Antes, pare os aplicativos do grupo.

1. Na aba do grupo, clique em [[ui:groups.details.resetOffsets]]. Enquanto o grupo tiver membros, o botão fica desativado; aponte para o botão para ver o motivo.
2. Escolha uma opção em [[ui:groups.reset.strategy]]. Consulte [Estratégias de redefinição](groups.md#reset-strategies).
3. Escolha uma opção em [[ui:groups.reset.scope]]. Consulte [Escopo da redefinição](groups.md#reset-scope).
4. Clique em [[ui:groups.reset.preview]].
5. Confira a tabela e clique no botão de aplicar. Ele mostra o número de partições que vão mudar.

A pré-visualização não altera nada. Para cada partição, ela mostra o tópico, a partição, [[ui:groups.reset.colCurrent]] (o offset com commit), [[ui:groups.reset.colNew]] (o offset de destino) e `Low / High` (o primeiro offset e o offset final da partição). Uma partição para a qual não é possível calcular um destino é marcada com [[ui:groups.reset.noTarget]], com o motivo. Essas partições são ignoradas.

Só o plano mostrado na pré-visualização é aplicado. Qualquer alteração no formulário oculta a pré-visualização, e é preciso clicar em [[ui:groups.reset.preview]] de novo. Em uma conexão prod, a aplicação pede o nome da conexão; nas demais conexões, ela é feita na hora.

Antes de aplicar, o aplicativo verifica de novo se o grupo não tem membros. Depois de aplicar, a seção [[ui:groups.reset.result]] lista cada partição com o novo offset ou um erro.

Também é possível preparar uma redefinição a partir de uma mensagem: consulte [Redefinir um grupo para uma mensagem](browse.md#reset-group-here). [[ui:common.close]] fecha o painel de redefinição.

## Estratégias de redefinição {#reset-strategies}

[[ui:groups.reset.strategy]] define o offset de destino de cada partição do escopo:

- [[ui:groups.reset.strategyEarliest]] — o primeiro offset ainda armazenado. O grupo relê tudo o que o tópico mantém.
- [[ui:groups.reset.strategyLatest]] — o fim da partição. O grupo pula tudo o que se acumulou e lê apenas as mensagens novas.
- [[ui:groups.reset.strategyTimestamp]] — a primeira mensagem gravada na data e hora indicadas em [[ui:groups.reset.timestamp]] ou depois. Nas partições sem mensagens mais recentes, o destino é o fim.
- [[ui:groups.reset.strategyOffset]] — o valor indicado em [[ui:groups.reset.offset]], um inteiro maior ou igual a 0, em todas as partições. Se uma partição não tiver esse offset, o destino é o primeiro offset ou o offset final dela.
- [[ui:groups.reset.strategyShift]] — move o offset com commit pelo valor de [[ui:groups.reset.shift]]. Um número negativo relê N mensagens para trás, e um positivo pula N mensagens. O resultado fica entre o primeiro offset e o offset final. As partições sem offset com commit não podem ser deslocadas e são ignoradas.

O horário é o seu horário local, com segundos. O Kafka encontra o offset pelos timestamps das mensagens.

## Escopo da redefinição {#reset-scope}

[[ui:groups.reset.scope]] define quais partições são redefinidas. As demais partições mantêm os offsets.

- [[ui:groups.reset.scopeAll]] — todas as partições em que o grupo tem offsets com commit. Se o grupo ainda não tiver commits, escolha tópicos.
- [[ui:groups.reset.scopeTopics]] — todas as partições dos tópicos indicados, inclusive as que não têm commits. Separe os nomes com vírgulas, espaços ou quebras de linha. Use essa opção para que um grupo comece a ler um tópico novo a partir de um ponto escolhido.
- [[ui:groups.reset.scopePartitions]] — apenas as partições indicadas. Informe-as em [[ui:groups.reset.partitionsLabel]] como `topic:partition`, separadas por vírgulas ou espaços, por exemplo `orders:0, orders:3`.

## Excluir um grupo {#delete-group}

Excluir um grupo remove todos os offsets com commit dele. Só é possível excluir um grupo sem membros, no estado `Empty`.

1. Na aba do grupo, clique em [[ui:groups.details.deleteGroup]]. Enquanto o grupo tiver membros, o botão fica desativado; aponte para o botão para ver o motivo.
2. Digite o ID do grupo para confirmar. Em uma conexão prod, digite também o nome da conexão.
3. Clique em [[ui:common.delete]].

Depois da exclusão, a aba do grupo é fechada. Se um aplicativo com esse `group.id` for iniciado de novo, o Kafka cria o grupo novamente. Sem offsets com commit, o aplicativo começa onde a configuração `auto.offset.reset` dele indica: do início ou do fim.

Nas conexões com a opção [[ui:common.readOnly]] ativada, o botão fica oculto.
