# Cluster e tópicos

Este capítulo trata da visão geral do cluster, da lista de tópicos, da criação e exclusão de tópicos, da tela do tópico e da configuração de tópicos. Abaixo de cada conexão no painel [[ui:sidebar.connections]] há links para [[ui:sidebar.cluster]] e [[ui:sidebar.topics]]. Cada um abre em sua própria aba.

## Visão geral do cluster {#cluster-overview}

A tela [[ui:cluster.overview.title]] mostra o estado do cluster no momento em que foi carregada. Clique em [[ui:cluster.overview.refresh]] para carregá-la de novo.

- `Cluster ID` — o identificador do cluster.
- [[ui:cluster.overview.controller]] — o ID do broker que o cluster informa como controlador. Em clusters KRaft, os clientes não se comunicam diretamente com os controladores: aqui o cluster informa um broker aleatório, não o controlador ativo, e esse broker pode mudar de uma atualização para outra.
- [[ui:cluster.overview.brokers]] — o número de brokers.
- [[ui:cluster.overview.topicsInternal]] — todos os tópicos, com o número de tópicos internos entre parênteses.
- [[ui:cluster.overview.partitions]] — o número de partições em todos os tópicos.
- `Under-replicated` — partições em que nem todas as réplicas estão sincronizadas (no ISR). Normalmente, um broker está fora do ar ou não consegue acompanhar.
- `Offline` — partições sem leader. Ninguém consegue gravar nelas nem ler delas.

Um valor diferente de zero em `Under-replicated` ou `Offline` fica destacado.

Abaixo fica a tabela de brokers, com `ID` e [[ui:cluster.overview.address]]. O broker do campo [[ui:cluster.overview.controller]] é marcado com [[ui:cluster.overview.controllerBadge]]. Em clusters KRaft, é o mesmo broker aleatório, não o controlador ativo. Clique em um broker para ver a configuração dele abaixo da tabela. A configuração do broker pode apenas ser consultada. Use o campo [[ui:common.config.searchParam]] para encontrar um parâmetro pelo nome e a opção [[ui:common.config.onlyChanged]] para ocultar os parâmetros com valores padrão. Um ícone de cadeado marca os parâmetros somente leitura.

## Lista de tópicos {#topic-list}

A tela [[ui:sidebar.topics]] lista os tópicos do cluster. Clique em uma linha para abrir o tópico em sua própria aba.

As colunas são [[ui:topics.list.colName]], [[ui:topics.list.colPartitions]], `RF` (fator de replicação), [[ui:topics.list.colMessages]], `cleanup.policy` e [[ui:topics.list.colUnderRepl]] (o número de partições sub-replicadas). A contagem de mensagens é aproximada: é a diferença entre os offsets final e inicial das partições. A compactação e os marcadores de transação tornam o número real menor. A contagem de mensagens e o valor de `cleanup.policy` são carregados apenas para as linhas até as quais você rola.

Controles da barra de ferramentas:

- [[ui:topics.list.search]] — digite parte de um nome para encontrar tópicos; maiúsculas e minúsculas não importam. Para pesquisar com uma expressão regular, coloque-a entre barras, por exemplo `/^orders\..*/`.
- [[ui:topics.list.internal]] — mostra os tópicos internos, como `__consumer_offsets`. Eles são marcados com [[ui:topics.list.internalBadge]].
- A lista de atualização automática — [[ui:topics.list.autoRefreshOff]], [[ui:topics.list.every10]] ou [[ui:topics.list.every30]].
- [[ui:topics.list.refresh]] — recarrega a lista e a contagem de mensagens na hora.

Ao lado da barra de ferramentas, o aplicativo mostra quantos tópicos correspondem à pesquisa em relação ao total. Nas conexões com a opção [[ui:common.readOnly]] ativada, os botões [[ui:topics.list.create]] e [[ui:topics.list.select]] ficam ocultos.

## Criar um tópico {#create-topic}

1. Na lista de tópicos, clique em [[ui:topics.list.create]]. O formulário [[ui:topics.create.title]] é aberto acima da lista.
2. Em [[ui:topics.create.name]], digite o nome. Ele pode conter letras latinas, dígitos, `.`, `_` e `-`, com até 249 caracteres. O formulário avisa se o nome tiver `.` e `_` ao mesmo tempo: os nomes das métricas do Kafka desses tópicos podem colidir com os de outros tópicos.
3. Em [[ui:topics.create.partitions]], defina o número de partições. As partições são lidas em paralelo, e a ordem é garantida apenas dentro de uma partição. O número pode ser aumentado depois, mas nunca reduzido.
4. Em [[ui:topics.create.rf]], defina quantas cópias de cada partição os brokers mantêm. O valor não pode ser maior que o número de brokers, que o formulário mostra abaixo do campo. O padrão é 3, ou o número de brokers, se houver menos. `-1` usa o padrão do broker.
5. Em [[ui:topics.create.preset]], escolha uma opção:
   - [[ui:topics.create.presetDelete]] — as mensagens antigas são excluídas por tempo.
   - `compact` — só o valor mais recente de cada chave é mantido.
   - `compact + delete` — as duas coisas.
   - [[ui:topics.create.presetTest]] — as mensagens são mantidas por uma hora.
6. Se quiser, adicione parâmetros em [[ui:topics.create.extraConfigs]], um por linha, por exemplo `retention.ms=86400000`. Eles substituem os da predefinição. Linhas que começam com `#` são ignoradas.
7. Clique em [[ui:topics.create.submit]].

Quando o formulário é válido, ele mostra o comando `kafka-topics.sh` equivalente. Você pode copiá-lo, por exemplo, para criar o mesmo tópico em outro cluster.

Em uma conexão prod, você confirma a criação digitando o nome da conexão. Depois que o tópico é criado, a aba dele é aberta.

## Excluir tópicos {#delete-topics}

Para excluir um tópico, abra-o e clique em [[ui:topics.actions.deleteTopic]]. Digite o nome do tópico para confirmar e clique em [[ui:topics.delete.confirm]]. A aba do tópico é fechada.

Para excluir vários tópicos de uma vez:

1. Na lista de tópicos, clique em [[ui:topics.list.select]]. Aparecem caixas de seleção ao lado dos nomes.
2. Selecione os tópicos. Os tópicos internos não podem ser selecionados.
3. Clique no botão de exclusão. Ele mostra o número de tópicos selecionados.
4. Confira a lista de nomes, digite a quantidade para confirmar e clique em [[ui:topics.delete.confirm]].

[[ui:topics.list.cancelSelect]] oculta as caixas de seleção.

Em uma conexão prod, as duas formas de exclusão também pedem que você digite o nome da conexão.

A exclusão não pode ser desfeita: os dados são perdidos. Se alguns tópicos não forem excluídos, por exemplo por falta de permissão, o aplicativo os lista com o motivo. Os outros tópicos são excluídos.

## Tela do tópico {#topic-details}

A aba do tópico mostra o número de partições, o fator de replicação e a contagem aproximada de mensagens. Os botões na parte superior:

- [[ui:topics.details.refresh]] — recarrega os dados do tópico.
- [[ui:topics.details.produce]] — abre a tela [[ui:sidebar.producer]] com este tópico. Consulte [Producer](producer.md).
- [[ui:topics.details.consume]] — inicia uma sessão de consumer para este tópico. Exige o Pro. Consulte [Sessões de consumer](consumer-sessions.md).

Nas conexões com a opção [[ui:common.readOnly]] ativada, os dois botões ficam desativados.

A segunda linha tem as ações que alteram o tópico. Ela fica oculta nas conexões somente leitura e para os tópicos internos.

- [[ui:topics.actions.addPartitions]] — informe o novo total e clique em [[ui:topics.actions.addSubmit]]. O número de partições não pode ser reduzido. Depois de um aumento, mensagens com a mesma chave podem ir para outra partição, e a ordem por chave se perde.
- [[ui:topics.actions.purge]] — exclui todas as mensagens até o fim atual das partições selecionadas. O tópico em si permanece. Selecione as partições, digite o nome do tópico e clique em [[ui:topics.actions.purgeConfirm]]. A limpeza só funciona quando `cleanup.policy` inclui `delete`; caso contrário, o botão fica desativado e o motivo é mostrado. Em um tópico `compact,delete`, a limpeza também exclui os valores mais recentes das chaves.
- [[ui:topics.actions.deleteTopic]] — consulte [Excluir tópicos](topics.md#delete-topics).

Em uma conexão prod, cada uma dessas ações também pede o nome da conexão.

As abas abaixo:

- [[ui:topics.details.tabMessages]] — navegação pelas mensagens. Consulte [Navegar pelas mensagens](browse.md).
- [[ui:topics.details.tabPartitions]] — uma tabela com cada partição: [[ui:topics.details.colLeader]], [[ui:topics.details.colReplicas]], `ISR` (réplicas sincronizadas), os offsets inicial (`Low`) e final (`High`) e o número de mensagens. Em uma partição sem leader, aparece [[ui:topics.details.noLeader]].
- [[ui:topics.details.tabConfigs]] — os parâmetros do tópico. Consulte [Configuração do tópico](topics.md#topic-config).
- [[ui:topics.details.tabGroups]] — os consumer groups que têm offsets com commit neste tópico, com [[ui:topics.groups.colState]], [[ui:topics.groups.colPartitions]] e [[ui:topics.groups.colLag]]. Clique em um grupo para abri-lo. Consulte [Consumer groups](groups.md).

## Configuração do tópico {#topic-config}

A aba [[ui:topics.details.tabConfigs]] lista todos os parâmetros do tópico em três colunas:

- [[ui:common.config.colParam]] — o nome do parâmetro. Os nomes em negrito estão definidos explicitamente: no tópico ou na configuração do broker.
- [[ui:common.config.colValue]] — o valor atual. Os valores sensíveis ficam ocultos como `••••••`.
- [[ui:common.config.colSource]] — de onde vem o valor. Por exemplo, `DynamicTopic` está definido neste tópico, `StaticBroker` e `DynamicBroker` vêm da configuração do broker, e `Default` é o padrão do Kafka.

Use o campo [[ui:common.config.searchParam]] para encontrar um parâmetro pelo nome e a opção [[ui:common.config.onlyChanged]] para mostrar apenas os parâmetros em negrito. Um ícone de cadeado marca os parâmetros somente leitura. O ícone `?` ao lado de um nome explica o parâmetro.

## Alterar a configuração {#edit-config}

1. Na aba [[ui:topics.details.tabConfigs]], clique em [[ui:topics.details.edit]]. Nas conexões com a opção [[ui:common.readOnly]] ativada, esse botão fica oculto.
2. Altere os valores na coluna [[ui:common.config.colValue]]. Os parâmetros somente leitura e sensíveis não podem ser editados. Para remover o valor próprio do tópico, clique em [[ui:common.config.reset]]: passa a valer o valor do broker. Um valor vazio não é permitido.
3. Clique em [[ui:common.config.review]]. O aplicativo mostra cada alteração como valor antigo → valor novo; [[ui:common.config.defaultValue]] indica que o valor volta ao padrão.
4. Clique em [[ui:common.config.apply]], ou em [[ui:common.config.back]] para continuar editando.

Nada muda no cluster até você aplicar. [[ui:common.config.discard]] cancela todas as edições. Enquanto houver edições não salvas, o botão [[ui:topics.details.closeEditor]] fica desativado. Em uma conexão prod, você confirma as alterações digitando o nome da conexão.

Apenas os parâmetros alterados são enviados. Os demais parâmetros do tópico continuam como estão. O broker verifica os valores e rejeita os inválidos com um erro.

## Parâmetros do tópico {#config-parameters}

Os tópicos do Kafka têm dezenas de parâmetros. Os ícones `?` na tabela de configuração explicam cada um deles. Os mais usados são:

- `cleanup.policy` — o que acontece com os dados antigos. `delete` remove os segmentos antigos por tempo ou tamanho. `compact` mantém só o valor mais recente de cada chave. `compact,delete` faz as duas coisas.
- `retention.ms` — por quanto tempo as mensagens são mantidas, em milissegundos. O padrão é `604800000` (7 dias); `-1` as mantém para sempre. Funciona quando `cleanup.policy` inclui `delete`.
- `retention.bytes` — o tamanho máximo de cada partição antes que os segmentos antigos sejam excluídos. O padrão `-1` significa sem limite de tamanho.
- `segment.ms` e `segment.bytes` — quando o broker inicia um novo segmento do log. A retenção e a compactação atuam sobre segmentos inteiros, então uma mensagem pode ficar mais tempo que `retention.ms`, até que o segmento dela seja fechado.
- `min.insync.replicas` — quantas réplicas precisam confirmar uma gravação com `acks=all`. Normalmente 2 com fator de replicação 3.
- `max.message.bytes` — o maior batch que o broker aceita para este tópico. O padrão é de cerca de 1 MB. Os produtores precisam permitir o mesmo tamanho (`max.request.size`).
- `compression.type` — como o broker armazena as mensagens. `producer` mantém a compressão do produtor.
- `message.timestamp.type` — `CreateTime` (o horário definido pelo produtor) ou `LogAppendTime` (o horário em que o broker gravou a mensagem). É esse o horário mostrado na navegação pelas mensagens.
- `delete.retention.ms` — por quanto tempo os marcadores de exclusão (mensagens com valor nulo) permanecem em um tópico compactado. O padrão é 1 dia.

Alguns parâmetros existem apenas em versões mais recentes do Kafka, e alguns estão obsoletos. O ícone `?` avisa quando isso importa. Os parâmetros de tiered storage, como `remote.storage.enable`, só funcionam se o cluster tiver suporte a esse recurso.
