# Navegar pelas mensagens

A navegação pelas mensagens fica na aba [[ui:topics.details.tabMessages]] de um tópico. A navegação não entra em nenhum consumer group e não faz commit de nada, então o cluster não muda. Ela também funciona nas conexões com a opção [[ui:common.readOnly]] ativada. Para ler como membro de um grupo, use uma [sessão de consumer](consumer-sessions.md).

## Iniciar a leitura {#start-reading}

1. Abra um tópico na lista [[ui:sidebar.topics]]. A aba [[ui:topics.details.tabMessages]] é aberta primeiro.
2. Em [[ui:browse.form.from]], escolha onde começar. Consulte [Posição inicial](browse.md#start-position).
3. Se quiser, defina os formatos e os filtros.
4. Clique em [[ui:browse.panel.read]].

As mensagens aparecem na tabela à medida que chegam. A linha ao lado dos botões mostra quantas mensagens foram verificadas, quantas correspondem aos filtros e quantas estão na tabela. Ela também mostra o estado: [[ui:browse.panel.reading]], [[ui:browse.panel.finishLimit]], [[ui:browse.panel.finishEnd]] ou [[ui:browse.panel.finishCancelled]].

Sem `Live tail`, a leitura para no fim do tópico tal como estava quando você clicou em [[ui:browse.panel.read]], ou antes, ao atingir o valor de [[ui:browse.form.limit]]. O limite conta as mensagens que correspondem aos filtros. Ele pode ir de 1 a 100000; valores altos tornam a leitura mais lenta.

Com `Live tail` ativado, a leitura não para no fim: as novas mensagens são adicionadas à medida que são gravadas. Quando todas as mensagens antigas já foram lidas, o estado mostra [[ui:browse.panel.waiting]]. A tabela mantém as últimas mensagens até o limite e descarta as mais antigas.

[[ui:browse.panel.stop]] interrompe a leitura e mantém a tabela. Durante a leitura, o botão [[ui:browse.panel.read]] vira [[ui:browse.panel.restart]]: ele recomeça com as configurações atuais e limpa a tabela. Se a conexão com o broker cair, o aplicativo se reconecta sozinho e indica isso na linha de estado.

As mensagens de partições diferentes aparecem misturadas na tabela. O Kafka mantém a ordem apenas dentro de uma partição. A coluna `P` mostra a partição, e `Offset`, a posição nela. Selecione `UTC` para mostrar os horários em UTC em vez do horário local.

## Posição inicial {#start-position}

[[ui:browse.form.startPosition]] define onde a leitura começa em cada partição:

- [[ui:browse.form.startLatest]] — as últimas N mensagens. N é dividido igualmente entre as partições, então, com 100 mensagens e 4 partições, o aplicativo pega até 25 de cada uma.
- [[ui:browse.form.startEarliest]] — a partir da mensagem mais antiga ainda armazenada.
- [[ui:browse.form.startOffset]] — a partir do mesmo offset em cada partição. Se uma partição não tiver esse offset, a leitura começa no início ou no fim dela. Esse modo é mais útil com uma única partição.
- [[ui:browse.form.startTimestamp]] — a partir da primeira mensagem com timestamp igual ou posterior ao horário local informado. Nas partições sem mensagens mais recentes, a leitura começa no fim.
- [[ui:browse.form.startLastMinutes]] — o mesmo, contando para trás a partir de agora.

O campo com o texto [[ui:browse.form.partitionsPlaceholder]] limita a leitura a algumas partições. Informe os números das partições separados por vírgulas, por exemplo `0,2`. Deixe-o vazio para ler todas as partições.

## Filtros {#filters}

Os filtros mostram apenas as mensagens correspondentes. Mesmo assim, o aplicativo lê todas as mensagens a partir da posição inicial e verifica cada uma, então os filtros não deixam a leitura mais rápida. Todos os campos preenchidos precisam corresponder.

- [[ui:browse.filters.keyContains]] e [[ui:browse.filters.valueContains]] — um texto na chave ou no valor.
- `regex` — trata esses campos, e o valor do cabeçalho, como expressões regulares.
- `Aa` — diferencia maiúsculas de minúsculas. Por padrão, isso não importa.
- [[ui:browse.filters.header]] — a mensagem tem um cabeçalho exatamente com esse nome. [[ui:browse.filters.headerValue]] também verifica o valor dele.
- `JSONPath` — uma condição sobre um valor JSON, por exemplo `$.order.status == "FAILED"`. Sem `==` ou `!=`, basta que o caminho exista, por exemplo `$.error`. As mensagens cujo valor não é um JSON válido não correspondem.
- [[ui:browse.filters.timeFrom]] e [[ui:browse.filters.timeTo]] — o timestamp da mensagem está nesse intervalo, no horário local. Os dois extremos estão incluídos.

Os filtros verificam o texto mostrado na tabela, depois da decodificação. Se o formato do valor for `hex`, o filtro de valor pesquisa no texto hexadecimal. Os filtros são aplicados quando você clica em [[ui:browse.panel.read]]; depois de alterá-los, leia de novo.

Um filtro não muda a posição inicial. Por exemplo, [[ui:browse.form.startLatest]] com um filtro encontra correspondências apenas entre as últimas N mensagens. Para pesquisar o tópico inteiro, comece por [[ui:browse.form.startEarliest]].

## Formatos {#formats}

[[ui:browse.form.format]] define como as chaves e os valores são decodificados, separadamente para [[ui:browse.form.key]] e [[ui:browse.form.value]]. Só a exibição muda, não os dados.

- `auto` — o padrão. Objetos e arrays JSON válidos aparecem como JSON, outros textos UTF-8 como string, e dados binários em hexadecimal.
- `string` — texto em UTF-8. Os bytes que não são UTF-8 válidos são substituídos por `�`.
- `json` — JSON com recuos nos detalhes da mensagem. Se o valor não for um JSON válido, os detalhes mostram o erro.
- `hex` — cada byte como dois dígitos hexadecimais.
- `base64` — os bytes em Base64.

Os detalhes mostram o formato efetivamente usado, por exemplo `json` quando `auto` detectou JSON. Os valores dos cabeçalhos aparecem como texto; os valores binários aparecem como `0x` seguido de hexadecimal. Uma chave ou um valor ausente aparece como `null`.

Chaves e valores muito grandes são mostrados apenas em parte, aproximadamente o primeiro 1 MB de texto. Isso também vale para `hex` e `base64`. Nesse caso, os detalhes da mensagem mostram um aviso.

## Detalhes da mensagem {#message-details}

Clique em uma linha para abrir os detalhes da mensagem ao lado da tabela. Na parte superior ficam a partição, o offset e o horário. Abaixo:

- [[ui:browse.details.value]] e [[ui:browse.details.key]] — o texto completo, com o formato e o tamanho em bytes. O JSON aparece com recuos. [[ui:browse.details.copy]] copia o texto.
- [[ui:browse.table.colHeaders]] — uma tabela com os nomes e valores dos cabeçalhos.

As colunas da tabela são `P`, `Offset`, [[ui:browse.table.colTime]], [[ui:browse.table.colKey]], [[ui:browse.table.colValue]], [[ui:browse.table.colHeaders]] (a quantidade deles) e [[ui:browse.table.colSize]] (o tamanho da chave e do valor em bytes). O horário é o timestamp da mensagem. Dependendo da configuração `message.timestamp.type` do tópico, ele é definido pelo produtor ou pelo broker.

## Copiar mensagens {#export-messages}

O Glafira não salva em arquivo as mensagens navegadas. Para levar uma mensagem para fora do aplicativo, copie-a:

- [[ui:browse.details.copyJson]] copia a mensagem inteira como JSON: tópico, partição, offset, timestamp, chave, valor, cabeçalhos e tamanho. A chave e o valor são incluídos decodificados, com o formato deles.
- [[ui:browse.details.copy]] ao lado de [[ui:browse.details.value]] ou [[ui:browse.details.key]] copia apenas esse texto.

O texto copiado usa o formato escolhido para a leitura. Para copiar dados binários com exatidão, leia-os como `hex` ou `base64`. Chaves e valores muito grandes são copiados apenas em parte, como são mostrados, mesmo em `hex` e `base64`. Consulte [Formatos](browse.md#formats).

## Abrir no Producer {#open-in-producer}

[[ui:browse.details.resend]] abre a mensagem na tela [[ui:sidebar.producer]] da mesma conexão. O formulário recebe o tópico, a chave, o valor, os cabeçalhos e os formatos da mensagem. A partição fica a cargo do Kafka. Os marcadores no texto da mensagem não são substituídos: a cópia é enviada tal como foi lida.

Nada é enviado imediatamente. Confira o formulário, altere-o se necessário e envie-o pelo Producer. Consulte [Producer](producer.md).

O botão fica desativado em dois casos, e o motivo é mostrado:

- a chave ou o valor foi mostrado apenas em parte por ser grande demais;
- o texto foi decodificado com perdas por não ser UTF-8 válido. Leia-o como `hex` ou `base64` para enviar uma cópia exata.

Nas conexões com a opção [[ui:common.readOnly]] ativada, o botão fica oculto.

## Redefinir um grupo para uma mensagem {#reset-group-here}

[[ui:browse.details.resetHere]] prepara uma redefinição de offsets para que um consumer group comece a ler a partir desta mensagem.

1. Nos detalhes da mensagem, clique em [[ui:browse.details.resetHere]].
2. Escolha um grupo na lista. Apenas os grupos existentes aparecem.
3. Clique em [[ui:browse.details.goToReset]].

A aba do grupo é aberta com o formulário de redefinição preenchido. Em [[ui:groups.reset.strategy]], fica selecionado [[ui:groups.reset.strategyOffset]], com o offset da mensagem. Em [[ui:groups.reset.scope]], fica selecionado [[ui:groups.reset.scopePartitions]], com o tópico e a partição da mensagem. Nada muda ainda: clique em [[ui:groups.reset.preview]] para ver os novos offsets e, em seguida, aplique a redefinição ali. O grupo não pode ter membros ativos. Consulte [Consumer groups](groups.md).

Nas conexões com a opção [[ui:common.readOnly]] ativada, o botão fica oculto.
