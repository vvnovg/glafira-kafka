# Producer

A tela [[ui:sidebar.producer]] envia mensagens para o Kafka: uma de cada vez, a partir de um formulário, ou muitas de uma vez. Abra-a pela conexão no painel [[ui:sidebar.connections]] ou com [[ui:topics.details.produce]] na aba de um tópico, o que preenche o tópico. Cada conexão tem sua própria aba Producer.

Nas conexões com a opção [[ui:common.readOnly]] ativada, o Producer abre com o aviso [[ui:producer.readOnly]]. Os botões de envio ficam ocultos, mas ainda é possível consultar os modelos e o histórico.

## Enviar uma mensagem {#send-message}

1. Em [[ui:producer.topic]], informe o tópico. O campo sugere os tópicos do cluster, sem os internos.
2. Se quiser, informe o número da partição em [[ui:producer.partition]]. Quando o campo mostra [[ui:producer.partitionAuto]], a partição é escolhida pela chave: mensagens com a mesma chave vão para a mesma partição, como no cliente Java. Mensagens sem chave vão para uma partição aleatória.
3. Preencha os campos [[ui:producer.key]] e [[ui:producer.value]] e escolha os formatos deles. Consulte [Formatos de chave e valor](producer.md#key-value-formats).
4. Se quiser, adicione cabeçalhos.
5. Clique em [[ui:producer.send]].

Depois de um envio bem-sucedido, uma linha abaixo do formulário mostra a partição, o offset e o horário da mensagem. Com `acks` igual a `0`, o broker não retorna um offset, e aparece `—` no lugar.

Se o número da partição for grande demais, ou se o broker não confirmar a mensagem dentro do tempo limite da conexão, o aplicativo mostra um erro. Um tópico inexistente também gera erro, a menos que o broker crie tópicos automaticamente (`auto.create.topics.enable`). Com a criação automática desativada, esse erro pode levar até cerca de 30 s para aparecer (o tempo limite da conexão). Os erros no formulário, como um JSON inválido ou um marcador desconhecido, aparecem antes de qualquer envio.

Em uma conexão prod, o envio pede que você digite o nome da conexão e confirme com [[ui:producer.confirmLabel]]. Consulte [Ambientes e prod](connections.md#environments).

[[ui:producer.clearForm]] esvazia o formulário e restaura os padrões das configurações do produtor. O tópico permanece.

## Formatos de chave e valor {#key-value-formats}

[[ui:producer.keyFormat]] e [[ui:producer.valueFormat]] definem como o texto do campo é convertido em bytes:

- `string` — o texto como está, em UTF-8.
- `json` — o texto é verificado como JSON válido e enviado como foi digitado, sem reformatação.
- `hex` — dois dígitos hexadecimais por byte, por exemplo `48656c6c6f`. Espaços e quebras de linha são ignorados.
- `base64` — bytes em Base64 padrão.
- `null` — nenhuma chave ou nenhum valor. O campo de texto fica desativado.

O padrão é `string` para a chave e `json` para o valor. Um campo de chave vazio com `string` envia uma chave vazia, e não uma chave ausente: todas essas mensagens recebem o mesmo hash e vão para uma só partição. Para enviar uma mensagem sem chave, escolha `null`.

Uma mensagem com valor `null` é um tombstone. Em um tópico com `cleanup.policy=compact`, ela indica ao Kafka que remova as mensagens mais antigas com a mesma chave.

## Cabeçalhos {#headers}

Os cabeçalhos são pares de nome e valor enviados com a mensagem. Clique em [[ui:producer.headers.add]] para adicionar uma linha e preencha [[ui:producer.headers.colName]], [[ui:producer.headers.colValue]] e [[ui:producer.headers.colFormat]]. O botão `×` remove a linha.

O formato do valor de um cabeçalho é `string` (texto em UTF-8) ou `hex` (bytes como dígitos hexadecimais). As linhas com nome vazio não são enviadas. O Kafka permite vários cabeçalhos com o mesmo nome, e todos eles são enviados.

Os marcadores funcionam nos valores dos cabeçalhos, mas não nos nomes.

## Marcadores {#placeholders}

Os marcadores são substituídos por valores a cada envio. Eles funcionam na chave, no valor e nos valores dos cabeçalhos. A lista aparece abaixo dos cabeçalhos:

- `{{uuid}}` — um novo UUID aleatório, por exemplo `0b6f6f2c-3c1e-4a8e-9a54-2f0d6f1d7c21`.
- `{{now}}` — o horário atual em UTC no formato ISO 8601 com milissegundos, por exemplo `2026-10-15T12:30:00.000Z`.
- `{{nowMillis}}` — o horário atual em milissegundos desde 1970-01-01 UTC.
- `{{seq}}` — o número da cópia em um lote, a partir de 0. Em um envio individual, é `0`.
- `{{randomInt 1 100}}` — um inteiro aleatório entre os dois números, ambos incluídos.
- `{{randomString 8}}` — uma string aleatória de letras latinas e dígitos com o comprimento indicado, até 100000.
- `{{env NAME}}` — o valor da variável de ambiente `NAME`. Se ela não estiver definida, nada é enviado e um erro aparece.

Espaços entre as chaves duplas são permitidos. Um marcador desconhecido ou um `}}` ausente interrompe o envio com um erro. Para enviar `{{` como texto, escreva `\{{`.

Com o formato `json`, o JSON é verificado depois que os marcadores são substituídos. Os marcadores que geram texto precisam de aspas: `{"id": "{{uuid}}"}`. Os números não: `{"n": {{seq}}}`.

`{{env NAME}}` lê o ambiente do processo do Glafira. Se você iniciar o Glafira pelo Dock ou pelo menu Iniciar, as variáveis definidas apenas no perfil do terminal podem estar ausentes.

Quando uma mensagem vem de [Abrir no Producer](browse.md#open-in-producer) ou do histórico, cada `{{` nela é escrito como `\{{`. Assim, a mensagem é enviada exatamente como era, sem substituir nada.

## Modelos {#templates}

Um modelo salva o formulário inteiro: tópico, partição, chave, valor, cabeçalhos, formatos e configurações do produtor. Os marcadores são salvos como foram escritos e substituídos a cada envio.

Para salvar um modelo, clique em [[ui:producer.templates.saveAs]], digite o nome em [[ui:producer.templates.nameLabel]] e clique em [[ui:common.save]]. O tópico pode ficar vazio. Por padrão, o modelo pertence a esta conexão. Marque [[ui:producer.templates.global]] para mostrá-lo em todas as conexões; esses modelos aparecem na lista com [[ui:producer.templates.allSuffix]].

Para usar um modelo, escolha-o na lista [[ui:producer.templates.label]]. O formulário é substituído pelo modelo. É possível editar o formulário antes de enviar; o modelo não muda.

Salvar sempre cria um novo modelo. Para alterar um modelo, carregue-o, edite o formulário, salve-o com um novo nome e exclua o antigo. Para excluir o modelo selecionado, clique em [[ui:producer.templates.delete]] e confirme com [[ui:common.delete]].

Os modelos de uma conexão são excluídos junto com ela.

## Histórico {#history}

A seção [[ui:producer.history.title]] lista os últimos 100 envios individuais desta conexão, dos mais recentes para os mais antigos. As mensagens dos envios em lote não são adicionadas. Cada linha mostra o horário, o tópico, a partição e o offset, e o início da chave.

O histórico guarda a mensagem tal como foi enviada, com os marcadores já substituídos. O botão [[ui:producer.history.repeat]] carrega a mensagem no formulário junto com as configurações do produtor. Nada é enviado até você clicar em [[ui:producer.send]]. A cópia é enviada exatamente como foi guardada: por exemplo, o mesmo UUID é enviado de novo.

Uma chave, um valor ou um valor de cabeçalho com mais de 64 KB é guardado apenas em parte. Nessas linhas, [[ui:producer.history.repeat]] fica desativado.

O histórico é guardado neste computador em texto puro, inclusive os valores de `{{env NAME}}`. Se isso for importante, não envie segredos pelo Producer. O histórico de uma conexão é excluído junto com ela.

## N cópias do formulário {#batch-copies}

A seção [[ui:producer.batch.legend]], abaixo do formulário, envia muitas mensagens em uma só execução. No modo [[ui:producer.batch.modeCopies]], ela envia o formulário o número de vezes indicado.

1. Preencha o formulário como para um envio individual.
2. Selecione [[ui:producer.batch.modeCopies]] e informe o número em [[ui:producer.batch.count]], de 1 a 1000000.
3. Se quiser, defina um limite em [[ui:producer.batch.rate]]. Consulte [Taxa de envio](producer.md#batch-rate).
4. Clique em [[ui:producer.batch.start]].

Os marcadores são substituídos separadamente em cada cópia. `{{seq}}` é o número da cópia, de 0 a N−1, então cada mensagem pode receber a própria chave, por exemplo `order-{{seq}}`. Os erros de marcadores são verificados na primeira cópia, antes do início da execução.

Durante a execução, o painel mostra quantas mensagens foram enviadas e quantas falharam. No final, ele mostra [[ui:producer.batch.done]]. Os erros são listados com o número da cópia, contado a partir de 1. A lista mostra os primeiros 20 erros; se houver mais, uma nota abaixo informa o total.

[[ui:producer.batch.cancel]] interrompe a execução. As mensagens que ainda não saíram do aplicativo não são enviadas, e o painel mostra a quantidade delas. O aplicativo espera até 2 segundos para que o broker confirme as mensagens já enviadas. As que continuam sem confirmação contam como erros, porque não se sabe se foram gravadas. Fechar a aba Producer também interrompe a execução.

Em uma conexão prod, a execução pede o nome da conexão e é confirmada com [[ui:producer.batch.confirmLabel]].

## Enviar a partir de um arquivo {#batch-file}

Com o modo [[ui:producer.batch.modeFile]] selecionado, cada linha ou registro de um arquivo vira uma mensagem. Os arquivos suportados são JSON Lines (`.jsonl`, `.ndjson`) e CSV (`.csv`).

1. Em [[ui:producer.topic]], no formulário acima, informe o tópico. Todas as mensagens vão para esse tópico.
2. Selecione [[ui:producer.batch.modeFile]] e clique em [[ui:producer.batch.chooseFile]].
3. Clique em [[ui:producer.batch.start]].

A chave, o valor, os cabeçalhos e a partição do formulário não são usados, e os marcadores não são substituídos. Valem as configurações do produtor do formulário.

Em JSON Lines, cada linha é um objeto JSON, por exemplo `{"key": "k1", "value": {"id": 1}, "headers": {"source": "test"}, "partition": 0}`. Todos os campos são opcionais:

- `key` e `value` — uma string JSON é enviada como o próprio texto, sem aspas. Qualquer outro valor JSON, como um objeto ou um número, é enviado como texto JSON. `null` ou um campo ausente significa nenhuma chave ou nenhum valor.
- `headers` — um objeto com valores string.
- `partition` — um número de partição. Sem ele, a partição é escolhida pela chave.

As linhas vazias são ignoradas. O arquivo precisa estar em UTF-8.

Em CSV, a primeira linha traz os nomes das colunas. A coluna `value` é obrigatória; as colunas `key`, `partition` e `header.NAME` são opcionais, por exemplo `header.source`. Uma célula `key` vazia significa nenhuma chave, e uma célula de cabeçalho vazia significa que esse cabeçalho não existe. Uma célula `value` vazia envia um valor vazio, não `null`, então não é possível enviar um tombstone a partir de CSV.

O arquivo é lido como um fluxo, então o total não é conhecido de antemão e aparece como `—`. Uma linha ou registro com mais de 16 MB, um JSON inválido ou uma partição negativa conta como erro dessa linha, e a execução continua na linha seguinte. Os erros aparecem com o número da linha.

## Taxa de envio {#batch-rate}

O campo [[ui:producer.batch.rate]] limita um lote ao número indicado de mensagens por segundo. Informe um inteiro igual ou maior que 1. Um campo vazio significa [[ui:producer.batch.rateUnlimited]].

As mensagens são distribuídas igualmente ao longo do tempo: a 10 mensagens por segundo, sai uma mensagem a cada 100 ms, aproximadamente. Use o limite para não sobrecarregar um cluster compartilhado ou para simular uma carga constante.

Sem limite, o aplicativo envia tão rápido quanto o broker aceita. A taxa vale para os dois modos de lote.

## Configurações do produtor {#producer-settings}

As [[ui:producer.settings.legend]] valem para os envios individuais e para os lotes. Elas são salvas nos modelos e no histórico.

`acks` define quantas réplicas precisam confirmar uma gravação:

- `all` — o padrão. O leader espera todas as réplicas sincronizadas. É a opção mais segura. Se houver menos réplicas sincronizadas do que exige a configuração `min.insync.replicas` do tópico, a gravação falha.
- `1` — só o leader confirma. É mais rápido, mas uma mensagem pode se perder se o leader falhar antes que os followers a copiem.
- `0` — o produtor não espera confirmação. As mensagens podem se perder, e nenhum offset é retornado.

O campo [[ui:producer.settings.compression]] define a compressão dos batches de mensagens: `none` (o padrão), `gzip`, `snappy`, `lz4` ou `zstd`. A compressão economiza tráfego de rede e espaço em disco ao custo de um pouco de CPU. `zstd` e `lz4` são escolhas comuns. Para uma única mensagem pequena, o ganho é pequeno.

A opção [[ui:producer.settings.idempotence]] define `enable.idempotence`. Ela evita duplicatas quando o cliente repete um envio após um erro de rede. Ela exige `acks` igual a `all`; com outros valores, o aplicativo mostra um erro e não envia nada.
