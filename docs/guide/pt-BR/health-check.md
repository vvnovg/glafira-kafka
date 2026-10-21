# Verificação do cluster e notas

O Glafira verifica um cluster com um conjunto de regras integradas e mostra os problemas encontrados ao lado dos objetos: na árvore de conexões, nas listas de tópicos e grupos e nas telas de um tópico ou grupo. Um assistente LLM pode complementar essas constatações com as próprias notas. A verificação apenas lê o cluster e não muda nada nele.

## Executar uma verificação {#run}

Abra [[ui:sidebar.cluster]] ou [[ui:sidebar.findings]] de uma conexão e clique em [[ui:health.check]]. Uma verificação leva até 45 segundos: o Glafira lê os brokers, as partições, as configurações de tópicos (até 2000 tópicos) e os consumer groups (até 500). Se houver mais tópicos ou grupos, se algo não puder ser lido ou se os 45 segundos se esgotarem, o resultado é marcado como [[ui:health.partial]], e a constatação [[ui:health.ruleNames.check.partial]] mostra o que não foi lido.

O Glafira não consulta o cluster por conta própria: uma verificação só é executada quando você clica no botão ou quando um assistente LLM a solicita. O resultado fica guardado até a próxima verificação.

## O que é verificado {#rules}

- Brokers e partições: sem controlador, partições indisponíveis e sub-replicadas, cluster com um único broker, desequilíbrio de leaders, leaders fora da réplica preferida.
- Durabilidade dos tópicos: fator de replicação 1, `min.insync.replicas` maior que o fator de replicação, `min.insync.replicas` igual ao fator de replicação, `min.insync.replicas` igual a 1, eleição de leader não limpa.
- Configuração dos brokers: criação automática de tópicos, fator de replicação padrão 1, `min.insync.replicas` padrão 1, configurações importantes que diferem entre os brokers.
- Consumer groups: grupos vazios com lag, grupos em rebalanceamento, membros sem partições, lag que cresceu desde a verificação anterior.

## Gravidade e ambiente {#severity}

Cada constatação tem uma gravidade: [[ui:health.severity.critical]], [[ui:health.severity.warning]] ou [[ui:health.severity.info]]. Ela depende do ambiente da conexão. Por exemplo, fator de replicação 1 é crítico em prod, um aviso em stage e não é mostrado em dev, test e local. O ambiente é definido no formulário da conexão.

## A aba “Constatações” {#tab}

Na aba [[ui:sidebar.findings]], as constatações são agrupadas por regra; cada uma explica o problema e o que fazer. Filtre por [[ui:health.filterSeverity]], [[ui:health.filterSource]] e [[ui:health.filterKind]] ou pesquise pelo nome. O nome do objeto abre a tela dele.

Um selo com um número na árvore de conexões e nas listas de tópicos e grupos mostra quantas constatações e notas de LLM abertas com gravidade [[ui:health.severity.critical]] ou [[ui:health.severity.warning]] dizem respeito ao objeto. Aponte para o selo para ver os títulos delas.

## Ocultar constatações {#hide}

Se uma constatação é esperada para um objeto específico, por exemplo um tópico de teste com fator de replicação 1 em prod, clique em [[ui:health.hide]]. Uma constatação oculta não conta nos selos e continua oculta nas verificações seguintes. Para ver as ocultas, marque “Mostrar ocultas”; ali você encontra [[ui:health.restore]].

## Notas de LLM {#notes}

Um assistente LLM conectado por MCP (consulte [Assistentes LLM (MCP)](llm-mcp.md#how-it-works)) pode deixar uma nota sobre o cluster, um broker, um tópico ou um grupo. As notas aparecem em [[ui:health.notes]] e nas telas dos objetos, assinadas com o nome do cliente. O texto é mostrado como está, sem formatação nem links.

- [[ui:health.resolve]] e [[ui:health.dismiss]] fecham uma nota; as fechadas ficam em uma seção recolhida.
- [[ui:common.delete]] exclui uma nota, e [[ui:health.deleteAll]] exclui todas as notas da conexão.
- Uma nota é excluída automaticamente 7 dias depois de criada. Uma conexão pode ter no máximo 200 notas abertas.

## Com um assistente LLM {#with-llm}

O assistente executa a mesma verificação com a ferramenta `health_check` e vê as mesmas constatações, inclusive as ocultas, que aparecem marcadas. Se a última verificação tem menos de 5 minutos, o assistente recebe o resultado dela sem consultar o cluster de novo. Peça, por exemplo: “Verifique a saúde do cluster e deixe notas sobre os problemas mais importantes.”
