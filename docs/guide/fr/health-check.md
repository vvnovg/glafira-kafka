# Vérification du cluster et remarques

Glafira vérifie un cluster à l’aide de règles intégrées et affiche les problèmes trouvés à côté des objets : dans l’arborescence des connexions, dans les listes de topics et de groupes et sur les écrans d’un topic ou d’un groupe. Avec Glafira Pro, un assistant LLM peut compléter ces constats par ses propres remarques. La vérification ne fait que lire le cluster et n’y change rien.

## Lancer une vérification {#run}

Ouvrez [[ui:sidebar.cluster]] ou [[ui:sidebar.findings]] d’une connexion et cliquez sur [[ui:health.check]]. Une vérification dure jusqu’à 45 secondes : Glafira lit les brokers, les partitions, les configurations de topics (jusqu’à 2000 topics) et les consumer groups (jusqu’à 500). Sur les grands clusters, le résultat est marqué [[ui:health.partial]] et le constat [[ui:health.ruleNames.check.partial]] indique ce qui n’a pas été lu.

Glafira n’interroge pas le cluster d’elle-même : une vérification ne se lance que lorsque vous cliquez sur le bouton ou qu’un assistant LLM la demande. Le résultat est conservé jusqu’à la vérification suivante.

## Ce qui est vérifié {#rules}

- Brokers et partitions : absence de contrôleur, partitions indisponibles et sous-répliquées, cluster à un seul broker, déséquilibre des leaders, leaders hors de la réplique préférée.
- Durabilité des topics : facteur de réplication 1, `min.insync.replicas` supérieur ou égal au facteur de réplication ou égal à 1, élection de leader non propre.
- Configuration des brokers : création automatique de topics, facteur de réplication et `min.insync.replicas` par défaut, réglages importants qui diffèrent d’un broker à l’autre.
- Consumer groups : groupes vides avec du lag, groupes en rééquilibrage, membres sans partition, lag qui a augmenté depuis la vérification précédente.

## Gravité et environnement {#severity}

Chaque constat a une gravité : [[ui:health.severity.critical]], [[ui:health.severity.warning]] ou [[ui:health.severity.info]]. Elle dépend de l’environnement de la connexion. Par exemple, un facteur de réplication de 1 est critique en prod, un avertissement en stage et n’est pas affiché en dev, test et local. L’environnement se règle dans le formulaire de connexion.

## L’onglet « Constats » {#tab}

Dans l’onglet [[ui:sidebar.findings]], les constats sont regroupés par règle ; chacun explique le problème et ce qu’il faut faire. Filtrez-les par [[ui:health.filterSeverity]], [[ui:health.filterSource]] et [[ui:health.filterKind]], ou recherchez par nom. Le nom de l’objet ouvre son écran.

Un badge avec un nombre dans l’arborescence des connexions et dans les listes de topics et de groupes indique combien de constats critiques et d’avertissements concernent l’objet.

## Masquer des constats {#hide}

Si un constat est attendu pour un objet précis, par exemple un topic de test avec un facteur de réplication de 1 en prod, cliquez sur [[ui:health.hide]]. Un constat masqué n’est pas compté dans les badges et reste masqué lors des vérifications suivantes. Pour voir les constats masqués, cochez « Afficher les masqués » ; vous y trouverez [[ui:health.restore]].

## Remarques LLM {#notes}

Avec Glafira Pro, un assistant LLM connecté par MCP (voir [Assistants LLM (MCP)](llm-mcp.md#how-it-works)) peut laisser une remarque sur le cluster, un broker, un topic ou un groupe. Les remarques apparaissent dans [[ui:health.notes]] et sur les écrans des objets, signées du nom du client. Le texte est affiché tel quel, sans mise en forme ni liens.

- [[ui:health.resolve]] et [[ui:health.dismiss]] ferment une remarque ; les remarques fermées restent dans une section repliée.
- [[ui:common.delete]] supprime une remarque, [[ui:health.deleteAll]] supprime toutes les remarques de la connexion.
- Une remarque est supprimée automatiquement 7 jours après sa création. Une connexion peut avoir au plus 200 remarques ouvertes.

Sans Pro, un assistant peut toujours lancer la vérification et lire les constats, mais il ne peut pas laisser de remarques.

## Avec un assistant LLM {#with-llm}

L’assistant lance la même vérification avec l’outil `health_check` et voit les mêmes constats, y compris les constats masqués, qui sont signalés. Si la dernière vérification date de moins de 5 minutes, l’assistant reçoit son résultat sans interroger à nouveau le cluster. Demandez par exemple : « Vérifie l’état du cluster et laisse des remarques sur les problèmes les plus importants. »
