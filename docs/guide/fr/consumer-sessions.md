# Sessions consumer

Une session consumer lit des topics en tant que véritable membre d’un consumer group.

## Qu’est-ce qu’une session {#what-is}

La [lecture](browse.md) parcourt les messages sans rejoindre de groupe et ne modifie rien dans le cluster. Une session, elle, rejoint un groupe avec le `group.id` indiqué, reçoit des partitions de Kafka et peut commiter des offsets. Utilisez-la pour lire ce qu’un groupe lirait, pour déplacer les offsets d’un groupe en commitant, ou pour observer le rééquilibrage d’un groupe.

Une session a un effet sur le cluster. Lorsqu’elle rejoint ou quitte le groupe, Kafka redistribue les partitions du groupe, et les autres membres subissent un rééquilibrage. Les commits modifient les offsets du groupe, que ses applications utiliseront.

Les sessions ne sont pas disponibles sur les connexions en [[ui:common.readOnly]] : l’onglet affiche [[ui:consumer.readOnlyBlocked]]. Si une connexion passe en lecture seule pendant une session, la session quitte le groupe.

## Lancer une session {#start-session}

Ouvrez une session de l’une de ces trois façons :

- [[ui:sidebar.consumer]] sous la connexion dans le panneau [[ui:sidebar.connections]] — un formulaire vide ;
- [[ui:topics.details.consume]] dans l’onglet d’un topic — le topic est prérempli ;
- [[ui:groups.details.join]] dans l’onglet d’un groupe — le groupe et les topics pour lesquels il a des commits sont préremplis.

Chaque fois, un nouvel onglet s’ouvre : plusieurs sessions peuvent donc tourner en même temps.

Dans [[ui:consumer.form.groupTopics]] :

- `group.id` — le groupe à rejoindre. Si vous n’en saisissez pas, l’app propose un nouveau nom, par exemple `glafira-kafka-anna-1a2b3c`. Un nouveau groupe n’a aucun commit.
- [[ui:consumer.form.topicsLabel]] — des noms de topics séparés par des virgules ou des espaces. Ou cochez [[ui:consumer.form.useRegex]] et saisissez une [[ui:consumer.form.patternLabel]], par exemple `orders\..*`. L’expression est comparée à partir du début du nom : `^` est ajouté s’il manque. Les topics correspondants créés ultérieurement sont ajoutés en quelques minutes (lors de l’actualisation des métadonnées), ce qui provoque un rééquilibrage.
- `auto.offset.reset` — le point de départ dans les partitions pour lesquelles le groupe n’a pas d’offset commité : [[ui:consumer.form.resetLatest]] (par défaut) ou [[ui:consumer.form.resetEarliest]]. Là où le groupe a un offset commité, la lecture reprend à partir de celui-ci.
- [[ui:consumer.form.autoCommit]] — commite automatiquement les offsets lus. Désactivé par défaut. Voir [Commits](consumer-sessions.md#commits).
- `isolation.level` — `read_committed` (par défaut) n’affiche que les messages des transactions validées. `read_uncommitted` affiche aussi les messages des transactions ouvertes et annulées.

Dans [[ui:consumer.form.formatLegend]], choisissez [[ui:consumer.form.keyFormat]] et [[ui:consumer.form.valueFormat]], comme pour la lecture. Voir [Formats](browse.md#formats). [[ui:consumer.form.buffer]] définit combien de messages le tableau conserve, de 1 à 100000 ; la valeur par défaut est 5000.

Les filtres fonctionnent comme pour la lecture. Voir [Filtres](browse.md#filters).

Cliquez sur [[ui:consumer.actions.connect]]. Sur une connexion prod, l’app vous demande de saisir le nom de la connexion et de confirmer avec [[ui:consumer.dialog.confirm]]. Tant que la session tourne, le formulaire ne peut pas être modifié.

## Lecture {#reading}

La ligne d’état à côté des boutons affiche [[ui:consumer.status.connecting]], puis [[ui:consumer.status.inGroup]]. Elle indique aussi combien de messages ont été lus, combien correspondent aux filtres et combien figurent dans le tableau. Si la connexion au broker est perdue, la session se reconnecte d’elle-même et l’indique dans la ligne d’état.

Les messages apparaissent dans le tableau au fur et à mesure, et la session continue de lire les nouveaux messages jusqu’à ce que vous l’arrêtiez. Lorsque le tableau est plein, les messages les plus anciens sont supprimés. Cliquez sur une ligne pour voir les détails du message, comme pour la lecture. Voir [Détails du message](browse.md#message-details). Cochez `UTC` pour afficher les heures en UTC.

La session ne lit que les partitions que Kafka lui a attribuées. Si le groupe a d’autres membres, ceux-ci lisent les autres partitions. Si le groupe a plus de membres que de partitions, la session peut ne recevoir aucune partition.

[[ui:consumer.disconnect]] quitte le groupe, et les autres membres reçoivent ses partitions. Fermer l’onglet de la session quitte aussi le groupe. Une fois la session terminée, la ligne d’état affiche [[ui:consumer.status.disconnected]].

Si un topic n’existe pas, la session s’arrête avec une erreur et affiche les noms à vérifier.

## Commits {#commits}

Un commit enregistre un offset pour le groupe. Le prochain membre qui lit la partition, ou la même application après un redémarrage, commence à partir de cet offset.

Avec [[ui:consumer.form.autoCommit]] activé, la session commite les offsets lus en arrière-plan toutes les quelques secondes, puis une dernière fois en quittant le groupe. Les messages masqués par les filtres comptent comme lus.

Sans commit automatique, la session ne commite rien d’elle-même. Utilisez les boutons :

- [[ui:consumer.actions.commitSelected]] — commite l’offset qui suit le message sélectionné, uniquement dans sa partition. Le groupe reprendra au message suivant. Si le message sélectionné est plus ancien que le commit actuel, l’offset recule.
- [[ui:consumer.actions.commitAll]] — commite la position actuelle dans chaque partition attribuée. Cela inclut les messages masqués par les filtres et ceux déjà supprimés du tableau.

Après un commit, une ligne affiche les offsets commités sous la forme `topic:partition → offset`. Sur une connexion prod, chaque commit demande le nom de la connexion.

Si la session ne répond pas à temps, l’app avertit qu’on ignore si le commit a été appliqué. Vérifiez les offsets dans l’onglet du groupe. Voir [Détails du groupe](groups.md#group-details).

## Mettre des partitions en pause {#pause-partitions}

Le panneau à droite du tableau liste les partitions attribuées à la session sous la forme `topic:partition`. Cliquez sur [[ui:consumer.assignment.pause]] à côté d’une partition pour arrêter sa lecture, et sur [[ui:consumer.assignment.resume]] pour la reprendre. Une partition en pause est marquée [[ui:consumer.assignment.paused]].

La pause ne fait pas quitter le groupe : il n’y a donc pas de rééquilibrage. La partition reste attribuée à la session, et les autres membres ne la lisent pas tant qu’elle est en pause. La pause ne modifie rien dans le cluster et ne demande aucune confirmation.

Après un rééquilibrage, les partitions nouvellement attribuées démarrent sans pause. Lorsque la session quitte le groupe, toutes les pauses prennent fin.

## Journal des rééquilibrages {#rebalance-log}

Le [[ui:consumer.assignment.logTitle]], sous la liste des partitions, enregistre chaque changement d’attribution, du plus récent au plus ancien. Chaque entrée comporte une heure et l’un de ces trois types : partitions attribuées à la session, partitions qui lui sont retirées, ou erreur de rééquilibrage avec son texte.

Un rééquilibrage a lieu lorsqu’un membre rejoint ou quitte le groupe, lorsque les topics souscrits changent, ou lorsqu’un membre ne répond plus à temps. Pour l’observer, lancez une deuxième session avec le même `group.id` dans un autre onglet : les partitions sont réparties entre les deux sessions, et les deux journaux le montrent.

Le journal conserve les 200 dernières entrées. Avant le premier rééquilibrage, il affiche [[ui:consumer.assignment.logEmpty]].

## Limites {#limits}

- Les sessions ne sont pas disponibles sur les connexions en [[ui:common.readOnly]].
- Une session ne peut pas démarrer à partir d’un offset choisi. Pour définir le point de départ, réinitialisez d’abord les offsets du groupe : voir [Réinitialiser les offsets](groups.md#reset-offsets). La réinitialisation exige un groupe sans membres : déconnectez les sessions de ce groupe avant de réinitialiser.
- Le tableau conserve au plus 100000 messages. Les messages plus anciens sont supprimés, mais comptent tout de même comme lus.
- Les très grandes clés et valeurs ne sont affichées qu’en partie, comme pour la lecture.
- Ne rejoignez pas le groupe d’une application en cours d’exécution sans le vouloir. La session prend des partitions à l’application, qui ne les traite pas tant que la session les détient. Les commits de la session, y compris les commits automatiques, déplacent les offsets de l’application.
