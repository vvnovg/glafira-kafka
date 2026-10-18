# Producer

Le [[ui:sidebar.producer]] envoie des messages à Kafka : un par un depuis un formulaire, ou en nombre en une seule fois. Ouvrez-le depuis la connexion dans le panneau [[ui:sidebar.connections]], ou avec [[ui:topics.details.produce]] dans l’onglet d’un topic, ce qui préremplit le topic. Chaque connexion a son propre onglet Producer.

Sur les connexions en [[ui:common.readOnly]], le Producer s’ouvre avec l’avertissement [[ui:producer.readOnly]]. Les boutons d’envoi sont masqués, mais les modèles et l’historique restent consultables.

## Envoyer un message {#send-message}

1. Saisissez le [[ui:producer.topic]]. Le champ propose les topics du cluster, hors topics internes.
2. Si nécessaire, saisissez un numéro de [[ui:producer.partition]]. Lorsque le champ affiche [[ui:producer.partitionAuto]], la partition est choisie d’après la clé : les messages ayant la même clé vont dans la même partition, comme avec le client Java. Les messages sans clé vont dans une partition aléatoire.
3. Remplissez les champs [[ui:producer.key]] et [[ui:producer.value]], et choisissez leurs formats. Voir [Formats de la clé et de la valeur](producer.md#key-value-formats).
4. Si nécessaire, ajoutez des en-têtes.
5. Cliquez sur [[ui:producer.send]].

Après un envoi réussi, une ligne sous le formulaire affiche la partition, l’offset et l’heure du message. Lorsque `acks` vaut `0`, le broker ne renvoie pas d’offset, et `—` s’affiche à la place.

Si le numéro de partition est trop grand, ou si le broker ne confirme pas le message dans le délai de la connexion, l’app affiche une erreur. Un topic inexistant provoque aussi une erreur, sauf si le broker crée les topics automatiquement (`auto.create.topics.enable`). Lorsque la création automatique est désactivée, cette erreur peut mettre jusqu’à environ 30 s à apparaître (le délai de la connexion). Les erreurs du formulaire, comme un JSON non valide ou une variable inconnue, sont signalées avant tout envoi.

Sur une connexion prod, l’envoi vous demande de saisir le nom de la connexion et de confirmer avec [[ui:producer.confirmLabel]]. Voir [Environnements et prod](connections.md#environments).

[[ui:producer.clearForm]] vide le formulaire et rétablit les valeurs par défaut des paramètres du producteur. Le topic est conservé.

## Formats de la clé et de la valeur {#key-value-formats}

[[ui:producer.keyFormat]] et [[ui:producer.valueFormat]] définissent comment le texte du champ est converti en octets :

- `string` — le texte tel quel, en UTF-8.
- `json` — le texte est vérifié comme JSON valide et envoyé tel que saisi, sans reformatage.
- `hex` — deux chiffres hexadécimaux par octet, par exemple `48656c6c6f`. Les espaces et les sauts de ligne sont ignorés.
- `base64` — des octets en Base64 standard.
- `null` — aucune clé ou aucune valeur. Le champ de texte est désactivé.

Par défaut, le format est `string` pour la clé et `json` pour la valeur. Un champ de clé vide au format `string` envoie une clé vide, et non une clé absente : tous ces messages ont le même hachage et vont dans une même partition. Pour envoyer un message sans clé, choisissez `null`.

Un message dont la valeur est `null` est un tombstone. Dans un topic avec `cleanup.policy=compact`, il indique à Kafka de supprimer les messages plus anciens ayant la même clé.

## En-têtes {#headers}

Les en-têtes sont des paires nom-valeur envoyées avec le message. Cliquez sur [[ui:producer.headers.add]] pour ajouter une ligne, puis remplissez [[ui:producer.headers.colName]], [[ui:producer.headers.colValue]] et [[ui:producer.headers.colFormat]]. Le bouton `×` supprime la ligne.

Le format de la valeur d’un en-tête est `string` (texte en UTF-8) ou `hex` (octets en chiffres hexadécimaux). Les lignes sans nom ne sont pas envoyées. Kafka autorise plusieurs en-têtes portant le même nom, et ils sont tous envoyés.

Les variables fonctionnent dans les valeurs des en-têtes, mais pas dans leurs noms.

## Variables {#placeholders}

Les variables sont remplacées par des valeurs à chaque envoi. Elles fonctionnent dans la clé, la valeur et les valeurs des en-têtes. La liste s’affiche sous les en-têtes :

- `{{uuid}}` — un nouvel UUID aléatoire, par exemple `0b6f6f2c-3c1e-4a8e-9a54-2f0d6f1d7c21`.
- `{{now}}` — l’heure actuelle en UTC au format ISO 8601 avec millisecondes, par exemple `2026-10-15T12:30:00.000Z`.
- `{{nowMillis}}` — l’heure actuelle en millisecondes depuis le 1970-01-01 UTC.
- `{{seq}}` — le numéro de la copie dans un lot, à partir de 0. Pour un envoi unique, il vaut `0`.
- `{{randomInt 1 100}}` — un entier aléatoire entre les deux nombres, bornes incluses.
- `{{randomString 8}}` — une chaîne aléatoire de lettres latines et de chiffres de la longueur indiquée, jusqu’à 100000.
- `{{env NAME}}` — la valeur de la variable d’environnement `NAME`. Si elle n’est pas définie, rien n’est envoyé et une erreur s’affiche.

Les espaces entre les accolades sont autorisés. Une variable inconnue ou un `}}` manquant interrompt l’envoi avec une erreur. Pour envoyer `{{` comme texte, écrivez `\{{`.

Au format `json`, le JSON est vérifié après le remplacement des variables. Les variables qui produisent du texte doivent être entre guillemets : `{"id": "{{uuid}}"}`. Les nombres, non : `{"n": {{seq}}}`.

`{{env NAME}}` lit l’environnement du processus Glafira. Si vous lancez Glafira depuis le Dock ou le menu Démarrer, les variables définies uniquement dans le profil de votre terminal peuvent manquer.

Lorsqu’un message provient de la commande [Ouvrir dans le Producer](browse.md#open-in-producer) ou de l’historique, chaque `{{` qu’il contient est écrit `\{{`. Le message est alors envoyé exactement tel quel, sans aucun remplacement.

## Modèles {#templates}

Un modèle enregistre tout le formulaire : topic, partition, clé, valeur, en-têtes, formats et paramètres du producteur. Les variables sont enregistrées telles qu’écrites et remplacées à chaque envoi.

Pour enregistrer un modèle, cliquez sur [[ui:producer.templates.saveAs]], saisissez le [[ui:producer.templates.nameLabel]] et cliquez sur [[ui:common.save]]. Le topic peut être vide. Par défaut, le modèle appartient à cette connexion. Cochez [[ui:producer.templates.global]] pour l’afficher sur toutes les connexions ; ces modèles sont marqués [[ui:producer.templates.allSuffix]] dans la liste.

Pour utiliser un modèle, choisissez-le dans la liste [[ui:producer.templates.label]]. Le formulaire est remplacé par le modèle. Vous pouvez modifier le formulaire avant l’envoi ; le modèle n’est pas modifié.

L’enregistrement crée toujours un nouveau modèle. Pour en modifier un, chargez-le, modifiez le formulaire, enregistrez-le sous un nouveau nom et supprimez l’ancien. Pour supprimer le modèle sélectionné, cliquez sur [[ui:producer.templates.delete]] et confirmez avec [[ui:common.delete]].

Les modèles d’une connexion sont supprimés avec elle.

## Historique {#history}

[[ui:producer.history.title]] liste les 100 derniers envois unitaires de cette connexion, du plus récent au plus ancien. Les messages des envois par lot n’y sont pas ajoutés. Chaque ligne affiche l’heure, le topic, la partition et l’offset, ainsi que le début de la clé.

L’historique conserve le message tel qu’il a été envoyé, avec les variables déjà remplacées. [[ui:producer.history.repeat]] le charge dans le formulaire avec ses paramètres du producteur. Rien n’est envoyé tant que vous ne cliquez pas sur [[ui:producer.send]]. La copie est envoyée exactement telle qu’elle est conservée : par exemple, le même UUID est renvoyé.

Une clé, une valeur ou une valeur d’en-tête de plus de 64 KB n’est conservée qu’en partie. Pour ces lignes, [[ui:producer.history.repeat]] est désactivé.

L’historique est stocké en clair sur cet ordinateur, y compris les valeurs de `{{env NAME}}`. N’envoyez pas de secrets via le Producer si cela pose problème. L’historique d’une connexion est supprimé avec elle.

## N copies du formulaire {#batch-copies}

[[ui:producer.batch.legend]], sous le formulaire, envoie de nombreux messages en une seule exécution. En mode [[ui:producer.batch.modeCopies]], le formulaire est envoyé le nombre de fois indiqué.

1. Remplissez le formulaire comme pour un envoi unique.
2. Sélectionnez [[ui:producer.batch.modeCopies]] et saisissez le nombre de [[ui:producer.batch.count]], de 1 à 1000000.
3. Si nécessaire, définissez le [[ui:producer.batch.rate]]. Voir [Débit d’envoi](producer.md#batch-rate).
4. Cliquez sur [[ui:producer.batch.start]].

Les variables sont remplacées séparément pour chaque copie. `{{seq}}` est le numéro de la copie, de 0 à N−1 : chaque message peut ainsi avoir sa propre clé, par exemple `order-{{seq}}`. Les erreurs de variables sont vérifiées sur la première copie avant le début de l’exécution.

Pendant l’exécution, le panneau indique combien de messages ont été envoyés et combien ont échoué. À la fin, il affiche [[ui:producer.batch.done]]. Les erreurs sont listées avec le numéro de la copie, compté à partir de 1. La liste affiche les 20 premières erreurs ; s’il y en a davantage, une note en dessous indique le total.

[[ui:producer.batch.cancel]] arrête l’exécution. Les messages qui n’ont pas encore quitté l’app ne sont pas envoyés, et le panneau indique leur nombre. L’app attend jusqu’à 2 secondes que le broker confirme les messages déjà envoyés. Les messages toujours non confirmés comptent comme des erreurs, car on ignore s’ils ont été écrits. Fermer l’onglet Producer arrête aussi l’exécution.

Sur une connexion prod, l’exécution demande le nom de la connexion et se confirme avec [[ui:producer.batch.confirmLabel]].

## Envoi depuis un fichier {#batch-file}

En mode [[ui:producer.batch.modeFile]], chaque ligne ou enregistrement d’un fichier devient un message. Les fichiers pris en charge sont JSON Lines (`.jsonl`, `.ndjson`) et CSV (`.csv`).

1. Saisissez le [[ui:producer.topic]] dans le formulaire au-dessus. Tous les messages vont dans ce topic.
2. Sélectionnez [[ui:producer.batch.modeFile]] et cliquez sur [[ui:producer.batch.chooseFile]].
3. Cliquez sur [[ui:producer.batch.start]].

La clé, la valeur, les en-têtes et la partition du formulaire ne sont pas utilisés, et les variables ne sont pas remplacées. Les paramètres du producteur du formulaire s’appliquent.

En JSON Lines, chaque ligne est un objet JSON, par exemple `{"key": "k1", "value": {"id": 1}, "headers": {"source": "test"}, "partition": 0}`. Tous les champs sont facultatifs :

- `key` et `value` — une chaîne JSON est envoyée sous forme de texte, sans guillemets. Toute autre valeur JSON, comme un objet ou un nombre, est envoyée comme texte JSON. `null` ou un champ absent signifie aucune clé ou aucune valeur.
- `headers` — un objet à valeurs de type chaîne.
- `partition` — un numéro de partition. Sans lui, la partition est choisie d’après la clé.

Les lignes vides sont ignorées. Le fichier doit être en UTF-8.

En CSV, la première ligne contient les noms des colonnes. La colonne `value` est obligatoire ; les colonnes `key`, `partition` et `header.NAME` sont facultatives, par exemple `header.source`. Une cellule `key` vide signifie aucune clé, et une cellule d’en-tête vide signifie l’absence de cet en-tête. Une cellule `value` vide envoie une valeur vide, et non `null` : un tombstone ne peut donc pas être envoyé depuis un CSV.

Le fichier est lu en flux : le nombre total n’est pas connu à l’avance et s’affiche sous la forme `—`. Une ligne ou un enregistrement de plus de 16 MB, un JSON non valide ou une partition négative compte comme une erreur pour cette ligne, et l’exécution passe à la ligne suivante. Les erreurs sont affichées avec le numéro de ligne.

## Débit d’envoi {#batch-rate}

[[ui:producer.batch.rate]] limite un lot au nombre de messages par seconde indiqué. Saisissez un entier supérieur ou égal à 1. Un champ vide signifie [[ui:producer.batch.rateUnlimited]].

Les messages sont répartis uniformément dans le temps : à 10 messages par seconde, un message part environ toutes les 100 ms. Utilisez la limite pour ne pas surcharger un cluster partagé, ou pour simuler une charge régulière.

Sans limite, l’app envoie aussi vite que le broker accepte. Le débit s’applique aux deux modes d’envoi par lot.

## Paramètres du producteur {#producer-settings}

Les [[ui:producer.settings.legend]] s’appliquent aux envois unitaires et aux lots. Ils sont enregistrés dans les modèles et l’historique.

`acks` définit combien de répliques doivent confirmer une écriture :

- `all` — la valeur par défaut. Le leader attend toutes les répliques synchronisées. C’est le choix le plus sûr. Si moins de répliques sont synchronisées que ne l’exige le paramètre de topic `min.insync.replicas`, l’écriture échoue.
- `1` — seul le leader confirme. Plus rapide, mais un message peut être perdu si le leader tombe en panne avant que les followers ne l’aient copié.
- `0` — le producteur n’attend aucune confirmation. Des messages peuvent être perdus, et aucun offset n’est renvoyé.

[[ui:producer.settings.compression]] compresse les batchs de messages : `none` (par défaut), `gzip`, `snappy`, `lz4` ou `zstd`. La compression économise le trafic réseau et l’espace disque au prix d’un peu de CPU. `zstd` et `lz4` sont des choix courants. Pour un seul petit message, le gain est faible.

[[ui:producer.settings.idempotence]] définit `enable.idempotence`. Cette option évite les doublons lorsque le client renvoie un message après une erreur réseau. Elle nécessite que `acks` vaille `all` ; avec d’autres valeurs, l’app affiche une erreur et n’envoie rien.
