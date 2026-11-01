# Journal des modifications

Version française de [CHANGELOG.md](../../CHANGELOG.md), qui contient aussi les versions antérieures. La section d’une version s’affiche dans « À propos » comme notes de mise à jour.

## 0.5.5
- MCP : `null` dans les arguments des outils n’est plus retiré que là où le paramètre ne l’accepte pas. Là où `null` est une valeur valide, il parvient tel quel à l’outil. Les outils actuels se comportent comme avant ; la modification prépare les paramètres où `null` signifiera « supprimer la valeur ».

## 0.5.4
- MCP : un client avec « Jeu compact » activé reçoit les instructions du serveur sans les paragraphes sur les propositions de modification et les remarques. À la place, elles indiquent que ce client n’a pas ces outils et qu’une modification nécessaire doit être décrite dans le chat. Auparavant, le modèle cherchait des outils qu’il ne voyait pas.

## 0.5.3
- Erreurs TLS : lorsque le broker ferme la connexion pendant la négociation TLS, le message n’affirme plus que le port n’a pas de TLS. Il indique que le port n’a peut-être pas de TLS ou que le certificat client n’a pas été accepté, ce qui est le cas habituel avec mTLS.
- MCP fonctionne mieux avec les modèles locaux (Qwen, gpt-oss et autres) :
  - les schémas de paramètres des outils ne contiennent plus de références `$ref`, que certains serveurs (vLLM avec l’analyseur `qwen3_coder`) transmettaient aux outils sous forme de chaînes ;
  - un objet imbriqué envoyé sous forme de chaîne JSON est accepté, les valeurs `null` et les clés vides sont ignorées ; si les arguments ne conviennent toujours pas, l’erreur indique les arguments attendus ;
  - lorsqu’une fenêtre de temps ne contient aucun message, `read_messages` conseille de vérifier le fuseau horaire et d’élargir la fenêtre ; les descriptions des outils expliquent comment transmettre une heure avec un fuseau horaire et comment filtrer sur une seule clé exacte.
- « Paramètres » → « Assistants LLM (MCP) » :
  - « Taille maximale de réponse » : 100 KB (par défaut), 48 KB ou 24 KB — pour un modèle local au petit contexte, choisissez une valeur plus basse ;
  - « Jeu compact » pour chaque client approuvé : sans les outils de remarques et de propositions de modification, ce qui économise environ 1 000 jetons de contexte.
- Guide : un nouveau chapitre, « Modèles locaux » — où le modèle peut fonctionner, résultats de Qwen3.6 et Qwen3.8 sur un Mac, matériel, Ollama, configuration de Qwen Code et de Claude Code, et vérification du résultat.

## 0.5.2
- Masquage plus complet des messages lus par un assistant LLM :
  - dans le texte, les valeurs des cookies et des en-têtes `Authorization` (y compris `Proxy-Authorization`) sont masquées jusqu’à la fin de la ligne : tous les cookies, les paramètres Digest et les signatures AWS sont cachés ;
  - dans les paires `name=value`, les noms écrits dans n’importe quel alphabet sont reconnus, par exemple un nom de champ de la liste propre à la connexion écrit en cyrillique ;
  - les numéros de carte collés à des lettres (`id4111…`) ou écrits avec des groupements inhabituels sont détectés, de même que les numéros de carte négatifs et décimaux dans le JSON.
- Topic sans droit Describe : l’onglet « Messages » et les outils `read_messages`, `summarize_messages` et `get_group_commit_history` indiquent « Droits insuffisants pour effectuer l’opération » au lieu de « introuvable ».
- `get_group_commit_history` : si l’historique des commits ne tient pas dans 100 KB même sans séries de commits, l’assistant reçoit, au lieu d’un historique vide, une erreur « réponse trop volumineuse » qui lui conseille de restreindre la requête. `summarize_messages` consomme moins de mémoire avec de grandes clés.
- Formulaire de connexion :
  - lorsque deux formulaires de connexion étaient ouverts, le niveau d’accès LLM choisi dans l’un d’eux ne s’affichait pas, bien qu’il soit correctement enregistré ; désormais chaque formulaire affiche le sien ;
  - les lecteurs d’écran annoncent l’avertissement indiquant que le contenu des messages d’un cluster de production sera envoyé au fournisseur LLM ;
  - si le statut MCP n’a pas pu être chargé, une note explique pourquoi « Métadonnées et messages » ne peut pas être sélectionné ;
  - une liste de masquage cachée aux niveaux « Désactivé » et « Métadonnées uniquement » n’empêche plus l’enregistrement.
- Badges `LLM` dans l’arborescence des connexions : les lecteurs d’écran les lisent en entier, par exemple « Accès LLM : métadonnées et messages », et non plus « LLM » ; le texte est plus contrasté.
- Journal MCP : le bouton « Ouvrir dans “Messages” » explique qu’il est indisponible lorsque la liste des connexions n’a pas pu être chargée.

## 0.5.1
- Les notes de mise à jour dans « À propos » → « Mises à jour » s’affichent dans la langue de l’interface (en anglais faute de traduction) et sous forme de texte mis en forme, et non plus en Markdown brut. Cela concerne les mises à jour proposées à partir de la version 0.5.1 ; les versions antérieures affichent les notes en anglais, en texte brut.
- Le journal des modifications est désormais tenu en anglais (`CHANGELOG.md`) ; ses versions dans les autres langues de l’interface se trouvent dans `docs/changelog/`.

## 0.5.0
- Lecture des messages par un assistant LLM. Une connexion a trois niveaux d’accès LLM : « Désactivé », « Métadonnées uniquement », « Métadonnées et messages ». Au troisième niveau, l’assistant peut, via MCP :
  - lire les messages d’une fenêtre de temps ou d’une plage d’offsets, avec les mêmes filtres que dans l’onglet « Messages » (`read_messages`) ;
  - obtenir un aperçu d’une fenêtre sans le contenu : volume par partition et dans le temps, clés fréquentes, horodatages dans le désordre (`summarize_messages`) ;
  - voir quels offsets un consumer group a commités et quand, d’après `__consumer_offsets` (`get_group_commit_history`).
- Les heures ne sont transmises qu’avec un fuseau horaire. Un appel dure au plus 40 secondes, parcourt au plus un million d’enregistrements et renvoie au plus 100 KB.
- Masquage avant l’envoi des données à l’assistant :
  - les valeurs des champs d’une liste de noms — la liste standard ou celle propre à la connexion — sont remplacées par `***` ;
  - les adresses e-mail, numéros de carte, JWT et jetons d’autorisation sont masqués dans tout texte ;
  - les filtres portent sur les données non masquées : un message peut donc être retrouvé par une valeur tirée d’un journal.
- Le niveau d’accès et la liste de masquage ne sont ni exportés ni importés.
- La clé de politique `mcpMessagesEnabled` interdit la lecture des messages : le niveau « Métadonnées et messages » fonctionne alors comme « Métadonnées uniquement ».
- Journal MCP : pour les lectures de messages, il indique le topic ou le groupe, la quantité renvoyée par rapport à celle parcourue et le nombre de valeurs masquées. Le bouton « Ouvrir dans “Messages” » remplit le formulaire de l’écran « Messages » avec la même fenêtre et les mêmes filtres ; s’ils ne peuvent pas être repris exactement, le formulaire le signale.
- MCP : les clients du protocole MCP 2026-07-28 sont pris en charge (par exemple Claude Code 2.1.310) : le nom du client et l’approbation arrivent dès la connexion. Une session en attente a un bouton « Approuver ». Tous les outils sont explicitement marqués en lecture seule (`idempotentHint`).
- Le serveur catalogue `glafira-mcp-catalog` pour les registres MCP (Glama) : une image Docker, la vraie liste des outils, et les appels répondent que l’application de bureau est nécessaire. Le README est en anglais (en russe — `README.ru.md`).
- Réinitialisation des offsets proposée par un LLM : la composition du groupe est vérifiée à nouveau avant l’écriture. Si le broker a refusé parce que des membres ont rejoint le groupe entre-temps, une erreur claire « le groupe n’est pas vide » s’affiche, et le formulaire de réinitialisation montre aussitôt l’état actuel du groupe.
- Interface :
  - le texte indicatif des champs vides est en italique, pour ne pas le confondre avec une valeur saisie ;
  - les champs de position de départ et de partitions de l’écran « Messages » ont une aide au survol ;
  - au-dessus des badges d’environnement et LLM dans l’arborescence des connexions, le pointeur reste la flèche habituelle.
- Guide : section « Lecture des messages » dans le chapitre « Assistants LLM (MCP) », en 12 langues.
