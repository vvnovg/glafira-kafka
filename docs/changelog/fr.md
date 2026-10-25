# Journal des modifications

Version française de [CHANGELOG.md](../../CHANGELOG.md), qui contient aussi les versions antérieures. La section d’une version s’affiche dans « À propos » comme notes de mise à jour.

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
