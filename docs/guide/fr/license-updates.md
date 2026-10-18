# Licence et mises à jour

Ce chapitre décrit ce qu’apporte Pro, comment activer une clé de licence, ce qui se passe à l’expiration de la licence, comment mettre à jour l’app, l’écran [[ui:sidebar.about]] et l’archive de diagnostic.

## Free et Pro {#free-and-pro}

Free couvre le travail quotidien avec Kafka : tous les protocoles de sécurité, mTLS compris, l’aperçu du cluster et les topics, la lecture des messages, le Producer avec l’envoi par lot, les consumer groups et la réinitialisation des offsets. Toutes les langues de l’interface sont également incluses. Voir [Langue de l’interface](getting-started.md#language).

Pro ajoute :

- les sessions consumer : lecture en tant que membre d’un groupe, commit d’offsets, mise en pause de partitions. Voir [Sessions consumer](consumer-sessions.md#what-is) ;
- le mode prod : création de connexions prod, passage de connexions en prod et désactivation de [[ui:common.readOnly]] sur une connexion prod. Voir [Environnements et prod](connections.md#environments).

Dans Free, ces fonctionnalités sont visibles mais indisponibles. Les commandes des sessions consumer sont désactivées et marquées d’un cadenas et de `Pro`. L’option prod s’affiche sous la forme [[ui:connections.form.prodPro]]. Si une action nécessite Pro, l’erreur indique [[ui:errors.license.proRequired]] et propose le bouton [[ui:common.openLicense]].

L’écran [[ui:tabs.license]] liste les sessions consumer et le mode prod sous [[ui:license.whatProGives]].

## Activer une clé {#activate}

Une clé de licence est une ligne de texte émise pour une personne ou une organisation. L’app vérifie la clé sur votre ordinateur et ne contacte aucun serveur.

1. Cliquez sur [[ui:tabs.license]] en bas du panneau de gauche.
2. Collez la clé entière dans [[ui:license.keyLabel]].
3. Cliquez sur [[ui:license.activate]].

Les fonctionnalités Pro deviennent disponibles immédiatement. L’écran affiche [[ui:license.tier]] (Pro), [[ui:license.licensee]], [[ui:license.kind]] ( [[ui:license.kindPersonal]] ou [[ui:license.kindOrganization]] ) et [[ui:license.expires]] : une date, ou [[ui:license.perpetual]]. La clé est enregistrée dans le dossier de données de l’app : elle est conservée après un redémarrage ou une mise à jour. Une nouvelle clé remplace la précédente.

Si la clé n’est pas acceptée, la raison s’affiche sous le champ :

- [[ui:errors.license.malformed]] — une partie de la clé manque ou a été modifiée. Copiez de nouveau la clé sur une seule ligne, sans caractères superflus.
- [[ui:errors.license.badSignature]] — la clé n’a pas été émise pour Glafira, ou elle a été modifiée.
- La clé a expiré — le message indique sa date de fin. Demandez une nouvelle clé.
- La clé nécessite une version plus récente de l’app — le message indique la version de la clé. Mettez d’abord l’app à jour, voir [Mises à jour](license-updates.md#updates).

Pour supprimer la clé, cliquez sur [[ui:license.removeKey]]. L’app demande : [[ui:license.removeQuestion]] Cliquez sur [[ui:license.removeYes]] pour confirmer. Vos données sont conservées.

Une organisation peut activer Pro pour tous les utilisateurs au moyen d’une politique. Le champ de la clé et les boutons sont alors masqués, et l’écran indique : [[ui:license.managed]] Une clé que vous avez saisie vous-même n’est pas utilisée tant que la clé de la politique est définie. La section [[ui:license.policyTitle]] indique l’origine de la politique et liste ses erreurs. Si la clé de l’organisation n’est pas valide, la raison s’affiche sur l’écran [[ui:tabs.license]]. Contactez alors vos administrateurs. Les administrateurs trouveront les paramètres de la politique dans `docs/admin.md`, dans le code source de Glafira.

## Expiration {#expiry}

Une clé peut avoir une date de fin, affichée dans [[ui:license.expires]]. La licence est valable jusqu’à la fin de ce jour.

Lorsqu’il reste moins de 14 jours, une notification au-dessus des onglets indique le nombre de jours restants, et l’écran [[ui:tabs.license]] affiche la date de fin. [[ui:license.notice.more]] dans la notification ouvre l’écran [[ui:tabs.license]]. Le bouton `×` masque la notification jusqu’au prochain redémarrage de l’app.

Après la date de fin, l’offre passe à Free. La notification et l’écran [[ui:tabs.license]] indiquent que la licence a expiré et que les fonctionnalités Pro sont désactivées. Ce qui change :

- Aucune nouvelle session consumer ne peut être lancée. Dans une session déjà en cours, les commits et la mise en pause de partitions sont refusés avec une erreur qui indique la date de fin.
- Les connexions prod continuent de fonctionner, avec les mêmes confirmations. Aucune nouvelle connexion prod ne peut être créée, et [[ui:common.readOnly]] ne peut pas être désactivé sur une connexion prod.

Les connexions, mots de passe, modèles et l’historique d’envoi sont conservés. Pour continuer avec Pro, activez une nouvelle clé comme décrit dans [Activer une clé](license-updates.md#activate).

## Mises à jour {#updates}

Au démarrage, l’app vérifie une fois si une nouvelle version est disponible. Si c’est le cas, une notification au-dessus des onglets indique sa version. [[ui:license.notice.more]] ouvre l’écran [[ui:sidebar.about]]. Si la vérification échoue, par exemple sans accès à Internet, aucun message ne s’affiche.

Pour vérifier manuellement :

1. Ouvrez [[ui:sidebar.about]] en bas du panneau de gauche.
2. Dans [[ui:about.updates.title]], cliquez sur [[ui:about.updates.check]].
3. Si une nouvelle version est disponible, l’écran affiche son numéro et les notes de version. Cliquez sur [[ui:about.updates.install]].

L’app télécharge la mise à jour, vérifie sa signature, l’installe et redémarre. Un téléchargement interrompu par un problème réseau est relancé automatiquement, jusqu’à trois tentatives au total. Si les trois tentatives échouent, l’app signale [[ui:errors.updates.downloadInterrupted]]. Si la vérification de la signature échoue, l’app signale [[ui:errors.updates.badSignature]].

L’installation peut demander un mot de passe administrateur, car l’app est installée pour tous les utilisateurs de l’ordinateur : par le programme d’installation `.pkg` sur macOS, le programme d’installation `.msi` sur Windows, ou un paquet `.deb` ou `.rpm` sur Linux. Les utilisateurs sans droits d’administrateur ne peuvent pas mettre à jour une telle installation.

Si des messages sont en cours de lecture, si une session consumer tourne ou si un lot est en cours d’envoi, l’app vous demande d’abord confirmation et indique combien de sessions sont actives. Cliquez sur [[ui:about.updates.stopAndUpdate]] pour les arrêter et continuer, ou sur [[ui:common.cancel]]. Les connexions, les paramètres, la clé de licence et les autres données sont conservés après une mise à jour.

Les mises à jour sont téléchargées depuis la page des versions sur GitHub. Si la vérification échoue avec [[ui:errors.updates.checkFailed]], vérifiez votre réseau ou votre proxy. [[ui:errors.updates.notConfigured]] signifie que ce build a été produit sans la clé du système de mise à jour, par exemple un build de développement ou de test. Dans ce cas, téléchargez les nouvelles versions depuis la page des versions : https://github.com/vvnovg/glafira-kafka-releases/releases

Une organisation peut désactiver la recherche de mises à jour au moyen d’une politique, ou la rediriger vers un miroir interne. Lorsque la recherche est désactivée, la section [[ui:about.updates.title]] affiche [[ui:errors.updates.managed]] après un clic sur [[ui:about.updates.check]]. Comme [[ui:errors.updates.notConfigured]], ce message n’apparaît qu’après le clic ; dans ces cas, la vérification au démarrage n’affiche rien.

## À propos {#about}

Ouvrez [[ui:sidebar.about]] en bas du panneau de gauche. Sur macOS, cet écran est aussi accessible depuis le menu « Aide », sous [[ui:menu.about]].

L’écran affiche une courte description de l’app, ainsi que :

- [[ui:about.version]] — le numéro de version, et [[ui:about.build]] — l’identifiant du code source à partir duquel ce build a été produit. Indiquez les deux dans une demande d’assistance.
- [[ui:about.system]] — le système d’exploitation et l’architecture du processeur.
- [[ui:about.tier]] — Free ou Pro.
- [[ui:about.policy]] — [[ui:about.policySet]] ou [[ui:about.policyNone]]. Les erreurs de la politique sont listées en dessous.
- [[ui:about.dataDir]] — le dossier contenant les profils de connexion, les modèles, l’historique, les paramètres et la clé de licence.
- [[ui:about.logDir]] — le dossier des fichiers journaux. Voir [Journaux](troubleshooting.md#logs).

Les sections [[ui:about.updates.title]] et [[ui:about.diagnostics.title]] se trouvent aussi sur cet écran.

## Archive de diagnostic {#diagnostics}

L’archive de diagnostic rassemble ce dont l’assistance a besoin pour comprendre un problème. Pour la créer :

1. Ouvrez [[ui:sidebar.about]].
2. Dans [[ui:about.diagnostics.title]], cliquez sur [[ui:about.diagnostics.collect]]. Sur macOS, vous pouvez aussi choisir [[ui:menu.diagnostics]] dans le menu « Aide ».
3. Choisissez où enregistrer le fichier. Le nom proposé est `diagnostics-<date>.zip`.

Une fois l’archive enregistrée, l’écran affiche son chemin. Le fichier ZIP contient :

- `info.json` — la version et le build de l’app, le système d’exploitation, les chemins des dossiers de données et de journaux, l’état de la politique et l’état de la licence (offre, titulaire, type, date de fin) ;
- `connections.json` — les profils de connexion : noms, environnements, serveurs bootstrap, protocoles, noms d’utilisateur SASL, chemins des fichiers de certificats, propriétés supplémentaires et délais ;
- `logs/` — les fichiers journaux de l’app.

L’archive ne contient aucun mot de passe : ils sont conservés dans le trousseau du système, et les propriétés supplémentaires contenant des mots de passe ou des secrets sont refusées à l’enregistrement d’une connexion. La clé de licence et les fichiers de certificats n’y figurent pas non plus. Les noms d’hôte, noms d’utilisateur et chemins de fichiers sont stockés en clair, et les journaux contiennent les adresses des brokers issues des messages d’erreur Kafka. Parcourez l’archive avant de l’envoyer hors de votre entreprise.

L’app n’envoie rien d’elle-même. Joignez vous-même l’archive à votre demande d’assistance, voir [Contacter l’assistance](troubleshooting.md#support).
