# Modèles locaux

L’assistant n’a pas besoin de s’exécuter dans le cloud : Glafira répond de la même façon à tout client MCP, et le client peut dialoguer avec un modèle de votre réseau ou de votre ordinateur. Ce chapitre explique où le modèle peut s’exécuter, de quel matériel un modèle local a besoin, comment le connecter à Glafira et comment vérifier le résultat. Le fonctionnement de l’accès de l’assistant aux clusters est décrit dans [Assistants LLM (MCP)](llm-mcp.md#how-it-works).

## Où le modèle s’exécute {#where}

1. **Un modèle cloud** (Claude, GPT et d’autres) : l’option la plus puissante, mais les requêtes contenant des métadonnées de cluster — et, au niveau [[ui:connections.form.llmLevel.messages]], le contenu des messages — quittent votre réseau. Beaucoup d’entreprises l’interdisent.
2. **Un serveur de modèles d’entreprise** dans votre réseau : les données restent dans le périmètre, et les administrateurs choisissent le modèle et le matériel. S’il n’existe pas encore de tel serveur, c’est l’étape suivante.
3. **Un modèle sur l’ordinateur de l’ingénieur** : lorsque le cloud est interdit et qu’un serveur d’entreprise n’existe pas encore ou est en cours de mise en place. Les données ne quittent jamais l’ordinateur, mais le modèle est plus faible qu’un modèle cloud et répond plus lentement.

La suite du chapitre porte sur la troisième option ; un client pour la deuxième se configure de la même façon, seule l’adresse du serveur change.

## Ce que peut faire un modèle local {#capabilities}

Testé sur un Mac avec une puce M4 Pro et 48 GB de mémoire, le serveur vllm-metal, un contexte de 64k jetons et un cluster de test avec un broker et trois topics (7 partitions, environ 3 000 messages). Trois tâches typiques : une vérification du cluster avec explication des constats, le lag d’un groupe sans membres et la recherche d’un message d’après l’heure indiquée dans le journal d’un service. Chaque modèle a été exécuté trois fois.

| Modèle (4 bits) | Réponses correctes | Génération | Vérification du cluster | Lag du groupe | Recherche de message |
|---|---|---|---|---|---|
| Qwen3.6-35B-A3B | 9 sur 9 | 25–30 jetons/s | 30–50 s | 45–60 s | environ 2 min |
| Qwen3.8-27B | 9 sur 9 | 8–10 jetons/s | 8–16 min | 10–12 min | 28–46 min |

Qwen3.8-27B donne des réponses plus détaillées, mais c’est un modèle dense : il utilise les 27 milliards de paramètres pour chaque jeton, alors que Qwen3.6-35B-A3B en utilise environ 3 milliards. Pour le travail quotidien sur un Mac, choisissez Qwen3.6-35B-A3B.

Un modèle local gère moins bien qu’un modèle cloud les longues investigations en de nombreuses étapes : il fait plus d’erreurs dans les arguments des outils et peut tirer une conclusion erronée de données incomplètes. Vérifiez ses conclusions dans le journal MCP.

## Matériel et modèles {#hardware}

Sur du matériel ordinaire, les modèles MoE avec peu de paramètres actifs (environ 3 milliards, comme Qwen3.6-35B-A3B) fonctionnent le mieux : ils sont plus rapides que les modèles denses de même taille. Le contexte doit être d’au moins 32k jetons, de préférence 64k.

| Plateforme | Serveur de modèles | Ce qui convient |
|---|---|---|
| macOS, Apple Silicon, 48 GB ou plus | vllm-metal ou Ollama | Qwen3.6-35B-A3B (testé) |
| macOS, Apple Silicon, 32 GB | Ollama | gpt-oss-20b ; 35B-A3B est à la limite |
| Linux, NVIDIA avec 24 GB de mémoire vidéo | vLLM | 35B-A3B en 4 bits avec un contexte réduit |
| Windows, NVIDIA | Ollama ; vLLM uniquement sous WSL2 | comme sous Linux |
| x86 sans GPU, 32–64 GB de DDR5 | Ollama | modèles MoE uniquement, lentement |

Un Mac avec processeur Intel ne convient pas aux modèles locaux.

## Serveur de modèles {#server}

Le serveur doit fournir une API compatible OpenAI avec appel d’outils. Sur un Mac, [Ollama](https://ollama.com) est l’option la plus simple : installez l’application, téléchargez le modèle et créez-en une variante avec un contexte de 64k jetons — par défaut, Ollama utilise un contexte plus petit, et les descriptions des outils de Glafira n’y tiennent pas :

```bash
ollama pull qwen3.6:35b-a3b
printf 'FROM qwen3.6:35b-a3b\nPARAMETER num_ctx 65536\n' > Modelfile
ollama create qwen3.6-64k:35b-a3b -f Modelfile
```

Le client a besoin de l’adresse `http://127.0.0.1:11434/v1`, du modèle `qwen3.6-64k:35b-a3b` et d’une clé d’API quelconque, non vide. Ollama n’écoute que sur `127.0.0.1`, de sorte que les autres ordinateurs ne peuvent pas se connecter au modèle.

Sous Linux avec NVIDIA, vLLM est le choix habituel. Il analyse les appels d’outils grâce à l’option `--tool-call-parser` définie pour la famille du modèle (`qwen3_coder` pour Qwen3.6) ; dans Ollama, c’est le modèle de prompt qui s’en charge.

## Paramètres de Glafira {#glafira-settings}

Dans la section [[ui:settings.mcp.section]] :

- [[ui:settings.mcp.responseLimit]] — la plus grande réponse d’un appel d’outil. Une réponse de 100 KB occupe environ 30k jetons ; pour un contexte de 64k, choisissez donc 48 ou 24 KB : Glafira tronque les longues listes et demande au modèle de restreindre la requête.
- [[ui:settings.mcp.colCompact]] dans la liste [[ui:settings.mcp.clients]] — un jeu d’outils sans remarques ni propositions de modification : leurs descriptions occupent environ 9 000 jetons de contexte. La case apparaît une fois que le client se connecte et que vous l’approuvez ; le client voit la nouvelle liste après s’être reconnecté.

## Vérification {#check}

1. Connectez le client et approuvez-le dans la boîte de dialogue de Glafira.
2. Ouvrez une connexion pour l’assistant, de préférence une connexion de test, au niveau [[ui:connections.form.llmLevel.metadata]].
3. Posez trois ou quatre questions typiques : « Vérifie le cluster et explique les constats », « Pourquoi le groupe … a-t-il un lag important ? », « Quels topics n’ont pas de réplication ? ».
4. Dans [[ui:settings.mcp.audit.title]], vérifiez quels outils le modèle a appelés et avec quel résultat, et comparez sa réponse avec les données de Glafira.
5. Si vous ouvrez des messages, vérifiez le masquage : le bouton [[ui:settings.mcp.audit.openInBrowse]] affiche les mêmes messages sans masquage.
6. Coupez le réseau et répétez une question : la réponse doit arriver sans accès à internet.

## Limites {#limits}

- Un modèle local répond plusieurs fois plus lentement qu’un modèle cloud, et les modèles denses comme Qwen3.8-27B sont encore plus lents sur un Mac (voir le [tableau](local-llm.md#capabilities)).
- Pendant que le modèle s’exécute, l’ordinateur dispose de moins de mémoire : sur un Mac de 48 GB, un modèle de 35 milliards de paramètres occupe environ 20 GB.
- Le modèle peut tirer une conclusion erronée. Glafira ne lui permet rien de modifier dans le cluster, mais les décisions vous appartiennent.
