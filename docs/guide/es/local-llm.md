# Modelos locales

El asistente no tiene por qué ejecutarse en la nube: Glafira responde igual a todos los clientes MCP, y el cliente puede comunicarse con un modelo de tu red o de tu equipo. Este capítulo explica dónde puede ejecutarse el modelo, qué hardware necesita un modelo local, cómo conectarlo a Glafira y cómo comprobar el resultado. Cómo funciona el acceso del asistente a los clústeres se describe en [Asistentes LLM (MCP)](llm-mcp.md#how-it-works).

## Dónde se ejecuta el modelo {#where}

1. **Un modelo en la nube** (Claude, GPT y otros): la opción más potente, pero las solicitudes con metadatos del clúster —y, con el nivel [[ui:connections.form.llmLevel.messages]], también con el contenido de los mensajes— salen de tu red. Muchas empresas lo prohíben.
2. **Un servidor de modelos corporativo** dentro de tu red: los datos permanecen dentro del perímetro, y los administradores eligen el modelo y el hardware. Si todavía no existe un servidor así, este es el siguiente paso.
3. **Un modelo en el equipo del ingeniero**: para cuando la nube está prohibida y aún no hay un servidor corporativo o todavía se está configurando. Los datos nunca salen del equipo, pero el modelo es más débil que uno en la nube y responde más despacio.

El resto del capítulo trata de la tercera opción; el cliente para la segunda se configura igual, solo cambia la dirección del servidor.

## Qué puede hacer un modelo local {#capabilities}

Probado en un Mac con M4 Pro y 48 GB de memoria, un contexto de 64k tokens y un clúster de prueba con un broker y tres topics (7 particiones, unos 3000 mensajes). Tres tareas típicas: una comprobación del clúster con explicación de los hallazgos, el lag de un grupo sin miembros y la búsqueda de un mensaje por la hora de un log de servicio. Cada modelo se ejecutó tres veces.

| Modelo (4 bits) | Servidor | Respuestas correctas | Generación | Comprobación del clúster | Lag del grupo | Búsqueda de mensaje |
|---|---|---|---|---|---|---|
| Qwen3.6-35B-A3B | Ollama | 9 de 9 | 70–80 tokens/s | 23–29 s | 8–31 s | 34–43 s |
| Qwen3.6-35B-A3B | vllm-metal | 9 de 9 | 25–30 tokens/s | 33–52 s | 43–60 s | unos 2 min |
| Qwen3.8-27B | vllm-metal | 9 de 9 | 8–10 tokens/s | 8–16 min | 10–12 min | 28–46 min |

Qwen3.8-27B da respuestas más detalladas, pero es un modelo denso: usa los 27 000 millones de parámetros para cada token, mientras que Qwen3.6-35B-A3B usa unos 3000 millones. Para el trabajo diario en un Mac, elige Qwen3.6-35B-A3B en Ollama: allí el mismo modelo responde de 1,5 a 3 veces más rápido que en vllm-metal.

Un modelo local maneja peor que uno en la nube las investigaciones largas de muchos pasos: comete más errores en los argumentos de las herramientas y puede sacar una conclusión equivocada a partir de datos incompletos. Contrasta sus conclusiones con el registro de MCP.

## Hardware y modelos {#hardware}

En hardware corriente funcionan mejor los modelos MoE con pocos parámetros activos (unos 3000 millones, como Qwen3.6-35B-A3B): son más rápidos que los modelos densos del mismo tamaño. El contexto debe ser de al menos 32k tokens, preferiblemente 64k.

| Plataforma | Servidor de modelos | Qué cabe |
|---|---|---|
| macOS, Apple Silicon, 48 GB o más | vllm-metal u Ollama | Qwen3.6-35B-A3B (probado) |
| macOS, Apple Silicon, 32 GB | Ollama | gpt-oss-20b; 35B-A3B está en el límite |
| Linux, NVIDIA con 24 GB de memoria de vídeo | vLLM | 35B-A3B a 4 bits con un contexto menor |
| Windows, NVIDIA | Ollama; vLLM solo en WSL2 | como en Linux |
| x86 sin GPU, 32–64 GB de DDR5 | Ollama | solo modelos MoE, despacio |

Un Mac con procesador Intel no sirve para modelos locales.

## Servidor de modelos {#server}

El servidor debe ofrecer una API compatible con OpenAI con llamada a herramientas. En un Mac, [Ollama](https://ollama.com) es la opción más sencilla: instale la aplicación, descargue el modelo y cree una variante con un contexto de 64k tokens — por defecto, Ollama usa un contexto menor y las descripciones de herramientas de Glafira no caben en él:

```bash
ollama pull qwen3.6:35b-a3b
printf 'FROM qwen3.6:35b-a3b\nPARAMETER num_ctx 65536\n' > Modelfile
ollama create qwen3.6-64k:35b-a3b -f Modelfile
```

El cliente necesita la dirección `http://127.0.0.1:11434/v1`, el modelo `qwen3.6-64k:35b-a3b` y cualquier clave de API que no esté vacía. Ollama escucha solo en `127.0.0.1`, así que otros equipos no pueden conectarse al modelo.

Tras 5 minutos de inactividad, Ollama descarga el modelo de la memoria y la siguiente pregunta espera a que se vuelva a cargar. Para que el modelo permanezca más tiempo en memoria, define la variable de entorno `OLLAMA_KEEP_ALIVE`, por ejemplo con `1h`.

En Linux con NVIDIA, lo habitual es vLLM. Interpreta las llamadas a herramientas con la opción `--tool-call-parser`, que se define para la familia del modelo (`qwen3_coder` para Qwen3.6); en Ollama, lo hace la plantilla del modelo.

## Clientes {#clients}

Se probaron dos clientes de línea de comandos. En ambos, desactiva el shell integrado y las herramientas de edición de archivos y de acceso a la web: de lo contrario, el modelo puede saltarse Glafira y acceder por su cuenta al clúster o a internet. Glafira muestra un cliente nuevo en el cuadro de diálogo de aprobación con su nombre.

**Qwen Code** (código abierto, Apache 2.0). Sus ajustes están en `~/.qwen/settings.json`; crea el archivo antes del primer inicio, porque de lo contrario Qwen Code se actualiza solo y activa la aprobación automática de acciones:

```json
{
  "env": { "LOCAL_LLM_API_KEY": "ollama" },
  "modelProviders": { "openai": [ {
    "id": "qwen3.6-64k:35b-a3b",
    "envKey": "LOCAL_LLM_API_KEY",
    "baseUrl": "http://127.0.0.1:11434/v1",
    "generationConfig": { "contextWindowSize": 65536,
      "samplingParams": { "temperature": 0.2, "max_tokens": 8192 } } } ] },
  "security": { "auth": { "selectedType": "openai" } },
  "model": { "name": "qwen3.6-64k:35b-a3b" },
  "privacy": { "usageStatisticsEnabled": false },
  "general": { "enableAutoUpdate": false },
  "telemetry": { "enabled": false },
  "tools": { "approvalMode": "default",
    "disabled": ["run_shell_command", "edit", "write_file", "web_fetch", "web_search", "agent"] },
  "mcpServers": { "glafira-kafka": {
    "command": "/Applications/Glafira for Apache Kafka.app/Contents/MacOS/glafira-kafka",
    "args": ["mcp"], "timeout": 120000, "trust": true, "alwaysLoadTools": true } }
}
```

`envKey` es el nombre de la variable que contiene la clave, no la clave en sí. `alwaysLoadTools` entrega al modelo las herramientas de Glafira de inmediato, sin un paso adicional de búsqueda de herramientas. En Glafira el cliente se llama `qwen-cli-mcp-client-glafira-kafka`. Si el servidor está definido en los ajustes de una carpeta (`.qwen/settings.json`), apruébalo una vez con `qwen mcp approve glafira-kafka`.

**Claude Code** funciona directamente con Ollama: Ollama también responde en el formato de Anthropic. Define las variables de entorno y añade Glafira con el comando del bloque [[ui:settings.mcp.connectTitle]]:

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:11434
export ANTHROPIC_AUTH_TOKEN=ollama
export ANTHROPIC_MODEL=qwen3.6-64k:35b-a3b
export ANTHROPIC_DEFAULT_OPUS_MODEL=qwen3.6-64k:35b-a3b
export ANTHROPIC_DEFAULT_SONNET_MODEL=qwen3.6-64k:35b-a3b
export ANTHROPIC_DEFAULT_HAIKU_MODEL=qwen3.6-64k:35b-a3b
export CLAUDE_CODE_SUBAGENT_MODEL=qwen3.6-64k:35b-a3b
export CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1
claude --disallowedTools "Bash Edit Write WebFetch WebSearch Task"
```

`ANTHROPIC_BASE_URL` no lleva `/v1`. `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC` desactiva las actualizaciones, la telemetría y los informes de errores. No hace falta iniciar sesión con una cuenta de Anthropic. En Glafira el cliente se llama `claude-code`.

## Ajustes de Glafira {#glafira-settings}

En la sección [[ui:settings.mcp.section]]:

- [[ui:settings.mcp.responseLimit]] — la respuesta más grande de una llamada a una herramienta. Una respuesta de 100 KB ocupa unos 30k tokens, así que para un contexto de 64k elige 48 o 24 KB: Glafira recorta las listas largas e indica al modelo que acote la solicitud.
- [[ui:settings.mcp.colCompact]] en la lista [[ui:settings.mcp.clients]] — un conjunto de herramientas sin notas ni propuestas de cambio: sus descripciones ocupan unos 1000 tokens de contexto. La casilla aparece cuando el cliente se conecta y lo apruebas; el cliente ve la nueva lista después de volver a conectarse.

## Comprobación {#check}

1. Conecta el cliente y apruébalo en el diálogo de Glafira.
2. Abre una conexión para el asistente, preferiblemente de prueba, con el nivel [[ui:connections.form.llmLevel.metadata]].
3. Haz tres o cuatro preguntas típicas: “Comprueba el clúster y explica los hallazgos”, “¿Por qué el grupo … tiene un lag grande?”, “¿Qué topics no tienen replicación?”.
4. En [[ui:settings.mcp.audit.title]], comprueba qué herramientas llamó el modelo y con qué resultado, y compara su respuesta con los datos de Glafira.
5. Si abres mensajes, comprueba el enmascaramiento: el botón [[ui:settings.mcp.audit.openInBrowse]] muestra los mismos mensajes sin enmascarar.
6. Desconecta la red y repite una pregunta: la respuesta debe llegar sin acceso a internet.

## Limitaciones {#limits}

- Un modelo local responde varias veces más despacio que uno en la nube, y los modelos densos como Qwen3.8-27B son aún más lentos en un Mac (consulta la [tabla](local-llm.md#capabilities)).
- Mientras el modelo se ejecuta, al equipo le queda menos memoria: en un Mac de 48 GB, un modelo de 35 000 millones de parámetros ocupa unos 20 GB.
- El modelo puede sacar una conclusión equivocada. Glafira no le deja cambiar nada en el clúster, pero las decisiones son tuyas.
