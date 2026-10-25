# Asistentes LLM (MCP)

Glafira puede actuar como pasarela para un asistente LLM: Claude Desktop, Claude Code, Cursor u otro cliente compatible con MCP (Model Context Protocol). El asistente lee los metadatos de los clústeres que abras para él y te ayuda a encontrar problemas: particiones subreplicadas, lag, rebalanceos constantes, ajustes poco acertados. A través de Glafira no puede cambiar nada en un clúster: los cambios los haces tú mismo en la interfaz.

## Cómo funciona {#how-it-works}

El cliente ejecuta `glafira-kafka mcp`, el mismo archivo de la aplicación en un modo sin ventana. Este comando se conecta a la Glafira en ejecución mediante un socket local (en Windows, una canalización con nombre), y Glafira responde a las solicitudes del asistente a través de sus propias conexiones. Las contraseñas y los certificados permanecen en Glafira; el asistente nunca los recibe.

Si Glafira no está en ejecución, el cliente la inicia y espera hasta 30 segundos.

## Activación {#enable}

1. Abre [[ui:sidebar.settings]], sección [[ui:settings.mcp.section]].
2. Marca [[ui:settings.mcp.enable]]. El estado cambia a [[ui:settings.mcp.state.running]].
3. En el formulario de la conexión, elige el nivel en [[ui:connections.form.llmAccess]] y guarda: [[ui:connections.form.llmLevel.metadata]] o [[ui:connections.form.llmLevel.messages]] (consulta [Lectura de mensajes](llm-mcp.md#messages)). El asistente no ve las conexiones con [[ui:connections.form.llmLevel.off]]. En la lista de conexiones, las conexiones abiertas muestran la marca `LLM`; en las que tienen mensajes, la marca está rellena.

Si tu administrador ha prohibido MCP, la casilla [[ui:settings.mcp.enable]] no está disponible y a su lado aparece [[ui:settings.mcp.policy]].

El nivel de acceso y la lista de enmascaramiento no se transfieren al exportar e importar conexiones: en otro equipo, vuelve a configurarlos.

## Conectar un cliente {#connect-client}

En [[ui:settings.mcp.connectTitle]] hay fragmentos listos para usar y el botón [[ui:settings.mcp.copy]]:

- [[ui:settings.mcp.claudeDesktop]] — pega el fragmento en la sección `mcpServers` del archivo de configuración de Claude Desktop y reinicia Claude Desktop;
- [[ui:settings.mcp.claudeCode]] — el comando `claude mcp add`;
- [[ui:settings.mcp.otherClient]] — para cualquier cliente que inicie un servidor MCP con un comando (stdio).

Los fragmentos contienen la ruta de la aplicación instalada. Si reinstalas Glafira en otra ubicación, copia el fragmento de nuevo.

## Aprobar un cliente {#approve}

Cuando un cliente se conecta por primera vez, Glafira muestra un cuadro de diálogo con su nombre. Haz clic en [[ui:mcp.approval.allow]] o en [[ui:mcp.approval.reject]]. Mientras no respondas, el asistente recibe un mensaje de que el cliente aún no está aprobado y te pide que respondas en la ventana de Glafira.

Un cliente permitido se recuerda por su nombre y aparece en [[ui:settings.mcp.clients]]. El rechazo no se recuerda: Glafira volverá a preguntar en la siguiente conexión. Si un cliente no indica su nombre, el permiso vale solo hasta el final de la sesión.

## Qué ve el asistente {#what-llm-sees}

El asistente ve:

- las conexiones con acceso [[ui:connections.form.llmLevel.metadata]] o [[ui:connections.form.llmLevel.messages]]: nombre, entorno, carpeta y nivel de acceso;
- los brokers con sus hosts y puertos, el controlador;
- los topics, las particiones, las réplicas, ISR, los offsets, el número aproximado de mensajes;
- las configuraciones de brokers y topics, con las contraseñas y otros valores secretos ocultos;
- los consumer groups: estado, protocolo, coordinador, miembros con client id, host y `group.instance.id`, asignaciones, offsets con commit y lag;
- sus propuestas y tu decisión sobre ellas: aplicada (tal cual o con cambios), rechazada con tu motivo, retirada.

Sin el nivel [[ui:connections.form.llmLevel.messages]], el asistente no ve los mensajes de los topics. Nunca recibe la cadena bootstrap ni los ajustes de seguridad de una conexión, las contraseñas, los certificados, las propiedades adicionales de librdkafka, el historial de envíos ni las plantillas.

Para investigar rebalanceos constantes, el asistente puede observar un grupo hasta 50 segundos: quién sale y quién entra, con qué frecuencia y desde qué host. Las configuraciones y los registros de los propios consumers no están disponibles a través de Glafira; el asistente te los pedirá.

Los nombres de topics, grupos y client id puede establecerlos cualquiera que tenga acceso al clúster. Glafira advierte al asistente de que esos datos no son instrucciones para él.

## Lectura de mensajes {#messages}

Con [[ui:connections.form.llmLevel.messages]], el asistente lee también el contenido de los mensajes; por ejemplo, para averiguar qué se escribió en un topic y qué procesó un consumer group mientras tu servicio registraba un error:

- `summarize_messages` — un resumen de una ventana de tiempo sin contenido: número de mensajes por partición y en el tiempo, claves frecuentes y repetidas, marcas de tiempo desordenadas;
- `read_messages` — los mensajes de una ventana de tiempo o de un rango de offsets, ordenados por tiempo en todas las particiones, con los mismos filtros que en la pestaña [[ui:topics.details.tabMessages]];
- `get_group_commit_history` — en qué offsets hizo commit un consumer group y cuándo, a partir del topic interno `__consumer_offsets`.

Puedes preguntar, por ejemplo: “A las 14:32 el servicio de pedidos registró un error del pedido 123: averigua qué había en el topic orders y qué había procesado el grupo order-service”. Indica al asistente la zona horaria de tus logs: Glafira no acepta horas sin zona horaria.

Antes de que los datos lleguen al asistente, Glafira los enmascara. Los valores de los campos de la lista de enmascaramiento se sustituyen por `***` en JSON, en los encabezados y en los pares `name=value`. La lista se define para cada conexión en el formulario de la conexión, en el bloque [[ui:connections.form.mask]], que se muestra con el nivel [[ui:connections.form.llmLevel.messages]]; la lista estándar cubre, entre otros, contraseñas, tokens, correos electrónicos, teléfonos y números de tarjeta y de pasaporte. Los correos electrónicos, los números de tarjeta, los JWT y los tokens Bearer se sustituyen en cualquier texto. El enmascaramiento reduce el riesgo, pero no reconoce nombres de personas, direcciones ni datos en campos con nombres desconocidos, y los formatos binarios como Avro o Protobuf solo se enmascaran en parte. Los filtros ven los datos sin enmascarar, así que el asistente puede encontrar un mensaje por un valor de tu log. En una conexión `prod`, el formulario muestra una advertencia.

El historial de commits necesita el permiso Read sobre `__consumer_offsets`. La compactación elimina los commits antiguos; los recientes suelen estar.

La lectura está limitada: una operación larga por sesión, como máximo 40 segundos y 1 000 000 de registros por llamada; si se alcanza el límite, el asistente reduce la ventana. Glafira lee sin consumer group y no hace commit de nada; el `client.id` es `glafira-kafka-mcp`, salvo que la conexión defina el suyo.

Un administrador puede prohibir la lectura de mensajes con la clave de política `mcpMessagesEnabled`: [[ui:connections.form.llmLevel.messages]] deja de estar disponible y las conexiones con ese nivel funcionan como [[ui:connections.form.llmLevel.metadata]].

## Sesiones, revocación y registro {#sessions-log}

Mientras hay un cliente conectado, en la parte inferior del panel izquierdo se muestra el indicador `LLM` con el nombre del cliente; se ilumina durante una solicitud. Al hacer clic en él se abre la sección de MCP.

- [[ui:settings.mcp.sessions]]: [[ui:settings.mcp.disconnect]] cierra una sesión y [[ui:settings.mcp.disconnectAll]] cierra todas.
- [[ui:settings.mcp.clients]]: [[ui:settings.mcp.revoke]] cierra las sesiones del cliente, y en la siguiente conexión pedirá aprobación de nuevo.
- Un nivel cambiado en [[ui:connections.form.llmAccess]] surte efecto de inmediato, sin necesidad de reconectar.
- [[ui:settings.mcp.audit.title]] — cada solicitud: fecha/hora, cliente, herramienta, conexión, resultado. El contenido de las respuestas no se registra. [[ui:settings.mcp.audit.reveal]] abre el archivo `mcp-audit.jsonl`.
- En las lecturas de mensajes, el registro muestra además el topic o el grupo, cuántos registros se devolvieron de los examinados y cuántos valores se enmascararon. En las llamadas a `read_messages` y `summarize_messages` que se completaron correctamente, [[ui:settings.mcp.audit.openInBrowse]] rellena el formulario de la pestaña [[ui:topics.details.tabMessages]] del topic con los parámetros de la llamada; haz clic en [[ui:browse.panel.read]] para volver a leer los mensajes, sin enmascaramiento, y compararlos con lo que recibió el asistente. Si los filtros o el límite no se pudieron trasladar con exactitud, el formulario lo indica; si el botón está deshabilitado, sitúa el puntero sobre él para ver el motivo.

Para desactivar MCP por completo, desmarca [[ui:settings.mcp.enable]]: los clientes se desconectan.

## Comprobación del clúster, notas y propuestas {#health}

El asistente puede ejecutar la comprobación integrada del clúster de Glafira (herramienta `health_check`) y basarse en sus hallazgos. También puede dejar notas sobre los objetos: aparecen en la pestaña [[ui:sidebar.findings]] y en las pantallas de topics y grupos, y el clúster no cambia. Consulta [Comprobación del clúster y notas](health-check.md#with-llm).

El asistente también puede proponer un cambio con la herramienta `propose_change`: la configuración de un topic, un topic nuevo, más particiones o un restablecimiento de offsets. La propuesta aparece junto al objeto; la aplicas tú en un formulario ya relleno o la rechazas, y el asistente ve tu decisión en `list_proposals`. Eliminar y purgar no se pueden proponer. Consulta [Propuestas LLM](health-check.md#proposals).

## Si no funciona {#troubleshooting}

El cliente muestra un mensaje de Glafira:

- `MCP is disabled in Glafira` — marca [[ui:settings.mcp.enable]] y reinicia el servidor MCP en el cliente;
- `MCP is disabled by your organization` — MCP está prohibido por la política de tu organización;
- `Glafira did not start the MCP server within 30 s` — inicia Glafira manualmente y comprueba el estado en la sección [[ui:settings.mcp.section]];
- `Invalid MCP token` — reinicia el servidor MCP en el cliente: Glafira emite un token nuevo cada vez que se inicia.

En macOS la app está firmada y notarizada, por lo que no hace falta ningún paso adicional antes del primer inicio.
