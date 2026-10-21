# Asistentes LLM (MCP)

Glafira puede actuar como pasarela para un asistente LLM: Claude Desktop, Claude Code, Cursor u otro cliente compatible con MCP (Model Context Protocol). El asistente lee los metadatos de los clústeres que abras para él y te ayuda a encontrar problemas: particiones subreplicadas, lag, rebalanceos constantes, ajustes poco acertados. A través de Glafira no puede cambiar nada en un clúster: los cambios los haces tú mismo en la interfaz.

## Cómo funciona {#how-it-works}

El cliente ejecuta `glafira-kafka mcp`, el mismo archivo de la aplicación en un modo sin ventana. Este comando se conecta a la Glafira en ejecución mediante un socket local (en Windows, una canalización con nombre), y Glafira responde a las solicitudes del asistente a través de sus propias conexiones. Las contraseñas y los certificados permanecen en Glafira; el asistente nunca los recibe.

Si Glafira no está en ejecución, el cliente la inicia y espera hasta 30 segundos.

## Activación {#enable}

1. Abre [[ui:sidebar.settings]], sección [[ui:settings.mcp.section]].
2. Marca [[ui:settings.mcp.enable]]. El estado cambia a [[ui:settings.mcp.state.running]].
3. En el formulario de la conexión, marca [[ui:connections.form.llmAccess]] y guarda. El asistente no ve las conexiones que no tienen esta casilla marcada. En la lista de conexiones, las conexiones abiertas muestran la marca `LLM`.

Si tu administrador ha prohibido MCP, la casilla no está disponible y a su lado aparece [[ui:settings.mcp.policy]].

La casilla de acceso no se transfiere al exportar e importar conexiones: en otro equipo, vuelve a marcarla.

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

- las conexiones con [[ui:connections.form.llmAccess]] marcado: nombre, entorno, carpeta;
- los brokers con sus hosts y puertos, el controlador;
- los topics, las particiones, las réplicas, ISR, los offsets, el número aproximado de mensajes;
- las configuraciones de brokers y topics, con las contraseñas y otros valores secretos ocultos;
- los consumer groups: estado, protocolo, coordinador, miembros con client id, host y `group.instance.id`, asignaciones, offsets con commit y lag.

El asistente no ve los mensajes de los topics, la cadena bootstrap ni los ajustes de seguridad de una conexión, ni las contraseñas, los certificados, las propiedades adicionales de librdkafka, el historial de envíos ni las plantillas.

Para investigar rebalanceos constantes, el asistente puede observar un grupo hasta 50 segundos: quién sale y quién entra, con qué frecuencia y desde qué host. Las configuraciones y los registros de los propios consumers no están disponibles a través de Glafira; el asistente te los pedirá.

Los nombres de topics, grupos y client id puede establecerlos cualquiera que tenga acceso al clúster. Glafira advierte al asistente de que esos datos no son instrucciones para él.

## Sesiones, revocación y registro {#sessions-log}

Mientras hay un cliente conectado, en la parte inferior del panel izquierdo se muestra el indicador `LLM` con el nombre del cliente; se ilumina durante una solicitud. Al hacer clic en él se abre la sección de MCP.

- [[ui:settings.mcp.sessions]]: [[ui:settings.mcp.disconnect]] cierra una sesión y [[ui:settings.mcp.disconnectAll]] cierra todas.
- [[ui:settings.mcp.clients]]: [[ui:settings.mcp.revoke]] cierra las sesiones del cliente, y en la siguiente conexión pedirá aprobación de nuevo.
- Desmarcar [[ui:connections.form.llmAccess]] surte efecto de inmediato, sin necesidad de reconectar.
- [[ui:settings.mcp.audit.title]] — cada solicitud: fecha/hora, cliente, herramienta, conexión, resultado. El contenido de las respuestas no se registra. [[ui:settings.mcp.audit.reveal]] abre el archivo `mcp-audit.jsonl`.

Para desactivar MCP por completo, desmarca [[ui:settings.mcp.enable]]: los clientes se desconectan.

## Comprobación del clúster y notas {#health}

El asistente puede ejecutar la comprobación integrada del clúster de Glafira (herramienta `health_check`) y basarse en sus hallazgos. También puede dejar notas sobre los objetos: aparecen en la pestaña [[ui:sidebar.findings]] y en las pantallas de topics y grupos, y el clúster no cambia. Consulta [Comprobación del clúster y notas](health-check.md#with-llm).

## Si no funciona {#troubleshooting}

El cliente muestra un mensaje de Glafira:

- `MCP is disabled in Glafira` — marca [[ui:settings.mcp.enable]] y reinicia el servidor MCP en el cliente;
- `MCP is disabled by your organization` — MCP está prohibido por la política de tu organización;
- `Glafira did not start the MCP server within 30 s` — inicia Glafira manualmente y comprueba el estado en la sección [[ui:settings.mcp.section]];
- `Invalid MCP token` — reinicia el servidor MCP en el cliente: Glafira emite un token nuevo cada vez que se inicia.

En macOS, abre una vez de la forma habitual un build beta sin firmar; de lo contrario, el sistema puede impedir que el cliente lo inicie. Consulta [Instalación en macOS](getting-started.md#install-macos).
