# Comprobación del clúster y notas

Glafira comprueba un clúster con un conjunto de reglas integradas y muestra los problemas encontrados junto a los objetos: en el árbol de conexiones, en las listas de topics y grupos y en las pantallas de un topic o un grupo. Un asistente LLM puede completar estos hallazgos con sus propias notas. La comprobación solo lee el clúster y no cambia nada en él.

## Ejecutar una comprobación {#run}

Abre [[ui:sidebar.cluster]] o [[ui:sidebar.findings]] de una conexión y haz clic en [[ui:health.check]]. Una comprobación tarda hasta 45 segundos: Glafira lee los brokers, las particiones, las configuraciones de topics (hasta 2000 topics) y los consumer groups (hasta 500). Si hay más topics o grupos, si algo no se puede leer o si se agotan los 45 segundos, el resultado se marca como [[ui:health.partial]], y el hallazgo [[ui:health.ruleNames.check.partial]] muestra lo que no se leyó.

Glafira no consulta el clúster por su cuenta: una comprobación solo se ejecuta cuando haces clic en el botón o cuando la pide un asistente LLM. El resultado se conserva hasta la siguiente comprobación.

## Qué se comprueba {#rules}

- Brokers y particiones: sin controlador, particiones no disponibles y subreplicadas, un clúster de un solo broker, desequilibrio de leaders, leaders fuera de la réplica preferida.
- Durabilidad de los topics: factor de replicación 1, `min.insync.replicas` mayor que el factor de replicación, `min.insync.replicas` igual al factor de replicación, `min.insync.replicas` igual a 1, elección de leader no limpia.
- Configuración de los brokers: creación automática de topics, factor de replicación por defecto 1, `min.insync.replicas` por defecto 1, ajustes importantes que difieren entre brokers.
- Consumer groups: grupos vacíos con lag, grupos en rebalanceo, miembros sin particiones, lag que creció desde la comprobación anterior.

## Gravedad y entorno {#severity}

Cada hallazgo tiene una gravedad: [[ui:health.severity.critical]], [[ui:health.severity.warning]] o [[ui:health.severity.info]]. Depende del entorno de la conexión. Por ejemplo, el factor de replicación 1 es crítico en prod, una advertencia en stage y no se muestra en dev, test y local. El entorno se define en el formulario de la conexión.

## La pestaña “Hallazgos” {#tab}

En la pestaña [[ui:sidebar.findings]] los hallazgos se agrupan por regla; cada uno explica el problema y qué hacer. Fíltralos por [[ui:health.filterSeverity]], [[ui:health.filterSource]] y [[ui:health.filterKind]], o busca por nombre. El nombre del objeto abre su pantalla.

Una insignia con un número en el árbol de conexiones y en las listas de topics y grupos muestra cuántos hallazgos y notas LLM abiertas con gravedad [[ui:health.severity.critical]] o [[ui:health.severity.warning]] afectan al objeto. Sitúa el puntero sobre la insignia para ver sus títulos.

## Ocultar hallazgos {#hide}

Si un hallazgo es esperado para un objeto concreto, por ejemplo un topic de prueba con factor de replicación 1 en prod, haz clic en [[ui:health.hide]]. Un hallazgo oculto no cuenta en las insignias y sigue oculto en las comprobaciones siguientes. Para ver los ocultos, activa “Mostrar ocultos”; ahí encontrarás [[ui:health.restore]].

## Notas LLM {#notes}

Un asistente LLM conectado por MCP (consulta [Asistentes LLM (MCP)](llm-mcp.md#how-it-works)) puede dejar una nota sobre el clúster, un broker, un topic o un grupo. Las notas aparecen en [[ui:health.notes]] y en las pantallas de los objetos, firmadas con el nombre del cliente. El texto se muestra tal cual, sin formato ni enlaces.

- [[ui:health.resolve]] y [[ui:health.dismiss]] cierran una nota; las cerradas se guardan en una sección plegada.
- [[ui:common.delete]] elimina una nota y [[ui:health.deleteAll]] elimina todas las notas de la conexión.
- Una nota se elimina automáticamente 7 días después de crearse. Una conexión puede tener como máximo 200 notas abiertas.

## Propuestas LLM {#proposals}

Además de notas, un asistente LLM puede proponer un cambio: la configuración de un topic, un topic nuevo, más particiones o el restablecimiento de los offsets de un consumer group vacío. La propuesta aparece en [[ui:health.proposals.title]] de la pestaña [[ui:sidebar.findings]] y en la pantalla del topic o del grupo, con la justificación del asistente, una descripción del cambio y el nombre del cliente. Nada se aplica por sí solo; eliminar y vaciar no se pueden proponer en absoluto.

- [[ui:health.proposals.openForm]] abre la pestaña del objeto con el formulario habitual ya relleno: el editor de configuración con la revisión “antes → después”, el formulario de creación de topic, el diálogo de particiones o el restablecimiento de offsets con vista previa. Edita los valores si quieres y confirma como siempre; en una conexión prod, Glafira pide el nombre de la conexión. Tras un cambio correcto la propuesta se marca como aplicada (con cambios, si modificaste algo), y el asistente lo ve en `list_proposals`.
- [[ui:health.proposals.reject]] cierra una propuesta; puedes indicar un motivo, y el asistente lo verá. [[ui:health.reopen]] recupera una propuesta rechazada.
- En una conexión de solo lectura la propuesta se conserva, pero [[ui:health.proposals.openForm]] no está disponible hasta que quites la solo lectura en el formulario de conexión; al asistente también se le informa.
- [[ui:common.delete]] elimina una propuesta, [[ui:health.proposals.deleteAll]] elimina todas las propuestas de la conexión. Una propuesta se elimina automáticamente 7 días después de crearse; una conexión puede tener como máximo 50 propuestas abiertas.

## Con un asistente LLM {#with-llm}

El asistente ejecuta la misma comprobación con la herramienta `health_check` y ve los mismos hallazgos, incluidos los ocultos, que aparecen marcados. Si la última comprobación tiene menos de 5 minutos, el asistente recibe su resultado sin volver a consultar el clúster. Pide, por ejemplo: “Comprueba el estado del clúster y deja notas sobre los problemas más importantes”.
