# Clúster y topics

Este capítulo trata el resumen del clúster, la lista de topics, la creación y eliminación de topics, la pantalla del topic y su configuración. Bajo cada conexión del panel [[ui:sidebar.connections]] hay enlaces a [[ui:sidebar.cluster]] y [[ui:sidebar.topics]]. Cada uno se abre en su propia pestaña.

## Resumen del clúster {#cluster-overview}

[[ui:cluster.overview.title]] muestra el estado del clúster en el momento en que se cargó. Haz clic en [[ui:cluster.overview.refresh]] para volver a cargarlo.

- `Cluster ID`: el identificador del clúster.
- [[ui:cluster.overview.controller]]: el ID del broker que el clúster indica como controlador. En los clústeres KRaft, los clientes no se comunican directamente con los controladores: el clúster indica aquí un broker aleatorio, no el controlador activo, y puede cambiar entre actualizaciones.
- [[ui:cluster.overview.brokers]]: el número de brokers.
- [[ui:cluster.overview.topicsInternal]]: todos los topics, con el número de topics internos entre paréntesis.
- [[ui:cluster.overview.partitions]]: el número de particiones de todos los topics.
- `Under-replicated`: particiones en las que no todas las réplicas están sincronizadas (en el ISR). Normalmente un broker está caído o no da abasto.
- `Offline`: particiones sin leader. Nadie puede escribir en ellas ni leer de ellas.

Un valor distinto de cero en `Under-replicated` u `Offline` se resalta.

Debajo está la tabla de brokers con `ID` y [[ui:cluster.overview.address]]. El broker del campo [[ui:cluster.overview.controller]] lleva la marca [[ui:cluster.overview.controllerBadge]]. En los clústeres KRaft, es el mismo broker aleatorio, no el controlador activo. Haz clic en un broker para ver su configuración bajo la tabla. La configuración del broker solo se puede consultar. Usa [[ui:common.config.searchParam]] para buscar un parámetro por nombre y [[ui:common.config.onlyChanged]] para ocultar los parámetros con valores predeterminados. Un icono de candado indica los parámetros de solo lectura.

## Lista de topics {#topic-list}

[[ui:sidebar.topics]] muestra los topics del clúster. Haz clic en una fila para abrir el topic en su propia pestaña.

Las columnas son [[ui:topics.list.colName]], [[ui:topics.list.colPartitions]], `RF` (factor de replicación), [[ui:topics.list.colMessages]], `cleanup.policy` y [[ui:topics.list.colUnderRepl]] (el número de particiones subreplicadas). El número de mensajes es aproximado: es la diferencia entre los offsets final e inicial de las particiones. La compactación y los marcadores de transacción hacen que el número real sea menor. El número de mensajes y `cleanup.policy` se cargan solo para las filas a las que te desplazas.

Controles de la barra de herramientas:

- [[ui:topics.list.search]]: escribe parte de un nombre para buscar topics; no se distingue entre mayúsculas y minúsculas. Para buscar con una expresión regular, escríbela entre barras, por ejemplo `/^orders\..*/`.
- [[ui:topics.list.internal]]: muestra los topics internos, como `__consumer_offsets`. Llevan la marca [[ui:topics.list.internalBadge]].
- La lista de actualización automática: [[ui:topics.list.autoRefreshOff]], [[ui:topics.list.every10]] o [[ui:topics.list.every30]].
- [[ui:topics.list.refresh]]: vuelve a cargar al momento la lista y el número de mensajes.

Junto a la barra de herramientas, la app muestra cuántos topics coinciden con la búsqueda respecto al total. En las conexiones con la opción [[ui:common.readOnly]] activada, los botones [[ui:topics.list.create]] y [[ui:topics.list.select]] están ocultos.

## Crear un topic {#create-topic}

1. En la lista de topics, haz clic en [[ui:topics.list.create]]. El formulario [[ui:topics.create.title]] se abre sobre la lista.
2. En [[ui:topics.create.name]], escribe el nombre. Puede contener letras latinas, dígitos, `.`, `_` y `-`, hasta 249 caracteres. El formulario avisa si el nombre contiene a la vez `.` y `_`: los nombres de las métricas de Kafka de esos topics pueden coincidir con los de otros topics.
3. En [[ui:topics.create.partitions]], indica el número de particiones. Las particiones se leen en paralelo y el orden solo está garantizado dentro de una misma partición. El número puede aumentarse más adelante, pero nunca reducirse.
4. En [[ui:topics.create.rf]], indica cuántas copias de cada partición guardan los brokers. No puede ser mayor que el número de brokers, que el formulario muestra bajo el campo. El valor predeterminado es 3, o el número de brokers si hay menos. `-1` usa el valor predeterminado del broker.
5. En [[ui:topics.create.preset]], elige un preajuste:
   - [[ui:topics.create.presetDelete]]: los mensajes antiguos se eliminan por tiempo.
   - `compact`: solo se conserva el último valor de cada clave.
   - `compact + delete`: ambas cosas.
   - [[ui:topics.create.presetTest]]: los mensajes se conservan una hora.
6. Si quieres, añade parámetros en [[ui:topics.create.extraConfigs]], uno por línea, por ejemplo `retention.ms=86400000`. Tienen prioridad sobre el preajuste. Las líneas que empiezan por `#` se ignoran.
7. Haz clic en [[ui:topics.create.submit]].

Cuando el formulario es válido, muestra el comando `kafka-topics.sh` equivalente. Puedes copiarlo, por ejemplo, para crear el mismo topic en otro clúster.

En una conexión prod, confirmas la creación escribiendo el nombre de la conexión. Una vez creado el topic, se abre su pestaña.

## Eliminar topics {#delete-topics}

Para eliminar un topic, ábrelo y haz clic en [[ui:topics.actions.deleteTopic]]. Escribe el nombre del topic para confirmar y haz clic en [[ui:topics.delete.confirm]]. La pestaña del topic se cierra.

Para eliminar varios topics a la vez:

1. En la lista de topics, haz clic en [[ui:topics.list.select]]. Aparecen casillas junto a los nombres.
2. Marca los topics. Los topics internos no se pueden marcar.
3. Haz clic en el botón de eliminar. Muestra el número de topics marcados.
4. Revisa la lista de nombres, escribe su número para confirmar y haz clic en [[ui:topics.delete.confirm]].

[[ui:topics.list.cancelSelect]] oculta las casillas.

En una conexión prod, las dos formas de eliminar piden además escribir el nombre de la conexión.

La eliminación no se puede deshacer: los datos se pierden. Si algunos topics no se eliminan, por ejemplo por falta de permisos, la app los enumera con el motivo. Los demás topics se eliminan.

## Pantalla del topic {#topic-details}

La pestaña del topic muestra el número de particiones, el factor de replicación y el número aproximado de mensajes. Los botones de la parte superior:

- [[ui:topics.details.refresh]]: vuelve a cargar los datos del topic.
- [[ui:topics.details.produce]]: abre el [[ui:sidebar.producer]] con este topic. Consulta [Producer](producer.md).
- [[ui:topics.details.consume]]: inicia una sesión de consumer para este topic. Consulta [Sesiones de consumer](consumer-sessions.md).

En las conexiones con la opción [[ui:common.readOnly]] activada, ambos botones están deshabilitados.

La segunda fila contiene acciones que cambian el topic. Está oculta en las conexiones de solo lectura y en los topics internos.

- [[ui:topics.actions.addPartitions]]: escribe el nuevo total y haz clic en [[ui:topics.actions.addSubmit]]. El número de particiones no se puede reducir. Tras aumentarlo, los mensajes con la misma clave pueden ir a otra partición, así que se rompe el orden por clave.
- [[ui:topics.actions.purge]]: elimina todos los mensajes hasta el final actual de las particiones seleccionadas. El topic en sí se conserva. Selecciona las particiones, escribe el nombre del topic y haz clic en [[ui:topics.actions.purgeConfirm]]. La purga solo funciona si `cleanup.policy` incluye `delete`; si no, el botón está deshabilitado y se muestra el motivo. En un topic `compact,delete`, la purga también elimina los últimos valores de las claves.
- [[ui:topics.actions.deleteTopic]]: consulta [Eliminar topics](topics.md#delete-topics).

En una conexión prod, cada una de estas acciones pide además el nombre de la conexión.

Las pestañas de abajo:

- [[ui:topics.details.tabMessages]]: explorar mensajes. Consulta [Explorar mensajes](browse.md).
- [[ui:topics.details.tabPartitions]]: una tabla con cada partición: [[ui:topics.details.colLeader]], [[ui:topics.details.colReplicas]], `ISR` (réplicas sincronizadas), los offsets inicial (`Low`) y final (`High`) y el número de mensajes. Una partición sin leader muestra [[ui:topics.details.noLeader]].
- [[ui:topics.details.tabConfigs]]: los parámetros del topic. Consulta [Configuración del topic](topics.md#topic-config).
- [[ui:topics.details.tabGroups]]: los consumer groups que tienen offsets con commit para este topic, con [[ui:topics.groups.colState]], [[ui:topics.groups.colPartitions]] y [[ui:topics.groups.colLag]]. Haz clic en un grupo para abrirlo. Consulta [Consumer groups](groups.md).

## Configuración del topic {#topic-config}

La pestaña [[ui:topics.details.tabConfigs]] muestra todos los parámetros del topic en tres columnas:

- [[ui:common.config.colParam]]: el nombre del parámetro. Los nombres en negrita están establecidos explícitamente: en el topic o en la configuración del broker.
- [[ui:common.config.colValue]]: el valor actual. Los valores confidenciales se ocultan como `••••••`.
- [[ui:common.config.colSource]]: de dónde procede el valor. Por ejemplo, `DynamicTopic` se ha establecido en este topic, `StaticBroker` y `DynamicBroker` proceden de la configuración del broker y `Default` es el valor predeterminado de Kafka.

Usa [[ui:common.config.searchParam]] para buscar un parámetro por nombre y [[ui:common.config.onlyChanged]] para mostrar solo los parámetros en negrita. Un icono de candado indica los parámetros de solo lectura. El icono `?` junto a un nombre explica el parámetro.

## Cambiar la configuración {#edit-config}

1. En la pestaña [[ui:topics.details.tabConfigs]], haz clic en [[ui:topics.details.edit]]. En las conexiones con la opción [[ui:common.readOnly]] activada, este botón está oculto.
2. Cambia los valores en la columna [[ui:common.config.colValue]]. Los parámetros de solo lectura y los confidenciales no se pueden editar. Para quitar el valor propio del topic, haz clic en [[ui:common.config.reset]]: entonces se aplica el valor del broker. No se permite un valor vacío.
3. Haz clic en [[ui:common.config.review]]. La app muestra cada cambio como valor anterior → valor nuevo; [[ui:common.config.defaultValue]] significa que el valor se restablece.
4. Haz clic en [[ui:common.config.apply]], o en [[ui:common.config.back]] para seguir editando.

Nada cambia en el clúster hasta que aplicas los cambios. [[ui:common.config.discard]] cancela todas las ediciones. Mientras haya ediciones sin guardar, [[ui:topics.details.closeEditor]] está deshabilitado. En una conexión prod, confirmas los cambios escribiendo el nombre de la conexión.

Solo se envían los parámetros que has cambiado. Los demás parámetros del topic no se modifican. El broker comprueba los valores y rechaza con un error los que no son válidos.

## Parámetros del topic {#config-parameters}

Los topics de Kafka tienen decenas de parámetros. Los iconos `?` de la tabla de configuración explican cada uno. Los que más se usan son estos:

- `cleanup.policy`: qué ocurre con los datos antiguos. `delete` elimina los segmentos antiguos por tiempo o por tamaño. `compact` conserva solo el último valor de cada clave. `compact,delete` hace ambas cosas.
- `retention.ms`: cuánto tiempo se conservan los mensajes, en milisegundos. El valor predeterminado es `604800000` (7 días); `-1` los conserva para siempre. Funciona si `cleanup.policy` incluye `delete`.
- `retention.bytes`: el tamaño máximo de cada partición antes de que se eliminen los segmentos antiguos. El valor predeterminado `-1` significa sin límite de tamaño.
- `segment.ms` y `segment.bytes`: cuándo empieza el broker un nuevo segmento del log. La retención y la compactación trabajan con segmentos completos, así que un mensaje puede permanecer más tiempo que `retention.ms` hasta que se cierre su segmento.
- `min.insync.replicas`: cuántas réplicas deben confirmar una escritura con `acks=all`. Normalmente 2 con un factor de replicación de 3.
- `max.message.bytes`: el batch más grande que el broker acepta para este topic. El valor predeterminado es de 1 MB aproximadamente. Los productores deben permitir el mismo tamaño (`max.request.size`).
- `compression.type`: cómo guarda el broker los mensajes. `producer` mantiene la compresión del productor.
- `message.timestamp.type`: `CreateTime` (la hora establecida por el productor) o `LogAppendTime` (la hora en que el broker escribió el mensaje). Es la hora que se muestra al explorar mensajes.
- `delete.retention.ms`: cuánto tiempo permanecen los marcadores de eliminación (mensajes con valor nulo) en un topic compactado. El valor predeterminado es 1 día.

Algunos parámetros solo existen en versiones más recientes de Kafka y otros están obsoletos. El icono `?` lo indica cuando es relevante. Los parámetros de tiered storage, como `remote.storage.enable`, solo funcionan si el clúster lo admite.
