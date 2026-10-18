# Explorar mensajes

Los mensajes se exploran en la pestaña [[ui:topics.details.tabMessages]] de un topic. La exploración no se une a ningún consumer group ni hace commit de nada, así que el clúster no cambia. También funciona en las conexiones con la opción [[ui:common.readOnly]] activada. Para leer como miembro de un grupo, usa una [sesión de consumer](consumer-sessions.md).

## Empezar a leer {#start-reading}

1. Abre un topic desde la lista [[ui:sidebar.topics]]. Primero se abre la pestaña [[ui:topics.details.tabMessages]].
2. En [[ui:browse.form.from]], elige dónde empezar. Consulta [Posición inicial](browse.md#start-position).
3. Si quieres, configura los formatos y los filtros.
4. Haz clic en [[ui:browse.panel.read]].

Los mensajes aparecen en la tabla a medida que llegan. La línea junto a los botones muestra cuántos mensajes se han revisado, cuántos coinciden con los filtros y cuántos hay en la tabla. También muestra el estado: [[ui:browse.panel.reading]], [[ui:browse.panel.finishLimit]], [[ui:browse.panel.finishEnd]] o [[ui:browse.panel.finishCancelled]].

Sin `Live tail`, la lectura se detiene en el final del topic tal como estaba al hacer clic en [[ui:browse.panel.read]], o antes, al alcanzar el valor de [[ui:browse.form.limit]]. El límite cuenta los mensajes que coinciden con los filtros. Puede ir de 1 a 100000; los valores grandes hacen la lectura más lenta.

Con `Live tail` activado, la lectura no se detiene al final: los mensajes nuevos se añaden a medida que se escriben. Cuando se han leído todos los mensajes antiguos, el estado muestra [[ui:browse.panel.waiting]]. La tabla conserva los últimos mensajes hasta el límite y descarta los más antiguos.

[[ui:browse.panel.stop]] detiene la lectura y conserva la tabla. Mientras se lee, el botón [[ui:browse.panel.read]] pasa a ser [[ui:browse.panel.restart]]: vuelve a empezar con los ajustes actuales y vacía la tabla. Si se pierde la conexión con el broker, la app se vuelve a conectar sola y lo indica en la línea de estado.

En la tabla se mezclan mensajes de distintas particiones. Kafka mantiene el orden solo dentro de una partición. La columna `P` muestra la partición y `Offset`, la posición dentro de ella. Selecciona `UTC` para mostrar las horas en UTC en lugar de la hora local.

## Posición inicial {#start-position}

[[ui:browse.form.startPosition]] determina dónde empieza la lectura en cada partición:

- [[ui:browse.form.startLatest]]: los últimos N mensajes. N se reparte a partes iguales entre las particiones, así que con 100 mensajes y 4 particiones la app toma hasta 25 de cada una.
- [[ui:browse.form.startEarliest]]: desde el mensaje más antiguo que aún se conserva.
- [[ui:browse.form.startOffset]]: desde el mismo offset en cada partición. Si una partición no tiene ese offset, la lectura empieza en su principio o en su final. Este modo es más útil con una sola partición.
- [[ui:browse.form.startTimestamp]]: desde el primer mensaje con una marca de tiempo igual o posterior a la hora local indicada. En las particiones sin mensajes más recientes, la lectura empieza al final.
- [[ui:browse.form.startLastMinutes]]: lo mismo, contando hacia atrás desde ahora.

El campo con el texto [[ui:browse.form.partitionsPlaceholder]] limita la lectura a algunas particiones. Escribe los números de partición separados por comas, por ejemplo `0,2`. Déjalo vacío para leer todas las particiones.

## Filtros {#filters}

Los filtros muestran solo los mensajes que coinciden. La app sigue leyendo todos los mensajes desde la posición inicial y comprueba cada uno, así que los filtros no aceleran la lectura. Deben cumplirse todos los campos rellenados.

- [[ui:browse.filters.keyContains]] y [[ui:browse.filters.valueContains]]: texto en la clave o en el valor.
- `regex`: interpreta estos campos, y el valor del encabezado, como expresiones regulares.
- `Aa`: distingue entre mayúsculas y minúsculas. De forma predeterminada no se distinguen.
- [[ui:browse.filters.header]]: el mensaje tiene un encabezado con este nombre exacto. [[ui:browse.filters.headerValue]] comprueba además su valor.
- `JSONPath`: una condición sobre un valor JSON, por ejemplo `$.order.status == "FAILED"`. Sin `==` ni `!=`, basta con que la ruta exista, por ejemplo `$.error`. Los mensajes cuyo valor no es JSON válido no coinciden.
- [[ui:browse.filters.timeFrom]] y [[ui:browse.filters.timeTo]]: la marca de tiempo del mensaje está dentro de este intervalo, en hora local. Ambos extremos se incluyen.

Los filtros comprueban el texto que se muestra en la tabla, después de decodificarlo. Si el formato del valor es `hex`, el filtro de valor busca en el texto hexadecimal. Los filtros se aplican al hacer clic en [[ui:browse.panel.read]]; después de cambiarlos, vuelve a leer.

Un filtro no mueve la posición inicial. Por ejemplo, [[ui:browse.form.startLatest]] con un filtro solo encuentra coincidencias entre los últimos N mensajes. Para buscar en todo el topic, empieza con [[ui:browse.form.startEarliest]].

## Formatos {#formats}

[[ui:browse.form.format]] determina cómo se decodifican las claves y los valores, por separado para la [[ui:browse.form.key]] y el [[ui:browse.form.value]]. Solo cambia la visualización, no los datos.

- `auto`: el predeterminado. Los objetos y arrays JSON válidos se muestran como JSON, el resto del texto UTF-8 como cadena y los datos binarios en hexadecimal.
- `string`: texto en UTF-8. Los bytes que no son UTF-8 válido se sustituyen por `�`.
- `json`: JSON con sangría en los detalles del mensaje. Si el valor no es JSON válido, los detalles muestran el error.
- `hex`: cada byte como dos dígitos hexadecimales.
- `base64`: los bytes en Base64.

Los detalles muestran qué formato se ha usado realmente, por ejemplo `json` cuando `auto` ha detectado JSON. Los valores de los encabezados se muestran como texto; los valores binarios se muestran como `0x` seguido de hexadecimal. Una clave o un valor ausente se muestra como `null`.

Las claves y los valores muy grandes se muestran solo en parte, aproximadamente el primer MB de texto. Esto también se aplica a `hex` y `base64`. En ese caso, los detalles del mensaje muestran un aviso.

## Detalles del mensaje {#message-details}

Haz clic en una fila para abrir los detalles del mensaje junto a la tabla. Arriba están la partición, el offset y la hora. Debajo:

- [[ui:browse.details.value]] y [[ui:browse.details.key]]: el texto completo, con el formato y el tamaño en bytes. El JSON se muestra con sangría. [[ui:browse.details.copy]] copia el texto.
- [[ui:browse.table.colHeaders]]: una tabla con los nombres y valores de los encabezados.

Las columnas de la tabla son `P`, `Offset`, [[ui:browse.table.colTime]], [[ui:browse.table.colKey]], [[ui:browse.table.colValue]], [[ui:browse.table.colHeaders]] (su número) y [[ui:browse.table.colSize]] (el tamaño de la clave y el valor en bytes). La hora es la marca de tiempo del mensaje. Según el ajuste `message.timestamp.type` del topic, la establece el productor o el broker.

## Copiar mensajes {#export-messages}

Glafira no guarda en un archivo los mensajes explorados. Para sacar un mensaje de la app, cópialo:

- [[ui:browse.details.copyJson]] copia el mensaje completo como JSON: topic, partición, offset, marca de tiempo, clave, valor, encabezados y tamaño. La clave y el valor se incluyen decodificados, con su formato.
- [[ui:browse.details.copy]] junto a [[ui:browse.details.value]] o [[ui:browse.details.key]] copia solo ese texto.

El texto copiado usa el formato elegido para la lectura. Para copiar datos binarios con exactitud, léelos como `hex` o `base64`. Las claves y los valores muy grandes se copian solo en parte, tal como se muestran, incluso en `hex` y `base64`. Consulta [Formatos](browse.md#formats).

## Abrir en Producer {#open-in-producer}

[[ui:browse.details.resend]] abre el mensaje en el [[ui:sidebar.producer]] de la misma conexión. El formulario recibe el topic, la clave, el valor, los encabezados y los formatos del mensaje. La partición se deja a Kafka. Los marcadores del texto del mensaje no se sustituyen: la copia se envía tal como se leyó.

No se envía nada de inmediato. Revisa el formulario, cámbialo si hace falta y envíalo desde el Producer. Consulta [Producer](producer.md).

El botón está deshabilitado en dos casos, y se muestra el motivo:

- la clave o el valor se mostraron solo en parte por ser demasiado grandes;
- el texto se decodificó con pérdidas porque no es UTF-8 válido. Léelo como `hex` o `base64` para enviar una copia exacta.

En las conexiones con la opción [[ui:common.readOnly]] activada, el botón está oculto.

## Restablecer un grupo a un mensaje {#reset-group-here}

[[ui:browse.details.resetHere]] prepara un restablecimiento de offsets para que un consumer group empiece a leer desde este mensaje.

1. En los detalles del mensaje, haz clic en [[ui:browse.details.resetHere]].
2. Elige un grupo en la lista. Solo aparecen los grupos existentes.
3. Haz clic en [[ui:browse.details.goToReset]].

Se abre la pestaña del grupo con el formulario de restablecimiento ya rellenado. En [[ui:groups.reset.strategy]] está seleccionado [[ui:groups.reset.strategyOffset]], con el offset del mensaje. En [[ui:groups.reset.scope]] está seleccionado [[ui:groups.reset.scopePartitions]], con el topic y la partición del mensaje. Todavía no cambia nada: haz clic en [[ui:groups.reset.preview]] para ver los nuevos offsets y aplica el restablecimiento desde ahí. El grupo no debe tener miembros activos. Consulta [Consumer groups](groups.md).

En las conexiones con la opción [[ui:common.readOnly]] activada, el botón está oculto.
