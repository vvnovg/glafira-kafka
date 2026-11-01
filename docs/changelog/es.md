# Registro de cambios

Versión en español de [CHANGELOG.md](../../CHANGELOG.md), donde también están las versiones anteriores. La sección de una versión se muestra en “Acerca de” como notas de la actualización.

## 0.5.3
- Errores de TLS: cuando el broker cierra la conexión durante el handshake de TLS, el mensaje ya no afirma que el puerto no tiene TLS. Indica que puede que el puerto no tenga TLS o que no se haya aceptado el certificado de cliente, que es el caso habitual con mTLS.
- MCP funciona mejor con modelos locales (Qwen, gpt-oss y otros):
  - los esquemas de parámetros de las herramientas ya no contienen referencias `$ref`, que algunos servidores (vLLM con el analizador `qwen3_coder`) pasaban a las herramientas como cadenas;
  - se acepta un objeto anidado enviado como cadena JSON, y los valores `null` y las claves vacías se ignoran; si los argumentos siguen sin encajar, el error muestra los argumentos esperados;
  - cuando una ventana de tiempo no tiene mensajes, `read_messages` aconseja revisar la zona horaria y ampliar la ventana; las descripciones de las herramientas explican cómo pasar una hora con zona horaria y cómo filtrar por una sola clave exacta.
- “Ajustes” → “Asistentes LLM (MCP)”:
  - “Límite de tamaño de respuesta”: 100 KB (predeterminado), 48 KB o 24 KB; para un modelo local con poco contexto, elige un valor menor;
  - “Conjunto compacto” para cada cliente aprobado: sin las herramientas de notas y propuestas de cambios, lo que ahorra unos 1000 tokens de contexto.
- Guía: un capítulo nuevo, “Modelos locales”: dónde puede ejecutarse el modelo, resultados de Qwen3.6 y Qwen3.8 en un Mac, hardware, Ollama, configuración de Qwen Code y Claude Code, y comprobación del resultado.

## 0.5.2
- Enmascaramiento más completo de los mensajes que lee un asistente LLM:
  - en el texto, los valores de las cookies y de las cabeceras `Authorization` (incluida `Proxy-Authorization`) se enmascaran hasta el final de la línea, así que quedan ocultas todas las cookies, los parámetros Digest y las firmas de AWS;
  - en los pares `name=value` se reconocen nombres en cualquier alfabeto, por ejemplo un nombre de campo de la lista propia de la conexión escrito en cirílico;
  - se detectan los números de tarjeta pegados a letras (`id4111…`) o escritos con agrupaciones poco habituales, así como los números de tarjeta negativos y decimales en JSON.
- Topic sin permiso Describe: la pestaña “Mensajes” y las herramientas `read_messages`, `summarize_messages` y `get_group_commit_history` indican “No tienes autorización para realizar la operación” en lugar de “no encontrado”.
- `get_group_commit_history`: si el historial de commits no cabe en 100 KB ni siquiera sin series de commits, el asistente recibe, en lugar de un historial vacío, un error de “respuesta demasiado grande” con el consejo de acotar la consulta. `summarize_messages` usa menos memoria con claves grandes.
- Formulario de conexión:
  - cuando había dos formularios de conexión abiertos, el nivel de acceso para LLM elegido en uno de ellos no se mostraba, aunque se guardaba correctamente; ahora cada formulario muestra el suyo;
  - los lectores de pantalla anuncian la advertencia de que el contenido de los mensajes de un clúster de producción se enviará al proveedor de LLM;
  - si no se pudo cargar el estado de MCP, una nota explica por qué no se puede elegir “Metadatos y mensajes”;
  - una lista de enmascaramiento oculta en los niveles “Desactivado” y “Solo metadatos” ya no impide guardar.
- Marcas `LLM` en el árbol de conexiones: los lectores de pantalla las leen completas, por ejemplo “Acceso para LLM: metadatos y mensajes”, en lugar de “LLM”; el texto tiene más contraste.
- Registro de MCP: el botón “Abrir en ‘Mensajes’” explica que no está disponible cuando no se pudo cargar la lista de conexiones.

## 0.5.1
- Las notas de la actualización en “Acerca de” → “Actualizaciones” se muestran en el idioma de la interfaz (en inglés si no hay traducción) y como texto con formato en lugar de Markdown sin procesar. Esto se aplica a las actualizaciones que se ofrecen a la versión 0.5.1 y posteriores; las versiones anteriores muestran las notas como texto sin formato en inglés.
- El registro de cambios ahora está en inglés (`CHANGELOG.md`); sus versiones en los demás idiomas de la interfaz están en `docs/changelog/`.

## 0.5.0
- Lectura de mensajes por un asistente LLM. Una conexión tiene tres niveles de acceso para LLM: “Desactivado”, “Solo metadatos”, “Metadatos y mensajes”. En el tercer nivel, el asistente puede, a través de MCP:
  - leer mensajes de una ventana de tiempo o por offsets, con los mismos filtros que en la pestaña “Mensajes” (`read_messages`);
  - ver un resumen de una ventana sin el contenido: volumen por partición y en el tiempo, claves frecuentes, marcas de tiempo desordenadas (`summarize_messages`);
  - ver de qué offsets hizo commit un consumer group y cuándo, a partir de `__consumer_offsets` (`get_group_commit_history`).
- Las horas se pasan solo con zona horaria. Una llamada dura como máximo 40 segundos, recorre como máximo un millón de registros y devuelve como máximo 100 KB.
- Enmascaramiento antes de enviar los datos al asistente:
  - los valores de los campos de una lista de nombres — la estándar o la propia de la conexión — se sustituyen por `***`;
  - los correos electrónicos, números de tarjeta, JWT y tokens de autorización se enmascaran en cualquier texto;
  - los filtros trabajan con los datos sin enmascarar, así que un mensaje se puede encontrar por un valor tomado de un log.
- El nivel de acceso y la lista de enmascaramiento no se exportan ni se importan.
- La clave de política `mcpMessagesEnabled` prohíbe leer mensajes: el nivel “Metadatos y mensajes” funciona entonces como “Solo metadatos”.
- Registro de MCP: en las lecturas de mensajes se ve el topic o el grupo, cuánto se devolvió de lo recorrido y cuántos valores se enmascararon. El botón “Abrir en ‘Mensajes’” rellena el formulario de la pantalla “Mensajes” con la misma ventana y los mismos filtros; si no se pueden trasladar con exactitud, el formulario lo indica.
- MCP: se admiten clientes del protocolo MCP 2026-07-28 (por ejemplo, Claude Code 2.1.310): el nombre del cliente y la aprobación llegan al conectarse. Una sesión pendiente tiene el botón “Aprobar”. Todas las herramientas están marcadas explícitamente como de solo lectura (`idempotentHint`).
- El servidor de catálogo `glafira-mcp-catalog` para los directorios de servidores MCP (Glama): una imagen Docker, la lista real de herramientas, y las llamadas responden que se necesita la aplicación de escritorio. El README está en inglés (en ruso — `README.ru.md`).
- Restablecimiento de offsets propuesto por un LLM: antes de escribir se vuelve a comprobar la composición del grupo. Si el broker se negó porque entretanto se unieron miembros al grupo, se muestra un error claro, “el grupo no está vacío”, y el formulario de restablecimiento muestra de inmediato el estado actual del grupo.
- Interfaz:
  - los textos de ejemplo de los campos vacíos aparecen en cursiva para no confundirlos con valores introducidos;
  - los campos de posición inicial y de particiones de la pantalla “Mensajes” muestran una ayuda al pasar el puntero;
  - sobre las marcas de entorno y de LLM del árbol de conexiones aparece la flecha normal.
- Guía: la sección “Lectura de mensajes” del capítulo “Asistentes LLM (MCP)”, en 12 idiomas.
