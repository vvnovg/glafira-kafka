# Registro de cambios

Versión en español de [CHANGELOG.md](../../CHANGELOG.md), donde también están las versiones anteriores. La sección de una versión se muestra en “Acerca de” como notas de la actualización.

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
