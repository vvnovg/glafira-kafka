# Licencia y actualizaciones

Este capítulo explica qué añade Pro, cómo activar una clave de licencia, qué ocurre cuando la licencia caduca, cómo actualizar la app, la pantalla [[ui:sidebar.about]] y el archivo de diagnóstico.

## Free y Pro {#free-and-pro}

Free cubre el trabajo diario con Kafka: todos los protocolos de seguridad, incluido mTLS, el resumen del clúster y los topics, la exploración de mensajes, el Producer con envío por lotes, los consumer groups, el restablecimiento de offsets y la comprobación del clúster. Todos los idiomas de la interfaz también están incluidos. Consulta [Idioma de la interfaz](getting-started.md#language).

Pro añade:

- sesiones de consumer: lectura como miembro de un grupo, commit de offsets, pausa de particiones. Consulta [Sesiones de consumer](consumer-sessions.md#what-is);
- modo prod: crear conexiones prod, cambiar conexiones a prod y desactivar la opción [[ui:common.readOnly]] en una conexión prod. Consulta [Entornos y prod](connections.md#environments);
- notas LLM: los asistentes conectados por MCP dejan notas sobre el clúster, los topics y los grupos. Consulta [Comprobación del clúster y notas](health-check.md#notes).

En Free, estas funciones son visibles, pero no están disponibles. Los comandos de las sesiones de consumer están deshabilitados y llevan un candado y `Pro`. La opción prod se muestra como [[ui:connections.form.prodPro]]. Si una acción requiere Pro, el error indica [[ui:errors.license.proRequired]] y ofrece el botón [[ui:common.openLicense]].

La pantalla [[ui:tabs.license]] enumera las sesiones de consumer, el modo prod y las notas LLM en [[ui:license.whatProGives]].

## Activar una clave {#activate}

Una clave de licencia es una línea de texto emitida a nombre de una persona o de una organización. La app comprueba la clave en tu equipo y no se conecta a ningún servidor.

1. Haz clic en [[ui:tabs.license]] en la parte inferior del panel izquierdo.
2. Pega la clave completa en [[ui:license.keyLabel]].
3. Haz clic en [[ui:license.activate]].

Las funciones Pro quedan disponibles al instante. La pantalla muestra [[ui:license.tier]] (Pro), [[ui:license.licensee]], [[ui:license.kind]] ( [[ui:license.kindPersonal]] u [[ui:license.kindOrganization]] ) y [[ui:license.expires]]: una fecha o [[ui:license.perpetual]]. La clave se guarda en la carpeta de datos de la app, así que se conserva tras un reinicio o una actualización. Una clave nueva sustituye a la anterior.

Si la clave no se acepta, el motivo se muestra bajo el campo:

- [[ui:errors.license.malformed]]: falta una parte de la clave o se ha modificado. Vuelve a copiar la clave como una sola línea, sin caracteres de más.
- [[ui:errors.license.badSignature]]: la clave no se emitió para Glafira o se ha editado.
- La clave ha caducado: el mensaje indica su fecha de fin. Solicita una clave nueva.
- La clave requiere una versión más reciente de la app: el mensaje indica la versión de la clave. Actualiza antes la app; consulta [Actualizaciones](license-updates.md#updates).

Para quitar la clave, haz clic en [[ui:license.removeKey]]. La app pregunta: [[ui:license.removeQuestion]] Haz clic en [[ui:license.removeYes]] para confirmar. Tus datos se conservan.

Una organización puede activar Pro para todos los usuarios mediante una política. En ese caso, el campo de la clave y los botones están ocultos y la pantalla indica: [[ui:license.managed]] Mientras esté establecida la clave de la política, no se usa la clave que hayas introducido tú. La sección [[ui:license.policyTitle]] muestra de dónde procede la política y enumera sus errores. Si la clave de la organización no es válida, el motivo se muestra en la pantalla [[ui:tabs.license]]. En ese caso, contacta con tus administradores. Los administradores encontrarán los ajustes de la política en `docs/admin.md`, en el código fuente de Glafira.

## Caducidad {#expiry}

Una clave puede tener una fecha de fin, que se muestra en [[ui:license.expires]]. La licencia es válida hasta el final de ese día.

Cuando quedan menos de 14 días, un aviso sobre las pestañas indica cuántos días faltan y la pantalla [[ui:tabs.license]] muestra la fecha de fin. [[ui:license.notice.more]] en el aviso abre la pantalla [[ui:tabs.license]]. El botón `×` oculta el aviso hasta que se reinicia la app.

Tras la fecha de fin, el plan cambia a Free. El aviso y la pantalla [[ui:tabs.license]] indican que la licencia ha caducado y que las funciones Pro están desactivadas. Qué cambia:

- No se pueden iniciar sesiones de consumer nuevas. En una sesión que ya se está ejecutando, los commits y la pausa de particiones se rechazan con un error que indica la fecha de fin.
- Las conexiones prod siguen funcionando con las mismas confirmaciones. No se pueden crear conexiones prod nuevas ni desactivar la opción [[ui:common.readOnly]] en una conexión prod.

Las conexiones, las contraseñas, las plantillas y el historial de envíos se conservan. Para seguir con Pro, activa una clave nueva como se describe en [Activar una clave](license-updates.md#activate).

## Actualizaciones {#updates}

Al iniciarse, la app comprueba una vez si hay una versión nueva. Si la hay, un aviso sobre las pestañas indica su versión. [[ui:license.notice.more]] abre la pantalla [[ui:sidebar.about]]. Si la comprobación falla, por ejemplo sin acceso a internet, no se muestra ningún mensaje.

Para comprobarlo manualmente:

1. Abre [[ui:sidebar.about]] en la parte inferior del panel izquierdo.
2. En la sección [[ui:about.updates.title]], haz clic en [[ui:about.updates.check]].
3. Si hay una versión nueva, la pantalla muestra su número y las notas de la versión. Haz clic en [[ui:about.updates.install]].

La app descarga la actualización, comprueba su firma, la instala y se reinicia. Si la descarga se interrumpe por un problema de red, se reintenta automáticamente, hasta tres intentos en total. Si los tres intentos fallan, la app muestra [[ui:errors.updates.downloadInterrupted]]. Si la comprobación de la firma falla, la app muestra [[ui:errors.updates.badSignature]].

La instalación puede pedir una contraseña de administrador, porque la app está instalada para todos los usuarios del equipo: con el instalador `.pkg` en macOS, el instalador `.msi` en Windows o un paquete `.deb` o `.rpm` en Linux. Los usuarios sin derechos de administrador no pueden actualizar una instalación así.

Si se están leyendo mensajes, hay una sesión de consumer en ejecución o se está enviando un lote, la app pregunta antes e indica cuántas sesiones están activas. Haz clic en [[ui:about.updates.stopAndUpdate]] para detenerlas y continuar, o en [[ui:common.cancel]]. Las conexiones, los ajustes, la clave de licencia y los demás datos se conservan tras una actualización.

Las actualizaciones se descargan desde la página de versiones en GitHub. Si la comprobación falla con [[ui:errors.updates.checkFailed]], revisa tu red o tu proxy. [[ui:errors.updates.notConfigured]] significa que este build se creó sin la clave del actualizador, por ejemplo un build de desarrollo o de prueba. En ese caso, descarga las versiones nuevas desde la página de versiones: https://github.com/vvnovg/glafira-kafka-releases/releases

Una organización puede desactivar la búsqueda de actualizaciones mediante una política o dirigirla a una réplica interna. Cuando la búsqueda está desactivada, la sección [[ui:about.updates.title]] indica [[ui:errors.updates.managed]] después de hacer clic en [[ui:about.updates.check]]. Al igual que [[ui:errors.updates.notConfigured]], este mensaje solo aparece tras el clic; en estos casos, la comprobación al iniciar no muestra nada.

## Acerca de {#about}

Abre [[ui:sidebar.about]] en la parte inferior del panel izquierdo. En macOS, también está en el menú Ayuda como [[ui:menu.about]].

La pantalla muestra una breve descripción de la app y además:

- [[ui:about.version]] — el número de versión, y [[ui:about.build]] — el identificador del código fuente a partir del cual se creó. Incluye ambos en una solicitud de soporte.
- [[ui:about.system]] — el sistema operativo y la arquitectura del procesador.
- [[ui:about.tier]] — Free o Pro.
- [[ui:about.policy]] — [[ui:about.policySet]] o [[ui:about.policyNone]]. Los errores de la política se enumeran debajo.
- [[ui:about.dataDir]] — la carpeta con los perfiles de conexión, las plantillas, el historial, los ajustes y la clave de licencia.
- [[ui:about.logDir]] — la carpeta con los archivos de registro. Consulta [Registros](troubleshooting.md#logs).

Las secciones [[ui:about.updates.title]] y [[ui:about.diagnostics.title]] también están en esta pantalla.

## Archivo de diagnóstico {#diagnostics}

El archivo de diagnóstico reúne lo que el soporte necesita para entender un problema. Para crearlo:

1. Abre [[ui:sidebar.about]].
2. En la sección [[ui:about.diagnostics.title]], haz clic en [[ui:about.diagnostics.collect]]. En macOS, también puedes elegir [[ui:menu.diagnostics]] en el menú Ayuda.
3. Elige dónde guardar el archivo. El nombre propuesto es `diagnostics-<date>.zip`.

Una vez guardado el archivo, la pantalla muestra su ruta. El archivo ZIP contiene:

- `info.json`: la versión y el build de la app, el sistema operativo, las rutas de las carpetas de datos y de registros, el estado de la política y el estado de la licencia (plan, titular, tipo, fecha de fin);
- `connections.json`: los perfiles de conexión: nombres, entornos, servidores bootstrap, protocolos, usuarios SASL, rutas de los archivos de certificado, propiedades adicionales y tiempos de espera;
- `logs/`: los archivos de registro de la app.

El archivo no contiene contraseñas: se guardan en el almacén de secretos del sistema, y al guardar una conexión se rechazan las propiedades adicionales con contraseñas y secretos. Tampoco incluye la clave de licencia ni los archivos de certificado. Los nombres de host, los usuarios y las rutas de archivo se guardan como texto sin cifrar, y los registros contienen direcciones de brokers procedentes de los mensajes de error de Kafka. Revisa el archivo antes de enviarlo fuera de tu empresa.

La app no envía nada por sí sola. Adjunta tú mismo el archivo a tu solicitud de soporte; consulta [Contactar con el soporte](troubleshooting.md#support).
