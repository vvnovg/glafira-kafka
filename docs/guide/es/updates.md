# Actualizaciones y diagnóstico

Este capítulo explica cómo actualizar la app, la pantalla [[ui:sidebar.about]], el archivo de diagnóstico y cómo apoyar el proyecto.

## Actualizaciones {#updates}

Al iniciarse, la app comprueba una vez si hay una versión nueva. Si la hay, un aviso sobre las pestañas indica su versión. [[ui:about.notice.more]] abre la pantalla [[ui:sidebar.about]]. Si la comprobación falla, por ejemplo sin acceso a internet, no se muestra ningún mensaje.

Para comprobarlo manualmente:

1. Abre [[ui:sidebar.about]] en la parte inferior del panel izquierdo.
2. En la sección [[ui:about.updates.title]], haz clic en [[ui:about.updates.check]].
3. Si hay una versión nueva, la pantalla muestra su número y las notas de la versión. Haz clic en [[ui:about.updates.install]].

La app descarga la actualización, comprueba su firma, la instala y se reinicia. Si la descarga se interrumpe por un problema de red, se reintenta automáticamente, hasta tres intentos en total. Si los tres intentos fallan, la app muestra [[ui:errors.updates.downloadInterrupted]]. Si la comprobación de la firma falla, la app muestra [[ui:errors.updates.badSignature]].

La instalación puede pedir una contraseña de administrador, porque la app está instalada para todos los usuarios del equipo: con el instalador `.pkg` en macOS, el instalador `.msi` en Windows o un paquete `.deb` o `.rpm` en Linux. Los usuarios sin derechos de administrador no pueden actualizar una instalación así.

Si se están leyendo mensajes, hay una sesión de consumer en ejecución o se está enviando un lote, la app pregunta antes e indica cuántas sesiones están activas. Haz clic en [[ui:about.updates.stopAndUpdate]] para detenerlas y continuar, o en [[ui:common.cancel]]. Las conexiones, los ajustes y los demás datos se conservan tras una actualización.

Las actualizaciones se descargan desde la página de versiones en GitHub. Si la comprobación falla con [[ui:errors.updates.checkFailed]], revisa tu red o tu proxy. [[ui:errors.updates.notConfigured]] significa que este build se creó sin la clave del actualizador, por ejemplo un build de desarrollo o de prueba. En ese caso, descarga las versiones nuevas desde la página de versiones: https://github.com/vvnovg/glafira-kafka-releases/releases

Una organización puede desactivar la búsqueda de actualizaciones mediante una política o dirigirla a una réplica interna. Cuando la búsqueda está desactivada, la sección [[ui:about.updates.title]] indica [[ui:errors.updates.managed]] después de hacer clic en [[ui:about.updates.check]]. Al igual que [[ui:errors.updates.notConfigured]], este mensaje solo aparece tras el clic; en estos casos, la comprobación al iniciar no muestra nada.

## Acerca de {#about}

Abre [[ui:sidebar.about]] en la parte inferior del panel izquierdo. En macOS, también está en el menú Ayuda como [[ui:menu.about]].

La pantalla muestra una breve descripción de la app y además:

- [[ui:about.version]] — el número de versión, y [[ui:about.build]] — el identificador del código fuente a partir del cual se creó. Incluye ambos en una solicitud de soporte.
- [[ui:about.system]] — el sistema operativo y la arquitectura del procesador.
- [[ui:about.license]] — Apache 2.0; el enlace abre el texto de la licencia en el navegador.
- [[ui:about.policy]] — [[ui:about.policySet]] o [[ui:about.policyNone]]. Los errores de la política se enumeran debajo.
- [[ui:about.dataDir]] — la carpeta con los perfiles de conexión, las plantillas, el historial y los ajustes.
- [[ui:about.logDir]] — la carpeta con los archivos de registro. Consulta [Registros](troubleshooting.md#logs).

Las secciones [[ui:about.updates.title]] y [[ui:about.diagnostics.title]] también están en esta pantalla.

## Archivo de diagnóstico {#diagnostics}

El archivo de diagnóstico reúne lo que el soporte necesita para entender un problema. Para crearlo:

1. Abre [[ui:sidebar.about]].
2. En la sección [[ui:about.diagnostics.title]], haz clic en [[ui:about.diagnostics.collect]]. En macOS, también puedes elegir [[ui:menu.diagnostics]] en el menú Ayuda.
3. Elige dónde guardar el archivo. El nombre propuesto es `diagnostics-<date>.zip`.

Una vez guardado el archivo, la pantalla muestra su ruta. El archivo ZIP contiene:

- `info.json`: la versión y el build de la app, el sistema operativo, las rutas de las carpetas de datos y de registros, y el estado de la política;
- `connections.json`: los perfiles de conexión: nombres, entornos, servidores bootstrap, protocolos, usuarios SASL, rutas de los archivos de certificado, propiedades adicionales y tiempos de espera;
- `logs/`: los archivos de registro de la app.

El archivo no contiene contraseñas: se guardan en el almacén de secretos del sistema, y al guardar una conexión se rechazan las propiedades adicionales con contraseñas y secretos. Tampoco incluye los archivos de certificado. Los nombres de host, los usuarios y las rutas de archivo se guardan como texto sin cifrar, y los registros contienen direcciones de brokers procedentes de los mensajes de error de Kafka. Revisa el archivo antes de enviarlo fuera de tu empresa.

La app no envía nada por sí sola. Adjunta tú mismo el archivo a tu solicitud de soporte; consulta [Contactar con el soporte](troubleshooting.md#support).

## Apoyar el proyecto {#support}

Glafira es gratuita y de código abierto bajo la Apache License 2.0. Las donaciones la mantienen viva. [[ui:support.title]] en la parte inferior del panel izquierdo, en la pantalla [[ui:sidebar.about]] y, en macOS, en el menú Ayuda abre la página de apoyo en el navegador: https://github.com/vvnovg/glafira-kafka-releases/blob/main/SUPPORT.md
