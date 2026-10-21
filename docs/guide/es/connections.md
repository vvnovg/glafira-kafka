# Conexiones

Una conexión es un perfil guardado de un clúster de Kafka: direcciones, ajustes de seguridad y contraseñas. Las conexiones aparecen en el panel [[ui:sidebar.connections]], a la izquierda. Puedes organizarlas en carpetas y llevarlas a otro equipo mediante la exportación y la importación.

## Crear una conexión {#create}

1. Haz clic en `+` ( [[ui:sidebar.newConnection]] ) en la cabecera del panel [[ui:sidebar.connections]]. Para crear la conexión directamente en una carpeta, abre el menú de la carpeta y elige [[ui:sidebar.folder.newConnection]].
2. En [[ui:connections.form.name]], escribe un nombre. Se muestra en el panel y en los títulos de las pestañas.
3. En [[ui:connections.form.bootstrap]], escribe una o varias direcciones en formato `host:port`, separadas por comas, por ejemplo `kafka1:9092,kafka2:9092`. Basta con un broker accesible: el cliente descubre los demás por sí solo.
4. Elige el protocolo en [[ui:connections.form.protocol]] y rellena los campos que añade. Consulta [Protocolos de seguridad](connections.md#security).
5. Si quieres, elige valores en [[ui:connections.form.folder]], [[ui:connections.form.environment]] y [[ui:connections.form.color]] (el color del punto junto al nombre).
6. Haz clic en [[ui:connections.form.test]] y luego en [[ui:common.save]].

Al guardar, el formulario se cierra y se abre el resumen del clúster.

Hay dos campos más para casos especiales. [[ui:connections.form.timeout]] es el tiempo de espera de la respuesta de un broker. El valor predeterminado es `30000` ms y el mínimo, `1000`. Auméntalo en redes lentas y VPN. [[ui:connections.form.props]] admite cualquier ajuste de librdkafka como `key=value`, uno por línea. Las líneas que empiezan por `#` se ignoran. Ahí no se permiten contraseñas ni claves: al guardar se rechazan las propiedades cuyo nombre contiene `password` o `secret`, `sasl.jaas.config`, `ssl.key.pem` y `ssl.keystore.key`.

Para cambiar una conexión más adelante, haz clic en `✎` ( [[ui:sidebar.editConnection]] ) junto a su nombre. Para eliminarla, abre el formulario, haz clic en [[ui:common.delete]] y confirma con [[ui:connections.form.deleteYes]].

## Probar la conexión {#test}

[[ui:connections.form.test]] comprueba los ajustes del formulario paso a paso y se detiene en el primer paso que falla:

1. [[ui:connections.form.step.config]]: las direcciones, los archivos de certificado, las contraseñas y el usuario SASL.
2. [[ui:connections.form.step.tcp]]: los nombres de host se resuelven y se abre una conexión TCP con al menos un servidor bootstrap.
3. [[ui:connections.form.step.kafka]]: el handshake TLS y el inicio de sesión SASL se completan y el broker devuelve los metadatos. Si todo va bien, el paso muestra el número de brokers y de topics.

La prueba usa los valores que hay en el formulario en ese momento, incluidas las contraseñas escritas y aún no guardadas. No se guarda nada ni se cambia nada en el clúster. Si un paso falla, debajo se muestran el texto del error y los detalles. Los errores habituales se explican en [No se puede conectar](troubleshooting.md#cannot-connect).

## Protocolos de seguridad {#security}

El valor de [[ui:connections.form.protocol]] debe coincidir con el listener del broker al que te conectas. Tus administradores de Kafka saben cuál usar.

- `PLAINTEXT`: sin cifrado ni inicio de sesión. Habitual en clústeres locales y de prueba.
- `SSL`: el tráfico se cifra con TLS. Con un certificado de cliente, es mTLS.
- `SASL_PLAINTEXT`: inicio de sesión con usuario y contraseña, sin cifrado.
- `SASL_SSL`: inicio de sesión con usuario y contraseña sobre TLS.

Al cambiar el protocolo, el formulario muestra solo los campos que necesita.

## SASL {#sasl}

Para `SASL_PLAINTEXT` y `SASL_SSL`, rellena tres campos:

- [[ui:connections.form.saslMechanism]]: `PLAIN`, `SCRAM-SHA-256` o `SCRAM-SHA-512`. El clúster determina cuál funciona.
- [[ui:connections.form.username]].
- [[ui:connections.form.password]]. Se guarda en el almacén de secretos del sistema, no en el perfil. Consulta [Contraseñas](connections.md#passwords).

SCRAM nunca envía la contraseña en sí. `PLAIN` la envía tal cual, así que con `SASL_PLAINTEXT` la contraseña viaja en texto claro. Siempre que puedas, usa `SASL_SSL` para clústeres que no estén en tu equipo.

## SSL y la CA de confianza {#tls}

Para `SSL` y `SASL_SSL`, el formulario añade los ajustes de TLS.

[[ui:connections.tls.ca]] es el certificado de la autoridad (CA) que firmó los certificados de los brokers. Déjalo vacío si el sistema operativo ya confía en esa CA: entonces se usan los certificados raíz del sistema. Si no, escribe la ruta completa del archivo o haz clic en [[ui:connections.tls.pick]]. Al elegir un archivo, aparece el campo [[ui:connections.tls.caFormat]]. Para PKCS#12 y JKS, también aparece el campo [[ui:connections.tls.truststorePassword]].

[[ui:connections.tls.verifyHostname]] está activado de forma predeterminada. Comprueba que el certificado del broker se emitió para el nombre de host al que te conectas. Desactívalo solo en entornos de prueba, por ejemplo cuando los brokers usan certificados autofirmados con otros nombres.

## Certificado de cliente (mTLS) {#mtls}

Si los brokers autentican a los clientes por certificado, activa [[ui:connections.tls.mtls]]. El certificado y la clave los emiten tus administradores de Kafka. Después rellena:

- [[ui:connections.tls.cert]]: la ruta del archivo del certificado.
- [[ui:connections.tls.certFormat]]: PEM, PKCS#12 o JKS.
- Para PEM: [[ui:connections.tls.key]], la ruta del archivo de la clave privada, y [[ui:connections.tls.keyPasswordPem]] si la clave está cifrada.
- Para PKCS#12: [[ui:connections.tls.keystorePassword]].
- Para JKS: [[ui:connections.tls.keystorePassword]], [[ui:connections.tls.keyPasswordJks]] si la clave tiene su propia contraseña y [[ui:connections.tls.alias]] si el almacén tiene varias claves.

Las rutas deben ser absolutas. Los archivos se leen cada vez que la app se conecta, así que, tras renovar el certificado, basta con volver a conectarse. La app comprueba que la clave corresponde al certificado y que el certificado no ha caducado.

Un caso habitual es un clúster en Kubernetes, donde el secret del cliente contiene `tls.crt`, `tls.key` y `ca.crt`. Guárdalos en archivos, elige el protocolo `SSL`, indica `ca.crt` en [[ui:connections.tls.ca]] y usa `tls.crt` y `tls.key` como certificado y clave de cliente PEM con contraseñas vacías.

## Formatos de certificado {#certificate-formats}

Glafira lee tres formatos. El formato se elige automáticamente según la extensión del archivo; compruébalo en el campo de formato y cámbialo si hace falta.

- PEM: un archivo de texto que empieza por `-----BEGIN`. Extensiones `.pem`, `.crt`, `.cer`, `.key`. En un certificado de cliente, la clave va en un archivo aparte. La CA de confianza también puede ser un certificado DER binario.
- PKCS#12: un almacén binario con certificado y clave. Extensiones `.p12`, `.pfx`.
- JKS: un almacén de claves de Java. Extensiones `.jks`, `.keystore`, `.truststore`.

Dos tipos de archivo deben convertirse antes:

- Los almacenes JCEKS no son compatibles. Conviértelos a PKCS#12:

```bash
keytool -importkeystore -srcstoretype JCEKS -srckeystore client.jceks -deststoretype PKCS12 -destkeystore client.p12
```

- Los almacenes PKCS#12 cifrados con algoritmos heredados (RC2, 3DES) pueden no abrirse. Si la app informa de un algoritmo heredado, vuelve a crear el archivo con AES:

```bash
openssl pkcs12 -export -keypbe AES-256-CBC -certpbe AES-256-CBC -in tls.crt -inkey tls.key -out client.p12
```

## Contraseñas {#passwords}

La contraseña SASL y las contraseñas de claves y almacenes se guardan en el almacén de secretos del sistema: el Llavero en macOS, el Administrador de credenciales en Windows y Secret Service en Linux. Nunca se escriben en el perfil de la conexión, en los archivos de exportación ni en el archivo de diagnóstico.

Al editar una conexión guardada, los campos de contraseña están vacíos y muestran [[ui:connections.form.passwordKeep]]. Deja un campo vacío para conservar la contraseña guardada o escribe una nueva para sustituirla. Para eliminar una contraseña de certificado guardada, marca [[ui:connections.tls.clear]] junto al campo. Al guardar, se eliminan las contraseñas que la conexión ya no usa, por ejemplo después de cambiar el protocolo a `PLAINTEXT`.

Si la app no puede acceder al almacén de secretos, se muestra un error al guardar. En Linux, comprueba que GNOME Keyring, KWallet u otro proveedor de Secret Service se esté ejecutando en tu sesión.

## Entornos y prod {#environments}

[[ui:connections.form.environment]] etiqueta la conexión: `local`, `dev`, `test`, `stage` o `prod`. El entorno aparece como una insignia de color junto al nombre en el panel, para que siempre veas con qué clúster trabajas.

`prod` añade protección. Cualquier cambio en una conexión prod, como crear un topic, enviar un mensaje, restablecer offsets o iniciar una sesión de consumer, te pide escribir el nombre de la conexión. Eliminar y purgar piden el nombre del objeto en cualquier conexión (al eliminar varios topics se pide su número) y, en prod, hay que escribir ambos. Estas confirmaciones aparecen dentro de la ventana de la app.

Una conexión prod nueva se crea con la opción [[ui:common.readOnly]] activada. Desmarca la casilla si necesitas hacer cambios.

## Solo lectura {#read-only}

El modo [[ui:common.readOnly]] bloquea todo lo que cambia el clúster: crear, cambiar, purgar y eliminar topics, enviar mensajes, restablecer offsets, eliminar grupos e iniciar sesiones de consumer. Los botones de estas acciones se ocultan o se deshabilitan, y el núcleo de la app también rechaza esas operaciones. Explorar y leer siguen disponibles. Un icono de candado junto al nombre en el panel indica las conexiones de solo lectura.

Usa este modo cuando solo necesites ver los datos, sobre todo en clústeres compartidos. El modo de solo lectura es una protección dentro de la app: no sustituye a los permisos de acceso (ACL) de Kafka.

## Carpetas {#folders}

Las carpetas ayudan a organizar las conexiones, por ejemplo por proyecto o por entorno. Las carpetas se pueden anidar.

- Para crear una carpeta en el nivel superior, haz clic en `+📁` ( [[ui:sidebar.newFolder]] ) en la cabecera del panel. Para crear una subcarpeta, abre el menú de la carpeta y elige [[ui:sidebar.newFolder]]. Escribe el nombre y pulsa `Enter`, o pulsa `Esc` para cancelar.
- Para abrir el menú de una carpeta, haz clic en `⋯` ( [[ui:sidebar.menu]] ) en su fila o haz clic derecho en ella. El menú contiene [[ui:sidebar.folder.rename]], [[ui:sidebar.folder.delete]] y los comandos de exportación e importación.
- Solo se puede eliminar una carpeta vacía. Mueve o elimina antes sus conexiones y subcarpetas.
- Los nombres de carpeta deben ser únicos dentro de un mismo nivel y tener como máximo 100 caracteres.

Para mover una conexión, arrástrala a una carpeta, o a otra conexión para colocarla delante de ella. Para llevarla al nivel superior, suéltala en la cabecera del panel o en el espacio vacío bajo la lista. También puedes elegir una carpeta en [[ui:connections.form.folder]] en el formulario de la conexión; [[ui:connections.form.folderRoot]] corresponde al nivel superior. Las carpetas se arrastran del mismo modo. Suelta una carpeta en el borde superior de otra para colocarla delante. Una carpeta no se puede mover a una de sus subcarpetas.

Haz clic en el nombre de una carpeta para contraerla o expandirla. La app recuerda qué carpetas están contraídas. Usa [[ui:sidebar.filter]] sobre el árbol para buscar conexiones y carpetas por nombre. Las carpetas que coinciden se muestran con todo su contenido.

## Exportar {#export}

La exportación guarda conexiones y carpetas en un archivo, por ejemplo para compartirlas con compañeros o llevarlas a otro equipo.

1. Para exportarlo todo, abre el menú `⋯` de la cabecera del panel y elige [[ui:sidebar.exportAll]]. Para exportar una carpeta con sus subcarpetas, elige [[ui:sidebar.folder.export]] en su menú.
2. Elige dónde guardar el archivo. El nombre propuesto es `glafira-connections-<date>.glafira-connections.json` o `<folder>.glafira-connections.json`.

Una vez guardado el archivo, el panel muestra cuántas conexiones y carpetas contiene.

El archivo incluye los ajustes de las conexiones y la estructura de carpetas. No contiene contraseñas ni propiedades secretas. Tampoco incluye los archivos de certificado: solo guarda sus rutas. Los hosts, los usuarios y las rutas se guardan como texto sin cifrar, así que comparte el archivo solo con quien pueda verlos.

## Importar {#import}

1. Abre el menú `⋯` de la cabecera del panel y elige [[ui:sidebar.import]]. Para importar directamente en una carpeta, elige [[ui:sidebar.folder.importHere]] en su menú.
2. Elige un archivo `.glafira-connections.json`. Se abre el cuadro de diálogo [[ui:exchange.import.title]].
3. En [[ui:exchange.import.target]], elige adónde van las carpetas y conexiones nuevas. [[ui:connections.form.folderRoot]] es el nivel superior.
4. Deja activada la opción [[ui:exchange.import.updateExisting]] para actualizar con los datos del archivo las conexiones que ya existen. Desactívala para dejarlas como están.
5. Revisa la vista previa y haz clic en [[ui:exchange.import.apply]], o en [[ui:common.cancel]] para cerrar el cuadro de diálogo sin cambios.

La vista previa se actualiza tras cada cambio en el cuadro de diálogo. Muestra los totales y listas desplegables: [[ui:exchange.import.groupAdded]], [[ui:exchange.import.groupUpdated]], [[ui:exchange.import.groupUnchanged]] y [[ui:exchange.import.groupSkipped]], con el motivo de cada conexión omitida.

Cómo relaciona la app el archivo con tus conexiones:

- Las conexiones se relacionan por un ID interno, no por el nombre. Una conexión se considera existente si el archivo contiene esa misma conexión: por ejemplo, se exportó desde este equipo o se importó antes desde el mismo origen.
- Una conexión actualizada recibe los ajustes del archivo. Se conservan tus contraseñas y su posición en el árbol.
- Las carpetas existentes no se renombran ni se mueven. Las conexiones nuevas del archivo se colocan igualmente en ellas.
- Si una conexión nueva tiene el mismo nombre que otra de la misma carpeta, se le añade un número, por ejemplo `kafka-dev (2)`.
- Se omiten las conexiones que esta versión de Glafira no admite o que contienen secretos en las propiedades adicionales.

La vista previa también avisa cuando no se encuentran archivos de certificado en este equipo. Las contraseñas no están en el archivo, así que tras la importación el panel indica cuántas conexiones las necesitan. Abre cada una de esas conexiones, escribe las contraseñas y guarda.
