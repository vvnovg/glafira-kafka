# 连接

连接是保存下来的一个 Kafka 集群的配置：地址、安全设置和密码。连接列在左侧的[[ui:sidebar.connections]]面板中。你可以将连接整理到文件夹中，也可以通过导出和导入将其转移到另一台计算机。

## 创建连接 {#create}

1. 点击[[ui:sidebar.connections]]面板标题栏中的 `+`（[[ui:sidebar.newConnection]]）。如果要直接在某个文件夹中创建连接，请打开该文件夹的菜单，选择[[ui:sidebar.folder.newConnection]]。
2. 输入[[ui:connections.form.name]]。名称会显示在面板和标签页标题中。
3. 在 [[ui:connections.form.bootstrap]]中输入一个或多个 `host:port` 格式的地址，用逗号分隔，例如 `kafka1:9092,kafka2:9092`。只要有一个 Broker 可达即可，客户端会自行发现其他 Broker。
4. 选择[[ui:connections.form.protocol]]，并填写该协议增加的字段。参见[安全协议](connections.md#security)。
5. 可以选择[[ui:connections.form.folder]]、[[ui:connections.form.environment]]，以及名称旁圆点的[[ui:connections.form.color]]。
6. 点击[[ui:connections.form.test]]，然后点击[[ui:common.save]]。

保存后，表单关闭并打开集群概览。

另有两个字段用于特殊情况。[[ui:connections.form.timeout]]，即等待 Broker 响应的时长，默认为 `30000` ms，最小值为 `1000`。网络较慢或使用 VPN 时可适当调大。[[ui:connections.form.props]]接受任意 librdkafka 设置，格式为 `key=value`，每行一个。以 `#` 开头的行会被忽略。此处不允许填写密码和密钥：名称中含有 `password` 或 `secret` 的属性，以及 `sasl.jaas.config`、`ssl.key.pem` 和 `ssl.keystore.key`，会在保存时被拒绝。

以后要修改连接，请点击名称旁的 `✎`（[[ui:sidebar.editConnection]]）。要删除连接，请打开表单，点击[[ui:common.delete]]，然后点击[[ui:connections.form.deleteYes]]确认。

## 测试连接 {#test}

[[ui:connections.form.test]]会逐步检查表单中的设置，并在第一个失败的步骤处停止：

1. [[ui:connections.form.step.config]]：地址、证书文件、密码和 SASL 用户名。
2. [[ui:connections.form.step.tcp]]：主机名能够解析，并且至少能与一个 Bootstrap 服务器建立 TCP 连接。
3. [[ui:connections.form.step.kafka]]：TLS 握手和 SASL 登录成功，并且 Broker 返回元数据。成功时，该步骤会显示 Broker 数和主题数。

测试使用表单中的当前值，包括已输入但尚未保存的密码。不会保存任何内容，也不会对集群做任何更改。如果某个步骤失败，其下方会显示错误文本和详细信息。常见错误的说明见[无法连接](troubleshooting.md#cannot-connect)。

## 安全协议 {#security}

[[ui:connections.form.protocol]]必须与要连接的 Broker 监听器一致。应使用哪种协议，请询问你的 Kafka 管理员。

- `PLAINTEXT`：不加密，也不登录。常用于本地和测试集群。
- `SSL`：使用 TLS 加密流量。使用客户端证书时即为 mTLS。
- `SASL_PLAINTEXT`：使用用户名和密码登录，不加密。
- `SASL_SSL`：通过 TLS 使用用户名和密码登录。

更改协议后，表单只显示该协议所需的字段。

## SASL {#sasl}

对于 `SASL_PLAINTEXT` 和 `SASL_SSL`，需要填写三个字段：

- [[ui:connections.form.saslMechanism]]：`PLAIN`、`SCRAM-SHA-256` 或 `SCRAM-SHA-512`，具体取决于集群。
- [[ui:connections.form.username]]。
- [[ui:connections.form.password]]。密码保存在系统密钥存储中，而不是连接配置中。参见[密码](connections.md#passwords)。

SCRAM 从不发送密码本身。`PLAIN` 会原样发送密码，因此通过 `SASL_PLAINTEXT` 时密码以明文传输。对于不在本机上的集群，请尽量使用 `SASL_SSL`。

## SSL 和受信任的 CA {#tls}

对于 `SSL` 和 `SASL_SSL`，表单会增加 TLS 设置。

[[ui:connections.tls.ca]]是为 Broker 证书签名的证书颁发机构（CA）的证书。如果操作系统已信任该 CA，请留空，此时将使用系统根证书。否则请输入文件的完整路径，或点击[[ui:connections.tls.pick]]，选择文件后会出现 [[ui:connections.tls.caFormat]]字段。对于 PKCS#12 和 JKS，还有[[ui:connections.tls.truststorePassword]]字段。

[[ui:connections.tls.verifyHostname]]默认开启，用于检查 Broker 证书是否为你所连接的主机名签发。仅在测试环境中关闭它，例如 Broker 使用其他名称的自签名证书时。

## 客户端证书（mTLS） {#mtls}

如果 Broker 通过证书验证客户端身份，请开启[[ui:connections.tls.mtls]]。证书和私钥由你的 Kafka 管理员签发。然后填写：

- [[ui:connections.tls.cert]]：证书文件的路径。
- [[ui:connections.tls.certFormat]]：PEM、PKCS#12 或 JKS。
- PEM：[[ui:connections.tls.key]]，即私钥文件的路径；如果私钥已加密，还需填写[[ui:connections.tls.keyPasswordPem]]。
- PKCS#12：[[ui:connections.tls.keystorePassword]]。
- JKS：[[ui:connections.tls.keystorePassword]]；如果私钥有单独的密码，还需填写[[ui:connections.tls.keyPasswordJks]]；如果存储中有多个私钥，还需填写[[ui:connections.tls.alias]]。

路径必须为绝对路径。应用每次连接时都会读取这些文件，因此证书续期后只需重新连接。应用会检查私钥是否与证书匹配，以及证书是否已过期。

常见的情况是 Kubernetes 中的集群，其客户端 Secret 包含 `tls.crt`、`tls.key` 和 `ca.crt`。将它们保存为文件，选择协议 `SSL`，将[[ui:connections.tls.ca]]设为 `ca.crt`，并将 `tls.crt` 和 `tls.key` 用作 PEM 客户端证书和私钥，密码留空。

## 证书格式 {#certificate-formats}

Glafira 支持三种格式。格式会根据文件扩展名自动选择，请在格式字段中确认，必要时进行更改。

- PEM：以 `-----BEGIN` 开头的文本文件。扩展名为 `.pem`、`.crt`、`.cer`、`.key`。客户端证书的私钥是单独的文件。受信任的 CA 也可以是二进制 DER 证书。
- PKCS#12：包含证书和私钥的二进制存储。扩展名为 `.p12`、`.pfx`。
- JKS：Java 密钥库。扩展名为 `.jks`、`.keystore`、`.truststore`。

有两类文件需要先转换：

- 不支持 JCEKS 存储。请将其转换为 PKCS#12：

```bash
keytool -importkeystore -srcstoretype JCEKS -srckeystore client.jceks -deststoretype PKCS12 -destkeystore client.p12
```

- 使用旧算法（RC2、3DES）加密的 PKCS#12 存储可能无法打开。如果应用提示使用了旧算法，请用 AES 重新生成文件：

```bash
openssl pkcs12 -export -keypbe AES-256-CBC -certpbe AES-256-CBC -in tls.crt -inkey tls.key -out client.p12
```

## 密码 {#passwords}

SASL 密码以及私钥和存储的密码保存在系统密钥存储中：macOS 上为钥匙串，Windows 上为凭据管理器，Linux 上为 Secret Service。它们绝不会写入连接配置、导出文件或诊断归档。

编辑已保存的连接时，密码字段为空，并显示[[ui:connections.form.passwordKeep]]。将字段留空即保留已保存的密码，输入新密码则会替换它。要删除已保存的证书密码，请选中字段旁的[[ui:connections.tls.clear]]。保存时，连接不再使用的密码会被删除，例如将协议切换为 `PLAINTEXT` 之后。

如果应用无法访问密钥存储，保存时会显示错误。在 Linux 上，请检查你的会话中是否运行着 GNOME Keyring、KWallet 或其他 Secret Service 提供程序。

## 环境与 prod {#environments}

[[ui:connections.form.environment]]用于标记连接：`local`、`dev`、`test`、`stage` 或 `prod`。环境以彩色标记显示在面板中的名称旁，让你随时知道正在操作哪个集群。

`prod` 会增加保护。对生产环境连接的任何更改，例如创建主题、发送消息、重置偏移量或启动消费者会话，都会要求输入连接名称。在任何连接上，删除和清空都会要求输入对象名称（删除多个主题时要求输入数量），在生产环境连接上则两者都要输入。这些确认显示在应用窗口内。

新建的生产环境连接默认开启[[ui:common.readOnly]]。如需进行更改，请取消选中该复选框。

## 只读 {#read-only}

[[ui:common.readOnly]]会阻止所有更改集群的操作：创建、修改、清空和删除主题，发送消息，重置偏移量，删除组以及启动消费者会话。这些操作的按钮会被隐藏或禁用，应用内核也会拒绝此类操作。浏览和读取仍然可用。只读连接在面板中的名称旁带有锁形图标。

只需查看数据时，尤其是在共享集群上，请使用此模式。只读是应用内部的保护措施，不能替代 Kafka 中的访问权限（ACL）。

## 文件夹 {#folders}

文件夹用于整理连接，例如按项目或按环境分类。文件夹可以嵌套。

- 要在顶层创建文件夹，请点击面板标题栏中的 `+📁`（[[ui:sidebar.newFolder]]）。要创建子文件夹，请打开文件夹菜单，选择[[ui:sidebar.newFolder]]。输入名称后按 `Enter`，或按 `Esc` 取消。
- 要打开文件夹菜单，请点击文件夹行上的 `⋯`（[[ui:sidebar.menu]]），或右键点击文件夹。菜单中有[[ui:sidebar.folder.rename]]、[[ui:sidebar.folder.delete]]以及导出和导入命令。
- 只能删除空文件夹。请先移动或删除其中的连接和子文件夹。
- 同一层级中的文件夹名称必须唯一，长度不超过 100 个字符。

要移动连接，请将其拖到某个文件夹上，或拖到另一个连接上，使其排在该连接之前。要将其移到顶层，请将其放到面板标题栏或列表下方的空白处。也可以在连接表单中选择[[ui:connections.form.folder]]，其中“[[ui:connections.form.folderRoot]]”表示顶层。文件夹的拖动方式相同。将文件夹放到另一个文件夹的上边缘，可使其排在该文件夹之前。文件夹不能移入它自己的子文件夹。

点击文件夹名称可将其折叠或展开，应用会记住哪些文件夹已折叠。使用树上方的[[ui:sidebar.filter]]可按名称查找连接和文件夹。匹配的文件夹会连同其全部内容一起显示。

## 导出 {#export}

导出会将连接和文件夹保存到文件中，例如用于与同事共享或转移到另一台计算机。

1. 要导出全部内容，请打开面板标题栏菜单 `⋯`，选择“[[ui:sidebar.exportAll]]”。要导出一个文件夹及其子文件夹，请在该文件夹的菜单中选择“[[ui:sidebar.folder.export]]”。
2. 选择文件的保存位置。建议的文件名为 `glafira-connections-<date>.glafira-connections.json` 或 `<folder>.glafira-connections.json`。

文件保存后，面板会显示其中包含的连接数和文件夹数。

该文件包含连接设置和文件夹结构，不包含密码或机密属性，也不包含证书文件，只保留证书文件的路径。主机、用户名和路径以纯文本形式存储，因此请只与有权查看这些信息的人共享该文件。

## 导入 {#import}

1. 打开面板标题栏菜单 `⋯`，选择“[[ui:sidebar.import]]”。要直接导入到某个文件夹，请在该文件夹的菜单中选择“[[ui:sidebar.folder.importHere]]”。
2. 选择 `.glafira-connections.json` 文件，此时会打开[[ui:exchange.import.title]]对话框。
3. 在[[ui:exchange.import.target]]中，选择新文件夹和新连接的位置。“[[ui:connections.form.folderRoot]]”表示顶层。
4. 保持[[ui:exchange.import.updateExisting]]开启，即可用文件中的内容更新已有的连接。关闭它则保持这些连接不变。
5. 检查预览，然后点击[[ui:exchange.import.apply]]；或点击[[ui:common.cancel]]，不做任何更改并关闭对话框。

对话框中每次更改后，预览都会更新。预览显示汇总数量和可展开的列表：[[ui:exchange.import.groupAdded]]、[[ui:exchange.import.groupUpdated]]、[[ui:exchange.import.groupUnchanged]]和[[ui:exchange.import.groupSkipped]]，并列出每个被跳过的连接的原因。

应用如何将文件与你的连接进行匹配：

- 连接按内部 ID 匹配，而不是按名称。如果文件中包含同一个连接，则该连接视为已有，例如它是从这台计算机导出的，或之前从同一来源导入过。
- 被更新的连接会获得文件中的设置，你的密码及其在树中的位置保持不变。
- 已有的文件夹不会被重命名或移动，文件中的新连接仍会放入其中。
- 如果新连接与同一文件夹中的另一个连接同名，会在名称后添加编号，例如 `kafka-dev (2)`。
- 当前版本的 Glafira 不支持的连接，或额外属性中包含机密信息的连接，会被跳过。

如果在这台计算机上找不到证书文件，预览也会发出警告。文件中不包含密码，因此导入后面板会显示有多少个连接需要输入密码。请逐一打开这些连接，输入密码并保存。
