# 许可证与更新

本章介绍 Pro 增加的功能、如何激活许可证密钥、许可证到期后会发生什么、如何更新应用、[[ui:sidebar.about]]页面以及诊断归档。

## Free 和 Pro {#free-and-pro}

Free 涵盖 Kafka 的日常工作：包括 mTLS 在内的所有安全协议、集群概览和主题、浏览消息、支持批量发送的 Producer、消费者组以及偏移量重置。所有界面语言也都包含在内。参见[界面语言](getting-started.md#language)。

Pro 增加：

- 消费者会话：以组成员身份读取、提交偏移量、暂停分区。参见[消费者会话](consumer-sessions.md#what-is)；
- 生产模式：创建 prod 连接、将连接切换为 prod，以及在 prod 连接上取消[[ui:common.readOnly]]。参见[环境与 prod](connections.md#environments)。

在 Free 中，这些功能可见但不可用。消费者会话命令被禁用，并带有锁形图标和 `Pro` 标记。prod 选项显示为[[ui:connections.form.prodPro]]。如果某项操作需要 Pro，错误信息会显示[[ui:errors.license.proRequired]]，并提供按钮：[[ui:common.openLicense]]。

[[ui:tabs.license]]页面在[[ui:license.whatProGives]]下列出消费者会话和生产模式。

## 激活密钥 {#activate}

许可证密钥是签发给个人或组织的一行文本。应用在你的计算机上检查密钥，不会连接任何服务器。

1. 点击左侧面板底部的[[ui:tabs.license]]。
2. 将完整的密钥粘贴到[[ui:license.keyLabel]]中。
3. 点击[[ui:license.activate]]。

Pro 功能立即可用。页面会显示[[ui:license.tier]]（Pro）、[[ui:license.licensee]]、[[ui:license.kind]]（[[ui:license.kindPersonal]]或[[ui:license.kindOrganization]]）以及[[ui:license.expires]]：某个日期，或[[ui:license.perpetual]]。密钥保存在应用数据文件夹中，因此重启或更新后仍然有效。新密钥会替换之前的密钥。

如果密钥未被接受，字段下方会显示原因：

- [[ui:errors.license.malformed]]：密钥的一部分缺失或被更改。请重新复制密钥，确保为一行且没有多余字符。
- [[ui:errors.license.badSignature]]：该密钥不是为 Glafira 签发的，或已被编辑。
- 密钥已过期：消息中会给出其到期日期。请申请新密钥。
- 密钥需要更新版本的应用：消息中会给出密钥版本。请先更新应用，参见[更新](license-updates.md#updates)。

要移除密钥，请点击[[ui:license.removeKey]]。应用会询问“[[ui:license.removeQuestion]]”点击[[ui:license.removeYes]]确认。你的数据会保留。

组织可以通过策略为所有用户激活 Pro。此时密钥字段和按钮会被隐藏，页面会显示“[[ui:license.managed]]”设置了策略密钥时，你自己输入的密钥不会被使用。[[ui:license.policyTitle]]部分会显示策略的来源并列出其错误。如果组织密钥无效，[[ui:tabs.license]]页面上会显示原因，这时请联系你的管理员。管理员可以在 Glafira 源代码的 `docs/admin.md` 中找到策略设置。

## 到期 {#expiry}

密钥可以有到期日期，显示在[[ui:license.expires]]中。许可证在该日结束前有效。

剩余不到 14 天时，标签页上方的通知会显示剩余天数，[[ui:tabs.license]]页面会显示到期日期。通知中的[[ui:license.notice.more]]会打开[[ui:tabs.license]]页面。`×` 按钮会隐藏通知，直到应用重启。

到期日期过后，套餐切换为 Free。通知和[[ui:tabs.license]]页面会显示许可证已过期、Pro 功能已停用。变化如下：

- 无法启动新的消费者会话。在已运行的会话中，提交和暂停分区会被拒绝，错误信息中会给出到期日期。
- prod 连接继续可用，确认方式不变。无法创建新的 prod 连接，也无法在 prod 连接上取消[[ui:common.readOnly]]。

连接、密码、模板和发送历史记录都会保留。要继续使用 Pro，请按[激活密钥](license-updates.md#activate)中的说明激活新密钥。

## 更新 {#updates}

应用启动时会检查一次是否有新版本。如果有，标签页上方的通知会显示其版本号。[[ui:license.notice.more]]会打开[[ui:sidebar.about]]页面。如果检查失败（例如无法访问互联网），不会显示任何消息。

手动检查：

1. 打开左侧面板底部的[[ui:sidebar.about]]。
2. 在[[ui:about.updates.title]]中，点击[[ui:about.updates.check]]。
3. 如果有新版本，页面会显示其版本号和发行说明。点击[[ui:about.updates.install]]。

应用会下载更新、检查其签名、进行安装，然后重启。因网络问题中断的下载会自动重试，总共最多尝试三次。如果三次都失败，应用会报告[[ui:errors.updates.downloadInterrupted]]。如果签名检查失败，应用会报告[[ui:errors.updates.badSignature]]。

安装时可能会要求输入管理员密码，因为应用是为这台计算机的所有用户安装的：在 macOS 上通过 `.pkg` 安装程序，在 Windows 上通过 `.msi` 安装程序，在 Linux 上通过 `.deb` 或 `.rpm` 软件包。没有管理员权限的用户无法更新此类安装。

如果正在读取消息、消费者会话正在运行或正在批量发送，应用会先询问，并显示有多少个活动会话。点击[[ui:about.updates.stopAndUpdate]]可停止这些会话并继续，或点击[[ui:common.cancel]]。更新后，连接、设置、许可证密钥和其他数据都会保留。

更新从 GitHub 上的发布页面下载。如果检查失败并显示[[ui:errors.updates.checkFailed]]，请检查网络或代理。[[ui:errors.updates.notConfigured]]表示此 build 在构建时未包含更新程序密钥，例如开发或测试 build。这种情况下，请从发布页面下载新版本：https://github.com/vvnovg/glafira-kafka-releases/releases

组织可以通过策略关闭更新检查，或将其指向内部镜像。关闭检查后，点击[[ui:about.updates.check]]时，[[ui:about.updates.title]]部分会显示[[ui:errors.updates.managed]]。与[[ui:errors.updates.notConfigured]]一样，此消息只在点击后出现；在这些情况下，启动时的检查不会显示任何内容。

## 关于 {#about}

打开左侧面板底部的[[ui:sidebar.about]]。在 macOS 上，也可以在“帮助”菜单中找到[[ui:menu.about]]。

该页面显示应用的简短介绍以及：

- [[ui:about.version]]：版本号；[[ui:about.build]]：构建所用源代码的标识符。提交支持请求时请同时附上这两项。
- [[ui:about.system]]：操作系统和处理器架构。
- [[ui:about.tier]]：Free 或 Pro。
- [[ui:about.policy]]：[[ui:about.policySet]]或[[ui:about.policyNone]]。策略中的错误会列在下方。
- [[ui:about.dataDir]]（存放连接配置、模板、历史记录、设置和许可证密钥的文件夹）。
- [[ui:about.logDir]]（存放日志文件的文件夹）。参见[日志](troubleshooting.md#logs)。

[[ui:about.updates.title]]和[[ui:about.diagnostics.title]]部分也位于此页面。

## 诊断归档 {#diagnostics}

诊断归档会收集技术支持了解问题所需的信息。创建方法：

1. 打开[[ui:sidebar.about]]。
2. 在[[ui:about.diagnostics.title]]中，点击[[ui:about.diagnostics.collect]]。在 macOS 上，也可以在“帮助”菜单中选择[[ui:menu.diagnostics]]。
3. 选择文件的保存位置。建议的文件名为 `diagnostics-<date>.zip`。

归档保存后，页面会显示其路径。ZIP 文件包含：

- `info.json`：应用版本和 build、操作系统、数据和日志文件夹路径、策略状态以及许可证状态（套餐、授权对象、类型、到期日期）；
- `connections.json`：连接配置：名称、环境、Bootstrap 服务器、协议、SASL 用户名、证书文件路径、额外属性和超时设置；
- `logs/`：应用日志文件。

归档中不包含密码：密码保存在系统密钥存储中，而且保存连接时会拒绝包含密码和机密信息的额外属性。许可证密钥和证书文件也不包含在内。主机名、用户名和文件路径以纯文本形式存储，日志中还包含 Kafka 错误消息里的 Broker 地址。将归档发送到公司外部之前，请先检查其内容。

应用不会自行发送任何内容。请自行将归档附加到支持请求中，参见[联系技术支持](troubleshooting.md#support)。
