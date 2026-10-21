# 更新与诊断

本章介绍如何更新应用、[[ui:sidebar.about]]页面、诊断归档以及如何支持本项目。

## 更新 {#updates}

应用启动时会检查一次是否有新版本。如果有，标签页上方的通知会显示其版本号。[[ui:about.notice.more]]会打开[[ui:sidebar.about]]页面。如果检查失败（例如无法访问互联网），不会显示任何消息。

手动检查：

1. 打开左侧面板底部的[[ui:sidebar.about]]。
2. 在[[ui:about.updates.title]]中，点击[[ui:about.updates.check]]。
3. 如果有新版本，页面会显示其版本号和发行说明。点击[[ui:about.updates.install]]。

应用会下载更新、检查其签名、进行安装，然后重启。因网络问题中断的下载会自动重试，总共最多尝试三次。如果三次都失败，应用会报告[[ui:errors.updates.downloadInterrupted]]。如果签名检查失败，应用会报告[[ui:errors.updates.badSignature]]。

安装时可能会要求输入管理员密码，因为应用是为这台计算机的所有用户安装的：在 macOS 上通过 `.pkg` 安装程序，在 Windows 上通过 `.msi` 安装程序，在 Linux 上通过 `.deb` 或 `.rpm` 软件包。没有管理员权限的用户无法更新此类安装。

如果正在读取消息、消费者会话正在运行或正在批量发送，应用会先询问，并显示有多少个活动会话。点击[[ui:about.updates.stopAndUpdate]]可停止这些会话并继续，或点击[[ui:common.cancel]]。更新后，连接、设置和其他数据都会保留。

更新从 GitHub 上的发布页面下载。如果检查失败并显示[[ui:errors.updates.checkFailed]]，请检查网络或代理。[[ui:errors.updates.notConfigured]]表示此 build 在构建时未包含更新程序密钥，例如开发或测试 build。这种情况下，请从发布页面下载新版本：https://github.com/vvnovg/glafira-kafka-releases/releases

组织可以通过策略关闭更新检查，或将其指向内部镜像。关闭检查后，点击[[ui:about.updates.check]]时，[[ui:about.updates.title]]部分会显示[[ui:errors.updates.managed]]。与[[ui:errors.updates.notConfigured]]一样，此消息只在点击后出现；在这些情况下，启动时的检查不会显示任何内容。

## 关于 {#about}

打开左侧面板底部的[[ui:sidebar.about]]。在 macOS 上，也可以在“帮助”菜单中找到[[ui:menu.about]]。

该页面显示应用的简短介绍以及：

- [[ui:about.version]]：版本号；[[ui:about.build]]：此 build 所用源代码的标识符。提交支持请求时请同时附上这两项。
- [[ui:about.system]]：操作系统和处理器架构。
- [[ui:about.license]]：Apache 2.0，链接会在浏览器中打开许可证全文。
- [[ui:about.policy]]：[[ui:about.policySet]]或[[ui:about.policyNone]]。策略中的错误会列在下方。
- “[[ui:about.dataDir]]”一项显示存放连接配置、模板、历史记录、设置的文件夹。
- “[[ui:about.logDir]]”一项显示存放日志文件的文件夹。参见[日志](troubleshooting.md#logs)。

[[ui:about.updates.title]]和[[ui:about.diagnostics.title]]部分也位于此页面。

## 诊断归档 {#diagnostics}

诊断归档会收集技术支持了解问题所需的信息。创建方法：

1. 打开[[ui:sidebar.about]]页面。
2. 在[[ui:about.diagnostics.title]]中，点击[[ui:about.diagnostics.collect]]。在 macOS 上，也可以在“帮助”菜单中选择[[ui:menu.diagnostics]]。
3. 选择文件的保存位置。建议的文件名为 `diagnostics-<date>.zip`。

归档保存后，页面会显示其路径。ZIP 文件包含：

- `info.json`：应用版本和 build、操作系统、数据和日志文件夹路径、策略状态；
- `connections.json`：连接配置：名称、环境、Bootstrap 服务器、协议、SASL 用户名、证书文件路径、额外属性和超时设置；
- `logs/`：应用日志文件。

归档中不包含密码：密码保存在系统密钥存储中，而且保存连接时会拒绝包含密码和机密信息的额外属性。证书文件也不包含在内。主机名、用户名和文件路径以纯文本形式存储，日志中还包含 Kafka 错误消息里的 Broker 地址。将归档发送到公司外部之前，请先检查其内容。

应用不会自行发送任何内容。请自行将归档附加到支持请求中，参见[联系技术支持](troubleshooting.md#support)。

## 支持本项目 {#support}

Glafira 免费且开源，采用 Apache License 2.0。捐助帮助它持续发展。左侧面板底部、[[ui:sidebar.about]]页面以及 macOS 上“帮助”菜单中的[[ui:support.title]]会在浏览器中打开支持页面：https://github.com/vvnovg/glafira-kafka-releases/blob/main/SUPPORT.md
