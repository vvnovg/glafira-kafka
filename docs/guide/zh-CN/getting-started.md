# 快速入门

Glafira for Apache Kafka 是一款 Kafka 集群桌面客户端，可用于浏览主题和消费者组、读取和发送消息以及重置偏移量。本章介绍安装、首次启动、用于练习的本地 Kafka 以及界面语言。

## 在 macOS 上安装 {#install-macos}

Glafira 支持 macOS 11 及更高版本。请从发布页面下载安装程序：https://github.com/vvnovg/glafira-kafka-releases/releases

根据 Mac 机型选择文件：Apple 芯片（M1 及更新机型）选 `aarch64`，Intel 选 `x64`。

1. 打开 `.dmg` 文件。
2. 将 Glafira for Apache Kafka 拖到“应用程序”文件夹。
3. 从“应用程序”或启动台启动应用。

也可以使用 `.pkg` 安装程序，它会将应用安装到 `/Applications`，供所有用户使用。

Beta 版尚未经过 Apple 签名，因此 macOS 会阻止首次启动。允许启动的方法：

1. 先尝试打开一次应用，然后关闭警告。
2. 打开“系统设置”→“隐私与安全性”。
3. 向下滚动到有关 Glafira for Apache Kafka 的提示，点击“仍要打开”。如有要求，输入密码确认。

在 macOS 14 及更早版本上，也可以右键点击应用，选择“打开”，然后再次点击“打开”。

如果这样仍无效，请在“终端”中执行一次以下命令，移除隔离标记：

```bash
xattr -dr com.apple.quarantine "/Applications/Glafira for Apache Kafka.app"
```

如果下载的 `.dmg` 本身无法打开，请先移除它的标记：

```bash
xattr -d com.apple.quarantine ~/Downloads/Glafira_for_Apache_Kafka_*.dmg
```

通过应用内更新安装的版本无需执行这些步骤。

## 在 Windows 上安装 {#install-windows}

从发布页面下载 `.msi` 安装程序（`x64`）：https://github.com/vvnovg/glafira-kafka-releases/releases

1. 运行 `.msi` 文件。
2. 在 Windows 提示中确认安装。应用会为这台计算机的所有用户安装，因此需要管理员权限。
3. 从“开始”菜单启动 Glafira for Apache Kafka。

Beta 版尚未签名。如果 SmartScreen 阻止了安装程序，请点击“更多信息”，然后点击“仍要运行”。也可以在运行前通过 PowerShell 解除对文件的阻止：

```powershell
Unblock-File -Path "$env:USERPROFILE\Downloads\Glafira_for_Apache_Kafka_*_x64_en-US.msi"
```

## 在 Linux 上安装 {#install-linux}

提供适用于 `x86_64` 的三种格式。请从发布页面下载适合你的发行版的格式：https://github.com/vvnovg/glafira-kafka-releases/releases

- Debian、Ubuntu 及其衍生版：`sudo apt install ./<file>.deb`
- Fedora、RHEL 及其衍生版：`sudo dnf install ./<file>.rpm`
- 任意发行版：用 `chmod +x <file>.AppImage` 为 AppImage 添加执行权限，然后运行。

安装软件包后，从应用菜单启动应用，或在终端中运行 `glafira-kafka`。

Glafira 将密码保存在 Secret Service 中（例如 GNOME Keyring 或 KWallet）。请确保你的会话中运行着其中之一，否则应用无法保存连接的密码。

## 首次启动 {#first-run}

窗口分为两部分：左侧是[[ui:sidebar.connections]]面板，右侧是标签页。打开的每个页面（例如主题列表或 Producer）都有各自的标签页。

还没有连接时，面板中会显示简短提示，标签页区域会建议添加连接。开始使用：

1. 点击[[ui:sidebar.connections]]面板标题栏中的 `+`（[[ui:sidebar.newConnection]]）。
2. 填写[[ui:connections.form.name]]和[[ui:connections.form.bootstrap]]，例如 `localhost:9092`。
3. 点击[[ui:connections.form.test]]检查设置，然后点击[[ui:common.save]]。

保存后会打开集群概览。面板中每个连接下方都有指向[[ui:sidebar.cluster]]、[[ui:sidebar.topics]]、[[ui:sidebar.groups]]、[[ui:sidebar.producer]]和[[ui:sidebar.consumer]]的链接。所有连接设置的详细说明见[连接](connections.md#create)。

面板底部有[[ui:sidebar.settings]]、[[ui:tabs.license]]、[[ui:sidebar.help]]和[[ui:sidebar.about]]的链接。字段旁的 `?` 图标会显示该字段的简短提示。在窗口任意位置按 `F1`，可打开当前页面对应的指南章节。在 macOS 上，也可以通过“帮助”菜单打开指南。

如果你用过 Kafka Explorer（本应用的旧名称），Glafira 会在首次启动时复制其中的连接、模板、发送历史记录和许可证，密码也会从系统密钥存储中一并复制。已有的内容不会被覆盖，旧数据也不会被删除。macOS 可能会询问一次是否允许访问旧的钥匙串项目。

## 用于练习的本地 Kafka {#local-cluster}

如果想在没有共享集群的情况下试用本应用，可以在自己的计算机上用 Docker 启动 Kafka。一个 Broker 就够了：

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
```

然后创建一个连接：Bootstrap 服务器为 `localhost:9092`，协议为 `PLAINTEXT`，环境为 `local`。不再需要该 Broker 时，用 `docker rm -f kafka` 将其删除。

空的[[ui:sidebar.connections]]面板中的提示提到了 `docker compose -f dev/docker-compose.yml up -d`。该文件属于 Glafira 源代码。如果你有源代码，这条命令会启动一个带有三个监听器的 Broker：

- `localhost:9092`：`PLAINTEXT`。
- `localhost:9094`：`SASL_PLAINTEXT`，支持 `PLAIN`（用户 `tester` / `tester-secret` 和 `admin` / `admin-secret`）和 SCRAM（`SCRAM-SHA-256` 和 `SCRAM-SHA-512` 使用 `scram-user` / `scram-secret`）。
- `localhost:9095`：使用客户端证书的 `SSL`（mTLS）。测试证书位于 `dev/certs/out`。

这些用户和证书仅用于本地测试。

## 界面语言 {#language}

界面提供英语、俄语、德语、法语、西班牙语、意大利语、波兰语、葡萄牙语（巴西）、简体中文、繁体中文、日语和韩语版本。所有语言均免费。

更改语言：

1. 点击左侧面板底部的[[ui:sidebar.settings]]。
2. 在[[ui:settings.language.label]]中选择语言。

更改立即生效，包括本指南和 macOS 菜单。[[ui:settings.language.systemUnknown]]（默认）使用操作系统的语言。如果该语言不在列表中，界面将显示为英语。

## 由组织设置的选项 {#managed-settings}

在公司中，管理员可以通过策略为所有用户设置某些选项，这些选项无法在应用中更改。策略可以：

- 设置界面语言：此时[[ui:sidebar.settings]]中的语言列表不可用，并注明语言由你的组织设置；
- 使用组织许可证密钥激活 Pro：此时[[ui:tabs.license]]页面会显示许可证由你的组织管理；
- 关闭更新检查，或将其指向内部镜像。

要查看是否应用了策略，请打开[[ui:sidebar.about]]，查看[[ui:about.policy]]。如果策略有错误，会显示在该处以及[[ui:tabs.license]]页面上。有关策略的问题，请联系你的管理员。许可证和更新的说明见[许可证与更新](license-updates.md)。
