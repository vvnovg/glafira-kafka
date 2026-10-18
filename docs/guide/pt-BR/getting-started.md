# Primeiros passos

O Glafira for Apache Kafka é um cliente desktop para clusters Kafka. Com ele, você navega por tópicos e consumer groups, lê e envia mensagens e redefine offsets. Este capítulo trata da instalação, da primeira execução, de um Kafka local para praticar e do idioma da interface.

## Instalação no macOS {#install-macos}

O Glafira funciona no macOS 11 ou posterior. Baixe o instalador na página de releases: https://github.com/vvnovg/glafira-kafka-releases/releases

Escolha o arquivo adequado ao seu Mac: `aarch64` para Apple silicon (M1 e posteriores), `x64` para Intel.

1. Abra o arquivo `.dmg`.
2. Arraste o Glafira for Apache Kafka para a pasta Aplicativos.
3. Inicie o aplicativo pela pasta Aplicativos ou pelo Launchpad.

Também é possível usar o instalador `.pkg`. Ele coloca o aplicativo em `/Applications` para todos os usuários.

Os builds beta ainda não são assinados pela Apple, por isso o macOS bloqueia a primeira execução. Para permiti-la:

1. Tente abrir o aplicativo uma vez e feche o aviso.
2. Abra Ajustes do Sistema → Privacidade e Segurança.
3. Role até a mensagem sobre o Glafira for Apache Kafka e clique em “Abrir Mesmo Assim”. Se for solicitado, confirme com sua senha.

No macOS 14 e anteriores, você também pode clicar com o botão direito no aplicativo, escolher “Abrir” e depois “Abrir” novamente.

Se isso não resolver, remova uma vez o atributo de quarentena no Terminal:

```bash
xattr -dr com.apple.quarantine "/Applications/Glafira for Apache Kafka.app"
```

Se o próprio `.dmg` baixado não abrir, remova antes o atributo dele:

```bash
xattr -d com.apple.quarantine ~/Downloads/Glafira_for_Apache_Kafka_*.dmg
```

As atualizações instaladas pelo próprio aplicativo não exigem essas etapas.

## Instalação no Windows {#install-windows}

Baixe o instalador `.msi` (`x64`) na página de releases: https://github.com/vvnovg/glafira-kafka-releases/releases

1. Execute o arquivo `.msi`.
2. Confirme a instalação na janela do Windows. O aplicativo é instalado para todos os usuários do computador, por isso são necessários direitos de administrador.
3. Inicie o Glafira for Apache Kafka pelo menu Iniciar.

Os builds beta ainda não são assinados. Se o SmartScreen bloquear o instalador, clique em “Mais informações” e depois em “Executar assim mesmo”. Também é possível desbloquear o arquivo no PowerShell antes de executá-lo:

```powershell
Unblock-File -Path "$env:USERPROFILE\Downloads\Glafira_for_Apache_Kafka_*_x64_en-US.msi"
```

## Instalação no Linux {#install-linux}

Há builds para `x86_64` em três formatos. Baixe o adequado à sua distribuição na página de releases: https://github.com/vvnovg/glafira-kafka-releases/releases

- Debian, Ubuntu e derivados: `sudo apt install ./<file>.deb`
- Fedora, RHEL e derivados: `sudo dnf install ./<file>.rpm`
- Qualquer distribuição: torne o AppImage executável com `chmod +x <file>.AppImage` e execute-o.

Depois de instalar um pacote, inicie o aplicativo pelo menu de aplicativos ou execute `glafira-kafka` em um terminal.

O Glafira guarda as senhas no Secret Service (por exemplo, GNOME Keyring ou KWallet). Verifique se um deles está em execução na sua sessão. Caso contrário, o aplicativo não consegue salvar as senhas das conexões.

## Primeira execução {#first-run}

A janela tem duas partes. À esquerda fica o painel [[ui:sidebar.connections]]. À direita ficam as abas: cada tela aberta, como uma lista de tópicos ou o Producer, ganha sua própria aba.

Enquanto não há conexões, o painel mostra uma dica curta, e a área de abas sugere adicionar uma conexão. Para começar:

1. Clique em `+` ( [[ui:sidebar.newConnection]] ) no cabeçalho do painel [[ui:sidebar.connections]].
2. Preencha os campos [[ui:connections.form.name]] e [[ui:connections.form.bootstrap]], por exemplo `localhost:9092`.
3. Clique em [[ui:connections.form.test]] para verificar as configurações e depois em [[ui:common.save]].

Depois de salvar, abre-se a visão geral do cluster. Abaixo de cada conexão no painel há links para [[ui:sidebar.cluster]], [[ui:sidebar.topics]], [[ui:sidebar.groups]], [[ui:sidebar.producer]] e [[ui:sidebar.consumer]]. Todas as configurações de conexão estão descritas em [Conexões](connections.md#create).

Na parte inferior do painel ficam os links [[ui:sidebar.settings]], [[ui:tabs.license]], [[ui:sidebar.help]] e [[ui:sidebar.about]]. O sinal `?` ao lado de um campo mostra uma dica curta sobre ele. Pressione `F1` em qualquer lugar da janela para abrir o capítulo do guia sobre a tela atual. No macOS, o guia também está no menu Ajuda.

Se você usava o Kafka Explorer (o nome anterior do aplicativo), o Glafira copia na primeira execução as conexões, os modelos, o histórico de envios e a licença dele. As senhas também são copiadas do cofre de segredos do sistema. Nada do que já existe é sobrescrito, e os dados antigos não são excluídos. O macOS pode pedir uma vez acesso aos itens antigos das Chaves.

## Kafka local para praticar {#local-cluster}

Para experimentar o aplicativo sem um cluster compartilhado, inicie o Kafka no seu computador com o Docker. Um broker é suficiente:

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
```

Em seguida, crie uma conexão com o servidor bootstrap `localhost:9092`, o protocolo `PLAINTEXT` e o ambiente `local`. Quando não precisar mais do broker, remova-o com `docker rm -f kafka`.

A dica no painel [[ui:sidebar.connections]] vazio menciona `docker compose -f dev/docker-compose.yml up -d`. Esse arquivo faz parte do código-fonte do Glafira. Se você tiver o código-fonte, esse comando inicia um broker com três listeners:

- `localhost:9092` — `PLAINTEXT`.
- `localhost:9094` — `SASL_PLAINTEXT` com `PLAIN` (usuários `tester` / `tester-secret` e `admin` / `admin-secret`) e SCRAM (`scram-user` / `scram-secret` para `SCRAM-SHA-256` e `SCRAM-SHA-512`).
- `localhost:9095` — `SSL` com certificados de cliente (mTLS). Os certificados de teste ficam em `dev/certs/out`.

Esses usuários e certificados servem apenas para testes locais.

## Idioma da interface {#language}

A interface está disponível em inglês, russo, alemão, francês, espanhol, italiano, polonês e português (Brasil). Todos os idiomas são gratuitos.

Para mudar o idioma:

1. Clique em [[ui:sidebar.settings]] na parte inferior do painel esquerdo.
2. Escolha um idioma em [[ui:settings.language.label]].

A mudança vale na hora, inclusive para este guia e para o menu do macOS. A opção [[ui:settings.language.systemUnknown]] (o padrão) segue o idioma do sistema operacional. Se esse idioma não estiver na lista, a interface fica em inglês.

## Configurações definidas pela sua organização {#managed-settings}

Em uma empresa, os administradores podem definir algumas opções para todos os usuários por meio de uma política. Essas opções não podem ser alteradas no aplicativo. Uma política pode:

- definir o idioma da interface — a lista de idiomas em [[ui:sidebar.settings]] fica indisponível, com um aviso de que o idioma foi definido pela sua organização;
- ativar o Pro com uma chave de licença da organização — a tela [[ui:tabs.license]] informa que a licença é gerenciada pela sua organização;
- desativar a verificação de atualizações ou direcioná-la para um espelho interno.

Para saber se uma política está em vigor, abra [[ui:sidebar.about]] e veja o item [[ui:about.policy]]. Se a política tiver um erro, ele aparece ali e na tela [[ui:tabs.license]]. Para dúvidas sobre a política, fale com os administradores. Licenças e atualizações estão descritas em [Licença e atualizações](license-updates.md).
