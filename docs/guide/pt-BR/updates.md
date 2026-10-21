# Atualizações e diagnóstico

Este capítulo trata de como atualizar o aplicativo, da tela [[ui:sidebar.about]], do arquivo de diagnóstico e de como apoiar o projeto.

## Atualizações {#updates}

Ao iniciar, o aplicativo verifica uma vez se há uma nova versão disponível. Se houver, um aviso acima das abas informa a versão. Clicar em [[ui:about.notice.more]] abre a tela [[ui:sidebar.about]]. Se a verificação falhar, por exemplo sem acesso à internet, nenhuma mensagem aparece.

Para verificar manualmente:

1. Abra [[ui:sidebar.about]] na parte inferior do painel esquerdo.
2. Na seção [[ui:about.updates.title]], clique em [[ui:about.updates.check]].
3. Se houver uma nova versão, a tela mostra o número dela e as notas da versão. Clique em [[ui:about.updates.install]].

O aplicativo baixa a atualização, verifica a assinatura, instala e reinicia. Um download interrompido por um problema de rede é repetido automaticamente, até três tentativas no total. Se as três tentativas falharem, o aplicativo mostra a mensagem [[ui:errors.updates.downloadInterrupted]]. Se a verificação da assinatura falhar, o aplicativo mostra a mensagem [[ui:errors.updates.badSignature]].

A instalação pode pedir uma senha de administrador, porque o aplicativo é instalado para todos os usuários do computador: pelo instalador `.pkg` no macOS, pelo instalador `.msi` no Windows ou por um pacote `.deb` ou `.rpm` no Linux. Usuários sem direitos de administrador não podem atualizar uma instalação desse tipo.

Se houver mensagens sendo lidas, uma sessão de consumer em execução ou um lote sendo enviado, o aplicativo pergunta antes e informa quantas sessões estão ativas. Clique em [[ui:about.updates.stopAndUpdate]] para interrompê-las e continuar, ou em [[ui:common.cancel]]. As conexões, as configurações e os demais dados são mantidos depois de uma atualização.

As atualizações são baixadas da página de releases no GitHub. Se a verificação falhar com a mensagem [[ui:errors.updates.checkFailed]], verifique a rede ou o proxy. A mensagem [[ui:errors.updates.notConfigured]] significa que este build foi feito sem a chave do atualizador, por exemplo um build de desenvolvimento ou de teste. Nesse caso, baixe as novas versões na página de releases: https://github.com/vvnovg/glafira-kafka-releases/releases

Uma organização pode desativar a verificação de atualizações por meio de uma política ou direcioná-la para um espelho interno. Quando a verificação está desativada, a seção [[ui:about.updates.title]] mostra [[ui:errors.updates.managed]] depois que você clica em [[ui:about.updates.check]]. Assim como [[ui:errors.updates.notConfigured]], essa mensagem só aparece depois do clique; a verificação ao iniciar não mostra nada nesses casos.

## Sobre {#about}

Abra [[ui:sidebar.about]] na parte inferior do painel esquerdo. No macOS, ele também está no menu Ajuda, como [[ui:menu.about]].

A tela mostra uma descrição curta do aplicativo e:

- [[ui:about.version]] — o número da versão, e [[ui:about.build]] — o identificador do código-fonte a partir do qual ele foi compilado. Inclua os dois em uma solicitação de suporte.
- [[ui:about.system]] — o sistema operacional e a arquitetura do processador.
- [[ui:about.license]] — Apache 2.0; o link abre o texto da licença no navegador.
- [[ui:about.policy]] — [[ui:about.policySet]] ou [[ui:about.policyNone]]. Os erros na política são listados abaixo.
- [[ui:about.dataDir]] — a pasta com os perfis de conexão, os modelos, o histórico, as configurações.
- [[ui:about.logDir]] — a pasta com os arquivos de log. Consulte [Logs](troubleshooting.md#logs).

As seções [[ui:about.updates.title]] e [[ui:about.diagnostics.title]] também ficam nessa tela.

## Arquivo de diagnóstico {#diagnostics}

O arquivo de diagnóstico reúne o que o suporte precisa para entender um problema. Para criá-lo:

1. Abra [[ui:sidebar.about]].
2. Na seção [[ui:about.diagnostics.title]], clique em [[ui:about.diagnostics.collect]]. No macOS, também é possível escolher [[ui:menu.diagnostics]] no menu Ajuda.
3. Escolha onde salvar o arquivo. O nome sugerido é `diagnostics-<date>.zip`.

Quando o arquivo é salvo, a tela mostra o caminho dele. O arquivo ZIP contém:

- `info.json` — a versão e o build do aplicativo, o sistema operacional, os caminhos das pastas de dados e de logs, o status da política;
- `connections.json` — os perfis de conexão: nomes, ambientes, servidores bootstrap, protocolos, usuários SASL, caminhos dos arquivos de certificado, propriedades adicionais e tempos limite;
- `logs/` — os arquivos de log do aplicativo.

O arquivo não contém senhas: elas ficam no cofre de segredos do sistema, e as propriedades adicionais com senhas e segredos são recusadas ao salvar uma conexão. Os arquivos de certificado também não são incluídos. Os nomes de host, os usuários e os caminhos de arquivo ficam em texto puro, e os logs contêm endereços de brokers das mensagens de erro do Kafka. Revise o arquivo antes de enviá-lo para fora da sua empresa.

O aplicativo não envia nada por conta própria. Anexe você mesmo o arquivo à sua solicitação de suporte, consulte [Falar com o suporte](troubleshooting.md#support).

## Apoiar o projeto {#support}

O Glafira é gratuito e de código aberto sob a licença Apache License 2.0. As doações o mantêm vivo. [[ui:support.title]] na parte inferior do painel esquerdo, na tela [[ui:sidebar.about]] e, no macOS, no menu Ajuda abre a página de apoio no navegador: https://github.com/vvnovg/glafira-kafka-releases/blob/main/SUPPORT.md
