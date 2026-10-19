# Licença e atualizações

Este capítulo trata do que o Pro acrescenta, de como ativar uma chave de licença, do que acontece quando a licença expira, de como atualizar o aplicativo, da tela [[ui:sidebar.about]] e do arquivo de diagnóstico.

## Free e Pro {#free-and-pro}

O Free cobre o trabalho cotidiano com o Kafka: todos os protocolos de segurança, inclusive mTLS, a visão geral do cluster e os tópicos, a navegação pelas mensagens, o Producer com envio em lote, os consumer groups, a redefinição de offsets e a verificação do cluster. Todos os idiomas da interface também estão incluídos. Consulte [Idioma da interface](getting-started.md#language).

O Pro acrescenta:

- sessões de consumer: leitura como membro de um grupo, commit de offsets, pausa de partições. Consulte [Sessões de consumer](consumer-sessions.md#what-is);
- modo prod: criar conexões prod, mudar conexões para prod e desativar a opção [[ui:common.readOnly]] em uma conexão prod. Consulte [Ambientes e prod](connections.md#environments);
- notas de LLM: assistentes conectados por MCP deixam notas sobre o cluster, os tópicos e os grupos. Consulte [Verificação do cluster e notas](health-check.md#notes).

No Free, esses recursos ficam visíveis, mas indisponíveis. Os comandos das sessões de consumer ficam desativados e marcados com um cadeado e `Pro`. A opção prod aparece como [[ui:connections.form.prodPro]]. Se uma ação exigir o Pro, o erro mostra o texto [[ui:errors.license.proRequired]] e oferece o botão [[ui:common.openLicense]].

A tela [[ui:tabs.license]] lista as sessões de consumer, o modo prod e as notas de LLM na seção [[ui:license.whatProGives]].

## Ativar uma chave {#activate}

Uma chave de licença é uma linha de texto emitida para uma pessoa ou para uma organização. O aplicativo verifica a chave no seu computador e não se comunica com nenhum servidor.

1. Clique em [[ui:tabs.license]] na parte inferior do painel esquerdo.
2. Cole a chave inteira no campo [[ui:license.keyLabel]].
3. Clique em [[ui:license.activate]].

Os recursos Pro ficam disponíveis na hora. A tela mostra [[ui:license.tier]] (Pro), [[ui:license.licensee]], [[ui:license.kind]] ( [[ui:license.kindPersonal]] ou [[ui:license.kindOrganization]] ) e [[ui:license.expires]]: uma data ou [[ui:license.perpetual]]. A chave é salva na pasta de dados do aplicativo, então ela permanece depois de reiniciar ou atualizar. Uma nova chave substitui a anterior.

Se a chave não for aceita, o motivo aparece abaixo do campo:

- [[ui:errors.license.malformed]] — falta parte da chave ou ela foi alterada. Copie a chave de novo como uma única linha, sem caracteres extras.
- [[ui:errors.license.badSignature]] — a chave não foi emitida para o Glafira ou foi editada.
- A chave expirou — a mensagem informa a data de término. Peça uma nova chave.
- A chave exige uma versão mais recente do aplicativo — a mensagem informa a versão da chave. Atualize o aplicativo primeiro, consulte [Atualizações](license-updates.md#updates).

Para remover a chave, clique em [[ui:license.removeKey]]. O aplicativo pergunta: [[ui:license.removeQuestion]] Clique em [[ui:license.removeYes]] para confirmar. Seus dados permanecem.

Uma organização pode ativar o Pro para todos os usuários por meio de uma política. Nesse caso, o campo da chave e os botões ficam ocultos, e a tela informa: [[ui:license.managed]] Uma chave que você mesmo digitou não é usada enquanto a chave da política estiver definida. A seção [[ui:license.policyTitle]] mostra de onde vem a política e lista os erros dela. Se a chave da organização for inválida, o motivo aparece na tela [[ui:tabs.license]]. Nesse caso, fale com os administradores. Os administradores encontram as configurações da política em `docs/admin.md`, no código-fonte do Glafira.

## Expiração {#expiry}

Uma chave pode ter uma data de término, mostrada em [[ui:license.expires]]. A licença vale até o fim desse dia.

Quando faltam menos de 14 dias, um aviso acima das abas informa quantos dias restam, e a tela [[ui:tabs.license]] mostra a data de término. Clicar em [[ui:license.notice.more]] no aviso abre a tela [[ui:tabs.license]]. O botão `×` oculta o aviso até o aplicativo ser reiniciado.

Depois da data de término, o plano muda para Free. O aviso e a tela [[ui:tabs.license]] informam que a licença expirou e que os recursos Pro foram desativados. O que muda:

- Não é possível iniciar novas sessões de consumer. Em uma sessão que já está em execução, os commits e a pausa de partições são recusados com um erro que informa a data de término.
- As conexões prod continuam funcionando, com as mesmas confirmações. Não é possível criar novas conexões prod nem desativar a opção [[ui:common.readOnly]] em uma conexão prod.

As conexões, as senhas, os modelos e o histórico de envios são mantidos. Para continuar com o Pro, ative uma nova chave como descrito em [Ativar uma chave](license-updates.md#activate).

## Atualizações {#updates}

Ao iniciar, o aplicativo verifica uma vez se há uma nova versão disponível. Se houver, um aviso acima das abas informa a versão. Clicar em [[ui:license.notice.more]] abre a tela [[ui:sidebar.about]]. Se a verificação falhar, por exemplo sem acesso à internet, nenhuma mensagem aparece.

Para verificar manualmente:

1. Abra [[ui:sidebar.about]] na parte inferior do painel esquerdo.
2. Na seção [[ui:about.updates.title]], clique em [[ui:about.updates.check]].
3. Se houver uma nova versão, a tela mostra o número dela e as notas da versão. Clique em [[ui:about.updates.install]].

O aplicativo baixa a atualização, verifica a assinatura, instala e reinicia. Um download interrompido por um problema de rede é repetido automaticamente, até três tentativas no total. Se as três tentativas falharem, o aplicativo mostra a mensagem [[ui:errors.updates.downloadInterrupted]]. Se a verificação da assinatura falhar, o aplicativo mostra a mensagem [[ui:errors.updates.badSignature]].

A instalação pode pedir uma senha de administrador, porque o aplicativo é instalado para todos os usuários do computador: pelo instalador `.pkg` no macOS, pelo instalador `.msi` no Windows ou por um pacote `.deb` ou `.rpm` no Linux. Usuários sem direitos de administrador não podem atualizar uma instalação desse tipo.

Se houver mensagens sendo lidas, uma sessão de consumer em execução ou um lote sendo enviado, o aplicativo pergunta antes e informa quantas sessões estão ativas. Clique em [[ui:about.updates.stopAndUpdate]] para interrompê-las e continuar, ou em [[ui:common.cancel]]. As conexões, as configurações, a chave de licença e os demais dados são mantidos depois de uma atualização.

As atualizações são baixadas da página de releases no GitHub. Se a verificação falhar com a mensagem [[ui:errors.updates.checkFailed]], verifique a rede ou o proxy. A mensagem [[ui:errors.updates.notConfigured]] significa que este build foi feito sem a chave do atualizador, por exemplo um build de desenvolvimento ou de teste. Nesse caso, baixe as novas versões na página de releases: https://github.com/vvnovg/glafira-kafka-releases/releases

Uma organização pode desativar a verificação de atualizações por meio de uma política ou direcioná-la para um espelho interno. Quando a verificação está desativada, a seção [[ui:about.updates.title]] mostra [[ui:errors.updates.managed]] depois que você clica em [[ui:about.updates.check]]. Assim como [[ui:errors.updates.notConfigured]], essa mensagem só aparece depois do clique; a verificação ao iniciar não mostra nada nesses casos.

## Sobre {#about}

Abra [[ui:sidebar.about]] na parte inferior do painel esquerdo. No macOS, ele também está no menu Ajuda, como [[ui:menu.about]].

A tela mostra uma descrição curta do aplicativo e:

- [[ui:about.version]] — o número da versão, e [[ui:about.build]] — o identificador do código-fonte a partir do qual ele foi compilado. Inclua os dois em uma solicitação de suporte.
- [[ui:about.system]] — o sistema operacional e a arquitetura do processador.
- [[ui:about.tier]] — Free ou Pro.
- [[ui:about.policy]] — [[ui:about.policySet]] ou [[ui:about.policyNone]]. Os erros na política são listados abaixo.
- [[ui:about.dataDir]] — a pasta com os perfis de conexão, os modelos, o histórico, as configurações e a chave de licença.
- [[ui:about.logDir]] — a pasta com os arquivos de log. Consulte [Logs](troubleshooting.md#logs).

As seções [[ui:about.updates.title]] e [[ui:about.diagnostics.title]] também ficam nessa tela.

## Arquivo de diagnóstico {#diagnostics}

O arquivo de diagnóstico reúne o que o suporte precisa para entender um problema. Para criá-lo:

1. Abra [[ui:sidebar.about]].
2. Na seção [[ui:about.diagnostics.title]], clique em [[ui:about.diagnostics.collect]]. No macOS, também é possível escolher [[ui:menu.diagnostics]] no menu Ajuda.
3. Escolha onde salvar o arquivo. O nome sugerido é `diagnostics-<date>.zip`.

Quando o arquivo é salvo, a tela mostra o caminho dele. O arquivo ZIP contém:

- `info.json` — a versão e o build do aplicativo, o sistema operacional, os caminhos das pastas de dados e de logs, o status da política e o status da licença (plano, titular, tipo, data de término);
- `connections.json` — os perfis de conexão: nomes, ambientes, servidores bootstrap, protocolos, usuários SASL, caminhos dos arquivos de certificado, propriedades adicionais e tempos limite;
- `logs/` — os arquivos de log do aplicativo.

O arquivo não contém senhas: elas ficam no cofre de segredos do sistema, e as propriedades adicionais com senhas e segredos são recusadas ao salvar uma conexão. A chave de licença e os arquivos de certificado também não são incluídos. Os nomes de host, os usuários e os caminhos de arquivo ficam em texto puro, e os logs contêm endereços de brokers das mensagens de erro do Kafka. Revise o arquivo antes de enviá-lo para fora da sua empresa.

O aplicativo não envia nada por conta própria. Anexe você mesmo o arquivo à sua solicitação de suporte, consulte [Falar com o suporte](troubleshooting.md#support).
