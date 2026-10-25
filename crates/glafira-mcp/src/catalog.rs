//! Сервер-каталог для реестров MCP (Glama и др.). Реестры запускают сервер в контейнере и
//! проверяют, что он отвечает на `initialize` и `tools/list`. Настоящий сервер Glafira живёт
//! внутри desktop-приложения, поэтому для реестров есть [`CatalogBackend`]: тот же сервер и
//! тот же список инструментов, но без подключений — каждый вызов отвечает ошибкой
//! [`APP_REQUIRED_CODE`] со ссылкой на установщики. Бинарь — `glafira-mcp-catalog`.

use crate::health::{HealthOp, HealthOutput};
use crate::messages::{MessagesOp, MessagesOutput, ScanCtx};
use crate::model::{
    Backend, BackendError, BoxFuture, ReadOp, ReadOutput, SessionAccess, SessionView,
};
use crate::proposals::{ProposalOp, ProposalOutput};

/// Код ошибки каждого вызова инструмента в каталоге.
pub const APP_REQUIRED_CODE: &str = "desktop_app_required";

/// Текст для LLM и пользователя: по-английски, как все сообщения шлюза.
pub const APP_REQUIRED_MESSAGE: &str = "This is the catalog build of the Glafira MCP server: \
it lists the tools but has no Kafka connections. Install Glafira for Apache Kafka from \
https://github.com/vvnovg/glafira-kafka-releases/releases, enable \"LLM assistants (MCP)\" in its \
settings and point your MCP client at `glafira-kafka mcp`.";

fn app_required() -> BackendError {
    BackendError::new(APP_REQUIRED_CODE, APP_REQUIRED_MESSAGE)
}

/// Backend без приложения: сессии разрешены (реестр должен увидеть инструменты), данных нет.
pub struct CatalogBackend;

impl Backend for CatalogBackend {
    fn access(&self, _session: &SessionView) -> SessionAccess {
        SessionAccess::Approved
    }

    fn read(&self, _op: ReadOp) -> BoxFuture<'_, Result<ReadOutput, BackendError>> {
        Box::pin(async { Err(app_required()) })
    }

    fn connection_name(&self, _id: &str) -> Option<String> {
        None
    }

    fn session_closed(&self, _session_id: &str) {}

    fn sessions_changed(&self) {}

    fn health(&self, _op: HealthOp) -> BoxFuture<'_, Result<HealthOutput, BackendError>> {
        Box::pin(async { Err(app_required()) })
    }

    fn proposals(&self, _op: ProposalOp) -> BoxFuture<'_, Result<ProposalOutput, BackendError>> {
        Box::pin(async { Err(app_required()) })
    }

    fn messages(
        &self,
        _op: MessagesOp,
        _ctx: ScanCtx,
    ) -> BoxFuture<'_, Result<MessagesOutput, BackendError>> {
        Box::pin(async { Err(app_required()) })
    }
}
