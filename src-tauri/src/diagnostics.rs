//! Архив диагностики: пользователь сам прикладывает его к обращению. Наружу ничего не отправляется.
//! Секретов в источниках нет: профили не содержат паролей (они в keychain).

use std::io::Write;
use std::path::{Path, PathBuf};

use serde::Serialize;
use zip::write::SimpleFileOptions;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::policy::PolicyStatus;
use crate::storage::connections::ConnectionProfile;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppInfo {
    pub name: String,
    pub version: String,
    pub commit: String,
    pub os: String,
    pub arch: String,
    pub data_dir: String,
    pub log_dir: String,
}

#[derive(Serialize)]
struct InfoFile<'a> {
    app: &'a AppInfo,
    policy: &'a PolicyStatus,
}

fn zip_err(e: impl std::fmt::Display) -> AppError {
    app_err!(
        ErrorCode::Internal,
        "errors.diagnostics.writeFailed",
        "Failed to write the diagnostics archive"
    )
    .with_details(e.to_string())
}

pub fn write_archive(
    out: &Path,
    info: &AppInfo,
    profiles: &[ConnectionProfile],
    policy: &PolicyStatus,
    log_files: &[PathBuf],
) -> AppResult<()> {
    let file = std::fs::File::create(out)?;
    let mut zip = zip::ZipWriter::new(file);
    let opts = SimpleFileOptions::default();
    zip.start_file("info.json", opts).map_err(zip_err)?;
    zip.write_all(&serde_json::to_vec_pretty(&InfoFile { app: info, policy })?)?;
    zip.start_file("connections.json", opts).map_err(zip_err)?;
    zip.write_all(&serde_json::to_vec_pretty(profiles)?)?;
    for path in log_files {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            // Файл успел уйти при ротации между перечислением и чтением.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        zip.start_file(format!("logs/{name}"), opts)
            .map_err(zip_err)?;
        zip.write_all(&bytes)?;
    }
    zip.finish().map_err(zip_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::PolicyStatus;
    use crate::storage::connections::sample_profile;
    use std::io::Read;

    fn info() -> AppInfo {
        AppInfo {
            name: "X".into(),
            version: "0.2.0".into(),
            commit: "abc".into(),
            os: "macos".into(),
            arch: "aarch64".into(),
            data_dir: "/d".into(),
            log_dir: "/l".into(),
        }
    }

    #[test]
    fn archive_contains_info_profiles_and_logs_without_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("app.log");
        std::fs::write(&log, "запуск\n").unwrap();
        let out = dir.path().join("diag.zip");
        write_archive(
            &out,
            &info(),
            &[sample_profile("a")],
            &PolicyStatus::default(),
            &[log],
        )
        .unwrap();

        let mut zip = zip::ZipArchive::new(std::fs::File::open(&out).unwrap()).unwrap();
        let mut names: Vec<_> = (0..zip.len())
            .map(|i| zip.by_index(i).unwrap().name().unwrap().to_string())
            .collect();
        names.sort();
        assert_eq!(names, ["connections.json", "info.json", "logs/app.log"]);
        let mut info_json = String::new();
        zip.by_name("info.json")
            .unwrap()
            .read_to_string(&mut info_json)
            .unwrap();
        assert!(
            info_json.contains("\"version\": \"0.2.0\"")
                && info_json.contains("\"managed\": false")
        );
        assert!(!info_json.to_lowercase().contains("license"));
        for i in 0..zip.len() {
            let mut entry = zip.by_index(i).unwrap();
            let mut text = String::new();
            entry.read_to_string(&mut text).unwrap();
            assert!(
                !text.to_lowercase().contains("password"),
                "{}",
                entry.name().unwrap()
            );
        }
    }

    #[test]
    fn missing_log_file_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("app.log");
        std::fs::write(&log, "x").unwrap();
        let out = dir.path().join("diag.zip");
        write_archive(
            &out,
            &info(),
            &[],
            &PolicyStatus::default(),
            &[dir.path().join("app.1.log"), log],
        )
        .unwrap();
        let zip = zip::ZipArchive::new(std::fs::File::open(&out).unwrap()).unwrap();
        assert_eq!(zip.len(), 3);
    }
}
