use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{AppError, AppResult};

fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(".tmp");
    path.with_file_name(name)
}

/// Атомарная запись: временный файл → fsync → rename → fsync каталога.
/// При сбое на любом шаге исходный файл остаётся нетронутым.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> AppResult<()> {
    let dir = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let tmp = tmp_path(path);
    let result = (|| -> std::io::Result<()> {
        fs::create_dir_all(dir)?;
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, path)?;
        sync_dir(dir);
        Ok(())
    })();
    result.map_err(|e| {
        let _ = fs::remove_file(&tmp);
        AppError::internal(format!("Не удалось записать {}: {e}", path.display()))
    })
}

/// fsync каталога фиксирует rename на диске (POSIX). На Windows каталог так не открыть — пропускаем.
fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    #[cfg(not(unix))]
    let _ = dir;
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> AppResult<Option<T>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> AppResult<()> {
    atomic_write(path, &serde_json::to_vec_pretty(value)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn writes_replaces_and_leaves_no_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.json");
        atomic_write(&p, b"1").unwrap();
        atomic_write(&p, b"2").unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"2");
        assert!(!tmp_path(&p).exists());
    }

    #[test]
    fn creates_missing_directories() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("history").join("c1.json");
        atomic_write(&p, b"[]").unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"[]");
    }

    #[test]
    fn failure_keeps_original_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.json");
        atomic_write(&p, b"old").unwrap();
        // Каталог на месте временного файла: создать его не получится.
        std::fs::create_dir(tmp_path(&p)).unwrap();
        let err = atomic_write(&p, b"new").unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(std::fs::read(&p).unwrap(), b"old");
    }

    #[test]
    fn json_roundtrip_and_missing_file_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("v.json");
        assert_eq!(read_json::<Vec<i32>>(&p).unwrap(), None);
        write_json(&p, &vec![1, 2]).unwrap();
        assert_eq!(read_json::<Vec<i32>>(&p).unwrap(), Some(vec![1, 2]));
    }
}
