//! Утилита владельца: генерация ключей подписи, выдача и проверка лицензий.
//! Закрытый ключ хранится только у владельца и никогда не коммитится.

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use license_format::{
    FORMAT_VERSION, Kind, NaiveDate, Payload, SigningKey, Tier, encode_signing_key, encode_verifying_key,
    parse_signing_key, parse_verifying_key, sign, verify,
};

#[derive(Parser)]
#[command(about = "Выдача лицензий Pro")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, ValueEnum)]
enum KindArg {
    Personal,
    Organization,
}

#[derive(Subcommand)]
enum Command {
    /// Создать пару ключей: <out>/license-signing.key (секрет) и <out>/license-public.key.
    Keygen {
        #[arg(long)]
        out: PathBuf,
    },
    /// Выдать лицензию и напечатать ключ.
    Issue {
        #[arg(long)]
        key: PathBuf,
        #[arg(long, value_enum)]
        kind: KindArg,
        #[arg(long)]
        licensee: String,
        /// Последний день действия, YYYY-MM-DD. Без него — бессрочно.
        #[arg(long)]
        expires: Option<NaiveDate>,
        #[arg(long)]
        seats: Option<u32>,
    },
    /// Проверить ключ и напечатать его содержимое.
    Verify {
        #[arg(long)]
        public: PathBuf,
        token: String,
    },
}

fn keygen(out: &Path) -> Result<String, String> {
    let signing = out.join("license-signing.key");
    let public = out.join("license-public.key");
    if signing.exists() || public.exists() {
        return Err(format!("{} уже содержит ключи — не перезаписываю", out.display()));
    }
    std::fs::create_dir_all(out).map_err(|e| e.to_string())?;
    let key = SigningKey::generate(&mut rand::rngs::OsRng);
    std::fs::write(&signing, encode_signing_key(&key) + "\n").map_err(|e| e.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&signing, std::fs::Permissions::from_mode(0o600)).map_err(|e| e.to_string())?;
    }
    std::fs::write(&public, encode_verifying_key(&key.verifying_key()) + "\n").map_err(|e| e.to_string())?;
    Ok(format!(
        "Закрытый ключ: {} (храните в секрете, не коммитьте)\nОткрытый ключ: {} (скопируйте в src-tauri/license-public.key)",
        signing.display(),
        public.display()
    ))
}

fn issue(key: &Path, kind: KindArg, licensee: &str, expires: Option<NaiveDate>, seats: Option<u32>, today: NaiveDate) -> Result<String, String> {
    let text = std::fs::read_to_string(key).map_err(|e| format!("{}: {e}", key.display()))?;
    let key = parse_signing_key(&text).map_err(|e| e.to_string())?;
    if licensee.trim().is_empty() {
        return Err("--licensee не может быть пустым".into());
    }
    if expires.is_some_and(|d| d < today) {
        return Err("--expires в прошлом".into());
    }
    let payload = Payload {
        v: FORMAT_VERSION,
        id: format!("{}-{:08x}", today.format("%Y%m%d"), rand::random::<u32>()),
        tier: Tier::Pro,
        kind: match kind {
            KindArg::Personal => Kind::Personal,
            KindArg::Organization => Kind::Organization,
        },
        licensee: licensee.trim().to_string(),
        issued: today,
        expires,
        seats,
    };
    Ok(sign(&payload, &key))
}

fn verify_cmd(public: &Path, token: &str) -> Result<String, String> {
    let text = std::fs::read_to_string(public).map_err(|e| format!("{}: {e}", public.display()))?;
    let key = parse_verifying_key(&text).map_err(|e| e.to_string())?;
    let payload = verify(token, &key).map_err(|e| e.to_string())?;
    Ok(format!("Подпись верна.\n{}", describe(&payload)))
}

fn describe(p: &Payload) -> String {
    format!(
        "id: {}\nтариф: pro\nтип: {:?}\nвладелец: {}\nвыдан: {}\nдействует до: {}\nмест: {}",
        p.id,
        p.kind,
        p.licensee,
        p.issued,
        p.expires.map_or("бессрочно".to_string(), |d| d.to_string()),
        p.seats.map_or("—".to_string(), |s| s.to_string()),
    )
}

fn main() {
    let cli = Cli::parse();
    let today = chrono::Local::now().date_naive();
    let result = match cli.command {
        Command::Keygen { out } => keygen(&out),
        Command::Issue { key, kind, licensee, expires, seats } => issue(&key, kind, &licensee, expires, seats, today),
        Command::Verify { public, token } => verify_cmd(&public, &token),
    };
    match result {
        Ok(out) => println!("{out}"),
        Err(e) => {
            eprintln!("Ошибка: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keygen_issue_verify() {
        let dir = tempfile::tempdir().unwrap();
        keygen(dir.path()).unwrap();
        assert!(keygen(dir.path()).is_err(), "не перезаписывает");
        let today = NaiveDate::from_ymd_opt(2026, 10, 16).unwrap();
        let token = issue(
            &dir.path().join("license-signing.key"),
            KindArg::Organization,
            "ACME",
            NaiveDate::from_ymd_opt(2027, 3, 31),
            Some(500),
            today,
        )
        .unwrap();
        let out = verify_cmd(&dir.path().join("license-public.key"), &token).unwrap();
        assert!(out.contains("ACME") && out.contains("2027-03-31") && out.contains("500"), "{out}");
    }

    #[test]
    fn rejects_past_expiry_and_empty_licensee() {
        let dir = tempfile::tempdir().unwrap();
        keygen(dir.path()).unwrap();
        let key = dir.path().join("license-signing.key");
        let today = NaiveDate::from_ymd_opt(2026, 10, 16).unwrap();
        assert!(issue(&key, KindArg::Personal, "a@b", NaiveDate::from_ymd_opt(2026, 1, 1), None, today).is_err());
        assert!(issue(&key, KindArg::Personal, "  ", None, None, today).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn signing_key_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        keygen(dir.path()).unwrap();
        let mode = std::fs::metadata(dir.path().join("license-signing.key")).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
