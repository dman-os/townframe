use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::ExitStatus;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use keyring_core::Entry;
use keyring_core::Error;
use keyring_core::Result;
use keyring_core::api::CredentialApi;
use keyring_core::api::CredentialPersistence;
use keyring_core::api::CredentialStoreApi;

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A keyring-core store whose credentials are encrypted by systemd-creds.
///
/// The store keeps only encrypted files on disk. The systemd credential name
/// includes the original service and user, so copying a credential file to a
/// different entry cannot make it decrypt as that entry.
pub(crate) struct Store {
    root: PathBuf,
}

impl Store {
    pub(crate) fn new(root: impl AsRef<Path>) -> Result<Arc<Self>> {
        let store = Arc::new(Self {
            root: root.as_ref().to_path_buf(),
        });
        ensure_directory(&store.root)?;
        store.probe()?;
        Ok(store)
    }

    fn probe(&self) -> Result<()> {
        let service = "__secrets_rs_probe__";
        let user = format!(
            "{}-{}",
            std::process::id(),
            TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let credential = SystemdCredential::new(&self.root, service, &user)?;
        let secret = b"secrets_rs systemd-creds probe";
        credential.set_secret(secret)?;
        let result = credential.get_secret();
        let deletion = credential.delete_credential();
        match (result, deletion) {
            (Ok(value), Ok(())) if value == secret => Ok(()),
            (Ok(_), Ok(())) => Err(platform_error(
                "systemd-creds probe returned the wrong value",
            )),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(_deletion_error)) => Err(error),
        }
    }

    fn credential(&self, service: &str, user: &str) -> Result<SystemdCredential> {
        SystemdCredential::new(&self.root, service, user)
    }
}

pub(crate) fn default_root() -> Result<PathBuf> {
    let data_home = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share")))
        .ok_or_else(|| platform_error("neither XDG_DATA_HOME nor HOME is set"))?;
    Ok(data_home.join("secrets_rs/credentials"))
}

impl fmt::Debug for Store {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SystemdCredentialsStore")
            .field("root", &self.root)
            .finish()
    }
}

impl CredentialStoreApi for Store {
    fn vendor(&self) -> String {
        "secrets_rs systemd credentials".to_string()
    }

    fn id(&self) -> String {
        "systemd-creds-user".to_string()
    }

    fn build(
        &self,
        service: &str,
        user: &str,
        modifiers: Option<&HashMap<&str, &str>>,
    ) -> Result<Entry> {
        if modifiers.is_some_and(|modifiers| !modifiers.is_empty()) {
            return Err(Error::NotSupportedByStore(
                "systemd credentials do not support modifiers".to_string(),
            ));
        }
        let credential = self.credential(service, user)?;
        Ok(Entry::new_with_credential(Arc::new(credential)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn persistence(&self) -> CredentialPersistence {
        CredentialPersistence::UntilDelete
    }
}

struct SystemdCredential {
    service: String,
    user: String,
    name: String,
    path: PathBuf,
}

impl SystemdCredential {
    fn new(root: &Path, service: &str, user: &str) -> Result<Self> {
        validate_name("service", service)?;
        validate_name("user", user)?;
        let service_path = root.join(escape_filename(service));
        let path = service_path.join(format!("{}.cred", escape_filename(user)));
        Ok(Self {
            service: service.to_string(),
            user: user.to_string(),
            name: format!("{service}.{user}"),
            path,
        })
    }

    fn service_path(&self) -> &Path {
        self.path
            .parent()
            .expect("credential path has a service directory")
    }
}

impl fmt::Debug for SystemdCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SystemdCredential")
            .field("service", &self.service)
            .field("user", &self.user)
            .finish()
    }
}

impl CredentialApi for SystemdCredential {
    fn set_secret(&self, secret: &[u8]) -> Result<()> {
        ensure_directory(self.service_path())?;
        let temporary = temporary_path(self.service_path());

        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .map_err(|error| platform_io("creating temporary encrypted credential", error))?;
        let result = run_encrypt(&self.name, secret, &temporary).and_then(|()| {
            fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))
                .map_err(|error| platform_io("setting encrypted credential permissions", error))?;
            fs::rename(&temporary, &self.path)
                .map_err(|error| platform_io("installing encrypted credential", error))
        });
        if result.is_err() {
            drop(fs::remove_file(&temporary));
        }
        result
    }

    fn get_secret(&self) -> Result<Vec<u8>> {
        if !self.path.exists() {
            return Err(Error::NoEntry);
        }
        run_decrypt(&self.name, &self.path)
    }

    fn delete_credential(&self) -> Result<()> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(Error::NoEntry),
            Err(error) => Err(platform_io("deleting encrypted credential", error)),
        }
    }

    fn get_specifiers(&self) -> Option<(String, String)> {
        Some((self.service.clone(), self.user.clone()))
    }

    fn get_credential(&self) -> Result<Option<Arc<keyring_core::Credential>>> {
        self.get_secret()?;
        Ok(Some(Arc::new(Self {
            service: self.service.clone(),
            user: self.user.clone(),
            name: self.name.clone(),
            path: self.path.clone(),
        })))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn ensure_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .map_err(|error| platform_io("creating credential directory", error))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| platform_io("setting credential directory permissions", error))
}

fn temporary_path(directory: &Path) -> PathBuf {
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    directory.join(format!(".credential-{}.{}", std::process::id(), counter))
}

fn run_encrypt(name: &str, secret: &[u8], output: &Path) -> Result<()> {
    let mut child = encrypt_command(name, output)
        .spawn()
        .map_err(|error| platform_io("starting systemd-creds encrypt", error))?;
    let mut stdin = child.stdin.take().expect("encrypt command has piped stdin");
    let write_result = stdin.write_all(secret);
    drop(stdin);
    let output_result = child
        .wait_with_output()
        .map_err(|error| platform_io("waiting for systemd-creds encrypt", error))?;
    if !output_result.status.success() {
        return Err(command_error(
            "encrypt",
            output_result.status,
            &output_result.stderr,
        ));
    }
    write_result.map_err(|error| platform_io("writing systemd-creds encrypt input", error))
}

fn run_decrypt(name: &str, input: &Path) -> Result<Vec<u8>> {
    let output = decrypt_command(name, input)
        .output()
        .map_err(|error| platform_io("running systemd-creds decrypt", error))?;
    if !output.status.success() {
        return Err(command_error("decrypt", output.status, &output.stderr));
    }
    Ok(output.stdout)
}

fn encrypt_command(name: &str, output: &Path) -> Command {
    let mut command = systemd_command("encrypt", name);
    command
        .arg("-")
        .arg(output)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command
}

fn decrypt_command(name: &str, input: &Path) -> Command {
    let mut command = systemd_command("decrypt", name);
    command
        .arg(input)
        .arg("-")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn systemd_command(operation: &str, name: &str) -> Command {
    let mut command = Command::new("systemd-creds");
    command
        .arg("--user")
        .arg(operation)
        .arg("--with-key=auto")
        .arg(format!("--name={name}"));
    command
}

fn command_error(operation: &'static str, status: ExitStatus, stderr: &[u8]) -> Error {
    let stderr = String::from_utf8_lossy(stderr).trim().to_string();
    platform_error(format!(
        "systemd-creds {operation} failed ({status}): {stderr}"
    ))
}

fn platform_io(operation: &str, error: std::io::Error) -> Error {
    platform_error(format!("{operation}: {error}"))
}

fn platform_error(message: impl Into<String>) -> Error {
    Error::PlatformFailure(Box::new(std::io::Error::other(message.into())))
}

fn validate_name(kind: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::Invalid(
            kind.to_string(),
            "must not be empty".to_string(),
        ));
    }
    if value.as_bytes().contains(&0) {
        return Err(Error::Invalid(
            kind.to_string(),
            "must not contain NUL".to_string(),
        ));
    }
    if value.contains('/') {
        return Err(Error::Invalid(
            kind.to_string(),
            "must not contain '/'".to_string(),
        ));
    }
    Ok(())
}

fn escape_filename(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-') {
            escaped.push(byte as char);
        } else {
            escaped.push('%');
            escaped.push(char::from_digit((byte >> 4) as u32, 16).expect("hex digit"));
            escaped.push(char::from_digit((byte & 0x0f) as u32, 16).expect("hex digit"));
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    fn temporary_root() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock is after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("secrets-rs-systemd-creds-{nonce}"))
    }

    fn systemd_creds_available() -> bool {
        Command::new("systemd-creds").arg("--help").output().is_ok()
    }

    fn test_store() -> Option<(Arc<Store>, PathBuf)> {
        if !systemd_creds_available() {
            eprintln!("skipping systemd credentials test: systemd-creds binary absent");
            return None;
        }
        let root = temporary_root();
        match Store::new(&root) {
            Ok(store) => Some((store, root)),
            Err(error) => panic!("systemd-creds is present but unavailable: {error}"),
        }
    }

    #[test]
    fn filename_escaping_does_not_use_path_syntax() {
        assert_eq!(
            escape_filename("service.with.dots"),
            "service%2ewith%2edots"
        );
        assert_eq!(escape_filename("a%b"), "a%25b");
    }

    #[test]
    fn command_inputs_are_piped_instead_of_arguments() {
        let command = encrypt_command("service.user", Path::new("credential.cred"));

        let args: Vec<_> = command.get_args().collect();
        assert!(!args.iter().any(|arg| arg == &"secret material"));
        assert!(args.iter().any(|arg| arg == &"-"));
    }

    #[test]
    fn names_reject_path_separators_and_nul() {
        assert!(matches!(
            SystemdCredential::new(Path::new("."), "a/b", "user"),
            Err(Error::Invalid(_, _))
        ));
        assert!(matches!(
            SystemdCredential::new(Path::new("."), "service", "user\0"),
            Err(Error::Invalid(_, _))
        ));
    }

    #[test]
    fn round_trip_set_get_delete() {
        let Some((store, root)) = test_store() else {
            return;
        };
        let credential = store
            .build("service.with.dots", "user.with.dots", None)
            .unwrap();
        credential.set_secret(b"secret material").unwrap();
        assert_eq!(credential.get_secret().unwrap(), b"secret material");
        assert!(matches!(credential.delete_credential(), Ok(())));
        assert!(matches!(credential.get_secret(), Err(Error::NoEntry)));
        drop(fs::remove_dir_all(root));
    }

    #[test]
    fn missing_entry_is_no_entry() {
        let Some((store, root)) = test_store() else {
            return;
        };
        let credential = store.build("missing", "entry", None).unwrap();
        assert!(matches!(credential.get_secret(), Err(Error::NoEntry)));
        assert!(matches!(
            credential.delete_credential(),
            Err(Error::NoEntry)
        ));
        drop(fs::remove_dir_all(root));
    }

    #[test]
    fn copied_file_cannot_decrypt_as_another_entry() {
        let Some((store, root)) = test_store() else {
            return;
        };
        let first = store.build("service", "first", None).unwrap();
        let second = store.build("service", "second", None).unwrap();
        first.set_secret(b"secret material").unwrap();
        let first_path = store.credential("service", "first").unwrap().path;
        let second_path = store.credential("service", "second").unwrap().path;
        fs::copy(first_path, second_path).unwrap();
        let error = second.get_secret().unwrap_err();
        assert!(!matches!(error, Error::NoEntry));
        drop(fs::remove_dir_all(root));
    }
}
