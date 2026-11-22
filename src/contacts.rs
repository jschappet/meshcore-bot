use crate::diagnostics::Repeater;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Data {
    radio_contacts: Vec<Vec<u8>>,
    repeaters: Vec<Repeater>,
}

pub struct Contacts {
    path: PathBuf,
    data: Data,
    _lock: File,
}

impl Contacts {
    pub fn open(path: &Path) -> io::Result<Self> {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.with_extension("lock"))?;
        lock.try_lock()
            .map_err(|_| io::Error::other("Contact store is already in use"))?;
        let data = match fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::other)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Data::default(),
            Err(error) => return Err(error),
        };
        let store = Self {
            path: path.into(),
            data,
            _lock: lock,
        };
        Ok(store)
    }

    pub fn repeaters(&self) -> &[Repeater] {
        &self.data.repeaters
    }
    pub fn radio_contacts(&self) -> &[Vec<u8>] {
        &self.data.radio_contacts
    }

    pub fn sync(&mut self, frames: &[Vec<u8>]) -> io::Result<()> {
        self.data.radio_contacts = frames.to_vec();
        for repeater in frames.iter().filter_map(|frame| Repeater::parse(frame)) {
            self.merge(repeater);
        }
        self.save()
    }

    pub fn observe(&mut self, repeater: Repeater) -> io::Result<()> {
        if self.merge(repeater) {
            self.save()?;
        }
        Ok(())
    }

    fn merge(&mut self, repeater: Repeater) -> bool {
        if let Some(old) = self
            .data
            .repeaters
            .iter_mut()
            .find(|old| old.public_key == repeater.public_key)
        {
            if old.advertised_at > repeater.advertised_at || *old == repeater {
                return false;
            }
            *old = repeater;
        } else {
            self.data.repeaters.push(repeater);
        }
        true
    }

    fn save(&self) -> io::Result<()> {
        let temporary = self.path.with_extension("tmp");
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(&serde_json::to_vec(&self.data)?)?;
        file.sync_all()?;
        fs::rename(temporary, &self.path)?;
        #[cfg(unix)]
        File::open(
            self.path
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?
        .sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn contacts_and_repeaters_survive_reopening_and_corruption_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("contacts.json");
        let mut frame = vec![0; 148];
        frame[0] = 3;
        frame[1..33].fill(42);
        frame[33] = 2;
        frame[100..104].copy_from_slice(b"Hill");
        let mut store = Contacts::open(&path).unwrap();
        store.sync(&[frame.clone()]).unwrap();
        assert!(Contacts::open(&path).is_err());
        drop(store);
        let store = Contacts::open(&path).unwrap();
        assert_eq!(store.radio_contacts(), &[frame]);
        assert_eq!(store.repeaters().len(), 1);
        drop(store);
        fs::write(&path, b"broken").unwrap();
        assert!(Contacts::open(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"broken");
    }
}
