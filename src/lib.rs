#![allow(deprecated)] // error-chain uses Error::cause

#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;

use rand::random;
use serde::{Serialize, Deserialize};
use serde_json;
use std::fs::{self, File};
use std::io::{self, Write, BufWriter, BufReader};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::result::Result as StdResult;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use fancy_flocks::scoped::dirty_flock::{DirtyFlock, DirtyFlockShared,
                                        DirtyFlockExclusive, State};

error_chain! {
    foreign_links {
        Io(::std::io::Error);
    }
}

pub struct AtomBlob<T: Serialize + Deserialize<'static> + Default> {
    flock: DirtyFlock,
    v: Arc<RwLock<T>>,
    path: Arc<PathBuf>,
}

impl<T> AtomBlob<T>
    where for <'de> T: Serialize + Deserialize<'de> + Default,
{
    pub fn new<P>(p: P) -> Result<AtomBlob<T>>
        where P: AsRef<Path>,
    {
        let p = p.as_ref();

        if let Some(v) = ser_load(p) {
            debug!("loaded existing blobject");
            Ok(AtomBlob {
                flock: DirtyFlock::new(&p.with_extension("flock")),
                v: Arc::new(RwLock::new(v?)),
                path: Arc::new(p.to_owned()),
            })
        } else {
            debug!("created new blobject");
            Ok(AtomBlob {
                flock: DirtyFlock::new(&p.with_extension("flock")),
                v: Arc::new(RwLock::new(T::default())),
                path: Arc::new(p.to_owned()),
            })
        }
    }

    pub fn get(&mut self) -> Result<BlobRef<T>> {
        let flock = self.flock.lock_shared()?;
        if flock.state() == State::Dirty {
            let mut val = self.v.write().expect("poisoned blobject");
            if let Some(newval) = ser_load(&*self.path) {
                *val = newval?;
            } else {
                *val = T::default()
            }
        }
        let v = self.v.read().expect("poisoned blobject");
        Ok(BlobRef {
            flock: flock,
            v: v,
            ph: PhantomData,
        })
    }

    pub fn get_mut(&mut self) -> Result<BlobMutRef<T>> {
        let flock = self.flock.lock_exclusive().expect("flock");
        if flock.state() == State::Dirty {
            // FIXME try_write
            let mut val = self.v.write().expect("poisoned blobject");
            if let Some(newval) = ser_load(&*self.path) {
                *val = newval?;
            } else {
                *val = T::default()
            }
        }
        let v = self.v.write().expect("poisoned blobject");
        Ok(BlobMutRef {
            flock: flock,
            v: v,
            path: &*self.path,
            committed: false,
            ph: PhantomData,
        })
    }

    pub fn clone(&self) -> AtomBlob<T> {
        AtomBlob {
            flock: DirtyFlock::new(self.flock.path()),
            v: self.v.clone(),
            path: self.path.clone(),
        }
    }
}

// NB: Lock drop order
pub struct BlobRef<'a, T: 'a> {
    v: RwLockReadGuard<'a, T>,
    #[allow(dead_code)] // Using drop side-effect
    flock: DirtyFlockShared<'a>,
    ph: PhantomData<&'a mut ()>,
}

// NB: Lock drop order
pub struct BlobMutRef<'a, T: 'a + Serialize> {
    v: RwLockWriteGuard<'a, T>,
    #[allow(dead_code)] // Using drop side-effect
    flock: DirtyFlockExclusive<'a>,
    path: &'a Path,
    committed: bool,
    ph: PhantomData<&'a mut ()>,
}

impl<'a, T: 'a> Deref for BlobRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &*self.v
    }
}

impl<'a, T: 'a> Deref for BlobMutRef<'a, T>
    where T: Serialize
{
    type Target = T;

    fn deref(&self) -> &T {
        &*self.v
    }
}

impl<'a, T: 'a> DerefMut for BlobMutRef<'a, T>
    where T: Serialize
{
    fn deref_mut(&mut self) -> &mut T {
        &mut *self.v
    }
}

impl<'a, T: 'a> Drop for BlobMutRef<'a, T>
    where T: Serialize
{
    fn drop(&mut self) {
        if !self.committed {
            self.commit().expect("blobject failed to commit on drop");
        }
    }
}

impl<'a, T: 'a> BlobMutRef<'a, T>
    where T: Serialize
{
    fn commit(&mut self) -> Result<()> {
        ser_store(&self.path, &*self.v)?;
        self.committed = true;

        debug!("new blobject committed");

        Ok(())
    }
}

fn ser_load<P, T>(p: P) -> Option<Result<T>>
    where P: AsRef<Path>, for <'de> T: Deserialize<'de>
{
    let p = p.as_ref();

    let infile = match File::open(p) {
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
            return None;
        }
        Err(e) => {
            return Some(Err(e).chain_err(|| "opening blobject"))
        }
        Ok(f) => f
    };

    let infile = BufReader::new(infile);
    let value = serde_json::from_reader(infile)
         .chain_err(|| "loading blobject");

    Some(value)
}

fn ser_store<P, T>(p: P, t: &T) -> Result<()>
    where P: AsRef<Path>, T: Serialize
{
    let p = p.as_ref();
    let tmp_ext = format!("{:08x}.tmp", random::<u32>());
    let tmp_path = p.with_extension(tmp_ext);

    let out = File::create(&tmp_path)
        .chain_err(|| "creating tmp file for blobject")?;
    let mut out = BufWriter::new(out);

    serde_json::to_writer_pretty(&mut out, t)
        .chain_err(|| "serializing blobject to file")?;

    out.flush()
        .chain_err(|| "flushing blobject file")?;

    drop(out);

    atomic_file_rename(&tmp_path, p)
        .chain_err(|| "replacing blobject file")?;

    Ok(())
}

fn atomic_file_rename<P, Q>(src: P, dst: Q) -> StdResult<(), io::Error>
    where P: AsRef<Path>, Q: AsRef<Path>
{
    if cfg!(windows) {
        // TODO use ReplaceFile
        // This doesn't matter much because the rename is done under
        // an exclusive flock
        //panic!("unimplemented atomic file move");
    }

    fs::rename(src, dst)
}

