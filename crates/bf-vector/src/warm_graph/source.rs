//! Fixed-capacity source-block cache. Anonymous pages are unmapped before graph loading.
use crate::{Error, Result};
use half::f16;
use std::{cell::RefCell, collections::HashMap, fs::File, io, os::unix::fs::FileExt, ptr::NonNull};
const BLOCK: usize = 64 << 10;
struct Arena {
    ptr: NonNull<u8>,
    len: usize,
}
impl Arena {
    fn new(len: usize) -> io::Result<Self> {
        // SAFETY: this creates a new, private, zero-initialized anonymous mapping. The size is
        // positive, bounded by two GiB, and representable by a Rust slice on supported hosts.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANON,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let Some(ptr) = NonNull::new(ptr.cast::<u8>()) else {
            // SAFETY: this unusual successful mapping still belongs exclusively to us.
            unsafe {
                libc::munmap(ptr, len);
            }
            return Err(io::Error::other(
                "anonymous source mapping has a null address",
            ));
        };
        Ok(Self { ptr, len })
    }
    fn bytes(&self) -> &[u8] {
        // SAFETY: the mapping is initialized, remains live, and is borrowed immutably here.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
    fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: the arena's exclusive borrow prevents any overlapping mutable slice.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}
impl Drop for Arena {
    fn drop(&mut self) {
        // SAFETY: this is exactly the exclusively owned mapping returned by mmap.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.len);
        }
    }
}
struct Cache {
    arena: Arena,
    frames: Vec<Option<(u64, bool)>>,
    lookup: HashMap<u64, usize>,
    clock: usize,
}
pub(super) struct Rows<'a> {
    file: &'a File,
    dims: usize,
    rows: usize,
    bytes: u64,
    cache: RefCell<Cache>,
}
impl<'a> Rows<'a> {
    pub fn new(file: &'a File, dims: usize, rows: usize) -> Result<Self> {
        // This phase replaces the resident graph, rather than overlapping it. Its cache never
        // exceeds the SQ8 payload already included in the construction/load reservation.
        let budget = (rows as u64 * dims.next_power_of_two() as u64)
            .min(2 << 30)
            .min(isize::MAX as u64) as usize;
        let slots = (budget / BLOCK).max(1);
        Ok(Self {
            file,
            dims,
            rows,
            bytes: rows as u64 * dims as u64 * 2,
            cache: RefCell::new(Cache {
                arena: Arena::new(slots * BLOCK)?,
                frames: vec![None; slots],
                lookup: HashMap::with_capacity(slots),
                clock: 0,
            }),
        })
    }
    pub fn copy(&self, id: u32, out: &mut [u8]) -> Result<()> {
        if id as usize >= self.rows || out.len() != self.dims * 2 {
            return Err(Error::InvalidInput("invalid source row".into()));
        }
        let mut offset = id as u64 * self.dims as u64 * 2;
        let mut copied = 0;
        let mut cache = self.cache.borrow_mut();
        while copied < out.len() {
            let key = offset / BLOCK as u64;
            let slot = if let Some(&slot) = cache.lookup.get(&key) {
                cache.frames[slot].as_mut().unwrap().1 = true;
                slot
            } else {
                let slot = loop {
                    let slot = cache.clock;
                    cache.clock = (slot + 1) % cache.frames.len();
                    match cache.frames[slot] {
                        Some((_, true)) => cache.frames[slot].as_mut().unwrap().1 = false,
                        _ => break slot,
                    }
                };
                if let Some((old, _)) = cache.frames[slot].take() {
                    cache.lookup.remove(&old);
                }
                let at = key * BLOCK as u64;
                let count = (self.bytes - at).min(BLOCK as u64) as usize;
                self.file.read_exact_at(
                    &mut cache.arena.bytes_mut()[slot * BLOCK..slot * BLOCK + count],
                    at,
                )?;
                #[cfg(target_os = "linux")]
                {
                    use std::os::fd::AsRawFd;
                    // SAFETY: the source is immutable and flushed before this cache opens.
                    // The copied anonymous block is now the only needed resident copy.
                    let result = unsafe {
                        libc::posix_fadvise(
                            self.file.as_raw_fd(),
                            at as libc::off_t,
                            count as libc::off_t,
                            libc::POSIX_FADV_DONTNEED,
                        )
                    };
                    if result != 0 {
                        return Err(io::Error::from_raw_os_error(result).into());
                    }
                }
                cache.lookup.insert(key, slot);
                cache.frames[slot] = Some((key, false));
                slot
            };
            let within = (offset % BLOCK as u64) as usize;
            let count = (out.len() - copied).min(BLOCK - within);
            out[copied..copied + count].copy_from_slice(
                &cache.arena.bytes()[slot * BLOCK + within..slot * BLOCK + within + count],
            );
            offset += count as u64;
            copied += count;
        }
        Ok(())
    }
    pub fn row(&self, id: u32) -> Result<Vec<f16>> {
        let mut bytes = vec![0; self.dims * 2];
        self.copy(id, &mut bytes)?;
        Ok(bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| f16::from_le_bytes(*b))
            .collect())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    #[test]
    fn crossed_blocks_and_clock_eviction_preserve_exact_row_bits() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../.scratch")
            .join(format!("g4-source-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(root.clone());
        let mut file = File::create(root.join("rows")).unwrap();
        let count = 70000;
        for id in 0..count {
            for d in 0..3 {
                file.write_all(&f16::from_f32((id * 3 + d) as f32 / 100.).to_le_bytes())
                    .unwrap();
            }
        }
        file.sync_data().unwrap();
        drop(file);
        let file = File::open(root.join("rows")).unwrap();
        let rows = Rows::new(&file, 3, count).unwrap();
        for i in 0..4096 {
            let id = (i * 7919) % count;
            let got = rows.row(id as u32).unwrap();
            assert_eq!(
                got,
                (0..3)
                    .map(|d| f16::from_f32((id * 3 + d) as f32 / 100.))
                    .collect::<Vec<_>>()
            );
        }
        assert!(rows.cache.borrow().lookup.len() <= rows.cache.borrow().frames.len());
    }
}
