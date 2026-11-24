//! Serialized mutation with immutable, atomically published read snapshots.
use arc_swap::ArcSwap;
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tokio::sync::{Mutex, MutexGuard};

pub(crate) struct Published<T> {
    value: Mutex<Arc<T>>,
    snapshot: ArcSwap<T>,
}
impl<T: Clone> Published<T> {
    pub fn new(value: T) -> Self {
        let value = Arc::new(value);
        Self {
            value: Mutex::new(value.clone()),
            snapshot: ArcSwap::new(value),
        }
    }
    pub fn load(&self) -> Arc<T> {
        self.snapshot.load_full()
    }
    pub async fn lock(&self) -> Guard<'_, T> {
        Guard {
            value: self.value.lock().await,
            snapshot: &self.snapshot,
            dirty: false,
        }
    }
}
pub(crate) struct Guard<'a, T> {
    value: MutexGuard<'a, Arc<T>>,
    snapshot: &'a ArcSwap<T>,
    dirty: bool,
}
impl<T> Deref for Guard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
impl<T: Clone> DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.dirty = true;
        Arc::make_mut(&mut self.value)
    }
}
impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        if self.dirty {
            self.snapshot.store(self.value.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn readers_keep_their_snapshot_until_the_next_publication() {
        let published = Published::new(vec![1]);
        let before = published.load();
        let mut writer = published.lock().await;
        writer.push(2);
        assert_eq!(*published.load(), [1]);
        drop(writer);
        let after = published.load();
        assert_eq!(*before, [1]);
        assert_eq!(*after, [1, 2]);
        drop(published.lock().await);
        assert!(Arc::ptr_eq(&after, &published.load()));
    }
}
