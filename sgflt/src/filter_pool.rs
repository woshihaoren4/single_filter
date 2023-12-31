use crate::bloom_group::FilterGroup;
use crate::{FilterExpandStrategy, Pool};
use std::sync::Arc;
use wd_tools::{PFArc, PFBox};

pub struct FiltersPool {
    pool: Box<dyn Pool<FilterGroup>>,
}

impl FiltersPool {
    pub fn new<P: Pool<FilterGroup> + 'static>(pool: P) -> Self {
        Self {
            pool: pool.to_box(),
        }
    }
    pub fn set_pool<P: Pool<FilterGroup> + 'static>(&mut self, pool: P) {
        self.pool = pool.to_box();
    }
}

impl FiltersPool {
    pub async fn contain(&self, group: &str, keys: Vec<String>) -> anyhow::Result<Vec<bool>> {
        let fg = self.pool.get(group);
        // fixme
        let _ = fg.extend().await;
        fg.contain(keys).await
    }
    pub async fn insert(&self, group: &str, keys: Vec<String>) -> anyhow::Result<()> {
        let fg = self.pool.get(group);
        // fixme
        let _ = fg.extend().await;
        fg.insert(keys).await
    }
}

impl<T: FilterExpandStrategy + 'static> From<T> for FiltersPool {
    fn from(value: T) -> Self {
        let strategy = Arc::new(value);
        let pi = DefaultPoolImpl { strategy };
        let pool = Box::new(pi);
        FiltersPool { pool }
    }
}

pub struct DefaultPoolImpl {
    strategy: Arc<dyn FilterExpandStrategy + 'static>,
}

impl Pool<FilterGroup> for DefaultPoolImpl {
    fn add(&self, group: &str, val: FilterGroup) {
        //todo
    }

    fn get(&self, group: &str) -> Arc<FilterGroup> {
        FilterGroup::new(group.to_string(), self.strategy.clone()).arc()
    }
}
