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
    pub async fn contain(&self, group: &str, key: String) -> anyhow::Result<bool> {
        let fg = self.pool.get(group);
        let _ = fg.try_extend().await;
        let res = fg.contain(vec![key]).await?;
        Ok(res[0])
    }
    pub async fn insert(&self, group: &str, keys: String) -> anyhow::Result<()> {
        let fg = self.pool.get(group);
        let _ = fg.try_extend().await;
        fg.insert(vec![keys]).await
    }
    pub async fn batch_contain(&self, group: &str, keys: Vec<String>) -> anyhow::Result<Vec<bool>> {
        let fg = self.pool.get(group);
        let _ = fg.try_extend().await;
        fg.batch_contain(keys).await
    }
    pub async fn batch_insert(&self, group: &str, keys: Vec<String>) -> anyhow::Result<()> {
        let fg = self.pool.get(group);
        let _ = fg.try_extend().await;
        fg.batch_insert(keys).await
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
    fn add(&self, _group: &str, _val: FilterGroup) {
        //todo
    }

    fn get(&self, group: &str) -> Arc<FilterGroup> {
        FilterGroup::new(group.to_string(), self.strategy.clone()).arc()
    }
}
