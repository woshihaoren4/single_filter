mod bloom_expand_strategy;
mod bloom_filter;
mod bloom_group;
mod error;
mod filter_pool;
mod fiterinfo_bitmap_redis;
mod util;

pub use bloom_expand_strategy::*;
pub use bloom_filter::*;
pub use bloom_group::*;
pub use error::*;
pub use filter_pool::*;
pub use fiterinfo_bitmap_redis::*;
use std::collections::{HashMap, HashSet};
pub use util::*;

use std::sync::Arc;

// bitmap实现，是分布式实现消重的的重要抽象
// 推荐bitmap命名方式 {{appid}}/{{group}}/{{timestamp}}
#[async_trait::async_trait]
pub trait Bitmap: Send + Sync {
    // fn init(&mut self,code:String);
    async fn set(&self, key: &str, offset: usize, value: bool) -> anyhow::Result<()>;
    async fn get(&self, key: &str, offset: usize) -> anyhow::Result<bool>;

    async fn mul_set(&self, key: &str, list: HashSet<usize>) -> anyhow::Result<()>;
    async fn mul_get(&self, key: &str) -> anyhow::Result<Vec<u8>>;
}

// 过滤器信息加载方法
#[async_trait::async_trait]
pub trait FiltersInfo: Send + Sync {
    async fn list(&self, group: &str) -> anyhow::Result<Vec<(String, usize)>>;
    async fn count(&self, group: &str, key: &str) -> anyhow::Result<usize>;
    async fn add(&self, group: &str, key: &str, count: usize) -> anyhow::Result<()>;
    // async fn chunk(&self,key:String)->anyhow::Result<()>;
}

// 过滤器的抽象
#[async_trait::async_trait]
pub trait SingleKeyFilter: Send + Sync {
    fn code(&self) -> String;
    async fn is_full(&self) -> anyhow::Result<bool>;
    async fn insert(&self, item: &str) -> anyhow::Result<()>;
    async fn contain(&self, item: &str) -> anyhow::Result<bool>;

    async fn pre_insert(
        &self,
        item: &str,
        total: &mut HashMap<String, usize>,
        growth: &mut HashMap<String, usize>,
    ) -> anyhow::Result<Vec<usize>>;
    async fn commit_insert(
        &self,
        buf: &mut HashMap<String, HashSet<usize>>,
        growth: &mut HashMap<String, usize>,
    ) -> anyhow::Result<()>;
    async fn pre_contain(
        &self,
        item: &str,
        buf: &mut HashMap<String, Vec<u8>>,
    ) -> anyhow::Result<bool>;
}

// 过滤器组加载和扩展规则
#[async_trait::async_trait]
pub trait FilterExpandStrategy: Send + Sync {
    async fn load_filter_group(&self, group: &str)
        -> anyhow::Result<Vec<Arc<dyn SingleKeyFilter>>>;
    async fn expand_chunk(
        &self,
        group: &str,
        index: isize,
    ) -> anyhow::Result<Arc<dyn SingleKeyFilter>>;
}

// 本地缓存
pub trait Pool<T>: Send + Sync {
    fn add(&self, group: &str, val: T);
    fn get(&self, group: &str) -> Arc<T>;
}

#[cfg(test)]
mod tests {
    use crate::bloom_filter::BasicBloomFilter;
    use crate::fiterinfo_bitmap_redis::{BitmapRedis, FilterInfoRedis};
    use crate::{BloomExpandStrategy, FiltersInfo, FiltersPool, SingleKeyFilter};
    use std::sync::Arc;
    use std::time;

    #[tokio::test]
    async fn test_bloom_filter_by_redis() {
        let br = BitmapRedis::redis_single_node("redis://:root@1.116.41.230/").unwrap();
        let info = FilterInfoRedis::redis_single_node("redis://:root@1.116.41.230/").unwrap();

        let bbf = BasicBloomFilter::new(
            "SFP_test01_0001",
            "SFP_test01_0001_0_1703923200",
            Arc::new(info),
            Arc::new(br),
            100,
            0.001,
        );
        let res = bbf.contain("test_key03").await.unwrap();
        assert_eq!(res, false);
        bbf.insert("test_key03").await.unwrap();
        let res = bbf.contain("test_key03").await.unwrap();
        assert_eq!(res, true);
    }

    #[tokio::test]
    async fn test_bloom_filter_list() {
        let info = FilterInfoRedis::redis_single_node("redis://:root@1.116.41.230/").unwrap();
        let map = info.list("SFP_biz02_user001").await.unwrap();
        println!("-->{:?}", map);
    }

    #[tokio::test]
    async fn test_filter_pool() {
        let key = "test_key03";

        let strategy =
            BloomExpandStrategy::build_from_redis("test01", "redis://:root@1.116.41.230/").unwrap();
        let pool = FiltersPool::from(strategy);
        let result = pool.contain("0001", key.to_string()).await.unwrap();
        assert_eq!(result, false);
        pool.insert("0001", key.to_string()).await.unwrap();
        let result = pool.contain("0001", key.to_string()).await.unwrap();
        assert_eq!(result, true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_pressure() {
        let strategy =
            BloomExpandStrategy::build_from_redis("biz02", "redis://:root@1.116.41.230/")
                .unwrap()
                .set_strategy_fixed(100);
        let pool = FiltersPool::from(strategy);

        let key_count = 200;
        let group = "user001";
        let mut keys = Vec::with_capacity(key_count);
        for i in 0..key_count {
            keys.push(format!("key_{}", i));
        }

        let out_log = wd_log::log_field("KEY_COUNT", key_count).field("group", group);

        let user_time = time::Instant::now();
        let _ = pool.batch_contain(group, keys.clone()).await.unwrap();
        let user_time = user_time.elapsed().as_millis();
        let out_log = out_log.field("first_search_user_time_ms", user_time);

        let user_time = time::Instant::now();
        let _ = pool.batch_insert(group, keys.clone()).await.unwrap();
        let user_time = user_time.elapsed().as_millis();
        let out_log = out_log.field("insert_user_time_ms", user_time);

        let user_time = time::Instant::now();
        let result = pool.batch_contain(group, keys.clone()).await.unwrap();
        let user_time = user_time.elapsed().as_millis();
        let out_log = out_log.field("second_search_user_time_ms", user_time);
        let mut accuracy = 0;
        for i in result {
            if i {
                accuracy += 1;
            }
        }
        out_log
            .field("accuracy", accuracy)
            .info("pressure test result");
    }
}
