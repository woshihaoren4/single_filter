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
pub use util::*;

use std::sync::Arc;

// bitmap实现，是分布式实现消重的的重要抽象
// 推荐bitmap命名方式 {{appid}}/{{group}}/{{timestamp}}
#[async_trait::async_trait]
pub trait Bitmap: Send + Sync {
    // fn init(&mut self,code:String);
    async fn set(&self, key: &str, offset: usize, value: bool) -> anyhow::Result<()>;
    async fn get(&self, key: &str, offset: usize) -> anyhow::Result<bool>;
}

// 过滤器信息加载方法
#[async_trait::async_trait]
pub trait FiltersInfo: Send + Sync {
    async fn list(&self, group: &str) -> anyhow::Result<Vec<(String, usize)>>;
    async fn count(&self, group: &str, key: &str) -> anyhow::Result<usize>;
    async fn add(&self, group: &str, key: &str) -> anyhow::Result<()>;
    // async fn chunk(&self,key:String)->anyhow::Result<()>;
}

// 过滤器的抽象
#[async_trait::async_trait]
pub trait SingleKeyFilter: Send + Sync {
    fn code(&self) -> String;
    async fn insert(&self, item: &str) -> anyhow::Result<()>;
    async fn contain(&self, item: &str) -> anyhow::Result<bool>;
}

// 过滤器组加载和扩展规则
#[async_trait::async_trait]
pub trait FilterExpandStrategy: Send + Sync {
    async fn load_filter_group(&self, group: &str)
        -> anyhow::Result<Vec<Arc<dyn SingleKeyFilter>>>;
    async fn expand_chunk(&self, group: &str) -> anyhow::Result<Arc<dyn SingleKeyFilter>>;
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

    #[tokio::test]
    async fn test_bloom_filter_by_redis() {
        let br = BitmapRedis::redis_single_node("redis://:root@1.116.41.230/").unwrap();
        let info = FilterInfoRedis::redis_single_node("redis://:root@1.116.41.230/").unwrap();

        // SFP_test01_0001 SFP_test01_0001_0_1703923200 test_key03 10 100
        // SFP_test01_0001 SFP_test01_0001_0_1703923200 test_key03 10 100
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
        let map = info.list("test01").await.unwrap();
        println!("-->{:?}", map);
    }

    #[tokio::test]
    async fn test_filter_pool() {
        let key = "test_key03";

        let strategy =
            BloomExpandStrategy::build_from_redis("test01", "redis://:root@1.116.41.230/").unwrap();
        let pool = FiltersPool::from(strategy);
        let result = pool.contain("0001", vec![key.to_string()]).await.unwrap();
        assert_eq!(result[0], false);
        pool.insert("0001", vec![key.to_string()]).await.unwrap();
        let result = pool.contain("0001", vec![key.to_string()]).await.unwrap();
        assert_eq!(result[0], true);
    }
}
