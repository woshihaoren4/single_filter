mod bloom_filter;
mod fiterinfo_bitmap_redis;
mod filter_pool;

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

// bitmap实现，是分布式实现消重的的重要抽象
#[async_trait::async_trait]
pub trait Bitmap:Send+Sync{
    // fn init(&mut self,code:String);
    async fn set(&self,offset:usize,value:bool)->anyhow::Result<()>;
    async fn get(&self,offset:usize)->anyhow::Result<bool>;
}

// 过滤器信息加载方法
#[async_trait::async_trait]
pub trait FiltersInfo:Send+Sync{
    async fn list(&self)->anyhow::Result<Vec<(String,usize)>>;
    async fn count(&self,key:String)->anyhow::Result<usize>;
    async fn add(&self,key:String)->anyhow::Result<()>;
    // async fn chunk(&self,key:String)->anyhow::Result<()>;
}

// 过滤器的抽象
#[async_trait::async_trait]
pub trait SingleKeyFilter:Send+Sync{
    async fn insert<KEY:Hash+Send+Sync+ ?Sized>(&self,item:&KEY)->anyhow::Result<()> where Self: Sized;
    async fn contain<KEY:Hash+Send+Sync+ ?Sized>(&self,item:&KEY)-> anyhow::Result<bool> where Self: Sized;
}

// 过滤器组加载和扩展规则
#[async_trait::async_trait]
pub trait FilterExpandStrategy:Send+Sync{
    async fn load_filter_group(&self, group:String) ->anyhow::Result<Vec<Box<dyn SingleKeyFilter>>>;
    async fn expand_chunk(&self, group:String) ->anyhow::Result<(Box<dyn SingleKeyFilter>)>;
}

// 本地缓存
pub trait Pool<T>:Send+Sync{
    fn add(&self,code:&str,val:T);
    fn get(&self,code:&str)->Option<T>;
}

#[cfg(test)]
mod tests {
    use crate::fiterinfo_bitmap_redis::{BitmapRedis, FilterInfoRedis};
    use crate::bloom_filter::BasicBloomFilter;
    use crate::{FiltersInfo, SingleKeyFilter};

    #[tokio::test]
    async fn  test_bloom_filter_by_redis(){
        let br = BitmapRedis::redis_single_node("test-chunk-1214","redis://:root@1.116.41.230/").unwrap();
        let info = FilterInfoRedis::redis_single_node("test", "redis://:root@1.116.41.230/").unwrap();

        let bbf = BasicBloomFilter::new("test-chunk-1214",info, br, 100, 0.001);
        let res = bbf.contain("test_key_1").await.unwrap();
        assert_eq!(res,false);
        bbf.insert("test_key_1").await.unwrap();
        let res = bbf.contain("test_key_1").await.unwrap();
        assert_eq!(res,true);
    }

    #[tokio::test]
    async fn  test_bloom_filter_list(){
        let info = FilterInfoRedis::redis_single_node("test", "redis://:root@1.116.41.230/").unwrap();
        let map = info.list().await.unwrap();
        println!("-->{:?}",map);
    }
}
