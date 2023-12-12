mod bloom_filter;
mod bitmap_redis;

use std::hash::Hash;


#[async_trait::async_trait]
pub trait Bitmap{
    fn init(&mut self,code:String);
    async fn set(&self,offset:usize,value:bool)->anyhow::Result<()>;
    async fn get(&self,offset:usize)->anyhow::Result<bool>;
}

#[async_trait::async_trait]
pub trait SingleKeyFilter{
    async fn insert<KEY:Hash+Send+Sync+ ?Sized>(&self,item:&KEY)->anyhow::Result<()>;
    async fn contain<KEY:Hash+Send+Sync+ ?Sized>(&self,item:&KEY)-> anyhow::Result<bool>;
}

#[cfg(test)]
mod tests {
    use crate::bitmap_redis::BitmapRedis;
    use crate::bloom_filter::BasicBloomFilter;
    use crate::{Bitmap, SingleKeyFilter};

    #[tokio::test]
    async fn  test_bloom_filter_by_redis(){
        let br = BitmapRedis::redis_single_node("redis://:root@1.116.41.230/").unwrap();
        let bbf = BasicBloomFilter::new("test", br, 100, 0.001).await;
        let res = bbf.contain("test_key_1").await.unwrap();
        assert_eq!(res,false);
        bbf.insert("test_key_1").await.unwrap();
        let res = bbf.contain("test_key_1").await.unwrap();
        assert_eq!(res,true);
    }
}
