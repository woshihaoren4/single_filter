use std::collections::HashMap;
use redis::cluster::{ClusterClient};
use redis::{AsyncCommands, Client, IntoConnectionInfo};
use wd_tools::PFOk;
use wd_tools::ptr::ToStaticStr;
use crate::{Bitmap, FiltersInfo};

const BITMAP_REDIS_PREFIX:&'static str = "brp";
const FILTER_INFO_REDIS_PREFIX:&'static str = "fip";

#[derive(Clone)]
#[allow(dead_code)]
enum RedisClient{
    CLUSTER(ClusterClient),
    SINGLE(Client),
}

pub struct BitmapRedis{
    code: &'static str,
    client:RedisClient,
}

impl BitmapRedis{
    #[allow(dead_code)]
    pub fn new_from_cluster(code:&str,client:ClusterClient)->Self{
        let client = RedisClient::CLUSTER(client);
        let code = format!("{}_{}",BITMAP_REDIS_PREFIX,code).to_static_str();
        Self{client,code}
    }
    #[allow(dead_code)]
    pub fn redis_single_node(code:&str,url:&str)->anyhow::Result<Self>{
        let client = redis::Client::open(url)?;
        let client = RedisClient::SINGLE(client);
        let code = format!("{}_{}",BITMAP_REDIS_PREFIX,code).to_static_str();
        Ok(Self{client,code})
    }
    #[allow(dead_code)]
    pub fn redis_cluster<T: IntoConnectionInfo>(nodes: impl IntoIterator<Item = T>) ->anyhow::Result<ClusterClient>{
        let client = ClusterClient::new(nodes)?;Ok(client)
    }

    // pub async fn redis_lock(&self, conn: &mut ClusterConnection) ->anyhow::Result<String>{
    //     for i in 0..5{
    //         let uuid = wd_tools::uuid::v4();
    //         let opt = SetOptions::default().conditional_set(ExistenceCheck::NX).with_expiration(SetExpiry::EX(30));
    //         let result:Option<bool> = conn.set_options(self.code, uuid.as_str(), opt).await?;
    //         if let Some(b) = result{
    //             if b {
    //                 return Ok(uuid)
    //             }
    //         }
    //     }
    //     Err(anyhow::anyhow!("redis lock timeout,key = {}",self.code))
    // }
    // // 0:unlock 1:不是自己的锁 2:锁超时
    // pub async fn redis_unlock(&self, conn: &mut ClusterConnection,val:String) ->anyhow::Result<usize>{
    //     let result:Option<String> = conn.get(self.code).await?;
    //     if let Some(v) = result{
    //         if v == val{
    //             let _ : Option<usize> = conn.del(self.code).await?;
    //             return Ok(0)
    //         }
    //         return Ok(1)
    //     }
    //     return Ok(2)
    // }
}

#[async_trait::async_trait]
impl Bitmap for BitmapRedis{
    // fn init(&mut self, code: String) {
    //     self.code = format!("{}_{}",BITMAP_REDIS_PREFIX,code).to_static_str();
    // }

    async fn set(&self, offset: usize, value: bool) -> anyhow::Result<()> {
        return match self.client {
            RedisClient::CLUSTER(ref clu) => {
                let mut conn = clu.get_async_connection().await?;
                let _ = conn.setbit(self.code, offset, value).await?;
                Ok(())
            }
            RedisClient::SINGLE(ref sin) => {
                let mut conn = sin.get_async_connection().await?;
                let _ = conn.setbit(self.code, offset, value).await?;
                Ok(())
            }
        }

    }

    async fn get(&self, offset: usize) -> anyhow::Result<bool> {
        return match self.client {
            RedisClient::CLUSTER(ref clu) => {
                let mut conn = clu.get_async_connection().await?;
                let result: bool = conn.getbit(self.code, offset).await?;
                Ok(result)
            }
            RedisClient::SINGLE(ref sin) => {
                let mut conn = sin.get_async_connection().await?;
                let result: bool = conn.getbit(self.code, offset).await?;
                Ok(result)
            }
        }
    }
}

#[derive(Clone)]
pub struct FilterInfoRedis{
    code: String,
    client:RedisClient,
}
impl FilterInfoRedis {
    #[allow(dead_code)]
    pub fn new_from_cluster(code:&str,client:ClusterClient)->Self{
        let client = RedisClient::CLUSTER(client);
        let code = format!("{}_{}",FILTER_INFO_REDIS_PREFIX,code);
        Self{client,code}
    }
    #[allow(dead_code)]
    pub fn redis_single_node(code:&str,url:&str)->anyhow::Result<Self>{
        let client = Client::open(url)?;
        let client = RedisClient::SINGLE(client);
        let code = format!("{}_{}",FILTER_INFO_REDIS_PREFIX,code);
        Ok(Self{client,code})
    }
    #[allow(dead_code)]
    pub fn set_code<S:Into<String>>(mut self,code:S)->Self{
        self.code = code.into();self
    }
}
#[async_trait::async_trait]
impl FiltersInfo for FilterInfoRedis{
    async fn list(&self) ->anyhow::Result<Vec<(String,usize)>>{
        match self.client {
            RedisClient::CLUSTER(ref clu) => {
                let mut client = clu.get_async_connection().await?;
                let result:Option<HashMap<String,usize>> = client.hgetall(self.code.as_str()).await?;
                let map = result.unwrap_or(HashMap::new());
                let mut list = vec![];
                for (k,v) in map.into_iter(){
                    list.push((k,v));
                }
                list.sort_by(|(a,_),(b,_)|a.cmp(b));
                Ok(list)
            }
            RedisClient::SINGLE(ref sin) => {
                let mut client = sin.get_async_connection().await?;
                let result:Option<HashMap<String,usize>> = client.hgetall(self.code.as_str()).await?;
                let map = result.unwrap_or(HashMap::new());
                let mut list = vec![];
                for (k,v) in map.into_iter(){
                    list.push((k,v));
                }
                list.sort_by(|(a,_),(b,_)|a.cmp(b));
                Ok(list)
            }
        }
    }

    async fn count(&self,key:String) -> anyhow::Result<usize> {
        match self.client {
            RedisClient::CLUSTER(ref clu) => {
                let mut client = clu.get_async_connection().await?;
                let result:Option<usize> = client.hget(self.code.as_str(), key).await?;
                result.unwrap_or(0).ok()
            }
            RedisClient::SINGLE(ref sin) => {
                let mut client = sin.get_async_connection().await?;
                let result:Option<usize> = client.hget(self.code.as_str(),key).await?;
                result.unwrap_or(0).ok()
            }
        }
    }

    async fn add(&self,key:String) -> anyhow::Result<()> {
        match self.client {
            RedisClient::CLUSTER(ref clu) => {
                let mut client = clu.get_async_connection().await?;
                let _:isize = client.hincr(self.code.as_str(), key,1).await?;
            }
            RedisClient::SINGLE(ref sin) => {
                let mut client = sin.get_async_connection().await?;
                let _:isize = client.hincr(self.code.as_str(),key,1).await?;
            }
        }
        Ok(())
    }
}