use std::future::Future;
use std::hash::Hash;
use redis::cluster::{ClusterClient};
use redis::{AsyncCommands, Client, Commands, ExistenceCheck, IntoConnectionInfo, SetExpiry, SetOptions};
use redis::cluster_async::ClusterConnection;
use wd_tools::ptr::ToStaticStr;
use crate::Bitmap;

const BITMAP_REDIS_PREFIX:&'static str = "brp";

enum RedisClient{
    CLUSTER(ClusterClient),
    SINGLE(Client),
}

pub struct BitmapRedis{
    code: &'static str,
    client:RedisClient,
}

impl BitmapRedis{
    pub fn new_from_cluster(client:ClusterClient)->Self{
        let client = RedisClient::CLUSTER(client);
        Self{client,code: ""}
    }
    pub fn redis_single_node(url:&str)->anyhow::Result<Self>{
        let client = redis::Client::open(url)?;
        let client = RedisClient::SINGLE(client);
        Ok(Self{client,code: ""})
    }
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
    fn init(&mut self, code: String) {
        self.code = format!("{}_{}",BITMAP_REDIS_PREFIX,code).to_static_str();
    }

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