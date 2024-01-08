use crate::{Bitmap, FiltersInfo};
use redis::cluster::ClusterClient;
use redis::{AsyncCommands, Client, IntoConnectionInfo};
use std::collections::{HashMap, HashSet};
use wd_tools::PFOk;

#[derive(Clone)]
#[allow(dead_code)]
pub enum RedisClient {
    CLUSTER(ClusterClient),
    SINGLE(Client),
}
impl TryFrom<&str> for RedisClient {
    type Error = anyhow::Error;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let client = Client::open(value)?;
        Ok(RedisClient::SINGLE(client))
    }
}
impl TryFrom<Vec<String>> for RedisClient {
    type Error = anyhow::Error;
    fn try_from(value: Vec<String>) -> Result<Self, Self::Error> {
        let cs = ClusterClient::new(value)?;
        Ok(RedisClient::CLUSTER(cs))
    }
}

pub struct BitmapRedis {
    client: RedisClient,
}
impl From<RedisClient> for BitmapRedis {
    fn from(client: RedisClient) -> Self {
        Self { client }
    }
}
impl BitmapRedis {
    #[allow(dead_code)]
    pub fn new_from_cluster(client: ClusterClient) -> Self {
        let client = RedisClient::CLUSTER(client);
        Self { client }
    }
    #[allow(dead_code)]
    pub fn redis_single_node(url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        let client = RedisClient::SINGLE(client);
        Ok(Self { client })
    }
    #[allow(dead_code)]
    pub fn redis_cluster<T: IntoConnectionInfo>(
        nodes: impl IntoIterator<Item = T>,
    ) -> anyhow::Result<ClusterClient> {
        let client = ClusterClient::new(nodes)?;
        Ok(client)
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
impl Bitmap for BitmapRedis {
    // fn init(&mut self, code: String) {
    //     self.code = format!("{}_{}",BITMAP_REDIS_PREFIX,code).to_static_str();
    // }

    async fn set(&self, key: &str, offset: usize, value: bool) -> anyhow::Result<()> {
        return match self.client {
            RedisClient::CLUSTER(ref clu) => {
                let mut conn = clu.get_async_connection().await?;
                let _ = conn.setbit(key, offset, value).await?;
                Ok(())
            }
            RedisClient::SINGLE(ref sin) => {
                let mut conn = sin.get_async_connection().await?;
                let _ = conn.setbit(key, offset, value).await?;
                Ok(())
            }
        };
    }

    async fn get(&self, key: &str, offset: usize) -> anyhow::Result<bool> {
        return match self.client {
            RedisClient::CLUSTER(ref clu) => {
                let mut conn = clu.get_async_connection().await?;
                let result: bool = conn.getbit(key, offset).await?;
                Ok(result)
            }
            RedisClient::SINGLE(ref sin) => {
                let mut conn = sin.get_async_connection().await?;
                let result: bool = conn.getbit(key, offset).await?;
                Ok(result)
            }
        };
    }

    async fn mul_set(&self, key: &str, list: HashSet<usize>) -> anyhow::Result<()> {
        return match self.client {
            RedisClient::CLUSTER(ref clu) => {
                let mut conn = clu.get_async_connection().await?;
                let result: Option<Vec<u8>> = conn.get(key).await?;
                let mut buf = if let Some(s) = result { s } else { vec![] };
                for i in list {
                    let l = i / 8;
                    if l >= buf.len() {
                        let mut avec = vec![0u8; l - buf.len() + 1];
                        buf.append(&mut avec);
                    }
                    buf[l] |= (0x01 << (i % 8))
                }
                conn.set(key, buf).await?;
                Ok(())
            }
            RedisClient::SINGLE(ref sin) => {
                let mut conn = sin.get_async_connection().await?;
                let result: Option<Vec<u8>> = conn.get(key).await?;
                let mut buf = if let Some(s) = result { s } else { vec![] };
                for i in list {
                    let l = i / 8;
                    if l >= buf.len() {
                        let mut avec = vec![0u8; l - buf.len() + 1];
                        buf.append(&mut avec);
                    }
                    buf[l] |= (0x01 << (i % 8))
                }
                conn.set(key, buf).await?;
                Ok(())
            }
        };
    }

    async fn mul_get(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        return match self.client {
            RedisClient::CLUSTER(ref clu) => {
                let mut conn = clu.get_async_connection().await?;
                let result: Option<Vec<u8>> = conn.get(key).await?;
                let buf = if let Some(s) = result { s } else { vec![] };
                Ok(buf)
            }
            RedisClient::SINGLE(ref sin) => {
                let mut conn = sin.get_async_connection().await?;
                let result: Option<Vec<u8>> = conn.get(key).await?;
                let buf = if let Some(s) = result { s } else { vec![] };
                Ok(buf)
            }
        };
    }
}

#[derive(Clone)]
pub struct FilterInfoRedis {
    client: RedisClient,
}
impl From<RedisClient> for FilterInfoRedis {
    fn from(client: RedisClient) -> Self {
        Self { client }
    }
}
impl FilterInfoRedis {
    #[allow(dead_code)]
    pub fn new_from_cluster(client: ClusterClient) -> Self {
        let client = RedisClient::CLUSTER(client);
        Self { client }
    }
    #[allow(dead_code)]
    pub fn redis_single_node(url: &str) -> anyhow::Result<Self> {
        let client = Client::open(url)?;
        let client = RedisClient::SINGLE(client);
        Ok(Self { client })
    }
}
#[async_trait::async_trait]
impl FiltersInfo for FilterInfoRedis {
    async fn list(&self, group: &str) -> anyhow::Result<Vec<(String, usize)>> {
        match self.client {
            RedisClient::CLUSTER(ref clu) => {
                let mut client = clu.get_async_connection().await?;
                let result: Option<HashMap<String, usize>> = client.hgetall(group).await?;
                let map = result.unwrap_or(HashMap::new());
                let mut list = vec![];
                for (k, v) in map.into_iter() {
                    list.push((k, v));
                }
                list.sort_by(|(a, _), (b, _)| a.cmp(b));
                Ok(list)
            }
            RedisClient::SINGLE(ref sin) => {
                let mut client = sin.get_async_connection().await?;
                let result: Option<HashMap<String, usize>> = client.hgetall(group).await?;
                let map = result.unwrap_or(HashMap::new());
                let mut list = vec![];
                for (k, v) in map.into_iter() {
                    list.push((k, v));
                }
                list.sort_by(|(a, _), (b, _)| a.cmp(b));
                Ok(list)
            }
        }
    }

    async fn count(&self, group: &str, key: &str) -> anyhow::Result<usize> {
        match self.client {
            RedisClient::CLUSTER(ref clu) => {
                let mut client = clu.get_async_connection().await?;
                let result: Option<usize> = client.hget(group, key).await?;
                result.unwrap_or(0).ok()
            }
            RedisClient::SINGLE(ref sin) => {
                let mut client = sin.get_async_connection().await?;
                let result: Option<usize> = client.hget(group, key).await?;
                result.unwrap_or(0).ok()
            }
        }
    }

    async fn add(&self, group: &str, key: &str, count: usize) -> anyhow::Result<()> {
        match self.client {
            RedisClient::CLUSTER(ref clu) => {
                let mut client = clu.get_async_connection().await?;
                let _: isize = client.hincr(group, key, count).await?;
            }
            RedisClient::SINGLE(ref sin) => {
                let mut client = sin.get_async_connection().await?;
                let _: isize = client.hincr(group, key, count).await?;
            }
        }
        Ok(())
    }
}
