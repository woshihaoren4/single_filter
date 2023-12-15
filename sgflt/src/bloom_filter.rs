use std::collections::hash_map::{DefaultHasher, RandomState};
use std::hash::{BuildHasher, Hash, Hasher};
use wd_tools::PFErr;
use crate::{Bitmap, FiltersInfo, SingleKeyFilter};

pub struct BasicBloomFilter {
    code: String,
    info:Box<dyn FiltersInfo + 'static>,
    bitmap:Box<dyn Bitmap+'static>,
    optimal_m: usize,
    optimal_k: u32,
    items_count: usize,
    hashes: [DefaultHasher; 2],
}

impl BasicBloomFilter{
    pub fn new<I:Into<String>,B:Bitmap+'static,F: FiltersInfo +'static>(code:I, info:F, bitmap:B, items_count: usize, fp_rate: f64) ->Self{
        let optimal_m = Self::bitmap_size(items_count, fp_rate);
        let optimal_k = Self::optimal_k(fp_rate);
        let hashes = [
            RandomState::new().build_hasher(),
            RandomState::new().build_hasher(),
        ];
        let code = code.into();
        let bitmap = Box::new(bitmap);
        let info = Box::new(info);
        BasicBloomFilter {
            code,
            bitmap,
            items_count,
            info,
            optimal_m,
            optimal_k,
            hashes,
        }
    }
    pub async fn is_full(&self)->anyhow::Result<bool>{
        Ok(self.info.count(self.code.clone()).await? >= self.items_count)
    }
    fn hash_kernel<T:Hash+Send+Sync+ ?Sized>(&self, item: &T) -> (u64, u64)
    {
        let hasher1 = &mut self.hashes[0].clone();
        let hasher2 = &mut self.hashes[1].clone();

        item.hash(hasher1);
        item.hash(hasher2);

        let hash1 = hasher1.finish();
        let hash2 = hasher2.finish();

        (hash1, hash2)
    }
    fn get_index(&self, h1: u64, h2: u64, k_i: u64) -> usize {
        h1.wrapping_add((k_i).wrapping_mul(h2)) as usize % self.optimal_m
    }
    fn bitmap_size(items_count: usize, fp_rate: f64) -> usize {
        let ln2_2 = core::f64::consts::LN_2 * core::f64::consts::LN_2;
        ((-1.0f64 * items_count as f64 * fp_rate.ln()) / ln2_2).ceil() as usize
    }
    fn optimal_k(fp_rate: f64) -> u32 {
        ((-1.0f64 * fp_rate.ln()) / core::f64::consts::LN_2).ceil() as u32
    }
}

#[async_trait::async_trait]
impl SingleKeyFilter for BasicBloomFilter{
    async fn insert<KEY: Hash+Send+Sync+ ?Sized>(&self, item: &KEY) -> anyhow::Result<()> {
        //先判断是不是满了
        if self.is_full().await? {
            return anyhow::anyhow!("the bloom[{}] cap[{}] is full",self.code,self.items_count).err()
        }

        let (h1, h2) = self.hash_kernel(item);

        for k_i in 0..self.optimal_k {
            let index = self.get_index(h1, h2, k_i as u64);

            self.bitmap.set(index, true).await?;
        }
        //插入成功，添加一条记录
        if let Err(e) = self.info.add(self.code.clone()).await {
            wd_log::log_field("error",e).field("code",self.code.as_str()).warn("BasicBloomFilter.info add failed")
        }
        Ok(())
    }

    async fn contain<KEY: Hash+Send+Sync+ ?Sized>(&self, item: &KEY) -> anyhow::Result<bool> {
        let (h1, h2) = self.hash_kernel(item);

        for k_i in 0..self.optimal_k {
            let index = self.get_index(h1, h2, k_i as u64);

            if !self.bitmap.get(index).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}