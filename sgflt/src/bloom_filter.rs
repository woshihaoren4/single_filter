use crate::error::SgfitErr;
use crate::{generate_hasher, Bitmap, FiltersInfo, SingleKeyFilter};
use std::collections::hash_map::{DefaultHasher, RandomState};
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::Arc;
use wd_tools::{PFErr, MD5};

pub struct BasicBloomFilter {
    group: String,
    code: String,
    info: Arc<dyn FiltersInfo + 'static>,
    bitmap: Arc<dyn Bitmap + 'static>,
    optimal_m: usize,
    optimal_k: u32,
    items_count: usize,
    hashes: [DefaultHasher; 2],
}

impl BasicBloomFilter {
    pub fn new<I: Into<String>>(
        group: I,
        code: I,
        info: Arc<dyn FiltersInfo + 'static>,
        bitmap: Arc<dyn Bitmap + 'static>,
        items_count: usize,
        fp_rate: f64,
    ) -> Self {
        let optimal_m = Self::bitmap_size(items_count, fp_rate);
        let optimal_k = Self::optimal_k(fp_rate);

        let group = group.into();
        let code = code.into();

        let hashes = [
            generate_hasher(group.as_str()),
            generate_hasher(code.as_str()),
        ];

        BasicBloomFilter {
            group,
            code,
            bitmap,
            items_count,
            info,
            optimal_m,
            optimal_k,
            hashes,
        }
    }
    // pub async fn is_full(&self) -> anyhow::Result<bool> {
    //     Ok(self
    //         .info
    //         .count(self.group.as_str(), self.code.as_str())
    //         .await?
    //         >= self.items_count)
    // }
    fn hash_kernel<T: Hash + Send + Sync + ?Sized>(&self, item: &T) -> (u64, u64) {
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
    async fn sync_mode_contain(&self, h1: u64, h2: u64, bitmap: &Vec<u8>) -> anyhow::Result<bool> {
        for k_i in 0..self.optimal_k {
            let index = self.get_index(h1, h2, k_i as u64);
            let i = index / 8;
            let offset = index % 8;
            if let Some(u) = bitmap.get(i) {
                if u & (0x01 << offset) == 0 {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }
        }
        Ok(true)
    }
    async fn sync_mode_insert(&self, h1: u64, h2: u64) -> anyhow::Result<Vec<usize>> {
        let mut result = vec![];
        for k_i in 0..self.optimal_k {
            let index = self.get_index(h1, h2, k_i as u64);
            result.push(index);
        }
        Ok(result)
    }
    async fn raw_insert(&self, item: &str, batch: bool) -> anyhow::Result<Vec<usize>> {
        //先判断是不是满了
        if self.is_full().await? {
            return anyhow::Error::new(SgfitErr::new_chunk_full(self.items_count)).err();
        }

        let (h1, h2) = self.hash_kernel(item);

        let bits = if batch {
            self.sync_mode_insert(h1, h2).await?
        } else {
            for k_i in 0..self.optimal_k {
                let index = self.get_index(h1, h2, k_i as u64);
                self.bitmap.set(self.code.as_str(), index, true).await?;
            }
            vec![]
        };

        //插入成功，添加一条记录
        if let Err(e) = self
            .info
            .add(self.group.as_str(), self.code.as_str(), 1)
            .await
        {
            wd_log::log_field("error", e)
                .field("code", self.code.as_str())
                .warn("BasicBloomFilter.info add failed")
        }
        Ok(bits)
    }
    async fn raw_contain(&self, item: &str, bits: Option<&Vec<u8>>) -> anyhow::Result<bool> {
        let (h1, h2) = self.hash_kernel(item);

        if let Some(v) = bits {
            return self.sync_mode_contain(h1, h2, v).await;
        } else {
            for k_i in 0..self.optimal_k {
                let index = self.get_index(h1, h2, k_i as u64);
                if !self.bitmap.get(self.code.as_str(), index).await? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn bitmap_size(items_count: usize, fp_rate: f64) -> usize {
        let ln2_2 = core::f64::consts::LN_2 * core::f64::consts::LN_2;
        ((-1.0f64 * items_count as f64 * fp_rate.ln()) / ln2_2).ceil() as usize
    }
    fn optimal_k(fp_rate: f64) -> u32 {
        ((-1.0f64 * fp_rate.ln()) / core::f64::consts::LN_2).ceil() as u32
    }
}

impl PartialEq for BasicBloomFilter {
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code
    }
}

#[async_trait::async_trait]
impl SingleKeyFilter for BasicBloomFilter {
    fn code(&self) -> String {
        self.code.clone()
    }

    async fn is_full(&self) -> anyhow::Result<bool> {
        Ok(self
            .info
            .count(self.group.as_str(), self.code.as_str())
            .await?
            >= self.items_count)
    }

    async fn insert(&self, item: &str) -> anyhow::Result<()> {
        //先判断是不是满了
        if self.is_full().await? {
            return anyhow::Error::new(SgfitErr::new_chunk_full(self.items_count)).err();
        }

        let (h1, h2) = self.hash_kernel(item);

        for k_i in 0..self.optimal_k {
            let index = self.get_index(h1, h2, k_i as u64);
            self.bitmap.set(self.code.as_str(), index, true).await?;
        }

        //插入成功，添加一条记录
        if let Err(e) = self
            .info
            .add(self.group.as_str(), self.code.as_str(), 1)
            .await
        {
            wd_log::log_field("error", e)
                .field("code", self.code.as_str())
                .warn("BasicBloomFilter.insert add count failed")
        }

        Ok(())
    }

    async fn contain(&self, item: &str) -> anyhow::Result<bool> {
        self.raw_contain(item, None).await
    }

    async fn pre_insert(
        &self,
        item: &str,
        total: &mut HashMap<String, usize>,
        growth: &mut HashMap<String, usize>,
    ) -> anyhow::Result<Vec<usize>> {
        //先判断是不是满了
        if !total.contains_key(self.code.as_str()) {
            let current_total = self
                .info
                .count(self.group.as_str(), self.code.as_str())
                .await?;
            total.insert(self.code.clone(), current_total);
        }
        let mut count = total.get(self.code.as_str()).unwrap().clone();
        if let Some(i) = growth.get(self.code.as_str()) {
            count += *i;
        }
        if count >= self.items_count {
            return anyhow::Error::new(SgfitErr::new_chunk_full(self.items_count)).err();
        }

        let (h1, h2) = self.hash_kernel(item);

        let bits = self.sync_mode_insert(h1, h2).await?;

        if let Some(i) = growth.get_mut(self.code.as_str()) {
            *i += 1;
        } else {
            growth.insert(self.code.clone(), 1);
        }
        Ok(bits)
    }

    async fn commit_insert(
        &self,
        buf: &mut HashMap<String, HashSet<usize>>,
        growth: &mut HashMap<String, usize>,
    ) -> anyhow::Result<()> {
        if let Some(s) = buf.remove(self.code.as_str()) {
            self.bitmap.mul_set(self.code.as_str(), s).await?;
        }
        if let Some(i) = growth.remove(self.code.as_str()) {
            self.info
                .add(self.group.as_str(), self.code.as_str(), i)
                .await?;
        }
        Ok(())
    }

    async fn pre_contain(
        &self,
        item: &str,
        buf: &mut HashMap<String, Vec<u8>>,
    ) -> anyhow::Result<bool> {
        if !buf.contains_key(self.code.as_str()) {
            let bit = self.bitmap.mul_get(self.code.as_str()).await?;
            buf.insert(self.code.clone(), bit);
        }
        let bits = buf.get(self.code.as_str()).unwrap();
        self.raw_contain(item, Some(bits)).await
    }
}
