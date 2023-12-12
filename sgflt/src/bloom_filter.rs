use std::collections::hash_map::{DefaultHasher, RandomState};
use std::hash::{BuildHasher, Hash, Hasher};
use crate::{Bitmap, SingleKeyFilter};

pub struct BasicBloomFilter {
    code: String,
    bitmap:Box<dyn Bitmap+Send+Sync+'static>,
    optimal_m: usize,
    optimal_k: u32,
    hashes: [DefaultHasher; 2],
}

impl BasicBloomFilter{
    pub async fn new<I:Into<String>,B:Bitmap+Send+Sync+'static>(code:I,bitmap:B,items_count: usize, fp_rate: f64)->Self{
        let optimal_m = Self::bitmap_size(items_count, fp_rate);
        let optimal_k = Self::optimal_k(fp_rate);
        let hashes = [
            RandomState::new().build_hasher(),
            RandomState::new().build_hasher(),
        ];
        let code = code.into();
        let mut bitmap = Box::new(bitmap);
        bitmap.init(code.clone());
        BasicBloomFilter {
            code,
            bitmap,
            optimal_m,
            optimal_k,
            hashes,
        }
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
        let (h1, h2) = self.hash_kernel(item);

        for k_i in 0..self.optimal_k {
            let index = self.get_index(h1, h2, k_i as u64);

            self.bitmap.set(index, true).await?;
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