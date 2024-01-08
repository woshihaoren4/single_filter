use crate::bloom_filter::BasicBloomFilter;
use crate::util::assembly_prefix;
use crate::{
    Bitmap, BitmapRedis, FilterExpandStrategy, FilterInfoRedis, FiltersInfo, RedisClient,
    SingleKeyFilter,
};
use std::sync::Arc;
use wd_tools::{PFArc, PFErr, PFOk};

pub enum Strategy {
    // 以一个固定的大小扩容
    Fixed(usize),
    // 固定梯度扩容
    Ladder(Vec<usize>),
    // 以某一种函数扩容，入参为chunk下标，从0开始
    Function(Box<dyn Fn(usize) -> usize + Send + Sync + 'static>),
}

impl Default for Strategy {
    fn default() -> Self {
        Strategy::Fixed(128)
    }
}

impl Strategy {
    pub fn chunk_size(&self, index: usize) -> anyhow::Result<usize> {
        return match self {
            Strategy::Fixed(n) => Ok(*n),
            Strategy::Ladder(list) => {
                if let Some(n) = list.get(index) {
                    Ok(*n)
                } else {
                    anyhow::anyhow!(
                        "need[{}], Strategy.Ladder max chunks[{}]",
                        index,
                        list.len()
                    )
                    .err()
                }
            }
            Strategy::Function(function) => function(index).ok(),
        };
    }
}

pub struct BloomExpandStrategy {
    appid: String,
    info: Arc<dyn FiltersInfo + 'static>,
    strategy: Strategy,
    bitmap: Arc<dyn Bitmap + 'static>,
    fp_rate: f64,
    timestamp_size: i64, //单位s
}

impl BloomExpandStrategy {
    pub fn build_from_redis<A: Into<String>, T: TryInto<RedisClient, Error = anyhow::Error>>(
        appid: A,
        t: T,
    ) -> anyhow::Result<Self> {
        let appid = appid.into();
        let client = t.try_into()?;
        let info = Arc::new(Into::<FilterInfoRedis>::into(client.clone()));
        let bitmap = Arc::new(Into::<BitmapRedis>::into(client));
        let strategy = Strategy::Ladder(vec![100, 1000, 5000]);
        let fp_rate = 0.001;
        let timestamp_size = 60 * 60;
        Ok(Self {
            appid,
            info,
            strategy,
            bitmap,
            fp_rate,
            timestamp_size,
        })
    }
    pub fn new<I: FiltersInfo + 'static, B: Bitmap + 'static>(
        appid: String,
        info: I,
        strategy: Strategy,
        bitmap: B,
        fp_rate: f64,
        timestamp_size: i64,
    ) -> Self {
        let info = info.arc();
        let bitmap = bitmap.arc();
        Self {
            appid,
            info,
            strategy,
            bitmap,
            fp_rate,
            timestamp_size,
        }
    }
    pub fn set_app_id(mut self, appid: String) -> Self {
        self.appid = appid;
        self
    }
    pub fn set_filter_info<I: FiltersInfo + 'static>(mut self, info: I) -> Self {
        self.info = info.arc();
        self
    }
    pub fn set_strategy_fixed(mut self, n: usize) -> Self {
        self.strategy = Strategy::Fixed(n);
        self
    }
    pub fn set_strategy_ladder(mut self, table: Vec<usize>) -> Self {
        self.strategy = Strategy::Ladder(table);
        self
    }
    pub fn set_strategy_function(
        mut self,
        function: impl Fn(usize) -> usize + Send + Sync + 'static,
    ) -> Self {
        self.strategy = Strategy::Function(Box::new(function));
        self
    }
    pub fn set_bitmap(mut self, bitmap: impl Bitmap + 'static) -> Self {
        self.bitmap = Arc::new(bitmap);
        self
    }
    pub fn set_fp_rate(mut self, rate: f64) -> Self {
        self.fp_rate = rate;
        self
    }
    pub fn set_timestamp_size(mut self, size: i64) -> Self {
        self.timestamp_size = size;
        self
    }

    fn next_chunk_key(&self, index: usize, group: &str) -> String {
        let ts = wd_tools::time::utc_timestamp();
        let ts = ts - ts % self.timestamp_size;
        format!("{}_{}_{}", group, ts, index)
    }
}

#[async_trait::async_trait]
impl FilterExpandStrategy for BloomExpandStrategy {
    async fn load_filter_group(
        &self,
        group: &str,
    ) -> anyhow::Result<Vec<Arc<dyn SingleKeyFilter>>> {
        let group = assembly_prefix(self.appid.as_str(), group);
        let items = self.info.list(group.as_str()).await?;
        let mut list: Vec<Arc<dyn SingleKeyFilter>> = Vec::with_capacity(items.len());
        // fixme: chunk存才多扩容的情况，i应该根据k拆解得出
        for (i, (k, _)) in items.into_iter().enumerate() {
            let bloom = BasicBloomFilter::new(
                group.clone(),
                k,
                self.info.clone(),
                self.bitmap.clone(),
                self.strategy.chunk_size(i)?,
                self.fp_rate,
            );
            list.push(bloom.arc());
        }
        list.ok()
    }

    async fn expand_chunk(
        &self,
        group: &str,
        index: isize,
    ) -> anyhow::Result<Arc<dyn SingleKeyFilter>> {
        let group = assembly_prefix(self.appid.as_str(), group);
        let current_list = self.info.list(group.as_str()).await?;
        let index = if index <= 0 {
            current_list.len()
        } else {
            index as usize
        };
        let size = self.strategy.chunk_size(index)?;
        let key = self.next_chunk_key(index, group.as_str());
        let bloom = BasicBloomFilter::new(
            group,
            key,
            self.info.clone(),
            self.bitmap.clone(),
            size,
            self.fp_rate,
        );
        let bloom: Arc<dyn SingleKeyFilter> = bloom.arc();
        bloom.ok()
    }
}
