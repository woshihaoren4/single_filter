use crate::{error::SgfitErr, FilterExpandStrategy, SingleKeyFilter};
use std::collections::{HashMap, HashSet};
use std::ops::Sub;
use std::sync::Arc;
use wd_tools::sync::Acl;
use wd_tools::PFErr;

pub struct FilterGroup {
    group: String,
    strategy: Arc<dyn FilterExpandStrategy>,
    list: Acl<Vec<Arc<dyn SingleKeyFilter>>>,
    try_max: usize,
}

impl FilterGroup {
    pub fn new(group: String, strategy: Arc<dyn FilterExpandStrategy>) -> Self {
        Self {
            group,
            strategy,
            list: Acl::new(vec![]),
            try_max: 5,
        }
    }
    pub async fn init_chunks_list(self) -> Self {
        let _ = self.try_extend().await;
        self
    }
    pub async fn set_try_max(mut self, max: usize) -> Self {
        self.try_max = max;
        self
    }
}

impl Clone for FilterGroup {
    fn clone(&self) -> Self {
        let Self {
            group,
            strategy,
            list,
            try_max,
        } = self;
        Self {
            group: group.clone(),
            strategy: strategy.clone(),
            list: list.clone(),
            try_max: try_max.clone(),
        }
    }
}

impl FilterGroup {
    pub async fn contain(&self, keys: Vec<String>) -> anyhow::Result<Vec<bool>> {
        let mut result = Vec::with_capacity(keys.len());

        for i in keys.into_iter() {
            let mut exist = false;
            for skf in self.list.share().iter() {
                if skf.contain(i.as_str()).await? {
                    exist = true;
                    break;
                }
            }
            result.push(exist);
        }
        Ok(result)
    }
    pub async fn insert(&self, keys: Vec<String>) -> anyhow::Result<()> {
        'lp: for mut i in keys.into_iter() {
            for _ in 0..self.try_max {
                let chunk = self.get_last_chunk().await?;
                if let Err(e) = chunk.insert(i.as_str()).await {
                    if let Some(se) = e.downcast_ref::<SgfitErr>() {
                        if let SgfitErr::ChunkFull(_) = se {
                            self.try_extend().await?;
                        }
                    }
                }
                continue 'lp;
            }
            return anyhow::anyhow!("FilterGroup.insert failed,try_max[{}]", self.try_max).err();
        }
        Ok(())
    }
    pub async fn batch_contain(&self, keys: Vec<String>) -> anyhow::Result<Vec<bool>> {
        let mut result = Vec::with_capacity(keys.len());
        let mut map = HashMap::new();
        for i in keys.into_iter() {
            let mut exist = false;
            for skf in self.list.share().iter() {
                if skf.pre_contain(i.as_str(), &mut map).await? {
                    exist = true;
                    break;
                }
            }
            result.push(exist);
        }
        Ok(result)
    }
    pub async fn batch_insert(&self, keys: Vec<String>) -> anyhow::Result<()> {
        let mut map: HashMap<String, HashSet<usize>> = HashMap::new();
        let mut total: HashMap<String, usize> = HashMap::new();
        let mut growth: HashMap<String, usize> = HashMap::new();
        'lp: for mut i in keys.into_iter() {
            for _ in 0..self.try_max {
                let chunk = self.get_last_chunk().await?;
                let result = chunk.pre_insert(i.as_str(), &mut total, &mut growth).await;
                let index_list = match result {
                    Ok(l) => l,
                    // 如果错误是区块已满，则尝试扩容
                    Err(e) => {
                        if let Some(se) = e.downcast_ref::<SgfitErr>() {
                            if let SgfitErr::ChunkFull(_) = se {
                                self.try_extend().await?;
                                continue;
                            }
                        }
                        return Err(e);
                    }
                };
                let code = chunk.code();
                let mut set = HashSet::new();
                for j in index_list {
                    set.insert(j);
                }
                if let Some(s) = map.get_mut(code.as_str()) {
                    s.extend(&set);
                } else {
                    map.insert(code, set);
                };
                continue 'lp;
            }
            return anyhow::anyhow!("FilterGroup.batch_insert failed,try_max[{}]", self.try_max)
                .err();
        }
        let list = self.list.share();
        for i in list.iter() {
            i.commit_insert(&mut map, &mut growth).await?;
        }
        Ok(())
    }

    async fn get_last_chunk(&self) -> anyhow::Result<Arc<dyn SingleKeyFilter>> {
        let list = self.list.share();
        if list.is_empty() {
            self.try_extend().await?;
            let list = self.list.share();
            let chunk = list.last().unwrap().clone();
            return Ok(chunk);
        }
        let chunk = list.last().unwrap().clone();
        Ok(chunk)
    }

    pub async fn try_extend(&self) -> anyhow::Result<()> {
        //先更新再扩容
        let current_list = self.list.share();
        // if let Some(s) = current_list.last() {
        //     if !s.is_full().await? {
        //         return Ok(())
        //     }
        // }
        //最后一个满了，先尝试更新
        let last_list = self.strategy.load_filter_group(self.group.as_str()).await?;
        if !Self::skfs_eq(&current_list, &last_list) {
            self.list.update(|_| last_list);
            return Ok(());
        }
        //已经最新，则直接扩容
        let chunk = self
            .strategy
            .expand_chunk(self.group.as_str(), current_list.len() as isize)
            .await?;
        self.list.update(|x| {
            let mut vec = (&*x).clone();
            vec.push(chunk);
            vec
        });
        Ok(())
    }
    fn skfs_eq(
        cl: &Arc<Vec<Arc<dyn SingleKeyFilter>>>,
        ll: &Vec<Arc<dyn SingleKeyFilter>>,
    ) -> bool {
        let ll_len = ll.len();
        if ll_len > cl.len() {
            return false;
        }
        for i in 0..ll_len {
            if cl[i].code() != ll[i].code() {
                return false;
            }
        }
        true
    }
}
