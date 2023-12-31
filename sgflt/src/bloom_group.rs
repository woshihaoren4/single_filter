use crate::{error::SgfitErr, FilterExpandStrategy, SingleKeyFilter};
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
            try_max: 3,
        }
    }
    pub async fn init_chunks_list(self) -> Self {
        let _ = self.extend().await;
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
            println!("start contain---> [{}]", i.as_str());
            let mut exist = false;
            for skf in self.list.share().iter() {
                println!("--- {}", skf.code());
                if skf.contain(i.as_str()).await? {
                    exist = true;
                    break;
                }
            }
            println!("<--- end ,result [{}]", exist);
            result.push(exist);
        }
        Ok(result)
    }
    pub async fn insert(&self, keys: Vec<String>) -> anyhow::Result<()> {
        'lp: for mut i in keys.into_iter() {
            for _ in 0..self.try_max {
                i = if let Some(i) = self.insert_or_extend(i).await? {
                    i
                } else {
                    continue 'lp;
                }
            }
            return anyhow::anyhow!("FilterGroup.insert failed,try_max[{}]", self.try_max).err();
        }
        Ok(())
    }
    pub(crate) async fn insert_or_extend(&self, key: String) -> anyhow::Result<Option<String>> {
        let list = self.list.share();
        let skf = match list.last() {
            Some(t) => t,
            None => {
                self.extend().await?;
                return Ok(Some(key));
            }
        };
        if let Err(e) = skf.insert(key.as_str()).await {
            return match e.downcast_ref::<SgfitErr>() {
                Some(_) => Ok(Some(key)),
                None => Err(e),
            };
        }
        Ok(None)
    }
    pub async fn extend(&self) -> anyhow::Result<()> {
        //先更新再扩容
        let current_list = self.list.share();
        let last_list = self.strategy.load_filter_group(self.group.as_str()).await?;
        if !Self::skfs_eq(current_list, &last_list) {
            self.list.update(|_| last_list);
            return Ok(());
        }
        //已经最新 则直接扩容
        let chunk = self.strategy.expand_chunk(self.group.as_str()).await?;
        self.list.update(|x| {
            let mut vec = (&*x).clone();
            vec.push(chunk);
            vec
        });
        Ok(())
    }
    fn skfs_eq(cl: Arc<Vec<Arc<dyn SingleKeyFilter>>>, ll: &Vec<Arc<dyn SingleKeyFilter>>) -> bool {
        let cl_len = cl.len();
        if cl_len != ll.len() {
            return false;
        }
        for i in 0..cl_len {
            if cl[i].code() != ll[i].code() {
                return false;
            }
        }
        true
    }
}
