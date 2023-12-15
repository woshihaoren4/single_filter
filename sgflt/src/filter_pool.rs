use crate::{FilterExpandStrategy, Pool, SingleKeyFilter};

struct FilterPool{
    strategy : Box<dyn FilterExpandStrategy>,
    pool : Box<dyn Pool<Vec<Box<dyn SingleKeyFilter>>>>
}

impl FilterPool{
    pub fn new<S:FilterExpandStrategy,P:Pool<Vec<Box<dyn SingleKeyFilter>>>>(strategy:S,pool:P)->Self{
        let strategy = Box::new(strategy);
        let pool = Box::new(pool);
        Self{strategy,pool}
    }
}

impl FilterPool{
    pub(crate) async fn get_filter(&self,group:String)->anyhow::Result<Vec<Box<dyn SingleKeyFilter>>>{
        if let Some(vec) = self.pool.get(group.as_str()){
            return Ok(vec);
        }
        self.strategy.load_filter_group(group.clone()).await
    }

    pub async fn contain(&self,group:String,key:String)->anyhow::Result<bool>{
        Ok(true)
    }
    pub async fn insert(&self,group:String,key:String)->anyhow::Result<()>{
        Ok(())
    }
}