use async_trait::async_trait;
use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain};



#[derive(Debug, Clone)]
pub struct CassandraCluster {
    name: &'static str,
}

impl CassandraCluster {
    pub fn new() -> CassandraCluster {
        CassandraCluster{
            name: "Cassandra Cluster",
        }
    }
}

#[async_trait]
impl<'a, 'c> Transform<'a, 'c> for CassandraCluster {
    async fn transform(&self, mut qd: Wrapper, t: & TransformChain<'a,'c>) -> ChainResponse<'c> {
        return ChainResponse::Ok(qd.message.clone());
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}