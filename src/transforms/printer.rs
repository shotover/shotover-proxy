use crate::transforms::chain::{Transform, ChainResponse, Wrapper, TransformChain};

use async_trait::async_trait;


#[derive(Debug, Clone)]
pub struct Printer {
    name: &'static str,
}

impl Printer {
    pub fn new() -> Printer {
        Printer{
            name: "Printer",
        }
    }
}

#[async_trait]
impl<'a, 'c> Transform<'a, 'c> for Printer {
    async fn transform(&self, mut qd: Wrapper, t: & TransformChain<'a,'c>) -> ChainResponse<'c> {
        println!("Message content: {:?}", qd.message);
        return self.call_next_transform(qd, t).await
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}
