use crate::config::topology::TopicHolder;
use crate::config::ConfigError;
use crate::message::{Message, QueryMessage, QueryResponse};
use crate::runtimes::python::PythonEnvironment;
use crate::transforms::chain::{ChainResponse, RequestError, Transform, TransformChain, Wrapper};
use crate::transforms::{Transforms, TransformsFromConfig};
use async_trait::async_trait;
use core::mem;
use pyo3::prelude::*;
use pyo3::types::IntoPyDict;
use pyo3::PyCell;
use serde::{Deserialize, Serialize};
use slog::Logger;

#[derive(Clone)]
pub struct PythonFilterTransform {
    name: &'static str,
    logger: Logger,
    pub query_filter: Option<String>,
    pub response_filter: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct PythonConfig {
    pub query_filter: Option<String>,
    pub response_filter: Option<String>,
}

#[async_trait]
impl TransformsFromConfig for PythonConfig {
    async fn get_source(
        &self,
        _: &TopicHolder,
        logger: &Logger,
    ) -> Result<Transforms, ConfigError> {
        Ok(Transforms::Python(PythonFilterTransform {
            name: "python",
            logger: logger.clone(),
            query_filter: self.query_filter.clone(),
            response_filter: self.response_filter.clone(),
        }))
    }
}

#[async_trait]
impl Transform for PythonFilterTransform {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        if let Some(query_script) = &self.query_filter {
            if let Message::Query(qm) = &qd.message {
                let gil = Python::acquire_gil();
                let py = gil.python();

                // TODO: these can probably get marshalled without a ref to py
                let query_message_py = PyCell::new(py, qm.clone()).unwrap();
                let locals = [("qm", query_message_py.to_object(py))].into_py_dict(py);

                PythonEnvironment::eval_script(locals, query_script.as_str(), py)?;

                let mod_qm = locals.get_item("qm").unwrap().extract::<QueryMessage>()?;
                let _ = mem::replace(&mut qd.message, Message::Query(mod_qm));
            }
        }
        // Note we do this so we don't hold the GIL during downstream chain execution
        // We could try pyo3 support for rust functions and use process_threaded to release the GIL
        // when the python script calls back into a rest backed  call_next_transform
        // but as its across an await barrier we might deadlock ourselves.
        // Ideally we need to wait until https://github.com/PyO3/pyo3/issues/576
        // Which would depend on https://mail.python.org/archives/list/python-dev@python.org/thread/ZSE2G37E24YYLNMQKOQSBM46F7KLAOZF/
        // As pythons FFI doesn't lend itself to multiple interpreters per process space
        // We could try a multi-process path... but IPC is gross
        // Or we could do Lua instead of python which probably is the best bet
        // Or... if you want speed and no bottlenecks... just implement the transform in Rust
        let mut result = self.call_next_transform(qd, t).await?;
        if let Some(response_script) = &self.response_filter {
            if let Message::Response(rm) = &mut result {
                let gil = Python::acquire_gil();
                let py = gil.python();
                let response_message_py = PyCell::new(py, rm.clone()).unwrap();

                let locals = [("qr", response_message_py.to_object(py))].into_py_dict(py);

                PythonEnvironment::eval_script(locals, response_script.as_str(), py)?;

                let mod_qr = locals.get_item("qr").unwrap().extract::<QueryResponse>()?;

                let _ = mem::replace(&mut rm.error, mod_qr.error);
                let _ = mem::replace(&mut rm.result, mod_qr.result);
            }
        }
        return Ok(result);
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod python_transform_tests {
    use super::PythonConfig;
    use crate::config::topology::TopicHolder;
    use crate::message::{Message, QueryMessage, QueryResponse, QueryType, Value};
    use crate::protocols::cassandra_protocol2::RawFrame;
    use crate::transforms::chain::{ChainResponse, Transform, TransformChain, Wrapper};
    use crate::transforms::null::Null;
    use crate::transforms::printer::Printer;
    use crate::transforms::{Transforms, TransformsFromConfig};
    use async_trait::async_trait;
    use slog::info;
    use sloggers::terminal::{Destination, TerminalLoggerBuilder};
    use sloggers::types::Severity;
    use sloggers::Build;
    use std::error::Error;
    use std::sync::Arc;

    const REQUEST_STRING: &str = r###"
qm.namespace = ["aaaaaaaaaa", "bbbbb"]
"###;

    const RESPONSE_STRING: &str = r###"
qr.result = 42
"###;

    #[tokio::test(threaded_scheduler)]
    async fn test_python_script() -> Result<(), Box<dyn Error>> {
        let t_holder = TopicHolder {
            topics_rx: Default::default(),
            topics_tx: Default::default(),
        };
        let python_t = PythonConfig {
            query_filter: Some(String::from(REQUEST_STRING)),
            response_filter: Some(String::from(RESPONSE_STRING)),
        };

        let wrapper = Wrapper::new(Message::Query(QueryMessage {
            original: RawFrame::NONE,
            query_string: "".to_string(),
            namespace: vec![String::from("keyspace"), String::from("old")],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::Read,
            ast: None,
        }));

        let mut builder = TerminalLoggerBuilder::new();
        builder.level(Severity::Debug);
        builder.destination(Destination::Stderr);

        let logger = builder.build().unwrap();

        let transforms: Vec<Transforms> = vec![
            Transforms::Printer(Printer::new()),
            Transforms::Null(Null::new()),
        ];

        let chain = TransformChain::new(transforms, String::from("test_chain"));

        if let Transforms::Python(mut python) = python_t.get_source(&t_holder, &logger).await? {
            let result = python.transform(wrapper, &chain).await;
            if let Ok(m) = result {
                if let Message::Response(QueryResponse {
                    matching_query: Some(oq),
                    original: _,
                    result: _,
                    error: _,
                }) = &m
                {
                    assert_eq!(oq.namespace.get(0).unwrap(), "aaaaaaaaaa");
                } else {
                    panic!()
                }
                if let Message::Response(QueryResponse {
                    matching_query: _,
                    original: _,
                    result: Some(x),
                    error: _,
                }) = m
                {
                    assert_eq!(x, Value::Integer(42));
                } else {
                    panic!()
                }
                return Ok(());
            }
        } else {
            panic!()
        }
        Ok(())
    }
}
