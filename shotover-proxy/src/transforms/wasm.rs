use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::transforms::{Messages, Transform, Transforms, TransformsFromConfig, Wrapper};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;
use wasmer::{imports, Instance, Module, NativeFunc, Store};

type WasmAddress = u64;

#[derive(Clone)]
pub struct WasmTransform {
    module: Module,
    instance: Instance,
    run: NativeFunc<(WasmAddress, u64), u64>,
    allocate: NativeFunc<u64, WasmAddress>,
    deallocate: NativeFunc<WasmAddress, ()>,
}

impl WasmTransform {
    fn new() -> Result<Self> {
        let wasm_bytes = include_bytes!(
            "../../../wasm-transform/target/wasm32-unknown-unknown/release/wasm_transform.wasm"
        );

        let store = Store::default();

        info!("Compiling module...");
        let module = Module::new(&store, wasm_bytes)?;
        let import_object = imports! {};

        info!("Instantiating module...");
        let instance = Instance::new(&module, &import_object)?;

        let run = instance
            .exports
            .get_function("run")?
            .native::<(WasmAddress, u64), u64>()?;

        let allocate = instance
            .exports
            .get_function("allocate")?
            .native::<u64, u64>()?;

        let deallocate = instance
            .exports
            .get_function("deallocate")?
            .native::<u64, ()>()?;

        Ok(WasmTransform {
            module,
            instance,
            run,
            allocate,
            deallocate,
        })
    }

    unsafe fn write_messages_to_wasm_memory(
        &mut self,
        messages: &Messages,
    ) -> Result<(WasmAddress, u64)> {
        let memory = self.instance.exports.get_memory("memory").unwrap();
        let message_wrapper_bytes = bincode::serialize(messages)?;
        let length = message_wrapper_bytes.len() as u64;
        let address = self.allocate.call(length)?;
        let memory_data = memory.data_unchecked_mut();
        memory_data[address as usize..(address + length) as usize]
            .copy_from_slice(&message_wrapper_bytes);
        Ok((address, length))
    }

    unsafe fn read_messages_from_wasm_memory(
        &self,
        address: WasmAddress,
        length: u64,
    ) -> Result<Messages> {
        info!("a1");
        let memory = self.instance.exports.get_memory("memory").unwrap();
        info!("a2");
        let memory_data = memory.data_unchecked();
        info!("address: {}", address);
        info!("memory_data: {:?}", &memory_data[address as usize..(address + length) as usize]);
        //let messages = bincode::deserialize::<std::result::Result<Messages, String>>(
        let messages = bincode::deserialize::<Messages>(
            &memory_data[address as usize..(address + length) as usize],
        )?;
        info!("a4");
        self.deallocate.call(address)?;
        info!("a5");
        //messages.map_err(|x| anyhow!(x))
        Ok(messages)
    }
}

#[async_trait]
impl Transform for WasmTransform {
    async fn transform<'a>(&'a mut self, mut message_wrapper: Wrapper<'a>) -> ChainResponse {
        // TODO: is scoping the unsafe like this the best idea?
        //  as it is this entire function should be marked unsafe
        //  ... can we avoid unsafe altogether?
        unsafe {
            let (address, send_length) = self.write_messages_to_wasm_memory(&message_wrapper.message)?;

            let response_length = self.run.call(address, send_length)?;
            info!("send_length {:?}", send_length);
            info!("response_length {:?}", response_length);

            info!("before {:?}", message_wrapper.message);
            message_wrapper.message =
                self.read_messages_from_wasm_memory(address, response_length)?;
            info!("after {:?}", message_wrapper.message);
        }

        message_wrapper.call_next_transform().await
    }

    fn get_name(&self) -> &'static str {
        "wasm"
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct WasmTransformConfig {}

#[async_trait]
impl TransformsFromConfig for WasmTransformConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::Wasm(WasmTransform::new()?))
    }
}
