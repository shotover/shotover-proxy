use bytes::Bytes;
use redis_protocol::types::Frame;
use shotover_types::message::Messages;
use shotover_types::protocols::RawFrame;

// TODO: move into helper crate, would also be nicer as a derive proc macro
macro_rules! define_transform {
    ($transform_name:ident) => {
        #[no_mangle]
        pub extern "C" fn run(address: u64, length: u64) -> u64 {
            let mut data = unsafe {
                Vec::from_raw_parts(address as *mut u8, length as usize, length as usize)
            };
            let messages: Messages = bincode::deserialize(&data).unwrap();

            let messages = $transform_name(messages);

            let output_bytes = bincode::serialize::<Result<Messages, String>>(&messages).unwrap();
            data.clear();
            data.extend((output_bytes.len() as u64).to_be_bytes());
            data.extend(&output_bytes);

            let address = data.as_ptr() as u64;
            std::mem::forget(data);
            address
        }

        #[no_mangle]
        pub extern "C" fn allocate(length: u64) -> u64 {
            let data = Vec::<u8>::with_capacity(length as usize);
            let address = data.as_ptr() as u64;
            std::mem::forget(data);
            address
        }

        #[no_mangle]
        pub extern "C" fn deallocate(address: u64) {
            unsafe {
                // TODO: is it safe to just use a dummy length? I dont see why not, but ill need to research it a bit...
                std::mem::drop(Vec::<u8>::from_raw_parts(address as *mut u8, 0, 0));
            }
        }

        #[no_mangle]
        pub extern "C" fn api_version() -> u32 {
            0
        }
    };
}

fn transform(mut messages: Messages) -> Result<Messages, String> {
    let message = &mut messages.messages[0];

    if let RawFrame::Redis(Frame::Array(array)) = &mut message.original {
        if array[0] == "SET" {
            array[2] = Frame::BulkString(Bytes::from("something else"));
        }
    }

    Ok(messages)
}

define_transform!(transform);
