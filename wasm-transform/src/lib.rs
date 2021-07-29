use bytes::Bytes;
use redis_protocol::types::Frame;
use shotover_types::message::Messages;
use shotover_types::protocols::RawFrame;

// TODO: move into helper crate, would also be nicer as a derive proc macro
macro_rules! define_transform {
    ($transform_name:ident) => {
        #[no_mangle]
        pub extern "C" fn run(address: u64, length: u64) -> u64 {
            let mut data =
                unsafe { Vec::from_raw_parts(address as *mut u8, length as usize, length as usize) };
            let messages: Messages = bincode::deserialize(&data).unwrap();

            let messages = $transform_name(messages);

            //let messages = bincode::serialize::<Result<Messages, String>>(&messages).unwrap();
            let messages = bincode::serialize::<Messages>(&messages.unwrap()).unwrap();
            data.clear();
            data.extend(&messages); // TODO: Can we serialize directly onto data instead of copying here?

            let length = data.len() as u64;
            std::mem::forget(data);
            length
        }

        #[no_mangle]
        pub extern "C" fn allocate(length: u64) -> u64 {
            // TODO: so turns out this design is completely wrong, using the vec as a vec can cause it to reallocate to some other address.
            // The issue is mentioned here: https://doc.rust-lang.org/std/vec/struct.Vec.html#method.as_ptr-1
            // Maybe I need to:
            // *    allocate huge initial value and hope we dont go above?
            // *    write custom allocation logic...?
            let data = Vec::<u8>::with_capacity(length as usize);
            let address = data.as_ptr() as u64;
            std::mem::forget(data);
            return address;
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
    }
}

fn transform(mut messages: Messages) -> Result<Messages, String> {
    let message = &mut messages.messages[0];

    // if the message isnt a connection check used by the integration tests then mess with it.
    let check_connection = RawFrame::Redis(Frame::Array(vec![
        Frame::BulkString(Bytes::from("GET")),
        Frame::BulkString(Bytes::from("nosdjkghsdjghsdkghj")),
    ]));
    if message.original != check_connection {
        message.original = RawFrame::Redis(Frame::Array(vec![
            Frame::BulkString(Bytes::from("GET")),
            Frame::BulkString(Bytes::from("foo")),
        ]));
    }

    Ok(messages)
}

define_transform!(transform);
