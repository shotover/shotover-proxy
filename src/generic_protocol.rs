use std::collections::HashMap;
use bytes::{BytesMut};


pub struct MessageContainer {
   pub meta_data: Option<HashMap<String, String>>,
   pub body: Option<BytesMut>,
   pub original: Option<BytesMut>
}