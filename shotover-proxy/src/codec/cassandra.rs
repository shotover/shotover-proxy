use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::ops::DerefMut;

use crate::frame::CassandraFrame;
use anyhow::{anyhow, Result};

use bytes::{BufMut, BytesMut};
use cassandra_protocol::compression::Compression;
use cassandra_protocol::consistency::Consistency;
use cassandra_protocol::frame::frame_error::{AdditionalErrorInfo, ErrorBody};

use cassandra_protocol::frame::frame_result::{
    ColSpec, ColType, ColTypeOption, RowsMetadata, RowsMetadataFlags, TableSpec,
};
use cassandra_protocol::frame::{Frame as RawCassandraFrame, ParseFrameError, Version};
use cassandra_protocol::query::QueryValues;
use cassandra_protocol::types::value::Value as CValue;

use itertools::Itertools;
use sqlparser::ast::Expr::{BinaryOp, Identifier};
use sqlparser::ast::Statement::{Delete, Insert, Update};
use sqlparser::ast::{
    BinaryOperator, Expr, ObjectName, Select, SelectItem, SetExpr, Statement, TableFactor,
    Value as SQLValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{debug, error, info, warn};

use crate::frame::cassandra::{CassandraOperation, CassandraResult, CQL};
use crate::frame::Frame;
use crate::message::{
    ASTHolder, Message, MessageDetails, MessageValue, Messages, QueryMessage, QueryResponse,
    QueryType,
};

#[derive(Debug, Clone)]
pub struct CassandraCodec {
    compressor: Compression,
    pk_col_map: HashMap<String, Vec<String>>,
    bypass: bool,
    /// if force_close is Some then the connection will be closed the next time the
    /// system attempts to read data from it.  This is used in protocol errors where we
    /// need to return a message to the client so we can not immediately close the connection
    /// but we also do not know the state of the input stream.  For example if the protocol
    /// number does not match there may be too much or too little data in the buffer so we need
    /// to discard the connection.  The string is used in the error message.
    force_close: Option<String>,
}

pub(crate) struct ParsedCassandraQueryString {
    namespace: Option<Vec<String>>,
    pub(crate) colmap: Option<HashMap<String, MessageValue>>,
    projection: Option<Vec<String>>,
    primary_key: HashMap<String, MessageValue>,
    pub(crate) ast: Option<Statement>,
}

impl CassandraCodec {
    pub fn new(pk_col_map: HashMap<String, Vec<String>>, bypass: bool) -> CassandraCodec {
        CassandraCodec {
            compressor: Compression::None,
            pk_col_map,
            bypass,
            force_close: None,
        }
    }

    pub fn build_cassandra_query_frame(
        mut query: QueryMessage,
        consistency: Consistency,
    ) -> CassandraFrame {
        CassandraCodec::rebuild_ast_in_message(&mut query);
        CassandraCodec::rebuild_query_string_from_ast(&mut query);
        let QueryMessage { query_values, .. } = query;

        let params = cassandra_protocol::query::QueryParams {
            consistency,
            with_names: false,
            values: Some(QueryValues::SimpleValues(
                query_values
                    .unwrap()
                    .values()
                    .map(|x| CValue::new(x.clone()))
                    .collect(),
            )),
            page_size: None,
            paging_state: None,
            serial_consistency: None,
            timestamp: None,
        };

        CassandraFrame {
            version: Version::V4,
            operation: CassandraOperation::Query {
                query: CQL::Parsed(
                    query
                        .ast
                        .map(|ast| match ast {
                            ASTHolder::SQL(ast) => vec![*ast],
                            _ => unreachable!("Must be cassandra message"),
                        })
                        .unwrap_or_default(),
                ),
                params,
            },
            stream_id: 0,
            tracing_id: None,
            warnings: Vec::new(),
        }
    }

    pub fn build_cassandra_response_frame(
        resp: QueryResponse,
        query_frame: CassandraFrame,
    ) -> CassandraFrame {
        if let Some(MessageValue::Rows(rows)) = resp.result {
            if let Some(ref query) = resp.matching_query {
                if let Some(ref proj) = query.projection {
                    let col_spec = proj
                        .iter()
                        .map(|x| {
                            ColSpec {
                                table_spec: Some(TableSpec {
                                    ks_name: query.namespace.get(0).unwrap().clone(),
                                    table_name: query.namespace.get(1).unwrap().clone(),
                                }),
                                name: x.clone(),
                                col_type: ColTypeOption {
                                    id: ColType::Ascii, // TODO: get types working
                                    value: None,
                                },
                            }
                        })
                        .collect();

                    let count = rows.get(0).unwrap().len() as i32;
                    let metadata = RowsMetadata {
                        flags: RowsMetadataFlags::empty(),
                        columns_count: count,
                        paging_state: None,
                        global_table_spec: None,
                        col_specs: col_spec,
                    };

                    return CassandraFrame {
                        version: Version::V4,
                        operation: CassandraOperation::Result(CassandraResult::Rows {
                            metadata,
                            value: MessageValue::Rows(rows),
                        }),
                        stream_id: query_frame.stream_id,
                        tracing_id: query_frame.tracing_id,
                        warnings: Vec::new(),
                    };
                }
            }
        }
        unreachable!()
    }
}

impl CassandraCodec {
    fn value_to_bind(_v: &MessageValue) -> SQLValue {
        //TODO fix bind handling
        SQLValue::SingleQuotedString("XYz-1-zYX".to_string())
    }

    fn expr_to_string(v: &SQLValue) -> String {
        match v {
            SQLValue::Number(v, false) => v.to_string(),
            SQLValue::SingleQuotedString(v)
            | SQLValue::NationalStringLiteral(v)
            | SQLValue::HexStringLiteral(v) => v.to_string(),
            SQLValue::Boolean(v) => v.to_string(),
            _ => "NULL".to_string(),
        }
    }

    fn rebuild_binops_tree(
        node: &mut Expr,
        map: &mut HashMap<String, MessageValue>,
        use_bind: bool,
    ) {
        if let BinaryOp { left, op, right } = node {
            match op {
                BinaryOperator::And => {
                    CassandraCodec::rebuild_binops_tree(left, map, use_bind);
                    CassandraCodec::rebuild_binops_tree(right, map, use_bind);
                }
                BinaryOperator::Eq => {
                    if let Identifier(i) = left.borrow_mut() {
                        if let Expr::Value(v) = right.borrow_mut() {
                            if let Some((_, new_v)) = map.get_key_value(&i.to_string()) {
                                *v = if use_bind {
                                    CassandraCodec::value_to_bind(new_v)
                                } else {
                                    new_v.into()
                                };
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn binary_ops_to_hashmap(node: &Expr, map: &mut HashMap<String, MessageValue>) {
        if let BinaryOp { left, op, right } = node {
            match op {
                BinaryOperator::And => {
                    CassandraCodec::binary_ops_to_hashmap(left, map);
                    CassandraCodec::binary_ops_to_hashmap(right, map);
                }
                BinaryOperator::Eq => {
                    if let Identifier(i) = left.borrow() {
                        if let Expr::Value(v) = right.borrow() {
                            map.insert(i.to_string(), v.into());
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn get_column_values(expr: &SetExpr) -> Vec<String> {
        let mut cumulator: Vec<String> = Vec::new();
        if let SetExpr::Values(v) = expr {
            for value in &v.0 {
                for ex in value {
                    if let Expr::Value(v) = ex {
                        cumulator.push(CassandraCodec::expr_to_string(v));
                    }
                }
            }
        }
        cumulator
    }

    pub fn rebuild_query_string_from_ast(message: &mut QueryMessage) {
        if let QueryMessage {
            query_string,
            ast: Some(ASTHolder::SQL(ast)),
            ..
        } = message
        {
            *query_string = format!("{ast}");
        }
    }

    pub fn rebuild_ast_in_message(message: &mut QueryMessage) {
        if let QueryMessage {
            namespace,
            query_values: Some(query_values),
            projection: Some(qm_projection),
            ast: Some(ASTHolder::SQL(ast)),
            ..
        } = message
        {
            match &mut **ast {
                Statement::Query(query) => {
                    if let SetExpr::Select(select) = &mut query.body {
                        let Select {
                            projection,
                            from,
                            selection,
                            ..
                        } = select.deref_mut();

                        // Rebuild projection
                        *projection = qm_projection
                            .iter()
                            .map(|x| {
                                SelectItem::UnnamedExpr(Expr::Value(SQLValue::SingleQuotedString(
                                    x.clone(),
                                )))
                            })
                            .collect();

                        // Rebuild namespace
                        if let Some(table_ref) = from.get_mut(0) {
                            if let TableFactor::Table { name, .. } = &mut table_ref.relation {
                                *name = ObjectName(
                                    namespace
                                        .iter()
                                        .cloned()
                                        .map(sqlparser::ast::Ident::new)
                                        .collect(),
                                );
                            }
                        }

                        //Rebuild selection
                        // TODO allow user control of bind
                        if let Some(selection) = selection {
                            CassandraCodec::rebuild_binops_tree(selection, query_values, true);
                        }
                    }
                }
                Statement::Insert { .. } => {}
                Statement::Update { .. } => {}
                Statement::Delete { .. } => {}
                _ => {}
            }
        }
    }

    pub(crate) fn parse_query_string(
        query_string: &str,
        pk_col_map: &HashMap<String, Vec<String>>,
    ) -> ParsedCassandraQueryString {
        let dialect = GenericDialect {}; //TODO write CQL dialect
        let mut namespace: Vec<String> = Vec::new();
        let mut colmap: HashMap<String, MessageValue> = HashMap::new();
        let mut projection: Vec<String> = Vec::new();
        let mut primary_key: HashMap<String, MessageValue> = HashMap::new();
        let mut ast: Option<Statement> = None;
        let parsed_sql = Parser::parse_sql(&dialect, query_string);

        //TODO handle pks
        match parsed_sql {
            Ok(ast_list) => {
                //TODO: We absolutely don't handle multiple statements despite this loop indicating otherwise
                // for statement in ast_list.iter() {
                if let Some(statement) = ast_list.get(0) {
                    ast = Some(statement.clone());
                    match statement {
                        Statement::Query(q) => {
                            if let SetExpr::Select(s) = &q.body {
                                projection = s.projection.iter().map(|s| s.to_string()).collect();
                                if let TableFactor::Table { name, .. } =
                                    &s.from.get(0).unwrap().relation
                                {
                                    namespace = name.0.iter().map(|a| a.value.clone()).collect();
                                }
                                if let Some(sel) = &s.selection {
                                    CassandraCodec::binary_ops_to_hashmap(sel, &mut colmap);
                                }
                                if let Some(pk_col_names) = pk_col_map.get(&namespace.join(".")) {
                                    for pk_component in pk_col_names {
                                        if let Some(value) = colmap.get(pk_component) {
                                            primary_key.insert(pk_component.clone(), value.clone());
                                        } else {
                                            primary_key
                                                .insert(pk_component.clone(), MessageValue::NULL);
                                        }
                                    }
                                }
                            }
                        }
                        Insert {
                            table_name,
                            columns,
                            source,
                            ..
                        } => {
                            namespace = table_name.0.iter().map(|a| a.value.clone()).collect();
                            let values = CassandraCodec::get_column_values(&source.body);
                            for (i, c) in columns.iter().enumerate() {
                                projection.push(c.value.clone());
                                if let Some(v) = values.get(i) {
                                    colmap.insert(c.to_string(), MessageValue::Strings(v.clone()));
                                }
                            }

                            if let Some(pk_col_names) = pk_col_map.get(&namespace.join(".")) {
                                for pk_component in pk_col_names {
                                    if let Some(value) = colmap.get(pk_component) {
                                        primary_key.insert(pk_component.clone(), value.clone());
                                    } else {
                                        primary_key
                                            .insert(pk_component.clone(), MessageValue::NULL);
                                    }
                                }
                            }
                        }
                        Update {
                            table,
                            assignments,
                            selection,
                        } => {
                            match &table.relation {
                            TableFactor::Table { name, .. } => {
                                namespace = name.0.iter().map(|a| a.value.clone()).collect();
                            }
                            _ => error!(
                                "The cassandra query language does not support `update`s with table of {:?}",
                                table
                            ),
                        };
                            for assignment in assignments {
                                if let Expr::Value(v) = assignment.clone().value {
                                    let converted_value = (&v).into();
                                    colmap.insert(
                                        assignment.id.iter().map(|x| &x.value).join("."),
                                        converted_value,
                                    );
                                }
                            }
                            if let Some(s) = selection {
                                CassandraCodec::binary_ops_to_hashmap(s, &mut primary_key);
                            }
                        }
                        Delete {
                            table_name,
                            selection,
                        } => {
                            namespace = table_name.0.iter().map(|a| a.value.clone()).collect();
                            if let Some(s) = selection {
                                CassandraCodec::binary_ops_to_hashmap(s, &mut primary_key);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(err) => {
                // TODO: We should handle this error better but for now we can at least log it
                error!(
                    "Failed to parse cql for message details: {:?}\nError: {:?}",
                    query_string, err
                )
            }
        }

        ParsedCassandraQueryString {
            namespace: Some(namespace),
            colmap: Some(colmap),
            projection: Some(projection),
            primary_key,
            ast,
        }
    }

    fn build_response_message(message: &mut Message, matching_query: Option<QueryMessage>) {
        let mut result: Option<MessageValue> = None;
        let mut error: Option<MessageValue> = None;
        if let Frame::Cassandra(cassandra) = &message.original {
            match &cassandra.operation {
                CassandraOperation::Error(e) => {
                    error = Some(MessageValue::Strings(e.message.clone()))
                }
                CassandraOperation::Result(CassandraResult::Rows { value, metadata: _ }) => {
                    result = Some(value.clone())
                }
                _ => {}
            }
        }

        message.details = MessageDetails::Response(QueryResponse {
            matching_query,
            result,
            error,
            response_meta: None,
        })
    }

    pub fn process_cassandra_frame(&self, message: &mut Message) {
        if self.bypass {
            return;
        }

        if let Frame::Cassandra(cassandra) = &mut message.original {
            match &mut cassandra.operation {
                CassandraOperation::Query { query, .. } => {
                    let query_string = query.to_query_string();
                    let parsed_query =
                        CassandraCodec::parse_query_string(&query_string, &self.pk_col_map);
                    if parsed_query.ast.is_none() {
                        // TODO: Currently this will probably catch schema changes that don't match
                        // what the SQL parser expects
                        return;
                    }
                    message.details = MessageDetails::Query(QueryMessage {
                        query_string,
                        namespace: parsed_query.namespace.unwrap(),
                        primary_key: parsed_query.primary_key,
                        query_values: parsed_query.colmap,
                        projection: parsed_query.projection,
                        query_type: match &parsed_query.ast.as_ref() {
                            Some(Statement::Query(_x)) => QueryType::Read,
                            Some(Statement::Insert { .. }) => QueryType::Write,
                            Some(Statement::Update { .. }) => QueryType::Write,
                            Some(Statement::Delete { .. }) => QueryType::Write,
                            _ => QueryType::Read,
                        },
                        ast: parsed_query.ast.map(|x| ASTHolder::SQL(Box::new(x))),
                    });
                }
                CassandraOperation::Result(_) => {
                    CassandraCodec::build_response_message(message, None)
                }
                CassandraOperation::Error(body) => {
                    message.details = MessageDetails::Response(QueryResponse {
                        result: None,
                        matching_query: None,
                        response_meta: None,
                        error: Some(MessageValue::Strings(body.message.clone())),
                    });
                }
                _ => {}
            }
        }
    }

    fn encode_raw(&mut self, item: CassandraFrame, dst: &mut BytesMut) {
        let buffer = item.encode().encode_with(self.compressor).unwrap();
        if buffer.is_empty() {
            info!("trying to send 0 length frame");
        }
        dst.put(buffer.as_slice());
    }
}

impl Decoder for CassandraCodec {
    type Item = Messages;
    type Error = anyhow::Error;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        // if we need to close the connection return an error.
        if let Some(result) = self.force_close.take() {
            debug!("Closing errored connection: {:?}", &result);
            return Err(anyhow!(result));
        }

        match RawCassandraFrame::from_buffer(src, self.compressor) {
            Ok(parsed_frame) => {
                // Clear the read bytes from the FramedReader
                let bytes = src.split_to(parsed_frame.frame_len);

                let mut message = Message::from_frame(Frame::Cassandra(
                    CassandraFrame::from_bytes(bytes.freeze()).unwrap(),
                ));
                self.process_cassandra_frame(&mut message);
                Ok(Some(vec![message]))
            }
            Err(ParseFrameError::NotEnoughBytes) => Ok(None),
            Err(ParseFrameError::UnsupportedVersion(version)) => {
                // if we got an error force the close on the next read.
                // We can not immediately close as we are gong to queue a message
                // back to the client and we have to allow time for the message
                // to be sent.  We can not reuse the connection as it may/does contain excess
                // data from the failed parse.
                self.force_close = Some(format!(
                    "Received frame with unknown protocol version: {}",
                    version
                ));

                let message = Message {
                    details: MessageDetails::Unknown,
                    modified: false,
                    original: Frame::Cassandra(CassandraFrame {
                        version: Version::V4,
                        stream_id: 0,
                        operation: CassandraOperation::Error(ErrorBody {
                            error_code: 0xA,
                            message: "Invalid or unsupported protocol version".into(),
                            additional_info: AdditionalErrorInfo::Server,
                        }),
                        tracing_id: None,
                        warnings: vec![],
                    }),
                    return_to_sender: true,
                };
                Ok(Some(vec![message]))
            }
            err => Err(anyhow!("Failed to parse frame {:?}", err)),
        }
    }
}

fn get_cassandra_frame(rf: Frame) -> Result<CassandraFrame> {
    if let Frame::Cassandra(frame) = rf {
        Ok(frame)
    } else {
        warn!("Unsupported Frame detected - Dropping Frame {:?}", rf);
        Err(anyhow!("Unsupported frame found, not sending"))
    }
}

impl CassandraCodec {
    fn encode_message(&mut self, item: Message) -> Result<CassandraFrame> {
        let frame = if !item.modified {
            get_cassandra_frame(item.original)?
        } else {
            match item.details {
                MessageDetails::Query(qm) => {
                    CassandraCodec::build_cassandra_query_frame(qm, Consistency::LocalQuorum)
                }
                MessageDetails::Response(qr) => CassandraCodec::build_cassandra_response_frame(
                    qr,
                    get_cassandra_frame(item.original)?,
                ),
                MessageDetails::Unknown => get_cassandra_frame(item.original)?,
            }
        };
        debug!("Encoded message as {:?}", &frame);
        Ok(frame)
    }
}

impl Encoder<Messages> for CassandraCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: Messages,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error> {
        for m in item {
            debug!("Encoding {:?}", &m);
            match self.encode_message(m) {
                Ok(frame) => {
                    self.encode_raw(frame, dst);
                    debug!("Encoded frame as {:?}", dst);
                }
                Err(e) => {
                    warn!("Couldn't encode frame {:?}", e);
                }
            };
        }
        Ok(())
    }
}

#[cfg(test)]
mod cassandra_protocol_tests {
    use crate::codec::cassandra::CassandraCodec;
    use crate::frame::cassandra::{CassandraOperation, CassandraResult, CQL};
    use crate::frame::CassandraFrame;
    use crate::frame::Frame;
    use crate::message::{
        ASTHolder, Message, MessageDetails, MessageValue, QueryMessage, QueryResponse, QueryType,
    };
    use bytes::BytesMut;
    use cassandra_protocol::frame::frame_result::{
        ColSpec, ColType, ColTypeOption, ColTypeOptionValue, RowsMetadata, RowsMetadataFlags,
        TableSpec,
    };
    use cassandra_protocol::frame::Version;
    use cassandra_protocol::query::QueryParams;
    use hex_literal::hex;
    use sqlparser::ast::Expr::BinaryOp;
    use sqlparser::ast::Value::SingleQuotedString;
    use sqlparser::ast::{
        BinaryOperator, Expr, Ident, ObjectName, Query, Select, SelectItem, SetExpr, Statement,
        TableFactor, TableWithJoins, Value as SQLValue, Values,
    };
    use std::collections::HashMap;
    use tokio_util::codec::{Decoder, Encoder};

    fn test_frame_codec_roundtrip(
        codec: &mut CassandraCodec,
        raw_frame: &[u8],
        expected_messages: Vec<Message>,
    ) {
        // test decode
        let decoded_messages = codec
            .decode(&mut BytesMut::from(raw_frame))
            .unwrap()
            .unwrap();
        assert_eq!(decoded_messages, expected_messages);

        // test encode round trip
        let mut dest = BytesMut::new();
        codec.encode(decoded_messages, &mut dest).unwrap();
        assert_eq!(raw_frame, &dest);
    }

    #[test]
    fn test_codec_startup() {
        let mut codec = CassandraCodec::new(HashMap::new(), false);
        let bytes = hex!("0400000001000000160001000b43514c5f56455253494f4e0005332e302e30");
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            operation: CassandraOperation::Startup(vec![
                0, 1, 0, 11, 67, 81, 76, 95, 86, 69, 82, 83, 73, 79, 78, 0, 5, 51, 46, 48, 46, 48,
            ]),
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
        }))];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_options() {
        let mut codec = CassandraCodec::new(HashMap::new(), false);
        let bytes = hex!("040000000500000000");
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            operation: CassandraOperation::Options(vec![]),
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
        }))];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_ready() {
        let mut codec = CassandraCodec::new(HashMap::new(), false);
        let bytes = hex!("840000000200000000");
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            operation: CassandraOperation::Ready(vec![]),
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
        }))];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_register() {
        let mut codec = CassandraCodec::new(HashMap::new(), false);
        let bytes = hex!(
            "040000010b000000310003000f544f504f4c4f47595f4348414e4745
            000d5354415455535f4348414e4745000d534348454d415f4348414e4745"
        );
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            operation: CassandraOperation::Register(vec![
                0, 3, 0, 15, 84, 79, 80, 79, 76, 79, 71, 89, 95, 67, 72, 65, 78, 71, 69, 0, 13, 83,
                84, 65, 84, 85, 83, 95, 67, 72, 65, 78, 71, 69, 0, 13, 83, 67, 72, 69, 77, 65, 95,
                67, 72, 65, 78, 71, 69,
            ]),
            stream_id: 1,
            tracing_id: None,
            warnings: vec![],
        }))];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_result() {
        let mut codec = CassandraCodec::new(HashMap::new(), false);
        let bytes = hex!(
            "040000020800000099000000020000000100000009000673797374656
            d000570656572730004706565720010000b646174615f63656e746572000d0007686f73745f6964000c000c70726566
            65727265645f6970001000047261636b000d000f72656c656173655f76657273696f6e000d000b7270635f616464726
            573730010000e736368656d615f76657273696f6e000c0006746f6b656e730022000d00000000"
        );
        let messages = vec![Message::new(
            MessageDetails::Response(QueryResponse {
                matching_query: None,
                result: Some(MessageValue::Rows(vec![])),
                error: None,
                response_meta: None,
            }),
            false,
            Frame::Cassandra(CassandraFrame {
                version: Version::V4,
                operation: CassandraOperation::Result(CassandraResult::Rows {
                    value: MessageValue::Rows(vec![]),
                    metadata: RowsMetadata {
                        flags: RowsMetadataFlags::GLOBAL_TABLE_SPACE,
                        columns_count: 9,
                        paging_state: None,
                        global_table_spec: Some(TableSpec {
                            ks_name: "system".into(),
                            table_name: "peers".into(),
                        }),
                        col_specs: vec![
                            ColSpec {
                                table_spec: None,
                                name: "peer".into(),
                                col_type: ColTypeOption {
                                    id: ColType::Inet,
                                    value: None,
                                },
                            },
                            ColSpec {
                                table_spec: None,
                                name: "data_center".into(),
                                col_type: ColTypeOption {
                                    id: ColType::Varchar,
                                    value: None,
                                },
                            },
                            ColSpec {
                                table_spec: None,
                                name: "host_id".into(),
                                col_type: ColTypeOption {
                                    id: ColType::Uuid,
                                    value: None,
                                },
                            },
                            ColSpec {
                                table_spec: None,
                                name: "preferred_ip".into(),
                                col_type: ColTypeOption {
                                    id: ColType::Inet,
                                    value: None,
                                },
                            },
                            ColSpec {
                                table_spec: None,
                                name: "rack".into(),
                                col_type: ColTypeOption {
                                    id: ColType::Varchar,
                                    value: None,
                                },
                            },
                            ColSpec {
                                table_spec: None,
                                name: "release_version".into(),
                                col_type: ColTypeOption {
                                    id: ColType::Varchar,
                                    value: None,
                                },
                            },
                            ColSpec {
                                table_spec: None,
                                name: "rpc_address".into(),
                                col_type: ColTypeOption {
                                    id: ColType::Inet,
                                    value: None,
                                },
                            },
                            ColSpec {
                                table_spec: None,
                                name: "schema_version".into(),
                                col_type: ColTypeOption {
                                    id: ColType::Uuid,
                                    value: None,
                                },
                            },
                            ColSpec {
                                table_spec: None,
                                name: "tokens".into(),
                                col_type: ColTypeOption {
                                    id: ColType::Set,
                                    value: Some(ColTypeOptionValue::CSet(Box::new(
                                        ColTypeOption {
                                            id: ColType::Varchar,
                                            value: None,
                                        },
                                    ))),
                                },
                            },
                        ],
                    },
                }),
                stream_id: 2,
                tracing_id: None,
                warnings: vec![],
            }),
        )];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_query_select() {
        let mut codec = CassandraCodec::new(HashMap::new(), false);
        let bytes = hex!(
            "0400000307000000350000002e53454c454354202a2046524f4d20737973
            74656d2e6c6f63616c205748455245206b6579203d20276c6f63616c27000100"
        );
        let messages = vec![Message::new(
            MessageDetails::Query(QueryMessage {
                query_string: "SELECT * FROM system.local WHERE key = 'local'".into(),
                namespace: vec!["system".into(), "local".into()],
                primary_key: HashMap::new(),
                query_values: Some(HashMap::from([(
                    "key".into(),
                    MessageValue::Strings("local".into()),
                )])),
                projection: Some(vec!["*".into()]),
                query_type: QueryType::Read,
                ast: Some(ASTHolder::SQL(Box::new(Statement::Query(Box::new(
                    Query {
                        with: None,
                        body: SetExpr::Select(Box::new(Select {
                            distinct: false,
                            top: None,
                            projection: vec![SelectItem::Wildcard],
                            from: vec![TableWithJoins {
                                relation: TableFactor::Table {
                                    name: ObjectName(vec![
                                        Ident {
                                            value: "system".into(),
                                            quote_style: None,
                                        },
                                        Ident {
                                            value: "local".into(),
                                            quote_style: None,
                                        },
                                    ]),
                                    alias: None,
                                    args: vec![],
                                    with_hints: vec![],
                                },
                                joins: vec![],
                            }],
                            lateral_views: vec![],
                            selection: Some(BinaryOp {
                                left: Box::new(Expr::Identifier(Ident {
                                    value: "key".into(),
                                    quote_style: None,
                                })),
                                op: BinaryOperator::Eq,
                                right: Box::new(Expr::Value(SQLValue::SingleQuotedString(
                                    "local".into(),
                                ))),
                            }),
                            group_by: vec![],
                            cluster_by: vec![],
                            distribute_by: vec![],
                            sort_by: vec![],
                            having: None,
                        })),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                    },
                ))))),
            }),
            false,
            Frame::Cassandra(CassandraFrame {
                version: Version::V4,
                stream_id: 3,
                tracing_id: None,
                warnings: vec![],
                operation: CassandraOperation::Query {
                    query: CQL::Parsed(vec![Statement::Query(Box::new(Query {
                        with: None,
                        body: SetExpr::Select(Box::new(Select {
                            distinct: false,
                            top: None,
                            projection: vec![SelectItem::Wildcard],
                            from: vec![TableWithJoins {
                                relation: TableFactor::Table {
                                    name: ObjectName(vec![
                                        Ident {
                                            value: "system".into(),
                                            quote_style: None,
                                        },
                                        Ident {
                                            value: "local".into(),
                                            quote_style: None,
                                        },
                                    ]),
                                    alias: None,
                                    args: vec![],
                                    with_hints: vec![],
                                },
                                joins: vec![],
                            }],
                            lateral_views: vec![],
                            selection: Some(BinaryOp {
                                left: Box::new(Expr::Identifier(Ident {
                                    value: "key".into(),
                                    quote_style: None,
                                })),
                                op: BinaryOperator::Eq,
                                right: Box::new(Expr::Value(SQLValue::SingleQuotedString(
                                    "local".into(),
                                ))),
                            }),
                            group_by: vec![],
                            cluster_by: vec![],
                            distribute_by: vec![],
                            sort_by: vec![],
                            having: None,
                        })),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                    }))]),
                    params: QueryParams::default(),
                },
            }),
        )];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_codec_query_insert() {
        let mut codec = CassandraCodec::new(HashMap::new(), false);
        let bytes = hex!(
            "0400000307000000330000002c494e5345525420494e544f207379737465
            6d2e666f6f2028626172292056414c554553202827626172322729000100"
        );
        let messages = vec![Message::new(
            MessageDetails::Query(QueryMessage {
                query_string: "INSERT INTO system.foo (bar) VALUES ('bar2')".into(),
                namespace: vec!["system".into(), "foo".into()],
                primary_key: HashMap::new(),
                query_values: Some(HashMap::from([(
                    "bar".into(),
                    MessageValue::Strings("bar2".into()),
                )])),
                projection: Some(vec!["bar".into()]),
                query_type: QueryType::Write,
                ast: Some(ASTHolder::SQL(Box::new(Statement::Insert {
                    or: None,
                    table_name: ObjectName(vec![
                        Ident {
                            value: "system".into(),
                            quote_style: None,
                        },
                        Ident {
                            value: "foo".into(),
                            quote_style: None,
                        },
                    ]),
                    columns: (vec![Ident {
                        value: "bar".into(),
                        quote_style: None,
                    }]),
                    overwrite: false,
                    source: Box::new(Query {
                        with: None,
                        body: (SetExpr::Values(Values(vec![vec![sqlparser::ast::Expr::Value(
                            SingleQuotedString("bar2".to_string()),
                        )]]))),
                        order_by: vec![],
                        limit: None,
                        offset: None,
                        fetch: None,
                    }),
                    partitioned: None,
                    after_columns: (vec![]),
                    table: false,
                    on: None,
                }))),
            }),
            false,
            Frame::Cassandra(CassandraFrame {
                version: Version::V4,
                stream_id: 3,
                tracing_id: None,
                warnings: vec![],
                operation: CassandraOperation::Query {
                    query: CQL::Parsed(vec![Statement::Insert {
                        or: None,
                        table_name: ObjectName(vec![
                            Ident {
                                value: "system".into(),
                                quote_style: None,
                            },
                            Ident {
                                value: "foo".into(),
                                quote_style: None,
                            },
                        ]),
                        columns: (vec![Ident {
                            value: "bar".into(),
                            quote_style: None,
                        }]),
                        overwrite: false,
                        source: Box::new(Query {
                            with: None,
                            body: (SetExpr::Values(Values(vec![vec![
                                sqlparser::ast::Expr::Value(SingleQuotedString("bar2".to_string())),
                            ]]))),
                            order_by: vec![],
                            limit: None,
                            offset: None,
                            fetch: None,
                        }),
                        partitioned: None,
                        after_columns: (vec![]),
                        table: false,
                        on: None,
                    }]),
                    params: QueryParams::default(),
                },
            }),
        )];
        test_frame_codec_roundtrip(&mut codec, &bytes, messages);
    }

    #[test]
    fn test_parse_insert_string() {
        let query_str = "INSERT into tbl(col1,col2,col3) values('one', 2, 3);";
        let hash_map: HashMap<String, Vec<String>> = HashMap::new();
        let query = CassandraCodec::parse_query_string(query_str, &hash_map);
        let colmap = query.colmap.unwrap();
        let namespace = query.namespace.unwrap();
        let projection = query.projection.unwrap();
        assert_eq!(colmap.len(), 3);
        assert_eq!(
            colmap.get("col1").unwrap(),
            &MessageValue::Strings("one".to_string())
        );
        assert_eq!(
            colmap.get("col2").unwrap(),
            &MessageValue::Strings("2".to_string())
        );
        assert_eq!(
            colmap.get("col3").unwrap(),
            &MessageValue::Strings("3".to_string())
        );
        assert_eq!(namespace.len(), 1);
        assert_eq!(namespace[0], "tbl");
        assert_eq!(projection.len(), 3);
        assert_eq!(projection[0], "col1");
        assert_eq!(projection[1], "col2");
        assert_eq!(projection[2], "col3");
    }

    #[test]
    fn test_parse_update_string() {
        let query_str = "UPDATE keyspace.tbl Set col1='one', col2='2'  Where col3=3";
        let hash_map: HashMap<String, Vec<String>> = HashMap::new();
        let query = CassandraCodec::parse_query_string(query_str, &hash_map);
        let colmap = query.colmap.unwrap();
        let namespace = query.namespace.unwrap();
        let projection = query.projection.unwrap();
        assert_eq!(colmap.len(), 2);
        assert_eq!(
            colmap.get("col1").unwrap(),
            &MessageValue::Strings("one".to_string())
        );
        assert_eq!(
            colmap.get("col2").unwrap(),
            &MessageValue::Strings("2".to_string())
        );
        assert_eq!(namespace.len(), 2);
        assert_eq!(namespace[0], "keyspace");
        assert_eq!(namespace[1], "tbl");
        assert_eq!(projection.len(), 0);
    }

    #[test]
    fn test_parse_delete_column_string() {
        let query_str = "DELETE col1 from keyspace.tbl WHERE col3=3";
        let hash_map: HashMap<String, Vec<String>> = HashMap::new();
        let query = CassandraCodec::parse_query_string(query_str, &hash_map);
        assert_eq!(query.ast, None);
    }

    #[test]
    fn test_parse_delete_string() {
        let query_str = "DELETE from keyspace.tbl Where col3=3";
        let hash_map: HashMap<String, Vec<String>> = HashMap::new();
        let query = CassandraCodec::parse_query_string(query_str, &hash_map);
        let colmap = query.colmap.unwrap();
        let namespace = query.namespace.unwrap();
        let projection = query.projection.unwrap();
        let primary_key = query.primary_key;
        assert_eq!(primary_key.len(), 1);
        assert_eq!(
            primary_key.get("col3").unwrap(),
            &MessageValue::Strings("3".to_string())
        );
        assert_eq!(colmap.len(), 0);
        assert_eq!(namespace.len(), 2);
        assert_eq!(namespace[0], "keyspace");
        assert_eq!(namespace[1], "tbl");
        assert_eq!(projection.len(), 0);
    }
}
