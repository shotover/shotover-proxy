use self::token_map::TokenMap;
use crate::frame::{
    cassandra::{CassandraOperation, CassandraResult},
    CassandraFrame, Frame,
};
use crate::message::Message;
use crate::transforms::cassandra::sink_cluster::node::CassandraNode;
use cassandra_protocol::frame::message_execute::BodyReqExecuteOwned;
use cassandra_protocol::frame::message_result::PreparedMetadata;
use cassandra_protocol::frame::Version;
use cassandra_protocol::token::Murmur3Token;
use std::collections::HashMap;
use std::sync::Arc;

mod token_map;

#[derive(Clone)]
pub struct NodePool {
    prepared_metadata: HashMap<Vec<u8>, PreparedMetadata>,
    token_map: TokenMap,
    nodes: Vec<Arc<CassandraNode>>,
    version: Version,
    prev_idx: usize,
}

impl NodePool {
    pub fn new(nodes: Vec<CassandraNode>, version: Version) -> Self {
        let nodes = nodes
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<Arc<CassandraNode>>>();

        Self {
            token_map: TokenMap::new(&nodes),
            nodes,
            prepared_metadata: HashMap::new(),
            version,
            prev_idx: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn set_nodes(&mut self, nodes: Vec<CassandraNode>) {
        let nodes = nodes
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<Arc<CassandraNode>>>();

        self.nodes = nodes;
        self.token_map = TokenMap::new(&self.nodes);
    }

    pub fn add_prepared_result(&mut self, message: &mut Message) {
        if let Some(Frame::Cassandra(CassandraFrame {
            operation: CassandraOperation::Result(CassandraResult::Prepared(prepared)),
            ..
        })) = message.frame()
        {
            self.prepared_metadata.insert(
                prepared.id.clone().into_bytes().unwrap(),
                prepared.metadata.clone(),
            );
        }
    }

    fn get_prepared_metadata(&self, id: &Vec<u8>) -> &PreparedMetadata {
        self.prepared_metadata.get(id).unwrap()
    }

    pub fn nodes(&self) -> &Vec<Arc<CassandraNode>> {
        &self.nodes
    }

    pub fn round_robin_nodes(&mut self) -> Vec<Arc<CassandraNode>> {
        if self.nodes.is_empty() {
            return self.nodes.clone();
        }

        self.prev_idx += 1 % self.nodes.len();
        let cur_idx = self.prev_idx;

        self.nodes.rotate_left(cur_idx);
        self.nodes.clone()
    }

    pub fn execute_message_query_plan(
        &mut self,
        execute: &mut BodyReqExecuteOwned,
    ) -> impl Iterator<Item = Arc<CassandraNode>> + '_ {
        let metadata = self.get_prepared_metadata(&execute.id.clone().into_bytes().unwrap());

        let routing_key = routing_key::calculate(
            &metadata.pk_indexes,
            execute.query_parameters.values.as_ref().unwrap(),
            self.version,
        )
        .unwrap();

        // TODO replica is set to one, need to add keyspace metadata to handle
        // properly
        self.token_map
            .nodes_for_token_capped(Murmur3Token::generate(&routing_key), 1)
    }
}

mod routing_key {
    use cassandra_protocol::frame::{Serialize, Version};
    use cassandra_protocol::query::QueryValues;
    use cassandra_protocol::types::value::Value;
    use cassandra_protocol::types::CIntShort;
    use cassandra_protocol::types::SHORT_LEN;
    use itertools::Itertools;
    use std::io::{Cursor, Write};

    pub fn calculate(
        pk_indexes: &[i16],
        query_values: &QueryValues,
        version: Version,
    ) -> Option<Vec<u8>> {
        let values = match query_values {
            QueryValues::SimpleValues(values) => values,
            _ => panic!("handle named"),
        };

        serialize_routing_key_with_indexes(values, pk_indexes, version)
    }

    fn serialize_routing_key_with_indexes(
        values: &[Value],
        pk_indexes: &[i16],
        version: Version,
    ) -> Option<Vec<u8>> {
        match pk_indexes.len() {
            0 => None,
            1 => values
                .get(pk_indexes[0] as usize)
                .map(|value| value.serialize_to_vec(version)),
            _ => {
                let mut buf = vec![];
                if pk_indexes
                    .iter()
                    .map(|index| values.get(*index as usize))
                    .fold_options(Cursor::new(&mut buf), |mut cursor, value| {
                        serialize_routing_value(&mut cursor, value, version);
                        cursor
                    })
                    .is_some()
                {
                    Some(buf)
                } else {
                    None
                }
            }
        }
    }

    // https://github.com/apache/cassandra/blob/3a950b45c321e051a9744721408760c568c05617/src/java/org/apache/cassandra/db/marshal/CompositeType.java#L39
    fn serialize_routing_value(cursor: &mut Cursor<&mut Vec<u8>>, value: &Value, version: Version) {
        let temp_size: CIntShort = 0;
        temp_size.serialize(cursor, version);

        let before_value_pos = cursor.position();
        value.serialize(cursor, version);

        let after_value_pos = cursor.position();
        cursor.set_position(before_value_pos - SHORT_LEN as u64);

        let value_size: CIntShort = (after_value_pos - before_value_pos) as CIntShort;
        value_size.serialize(cursor, version);

        cursor.set_position(after_value_pos);
        let _ = cursor.write(&[0]);
    }

    fn serialize_routing_key(values: &[Value], version: Version) -> Vec<u8> {
        match values.len() {
            0 => vec![],
            1 => values[0].serialize_to_vec(version),
            _ => {
                let mut buf = vec![];
                let mut cursor = Cursor::new(&mut buf);

                for value in values {
                    serialize_routing_value(&mut cursor, value, version);
                }

                buf
            }
        }
    }
}

#[cfg(test)]
mod test_token_aware_router {
    use super::*;
    use cassandra_protocol::consistency::Consistency::One;
    use cassandra_protocol::frame::message_result::BodyResResultPrepared;
    use cassandra_protocol::frame::message_result::{
        ColSpec, ColType::Int, ColTypeOption, RowsMetadata, RowsMetadataFlags, TableSpec,
    };
    use cassandra_protocol::query::QueryParams;
    use cassandra_protocol::query::QueryValues::SimpleValues;
    use cassandra_protocol::types::value::Value;
    use cassandra_protocol::types::CBytesShort;
    use std::net::SocketAddr;
    use uuid::uuid;

    #[test]
    fn test_router() {
        let mut router = NodePool::new(prepare_nodes(), Version::V4);

        let prepared_metadata = PreparedMetadata {
            pk_indexes: vec![0],
            global_table_spec: Some(TableSpec {
                ks_name: "test_ks".into(),
                table_name: "my_test_table".into(),
            }),
            col_specs: vec![ColSpec {
                table_spec: None,
                name: "key".into(),
                col_type: ColTypeOption {
                    id: Int,
                    value: None,
                },
            }],
        };

        let prepared_result = &mut Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing_id: None,
            warnings: vec![],
            operation: CassandraOperation::Result(CassandraResult::Prepared(Box::new(
                BodyResResultPrepared {
                    id: CBytesShort::new(vec![
                        11, 241, 38, 11, 140, 72, 217, 34, 214, 128, 175, 241, 151, 73, 197, 227,
                    ]),
                    result_metadata_id: None,
                    metadata: prepared_metadata.clone(),
                    result_metadata: RowsMetadata {
                        flags: RowsMetadataFlags::NO_METADATA,
                        columns_count: 0,
                        paging_state: None,
                        new_metadata_id: None,
                        global_table_spec: None,
                        col_specs: vec![],
                    },
                },
            ))),
        }));

        let query_parameters = QueryParams {
            consistency: One,
            with_names: false,
            values: Some(SimpleValues(vec![Value::Some(vec![0, 0, 1, 164])])),
            page_size: None,
            paging_state: None,
            serial_consistency: None,
            timestamp: None,
            keyspace: None,
            now_in_seconds: None,
        };

        let execute_body = &mut BodyReqExecuteOwned {
            id: CBytesShort::new(vec![
                11, 241, 38, 11, 140, 72, 217, 34, 214, 128, 175, 241, 151, 73, 197, 227,
            ]),
            result_metadata_id: None,
            query_parameters: query_parameters.clone(),
        };

        // let execute = &mut Message::from_frame(Frame::Cassandra(CassandraFrame {
        //     version: Version::V4,
        //     stream_id: 0,
        //     tracing_id: None,
        //     warnings: vec![],
        //     operation: CassandraOperation::Execute(Box::new(BodyReqExecuteOwned {
        //         id: CBytesShort::new(vec![
        //             11, 241, 38, 11, 140, 72, 217, 34, 214, 128, 175, 241, 151, 73, 197, 227,
        //         ]),
        //         result_metadata_id: None,
        //         query_parameters: query_parameters.clone(),
        //     })),
        // }));

        router.add_prepared_result(prepared_result);

        let routing_key = routing_key::calculate(
            &prepared_metadata.pk_indexes,
            query_parameters.values.as_ref().unwrap(),
            Version::V4,
        )
        .unwrap();

        assert_eq!(routing_key, vec![0, 0, 0, 4, 0, 0, 1, 164]);

        let nodes: Vec<SocketAddr> = router
            .execute_message_query_plan(execute_body)
            .map(|node| node.address)
            .collect();

        println!("{:?}", nodes);

        let expected_node = "172.16.1.5:9044".parse().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], expected_node);
    }

    fn prepare_nodes() -> Vec<CassandraNode> {
        vec![
            CassandraNode::new(
                "172.16.1.6:9044".parse().unwrap(),
                "rack1".into(),
                vec![
                    Murmur3Token {
                        value: -1004586411270825271,
                    },
                    Murmur3Token {
                        value: -1049898883426448359,
                    },
                    Murmur3Token {
                        value: -1196694092263399271,
                    },
                    Murmur3Token {
                        value: -1316115266562913234,
                    },
                    Murmur3Token {
                        value: -1437503384414675840,
                    },
                    Murmur3Token {
                        value: -1492690172060031739,
                    },
                    Murmur3Token {
                        value: -1622592378891212448,
                    },
                    Murmur3Token {
                        value: -1842743390146252286,
                    },
                    Murmur3Token {
                        value: -1949245434784624084,
                    },
                    Murmur3Token {
                        value: -2002077183627636776,
                    },
                    Murmur3Token {
                        value: -227379397644692018,
                    },
                    Murmur3Token {
                        value: -2314754236554905672,
                    },
                    Murmur3Token {
                        value: -2579172864481376523,
                    },
                    Murmur3Token {
                        value: -2676613702496213986,
                    },
                    Murmur3Token {
                        value: -2815361415294043951,
                    },
                    Murmur3Token {
                        value: -3033676637841035495,
                    },
                    Murmur3Token {
                        value: -3174741696455712456,
                    },
                    Murmur3Token {
                        value: -3335738588135151910,
                    },
                    Murmur3Token {
                        value: -3413411464665450981,
                    },
                    Murmur3Token {
                        value: -3622469396849016710,
                    },
                    Murmur3Token {
                        value: -3778258991876732532,
                    },
                    Murmur3Token {
                        value: -3944423519899748504,
                    },
                    Murmur3Token {
                        value: -408548723798480815,
                    },
                    Murmur3Token {
                        value: -4099887910995471052,
                    },
                    Murmur3Token {
                        value: -4297436737392605445,
                    },
                    Murmur3Token {
                        value: -4457453502975673731,
                    },
                    Murmur3Token {
                        value: -4550467823097342026,
                    },
                    Murmur3Token {
                        value: -4671921701216069762,
                    },
                    Murmur3Token {
                        value: -4813541339653395163,
                    },
                    Murmur3Token {
                        value: -4857790404123314889,
                    },
                    Murmur3Token {
                        value: -518633915583549766,
                    },
                    Murmur3Token {
                        value: -5201977675961991527,
                    },
                    Murmur3Token {
                        value: -5458002809469666000,
                    },
                    Murmur3Token {
                        value: -5690881629999676290,
                    },
                    Murmur3Token {
                        value: -5803788346294443934,
                    },
                    Murmur3Token {
                        value: -585581484826712711,
                    },
                    Murmur3Token {
                        value: -5886024853459351552,
                    },
                    Murmur3Token {
                        value: -6018692686042317033,
                    },
                    Murmur3Token {
                        value: -6103663037451221569,
                    },
                    Murmur3Token {
                        value: -6241894580502106337,
                    },
                    Murmur3Token {
                        value: -6540739067831246048,
                    },
                    Murmur3Token {
                        value: -6665964825812149277,
                    },
                    Murmur3Token {
                        value: -674526913236869510,
                    },
                    Murmur3Token {
                        value: -6745580981824674461,
                    },
                    Murmur3Token {
                        value: -6904602089216567570,
                    },
                    Murmur3Token {
                        value: -7070333325755783848,
                    },
                    Murmur3Token {
                        value: -7265155612194582360,
                    },
                    Murmur3Token {
                        value: -7365816290716823761,
                    },
                    Murmur3Token {
                        value: -742806922306413198,
                    },
                    Murmur3Token {
                        value: -7476482088772037952,
                    },
                    Murmur3Token {
                        value: -7717445208237174971,
                    },
                    Murmur3Token {
                        value: -7826275601464283032,
                    },
                    Murmur3Token {
                        value: -7934793180941469751,
                    },
                    Murmur3Token {
                        value: -8020447783382961940,
                    },
                    Murmur3Token {
                        value: -8109757957276297228,
                    },
                    Murmur3Token {
                        value: -8253402817732093783,
                    },
                    Murmur3Token {
                        value: -8396870723162158406,
                    },
                    Murmur3Token {
                        value: -840385907874903380,
                    },
                    Murmur3Token {
                        value: -8615356091601293133,
                    },
                    Murmur3Token {
                        value: -8758603996461529033,
                    },
                    Murmur3Token {
                        value: -8933228678916646960,
                    },
                    Murmur3Token {
                        value: -9114712287317370781,
                    },
                    Murmur3Token {
                        value: -9188097403558402958,
                    },
                    Murmur3Token {
                        value: 1014225940988749945,
                    },
                    Murmur3Token {
                        value: 1291094681509279487,
                    },
                    Murmur3Token {
                        value: 1362251621862633807,
                    },
                    Murmur3Token {
                        value: 1453653646579813581,
                    },
                    Murmur3Token {
                        value: 151856288122840271,
                    },
                    Murmur3Token {
                        value: 1549482254500710122,
                    },
                    Murmur3Token {
                        value: 1714026322822950195,
                    },
                    Murmur3Token {
                        value: 1820775916415892536,
                    },
                    Murmur3Token {
                        value: 1923075332258322910,
                    },
                    Murmur3Token {
                        value: 2011725076499047162,
                    },
                    Murmur3Token {
                        value: 2151455718569764926,
                    },
                    Murmur3Token {
                        value: 2234063386417334439,
                    },
                    Murmur3Token {
                        value: 2429803642268605649,
                    },
                    Murmur3Token {
                        value: 2554737675108855876,
                    },
                    Murmur3Token {
                        value: 2708646363891939000,
                    },
                    Murmur3Token {
                        value: 2811828826556868389,
                    },
                    Murmur3Token {
                        value: 2956294011478622974,
                    },
                    Murmur3Token {
                        value: 3021028861032813427,
                    },
                    Murmur3Token {
                        value: 3137571630816492926,
                    },
                    Murmur3Token {
                        value: 330138877682856754,
                    },
                    Murmur3Token {
                        value: 3314536781905121741,
                    },
                    Murmur3Token {
                        value: 3410643356619280661,
                    },
                    Murmur3Token {
                        value: 3477731373497932403,
                    },
                    Murmur3Token {
                        value: 3603317769690085561,
                    },
                    Murmur3Token {
                        value: 4061906395596879065,
                    },
                    Murmur3Token {
                        value: 4261358146809905014,
                    },
                    Murmur3Token {
                        value: 4436518590820790964,
                    },
                    Murmur3Token {
                        value: 4556209993460516851,
                    },
                    Murmur3Token {
                        value: 4696477527783455168,
                    },
                    Murmur3Token {
                        value: 4807509623012428073,
                    },
                    Murmur3Token {
                        value: 4953362451465610645,
                    },
                    Murmur3Token {
                        value: 5055040305672540272,
                    },
                    Murmur3Token {
                        value: 5261157149539100646,
                    },
                    Murmur3Token {
                        value: 5330263840739223626,
                    },
                    Murmur3Token {
                        value: 5433052779235448427,
                    },
                    Murmur3Token {
                        value: 5494691595301440557,
                    },
                    Murmur3Token {
                        value: 5589565827656960666,
                    },
                    Murmur3Token {
                        value: 5723167885545727198,
                    },
                    Murmur3Token {
                        value: 602274989584119818,
                    },
                    Murmur3Token {
                        value: 6125674746828035940,
                    },
                    Murmur3Token {
                        value: 6260421621848793431,
                    },
                    Murmur3Token {
                        value: 6359886064138221182,
                    },
                    Murmur3Token {
                        value: 6499165623492705162,
                    },
                    Murmur3Token {
                        value: 6622932721156170867,
                    },
                    Murmur3Token {
                        value: 6753494579497117766,
                    },
                    Murmur3Token {
                        value: 6900507243567459996,
                    },
                    Murmur3Token {
                        value: 7026413613674043843,
                    },
                    Murmur3Token {
                        value: 7110175306909994417,
                    },
                    Murmur3Token {
                        value: 7234495386399743394,
                    },
                    Murmur3Token {
                        value: 7316133344878507011,
                    },
                    Murmur3Token {
                        value: 7431712047871810262,
                    },
                    Murmur3Token {
                        value: 7507859958060009729,
                    },
                    Murmur3Token {
                        value: 7615087462451601131,
                    },
                    Murmur3Token {
                        value: 7704806383419319520,
                    },
                    Murmur3Token {
                        value: 7838445446312906561,
                    },
                    Murmur3Token {
                        value: 7964672129524998277,
                    },
                    Murmur3Token {
                        value: 8139314725125917696,
                    },
                    Murmur3Token {
                        value: 8269550773979367715,
                    },
                    Murmur3Token {
                        value: 8384657922510232219,
                    },
                    Murmur3Token {
                        value: 8477619812599149096,
                    },
                    Murmur3Token {
                        value: 867261695441559513,
                    },
                    Murmur3Token {
                        value: 8759746538699238025,
                    },
                    Murmur3Token {
                        value: 8860942362594907195,
                    },
                    Murmur3Token {
                        value: 8954294001257725257,
                    },
                    Murmur3Token {
                        value: 9088232766780681447,
                    },
                ],
                uuid!("2dd022d6-2937-4754-89d6-02d2933a8f7a"),
            ),
            CassandraNode::new(
                "172.16.1.3:9044".parse().unwrap(),
                "rack1".into(),
                vec![
                    Murmur3Token {
                        value: -1053840800087214217,
                    },
                    Murmur3Token {
                        value: -113465043049150138,
                    },
                    Murmur3Token {
                        value: -1192694728300196761,
                    },
                    Murmur3Token {
                        value: -1383915210799419311,
                    },
                    Murmur3Token {
                        value: -1599723580136171393,
                    },
                    Murmur3Token {
                        value: -1709073710659861629,
                    },
                    Murmur3Token {
                        value: -1867632273751756159,
                    },
                    Murmur3Token {
                        value: -2000557267020498405,
                    },
                    Murmur3Token {
                        value: -2181044726180976979,
                    },
                    Murmur3Token {
                        value: -228123215269593212,
                    },
                    Murmur3Token {
                        value: -2354360481718475006,
                    },
                    Murmur3Token {
                        value: -2463237918860868428,
                    },
                    Murmur3Token {
                        value: -2627435216480011285,
                    },
                    Murmur3Token {
                        value: -2788726990670881156,
                    },
                    Murmur3Token {
                        value: -2899583156132622458,
                    },
                    Murmur3Token {
                        value: -3050794579961607627,
                    },
                    Murmur3Token {
                        value: -3159088493650116607,
                    },
                    Murmur3Token {
                        value: -3314033102918263478,
                    },
                    Murmur3Token {
                        value: -3488295023962070470,
                    },
                    Murmur3Token {
                        value: -3577163472257010827,
                    },
                    Murmur3Token {
                        value: -3706808023927612942,
                    },
                    Murmur3Token {
                        value: -3851741071355113184,
                    },
                    Murmur3Token {
                        value: -4035671949971870580,
                    },
                    Murmur3Token {
                        value: -416550967720883813,
                    },
                    Murmur3Token {
                        value: -4187428123401825775,
                    },
                    Murmur3Token {
                        value: -4301376626404604840,
                    },
                    Murmur3Token {
                        value: -4442723716638506583,
                    },
                    Murmur3Token {
                        value: -4605833295432776917,
                    },
                    Murmur3Token {
                        value: -4701320344529834974,
                    },
                    Murmur3Token {
                        value: -4857875462315846960,
                    },
                    Murmur3Token {
                        value: -4968396811158607169,
                    },
                    Murmur3Token {
                        value: -5105171016072900485,
                    },
                    Murmur3Token {
                        value: -5276324822452280023,
                    },
                    Murmur3Token {
                        value: -5379808796011357809,
                    },
                    Murmur3Token {
                        value: -5518479597864086123,
                    },
                    Murmur3Token {
                        value: -5664665272916336016,
                    },
                    Murmur3Token {
                        value: -5875370556094187934,
                    },
                    Murmur3Token {
                        value: -600276721187883934,
                    },
                    Murmur3Token {
                        value: -6067814960099121242,
                    },
                    Murmur3Token {
                        value: -6177621229906985531,
                    },
                    Murmur3Token {
                        value: -6336151530476436917,
                    },
                    Murmur3Token {
                        value: -6500658304314040954,
                    },
                    Murmur3Token {
                        value: -6609532915982162376,
                    },
                    Murmur3Token {
                        value: -6751140884921760181,
                    },
                    Murmur3Token {
                        value: -6861488313931508240,
                    },
                    Murmur3Token {
                        value: -699545308118806641,
                    },
                    Murmur3Token {
                        value: -7020311606194373731,
                    },
                    Murmur3Token {
                        value: -7148313116670467644,
                    },
                    Murmur3Token {
                        value: -7354002935218863933,
                    },
                    Murmur3Token {
                        value: -7523883294755272500,
                    },
                    Murmur3Token {
                        value: -7615864019601751660,
                    },
                    Murmur3Token {
                        value: -7765777213898929576,
                    },
                    Murmur3Token {
                        value: -7871206540013662013,
                    },
                    Murmur3Token {
                        value: -8017979149492526914,
                    },
                    Murmur3Token {
                        value: -8184891356549636767,
                    },
                    Murmur3Token {
                        value: -8287126509896968944,
                    },
                    Murmur3Token {
                        value: -839451763088075952,
                    },
                    Murmur3Token {
                        value: -8425503551558896746,
                    },
                    Murmur3Token {
                        value: -8519299456614179635,
                    },
                    Murmur3Token {
                        value: -8662573091515041846,
                    },
                    Murmur3Token {
                        value: -8797857477406528477,
                    },
                    Murmur3Token {
                        value: -8884291408287696420,
                    },
                    Murmur3Token {
                        value: -9025236831938193992,
                    },
                    Murmur3Token {
                        value: -9172932911419424640,
                    },
                    Murmur3Token {
                        value: 1051709283467724235,
                    },
                    Murmur3Token {
                        value: 116460704141993585,
                    },
                    Murmur3Token {
                        value: 1173453584793014824,
                    },
                    Murmur3Token {
                        value: 1371954850226264953,
                    },
                    Murmur3Token {
                        value: 1520513619424466396,
                    },
                    Murmur3Token {
                        value: 1652470684854880950,
                    },
                    Murmur3Token {
                        value: 1741812923122407530,
                    },
                    Murmur3Token {
                        value: 1937242203799017892,
                    },
                    Murmur3Token {
                        value: 2081115615364968951,
                    },
                    Murmur3Token {
                        value: 2214344469938654232,
                    },
                    Murmur3Token {
                        value: 2311111862202881614,
                    },
                    Murmur3Token {
                        value: 2478161036293613652,
                    },
                    Murmur3Token {
                        value: 25218878334956615,
                    },
                    Murmur3Token {
                        value: 2651997239834458594,
                    },
                    Murmur3Token {
                        value: 275589606998067451,
                    },
                    Murmur3Token {
                        value: 2792476880490058121,
                    },
                    Murmur3Token {
                        value: 3000171666264096278,
                    },
                    Murmur3Token {
                        value: 3166722307972190835,
                    },
                    Murmur3Token {
                        value: 3302331031103734700,
                    },
                    Murmur3Token {
                        value: 3399158422156730643,
                    },
                    Murmur3Token {
                        value: 3525236240730278239,
                    },
                    Murmur3Token {
                        value: 3617973573365047654,
                    },
                    Murmur3Token {
                        value: 3770622333006172197,
                    },
                    Murmur3Token {
                        value: 379229978206743660,
                    },
                    Murmur3Token {
                        value: 3914637031006869247,
                    },
                    Murmur3Token {
                        value: 4023094757908668455,
                    },
                    Murmur3Token {
                        value: 4174267149464790848,
                    },
                    Murmur3Token {
                        value: 4342666723916439141,
                    },
                    Murmur3Token {
                        value: 4467862479328320177,
                    },
                    Murmur3Token {
                        value: 4622598968789239455,
                    },
                    Murmur3Token {
                        value: 4722075676820914325,
                    },
                    Murmur3Token {
                        value: 4882487399060902202,
                    },
                    Murmur3Token {
                        value: 5097669687089380467,
                    },
                    Murmur3Token {
                        value: 5268009455678844139,
                    },
                    Murmur3Token {
                        value: 536930849294303746,
                    },
                    Murmur3Token {
                        value: 5441920355535131132,
                    },
                    Murmur3Token {
                        value: 5549256633829759325,
                    },
                    Murmur3Token {
                        value: 5688459239131198474,
                    },
                    Murmur3Token {
                        value: 5778088717624702437,
                    },
                    Murmur3Token {
                        value: 5914988492994124019,
                    },
                    Murmur3Token {
                        value: 6082694271483604604,
                    },
                    Murmur3Token {
                        value: 6195185766205709453,
                    },
                    Murmur3Token {
                        value: 6367581281277050779,
                    },
                    Murmur3Token {
                        value: 6472824644448520711,
                    },
                    Murmur3Token {
                        value: 6645298534064697614,
                    },
                    Murmur3Token {
                        value: 667619129873935890,
                    },
                    Murmur3Token {
                        value: 6796615980938727123,
                    },
                    Murmur3Token {
                        value: 6898196905074478273,
                    },
                    Murmur3Token {
                        value: 7055349609290082725,
                    },
                    Murmur3Token {
                        value: 7209282232508943257,
                    },
                    Murmur3Token {
                        value: 7309630571838566742,
                    },
                    Murmur3Token {
                        value: 7465044753600348680,
                    },
                    Murmur3Token {
                        value: 7577464396272549888,
                    },
                    Murmur3Token {
                        value: 758026795446087520,
                    },
                    Murmur3Token {
                        value: 7736595644756909873,
                    },
                    Murmur3Token {
                        value: 7922338238450936605,
                    },
                    Murmur3Token {
                        value: 8050002183798519606,
                    },
                    Murmur3Token {
                        value: 8249101186961752651,
                    },
                    Murmur3Token {
                        value: 8433851423504341367,
                    },
                    Murmur3Token {
                        value: 8580972292248071631,
                    },
                    Murmur3Token {
                        value: 8776976889215716977,
                    },
                    Murmur3Token {
                        value: 8898872692329954212,
                    },
                    Murmur3Token {
                        value: 899178602178914449,
                    },
                    Murmur3Token {
                        value: 9090684226636752640,
                    },
                ],
                uuid!("2dd022d6-2937-4754-89d6-02d2933a8f7b"),
            ),
            CassandraNode::new(
                "172.16.1.2:9044".parse().unwrap(),
                "rack1".into(),
                vec![
                    Murmur3Token {
                        value: -1045956966765682500,
                    },
                    Murmur3Token {
                        value: -119299301725162633,
                    },
                    Murmur3Token {
                        value: -1200693456226601780,
                    },
                    Murmur3Token {
                        value: -1325889211638482815,
                    },
                    Murmur3Token {
                        value: -1494288786090131109,
                    },
                    Murmur3Token {
                        value: -1645461177646253502,
                    },
                    Murmur3Token {
                        value: -1753918904548052709,
                    },
                    Murmur3Token {
                        value: -1897933602548749762,
                    },
                    Murmur3Token {
                        value: -2050582362189874303,
                    },
                    Murmur3Token {
                        value: -2143319694824643718,
                    },
                    Murmur3Token {
                        value: -226635580019790824,
                    },
                    Murmur3Token {
                        value: -2269397513398191315,
                    },
                    Murmur3Token {
                        value: -2366224904451187257,
                    },
                    Murmur3Token {
                        value: -2501833627582731122,
                    },
                    Murmur3Token {
                        value: -2668384269290825678,
                    },
                    Murmur3Token {
                        value: -2876079055064863837,
                    },
                    Murmur3Token {
                        value: -3016558695720463362,
                    },
                    Murmur3Token {
                        value: -3190394899261308305,
                    },
                    Murmur3Token {
                        value: -3357444073352040342,
                    },
                    Murmur3Token {
                        value: -3454211465616267723,
                    },
                    Murmur3Token {
                        value: -3587440320189953006,
                    },
                    Murmur3Token {
                        value: -3731313731755904063,
                    },
                    Murmur3Token {
                        value: -3926743012432514426,
                    },
                    Murmur3Token {
                        value: -400546479876077817,
                    },
                    Murmur3Token {
                        value: -4016085250700041005,
                    },
                    Murmur3Token {
                        value: -4148042316130455560,
                    },
                    Murmur3Token {
                        value: -4296601085328657003,
                    },
                    Murmur3Token {
                        value: -4495102350761907134,
                    },
                    Murmur3Token {
                        value: -4616846652087197721,
                    },
                    Murmur3Token {
                        value: -4769377333376007508,
                    },
                    Murmur3Token {
                        value: -4910529140108834436,
                    },
                    Murmur3Token {
                        value: -5000936805680986065,
                    },
                    Murmur3Token {
                        value: -5131625086260618211,
                    },
                    Murmur3Token {
                        value: -5289325957348178296,
                    },
                    Murmur3Token {
                        value: -5392966328556854505,
                    },
                    Murmur3Token {
                        value: -5552095231412928372,
                    },
                    Murmur3Token {
                        value: -5643337057219965341,
                    },
                    Murmur3Token {
                        value: -570886248465541488,
                    },
                    Murmur3Token {
                        value: -5782020978604072096,
                    },
                    Murmur3Token {
                        value: -5896679150824515170,
                    },
                    Murmur3Token {
                        value: -6085106903275805772,
                    },
                    Murmur3Token {
                        value: -6268832656742805894,
                    },
                    Murmur3Token {
                        value: -6368101243673728601,
                    },
                    Murmur3Token {
                        value: -6508007698642997912,
                    },
                    Murmur3Token {
                        value: -6722396735642136177,
                    },
                    Murmur3Token {
                        value: -6861250663855118721,
                    },
                    Murmur3Token {
                        value: -7052471146354341271,
                    },
                    Murmur3Token {
                        value: -7268279515691093353,
                    },
                    Murmur3Token {
                        value: -7377629646214783589,
                    },
                    Murmur3Token {
                        value: -7536188209306678119,
                    },
                    Murmur3Token {
                        value: -7669113202575420365,
                    },
                    Murmur3Token {
                        value: -7849600661735898939,
                    },
                    Murmur3Token {
                        value: -786068536494019755,
                    },
                    Murmur3Token {
                        value: -8022916417273396966,
                    },
                    Murmur3Token {
                        value: -8131793854415790388,
                    },
                    Murmur3Token {
                        value: -8295991152034933245,
                    },
                    Murmur3Token {
                        value: -8457282926225803116,
                    },
                    Murmur3Token {
                        value: -8568139091687544419,
                    },
                    Murmur3Token {
                        value: -8719350515516529589,
                    },
                    Murmur3Token {
                        value: -8827644429205038570,
                    },
                    Murmur3Token {
                        value: -8982589038473185441,
                    },
                    Murmur3Token {
                        value: -9156850959516992433,
                    },
                    Murmur3Token {
                        value: -946480258734007631,
                    },
                    Murmur3Token {
                        value: 109532782069780479,
                    },
                    Murmur3Token {
                        value: 1128060045383805164,
                    },
                    Murmur3Token {
                        value: 1229640969519556314,
                    },
                    Murmur3Token {
                        value: 1386793673735160766,
                    },
                    Murmur3Token {
                        value: 1540726296954021298,
                    },
                    Murmur3Token {
                        value: 1641074636283644783,
                    },
                    Murmur3Token {
                        value: 1796488818045426721,
                    },
                    Murmur3Token {
                        value: 1908908460717627929,
                    },
                    Murmur3Token {
                        value: 19903303576276516,
                    },
                    Murmur3Token {
                        value: 2068039709201987914,
                    },
                    Murmur3Token {
                        value: 2253782302896014646,
                    },
                    Murmur3Token {
                        value: 2381446248243597647,
                    },
                    Murmur3Token {
                        value: 246432557439202061,
                    },
                    Murmur3Token {
                        value: 2580545251406830692,
                    },
                    Murmur3Token {
                        value: 2765295487949419407,
                    },
                    Murmur3Token {
                        value: 2912416356693149671,
                    },
                    Murmur3Token {
                        value: 3108420953660795017,
                    },
                    Murmur3Token {
                        value: 3230316756775032252,
                    },
                    Murmur3Token {
                        value: 3422128291081830680,
                    },
                    Murmur3Token {
                        value: 3605255226735205015,
                    },
                    Murmur3Token {
                        value: 3752951306216435663,
                    },
                    Murmur3Token {
                        value: 3893896729866933233,
                    },
                    Murmur3Token {
                        value: 3980330660748101177,
                    },
                    Murmur3Token {
                        value: 4115615046639587808,
                    },
                    Murmur3Token {
                        value: 414138335928682646,
                    },
                    Murmur3Token {
                        value: 4258888681540450019,
                    },
                    Murmur3Token {
                        value: 4352684586595732909,
                    },
                    Murmur3Token {
                        value: 4491061628257660709,
                    },
                    Murmur3Token {
                        value: 4593296781604992886,
                    },
                    Murmur3Token {
                        value: 4760208988662102739,
                    },
                    Murmur3Token {
                        value: 4906981598140967639,
                    },
                    Murmur3Token {
                        value: 5012410924255700078,
                    },
                    Murmur3Token {
                        value: 5162324118552877993,
                    },
                    Murmur3Token {
                        value: 5254304843399357154,
                    },
                    Murmur3Token {
                        value: 526629830650787495,
                    },
                    Murmur3Token {
                        value: 5424185202935765722,
                    },
                    Murmur3Token {
                        value: 5629875021484162008,
                    },
                    Murmur3Token {
                        value: 5757876531960255922,
                    },
                    Murmur3Token {
                        value: 5916699824223121412,
                    },
                    Murmur3Token {
                        value: 6027047253232869472,
                    },
                    Murmur3Token {
                        value: 6168655222172467276,
                    },
                    Murmur3Token {
                        value: 6277529833840588699,
                    },
                    Murmur3Token {
                        value: 6442036607678192737,
                    },
                    Murmur3Token {
                        value: 6600566908247644121,
                    },
                    Murmur3Token {
                        value: 6710373178055508410,
                    },
                    Murmur3Token {
                        value: 6902817582060441719,
                    },
                    Murmur3Token {
                        value: 699025345722128821,
                    },
                    Murmur3Token {
                        value: 7113522865238293637,
                    },
                    Murmur3Token {
                        value: 7259708540290543532,
                    },
                    Murmur3Token {
                        value: 7398379342143271844,
                    },
                    Murmur3Token {
                        value: 7501863315702349629,
                    },
                    Murmur3Token {
                        value: 7673017122081729168,
                    },
                    Murmur3Token {
                        value: 7809791326996022484,
                    },
                    Murmur3Token {
                        value: 7920312675838782694,
                    },
                    Murmur3Token {
                        value: 804268708893598752,
                    },
                    Murmur3Token {
                        value: 8076867793624794678,
                    },
                    Murmur3Token {
                        value: 8172354842721852736,
                    },
                    Murmur3Token {
                        value: 8335464421516123071,
                    },
                    Murmur3Token {
                        value: 8476811511750024812,
                    },
                    Murmur3Token {
                        value: 8590760014752803878,
                    },
                    Murmur3Token {
                        value: 8742516188182759073,
                    },
                    Murmur3Token {
                        value: 8926447066799516469,
                    },
                    Murmur3Token {
                        value: 9071380114227016712,
                    },
                    Murmur3Token {
                        value: 9201024665897618824,
                    },
                    Murmur3Token {
                        value: 976742598509775655,
                    },
                ],
                uuid!("2dd022d6-2937-4754-89d6-02d2933a8f7c"),
            ),
            CassandraNode::new(
                "172.16.1.4:9044".parse().unwrap(),
                "rack1".into(),
                vec![
                    Murmur3Token {
                        value: -1159220452743613390,
                    },
                    Murmur3Token {
                        value: -1306341321487343652,
                    },
                    Murmur3Token {
                        value: -1491091558029932368,
                    },
                    Murmur3Token {
                        value: -1690190561193165412,
                    },
                    Murmur3Token {
                        value: -177740079569829826,
                    },
                    Murmur3Token {
                        value: -1817854506540748413,
                    },
                    Murmur3Token {
                        value: -2003597100234775146,
                    },
                    Murmur3Token {
                        value: -2162728348719135130,
                    },
                    Murmur3Token {
                        value: -2275147991391336337,
                    },
                    Murmur3Token {
                        value: -2430562173153118276,
                    },
                    Murmur3Token {
                        value: -2530910512482741760,
                    },
                    Murmur3Token {
                        value: -2684843135701602293,
                    },
                    Murmur3Token {
                        value: -2841995839917206745,
                    },
                    Murmur3Token {
                        value: -2943576764052957895,
                    },
                    Murmur3Token {
                        value: -3094894210926987406,
                    },
                    Murmur3Token {
                        value: -318685503220327397,
                    },
                    Murmur3Token {
                        value: -3267368100543164308,
                    },
                    Murmur3Token {
                        value: -3372611463714634239,
                    },
                    Murmur3Token {
                        value: -3545006978785975566,
                    },
                    Murmur3Token {
                        value: -3657498473508080414,
                    },
                    Murmur3Token {
                        value: -3825204251997561000,
                    },
                    Murmur3Token {
                        value: -3962104027366982581,
                    },
                    Murmur3Token {
                        value: -4051733505860486543,
                    },
                    Murmur3Token {
                        value: -4190936111161925695,
                    },
                    Murmur3Token {
                        value: -4298272389456553886,
                    },
                    Murmur3Token {
                        value: -4472183289312840879,
                    },
                    Murmur3Token {
                        value: -4642523057902304550,
                    },
                    Murmur3Token {
                        value: -466381582701558043,
                    },
                    Murmur3Token {
                        value: -4857705345930782817,
                    },
                    Murmur3Token {
                        value: -5018117068170770693,
                    },
                    Murmur3Token {
                        value: -5117593776202445562,
                    },
                    Murmur3Token {
                        value: -5272330265663364842,
                    },
                    Murmur3Token {
                        value: -5397526021075245877,
                    },
                    Murmur3Token {
                        value: -5565925595526894171,
                    },
                    Murmur3Token {
                        value: -5717097987083016564,
                    },
                    Murmur3Token {
                        value: -5825555713984815771,
                    },
                    Murmur3Token {
                        value: -5969570411985512824,
                    },
                    Murmur3Token {
                        value: -6122219171626637365,
                    },
                    Murmur3Token {
                        value: -6214956504261406780,
                    },
                    Murmur3Token {
                        value: -6341034322834954377,
                    },
                    Murmur3Token {
                        value: -6437861713887950319,
                    },
                    Murmur3Token {
                        value: -649508518354932379,
                    },
                    Murmur3Token {
                        value: -6573470437019494184,
                    },
                    Murmur3Token {
                        value: -6740021078727588740,
                    },
                    Murmur3Token {
                        value: -6947715864501626899,
                    },
                    Murmur3Token {
                        value: -7088195505157226424,
                    },
                    Murmur3Token {
                        value: -7262031708698071367,
                    },
                    Murmur3Token {
                        value: -7429080882788803404,
                    },
                    Murmur3Token {
                        value: -7525848275053030785,
                    },
                    Murmur3Token {
                        value: -7659077129626716068,
                    },
                    Murmur3Token {
                        value: -7802950541192667125,
                    },
                    Murmur3Token {
                        value: -7998379821869277488,
                    },
                    Murmur3Token {
                        value: -8087722060136804067,
                    },
                    Murmur3Token {
                        value: -8219679125567218622,
                    },
                    Murmur3Token {
                        value: -8368237894765420065,
                    },
                    Murmur3Token {
                        value: -841320052661730807,
                    },
                    Murmur3Token {
                        value: -8566739160198670197,
                    },
                    Murmur3Token {
                        value: -8688483461523960784,
                    },
                    Murmur3Token {
                        value: -8841014142812770571,
                    },
                    Murmur3Token {
                        value: -8982165949545597500,
                    },
                    Murmur3Token {
                        value: -9072573615117749129,
                    },
                    Murmur3Token {
                        value: -91306148688661883,
                    },
                    Murmur3Token {
                        value: -9203261895697381275,
                    },
                    Murmur3Token {
                        value: -963215855775968042,
                    },
                    Murmur3Token {
                        value: 1090687309116114932,
                    },
                    Murmur3Token {
                        value: 1182668033962594093,
                    },
                    Murmur3Token {
                        value: 1352548393499002661,
                    },
                    Murmur3Token {
                        value: 1558238212047398947,
                    },
                    Murmur3Token {
                        value: 1686239722523492861,
                    },
                    Murmur3Token {
                        value: 1845063014786358351,
                    },
                    Murmur3Token {
                        value: 187251872103686958,
                    },
                    Murmur3Token {
                        value: 1955410443796106411,
                    },
                    Murmur3Token {
                        value: 2097018412735704215,
                    },
                    Murmur3Token {
                        value: 2205893024403825638,
                    },
                    Murmur3Token {
                        value: 2370399798241429676,
                    },
                    Murmur3Token {
                        value: 2528930098810881060,
                    },
                    Murmur3Token {
                        value: 2638736368618745349,
                    },
                    Murmur3Token {
                        value: 281047777158969848,
                    },
                    Murmur3Token {
                        value: 2831180772623678658,
                    },
                    Murmur3Token {
                        value: 3041886055801530576,
                    },
                    Murmur3Token {
                        value: 3188071730853780471,
                    },
                    Murmur3Token {
                        value: 3326742532706508783,
                    },
                    Murmur3Token {
                        value: 3430226506265586568,
                    },
                    Murmur3Token {
                        value: 3601380312644966107,
                    },
                    Murmur3Token {
                        value: 3738154517559259423,
                    },
                    Murmur3Token {
                        value: 3848675866402019633,
                    },
                    Murmur3Token {
                        value: 4005230984188031617,
                    },
                    Murmur3Token {
                        value: 4100718033285089675,
                    },
                    Murmur3Token {
                        value: 419424818820897648,
                    },
                    Murmur3Token {
                        value: 4263827612079360010,
                    },
                    Murmur3Token {
                        value: 43978237202824747,
                    },
                    Murmur3Token {
                        value: 4405174702313261751,
                    },
                    Murmur3Token {
                        value: 4519123205316040817,
                    },
                    Murmur3Token {
                        value: 4670879378745996012,
                    },
                    Murmur3Token {
                        value: 4854810257362753408,
                    },
                    Murmur3Token {
                        value: 4999743304790253651,
                    },
                    Murmur3Token {
                        value: 5129387856460855763,
                    },
                    Murmur3Token {
                        value: 521659972168229825,
                    },
                    Murmur3Token {
                        value: 5218256304755796121,
                    },
                    Murmur3Token {
                        value: 5392518225799603113,
                    },
                    Murmur3Token {
                        value: 5547462835067749983,
                    },
                    Murmur3Token {
                        value: 5655756748756258965,
                    },
                    Murmur3Token {
                        value: 5806968172585244134,
                    },
                    Murmur3Token {
                        value: 5917824338046985437,
                    },
                    Murmur3Token {
                        value: 6079116112237855309,
                    },
                    Murmur3Token {
                        value: 6243313409856998164,
                    },
                    Murmur3Token {
                        value: 6352190846999391586,
                    },
                    Murmur3Token {
                        value: 6525506602536889613,
                    },
                    Murmur3Token {
                        value: 6705994061697368186,
                    },
                    Murmur3Token {
                        value: 6838919054966110433,
                    },
                    Murmur3Token {
                        value: 688572179225339678,
                    },
                    Murmur3Token {
                        value: 6997477618058004961,
                    },
                    Murmur3Token {
                        value: 7106827748581695198,
                    },
                    Murmur3Token {
                        value: 7322636117918447281,
                    },
                    Murmur3Token {
                        value: 7513856600417669830,
                    },
                    Murmur3Token {
                        value: 7652710528630652374,
                    },
                    Murmur3Token {
                        value: 7867099565629790639,
                    },
                    Murmur3Token {
                        value: 8007006020599059949,
                    },
                    Murmur3Token {
                        value: 8106274607529982657,
                    },
                    Murmur3Token {
                        value: 8290000360996982780,
                    },
                    Murmur3Token {
                        value: 835344788704204578,
                    },
                    Murmur3Token {
                        value: 8478428113448273380,
                    },
                    Murmur3Token {
                        value: 8593086285668716454,
                    },
                    Murmur3Token {
                        value: 8731770207052823208,
                    },
                    Murmur3Token {
                        value: 8823012032859860179,
                    },
                    Murmur3Token {
                        value: 8982140935715934045,
                    },
                    Murmur3Token {
                        value: 9085781306924610254,
                    },
                    Murmur3Token {
                        value: 940774114818937017,
                    },
                ],
                uuid!("2dd022d6-2937-4754-89d6-02d2933a8f7d"),
            ),
            CassandraNode::new(
                "172.16.1.7:9044".parse().unwrap(),
                "rack1".into(),
                vec![
                    Murmur3Token {
                        value: -1004586411270825271,
                    },
                    Murmur3Token {
                        value: -1049898883426448359,
                    },
                    Murmur3Token {
                        value: -1196694092263399271,
                    },
                    Murmur3Token {
                        value: -1316115266562913234,
                    },
                    Murmur3Token {
                        value: -1437503384414675840,
                    },
                    Murmur3Token {
                        value: -1492690172060031739,
                    },
                    Murmur3Token {
                        value: -1622592378891212448,
                    },
                    Murmur3Token {
                        value: -1842743390146252286,
                    },
                    Murmur3Token {
                        value: -1949245434784624084,
                    },
                    Murmur3Token {
                        value: -2002077183627636776,
                    },
                    Murmur3Token {
                        value: -227379397644692018,
                    },
                    Murmur3Token {
                        value: -2314754236554905672,
                    },
                    Murmur3Token {
                        value: -2579172864481376523,
                    },
                    Murmur3Token {
                        value: -2676613702496213986,
                    },
                    Murmur3Token {
                        value: -2815361415294043951,
                    },
                    Murmur3Token {
                        value: -3033676637841035495,
                    },
                    Murmur3Token {
                        value: -3174741696455712456,
                    },
                    Murmur3Token {
                        value: -3335738588135151910,
                    },
                    Murmur3Token {
                        value: -3413411464665450981,
                    },
                    Murmur3Token {
                        value: -3622469396849016710,
                    },
                    Murmur3Token {
                        value: -3778258991876732532,
                    },
                    Murmur3Token {
                        value: -3944423519899748504,
                    },
                    Murmur3Token {
                        value: -408548723798480815,
                    },
                    Murmur3Token {
                        value: -4099887910995471052,
                    },
                    Murmur3Token {
                        value: -4297436737392605445,
                    },
                    Murmur3Token {
                        value: -4457453502975673731,
                    },
                    Murmur3Token {
                        value: -4550467823097342026,
                    },
                    Murmur3Token {
                        value: -4671921701216069762,
                    },
                    Murmur3Token {
                        value: -4813541339653395163,
                    },
                    Murmur3Token {
                        value: -4857790404123314889,
                    },
                    Murmur3Token {
                        value: -518633915583549766,
                    },
                    Murmur3Token {
                        value: -5201977675961991527,
                    },
                    Murmur3Token {
                        value: -5458002809469666000,
                    },
                    Murmur3Token {
                        value: -5690881629999676290,
                    },
                    Murmur3Token {
                        value: -5803788346294443934,
                    },
                    Murmur3Token {
                        value: -585581484826712711,
                    },
                    Murmur3Token {
                        value: -5886024853459351552,
                    },
                    Murmur3Token {
                        value: -6018692686042317033,
                    },
                    Murmur3Token {
                        value: -6103663037451221569,
                    },
                    Murmur3Token {
                        value: -6241894580502106337,
                    },
                    Murmur3Token {
                        value: -6540739067831246048,
                    },
                    Murmur3Token {
                        value: -6665964825812149277,
                    },
                    Murmur3Token {
                        value: -674526913236869510,
                    },
                    Murmur3Token {
                        value: -6745580981824674461,
                    },
                    Murmur3Token {
                        value: -6904602089216567570,
                    },
                    Murmur3Token {
                        value: -7070333325755783848,
                    },
                    Murmur3Token {
                        value: -7265155612194582360,
                    },
                    Murmur3Token {
                        value: -7365816290716823761,
                    },
                    Murmur3Token {
                        value: -742806922306413198,
                    },
                    Murmur3Token {
                        value: -7476482088772037952,
                    },
                    Murmur3Token {
                        value: -7717445208237174971,
                    },
                    Murmur3Token {
                        value: -7826275601464283032,
                    },
                    Murmur3Token {
                        value: -7934793180941469751,
                    },
                    Murmur3Token {
                        value: -8020447783382961940,
                    },
                    Murmur3Token {
                        value: -8109757957276297228,
                    },
                    Murmur3Token {
                        value: -8253402817732093783,
                    },
                    Murmur3Token {
                        value: -8396870723162158406,
                    },
                    Murmur3Token {
                        value: -840385907874903380,
                    },
                    Murmur3Token {
                        value: -8615356091601293133,
                    },
                    Murmur3Token {
                        value: -8758603996461529033,
                    },
                    Murmur3Token {
                        value: -8933228678916646960,
                    },
                    Murmur3Token {
                        value: -9114712287317370781,
                    },
                    Murmur3Token {
                        value: -9188097403558402958,
                    },
                    Murmur3Token {
                        value: 1014225940988749945,
                    },
                    Murmur3Token {
                        value: 1291094681509279487,
                    },
                    Murmur3Token {
                        value: 1362251621862633807,
                    },
                    Murmur3Token {
                        value: 1453653646579813581,
                    },
                    Murmur3Token {
                        value: 151856288122840271,
                    },
                    Murmur3Token {
                        value: 1549482254500710122,
                    },
                    Murmur3Token {
                        value: 1714026322822950195,
                    },
                    Murmur3Token {
                        value: 1820775916415892536,
                    },
                    Murmur3Token {
                        value: 1923075332258322910,
                    },
                    Murmur3Token {
                        value: 2011725076499047162,
                    },
                    Murmur3Token {
                        value: 2151455718569764926,
                    },
                    Murmur3Token {
                        value: 2234063386417334439,
                    },
                    Murmur3Token {
                        value: 2429803642268605649,
                    },
                    Murmur3Token {
                        value: 2554737675108855876,
                    },
                    Murmur3Token {
                        value: 2708646363891939000,
                    },
                    Murmur3Token {
                        value: 2811828826556868389,
                    },
                    Murmur3Token {
                        value: 2956294011478622974,
                    },
                    Murmur3Token {
                        value: 3021028861032813427,
                    },
                    Murmur3Token {
                        value: 3137571630816492926,
                    },
                    Murmur3Token {
                        value: 330138877682856754,
                    },
                    Murmur3Token {
                        value: 3314536781905121741,
                    },
                    Murmur3Token {
                        value: 3410643356619280661,
                    },
                    Murmur3Token {
                        value: 3477731373497932403,
                    },
                    Murmur3Token {
                        value: 3603317769690085561,
                    },
                    Murmur3Token {
                        value: 4061906395596879065,
                    },
                    Murmur3Token {
                        value: 4261358146809905014,
                    },
                    Murmur3Token {
                        value: 4436518590820790964,
                    },
                    Murmur3Token {
                        value: 4556209993460516851,
                    },
                    Murmur3Token {
                        value: 4696477527783455168,
                    },
                    Murmur3Token {
                        value: 4807509623012428073,
                    },
                    Murmur3Token {
                        value: 4953362451465610645,
                    },
                    Murmur3Token {
                        value: 5055040305672540272,
                    },
                    Murmur3Token {
                        value: 5261157149539100646,
                    },
                    Murmur3Token {
                        value: 5330263840739223626,
                    },
                    Murmur3Token {
                        value: 5433052779235448427,
                    },
                    Murmur3Token {
                        value: 5494691595301440557,
                    },
                    Murmur3Token {
                        value: 5589565827656960666,
                    },
                    Murmur3Token {
                        value: 5723167885545727198,
                    },
                    Murmur3Token {
                        value: 602274989584119818,
                    },
                    Murmur3Token {
                        value: 6125674746828035940,
                    },
                    Murmur3Token {
                        value: 6260421621848793431,
                    },
                    Murmur3Token {
                        value: 6359886064138221182,
                    },
                    Murmur3Token {
                        value: 6499165623492705162,
                    },
                    Murmur3Token {
                        value: 6622932721156170867,
                    },
                    Murmur3Token {
                        value: 6753494579497117766,
                    },
                    Murmur3Token {
                        value: 6900507243567459996,
                    },
                    Murmur3Token {
                        value: 7026413613674043843,
                    },
                    Murmur3Token {
                        value: 7110175306909994417,
                    },
                    Murmur3Token {
                        value: 7234495386399743394,
                    },
                    Murmur3Token {
                        value: 7316133344878507011,
                    },
                    Murmur3Token {
                        value: 7431712047871810262,
                    },
                    Murmur3Token {
                        value: 7507859958060009729,
                    },
                    Murmur3Token {
                        value: 7615087462451601131,
                    },
                    Murmur3Token {
                        value: 7704806383419319520,
                    },
                    Murmur3Token {
                        value: 7838445446312906561,
                    },
                    Murmur3Token {
                        value: 7964672129524998277,
                    },
                    Murmur3Token {
                        value: 8139314725125917696,
                    },
                    Murmur3Token {
                        value: 8269550773979367715,
                    },
                    Murmur3Token {
                        value: 8384657922510232219,
                    },
                    Murmur3Token {
                        value: 8477619812599149096,
                    },
                    Murmur3Token {
                        value: 867261695441559513,
                    },
                    Murmur3Token {
                        value: 8759746538699238025,
                    },
                    Murmur3Token {
                        value: 8860942362594907195,
                    },
                    Murmur3Token {
                        value: 8954294001257725257,
                    },
                    Murmur3Token {
                        value: 9088232766780681447,
                    },
                ],
                uuid!("2dd022d6-2937-4754-89d6-02d2933a8f7e"),
            ),
            CassandraNode::new(
                "172.16.1.5:9044".parse().unwrap(),
                "rack1".into(),
                vec![
                    Murmur3Token {
                        value: -1051869841756831288,
                    },
                    Murmur3Token {
                        value: -1354902211218951063,
                    },
                    Murmur3Token {
                        value: -1464297471222304104,
                    },
                    Murmur3Token {
                        value: -1493489479075081424,
                    },
                    Murmur3Token {
                        value: -1547006183113151251,
                    },
                    Murmur3Token {
                        value: -1699632135926513521,
                    },
                    Murmur3Token {
                        value: -1785886705544400561,
                    },
                    Murmur3Token {
                        value: -1923589518666686923,
                    },
                    Murmur3Token {
                        value: -2002837141931205961,
                    },
                    Murmur3Token {
                        value: -2153024021771889424,
                    },
                    Murmur3Token {
                        value: -2225221119789584147,
                    },
                    Murmur3Token {
                        value: -227751306457142615,
                    },
                    Murmur3Token {
                        value: -2446900046006993352,
                    },
                    Murmur3Token {
                        value: -2555041688482059142,
                    },
                    Murmur3Token {
                        value: -2680728419098908140,
                    },
                    Murmur3Token {
                        value: -2736785063186241725,
                    },
                    Murmur3Token {
                        value: -2887831105598743148,
                    },
                    Murmur3Token {
                        value: -2980067729886710629,
                    },
                    Murmur3Token {
                        value: -3126991352288552007,
                    },
                    Murmur3Token {
                        value: -3290700601730713893,
                    },
                    Murmur3Token {
                        value: -3433811465140859352,
                    },
                    Murmur3Token {
                        value: -3516651001374023018,
                    },
                    Murmur3Token {
                        value: -359615991548202607,
                    },
                    Murmur3Token {
                        value: -3682153248717846678,
                    },
                    Murmur3Token {
                        value: -3801731621937146766,
                    },
                    Murmur3Token {
                        value: -3889242041893813805,
                    },
                    Murmur3Token {
                        value: -4167735219766140668,
                    },
                    Murmur3Token {
                        value: -4299824507930579363,
                    },
                    Murmur3Token {
                        value: -4372050171521555712,
                    },
                    Murmur3Token {
                        value: -4578150559265059472,
                    },
                    Murmur3Token {
                        value: -4735348838952921241,
                    },
                    Murmur3Token {
                        value: -4791459336514701336,
                    },
                    Murmur3Token {
                        value: -4857832933219580925,
                    },
                    Murmur3Token {
                        value: -4939462975633720803,
                    },
                    Murmur3Token {
                        value: -5061644042121835589,
                    },
                    Murmur3Token {
                        value: -5124609431231531887,
                    },
                    Murmur3Token {
                        value: -5237153970812678185,
                    },
                    Murmur3Token {
                        value: -5427764415272455939,
                    },
                    Murmur3Token {
                        value: -544760082024545627,
                    },
                    Murmur3Token {
                        value: -5604631326373429756,
                    },
                    Murmur3Token {
                        value: -5749559482843544330,
                    },
                    Murmur3Token {
                        value: -5933124781405013997,
                    },
                    Murmur3Token {
                        value: -6043253823070719138,
                    },
                    Murmur3Token {
                        value: -6196288867084196156,
                    },
                    Murmur3Token {
                        value: -6338592926655695647,
                    },
                    Murmur3Token {
                        value: -6469260009100995637,
                    },
                    Murmur3Token {
                        value: -6637748870897155827,
                    },
                    Murmur3Token {
                        value: -6748360933373217321,
                    },
                    Murmur3Token {
                        value: -6861369488893313481,
                    },
                    Murmur3Token {
                        value: -6926158976859097235,
                    },
                    Murmur3Token {
                        value: -7036391376274357501,
                    },
                    Murmur3Token {
                        value: -7079264415456505136,
                    },
                    Murmur3Token {
                        value: -7205172412684269506,
                    },
                    Murmur3Token {
                        value: -721176115212609920,
                    },
                    Murmur3Token {
                        value: -7452781485780420678,
                    },
                    Murmur3Token {
                        value: -7531018242179854452,
                    },
                    Murmur3Token {
                        value: -7576026114454214890,
                    },
                    Murmur3Token {
                        value: -7693279205406297668,
                    },
                    Murmur3Token {
                        value: -7860403600874780476,
                    },
                    Murmur3Token {
                        value: -7966586501405373620,
                    },
                    Murmur3Token {
                        value: -8202285241058427695,
                    },
                    Murmur3Token {
                        value: -8332114523400176655,
                    },
                    Murmur3Token {
                        value: -840852980268317094,
                    },
                    Murmur3Token {
                        value: -8411187137360527576,
                    },
                    Murmur3Token {
                        value: -8488291191419991376,
                    },
                    Murmur3Token {
                        value: -8638964591558167490,
                    },
                    Murmur3Token {
                        value: -8862652775550233496,
                    },
                    Murmur3Token {
                        value: -9048905223527971561,
                    },
                    Murmur3Token {
                        value: -954848057254987837,
                    },
                    Murmur3Token {
                        value: -983901133523396657,
                    },
                    Murmur3Token {
                        value: 1071198296291919583,
                    },
                    Murmur3Token {
                        value: 1260367825514417900,
                    },
                    Murmur3Token {
                        value: 1321821537504141074,
                    },
                    Murmur3Token {
                        value: 1420223660157487173,
                    },
                    Murmur3Token {
                        value: 1530619958189243847,
                    },
                    Murmur3Token {
                        value: 1599656424165521865,
                    },
                    Murmur3Token {
                        value: 1769150870583917125,
                    },
                    Murmur3Token {
                        value: 1983567760147576786,
                    },
                    Murmur3Token {
                        value: 216842214771444509,
                    },
                    Murmur3Token {
                        value: 22561090955616565,
                    },
                    Murmur3Token {
                        value: 2340755830222155645,
                    },
                    Murmur3Token {
                        value: 2453982339281109650,
                    },
                    Murmur3Token {
                        value: 2680321801863198797,
                    },
                    Murmur3Token {
                        value: 2871798564658414164,
                    },
                    Murmur3Token {
                        value: 2978232838871359626,
                    },
                    Murmur3Token {
                        value: 3075153504731162796,
                    },
                    Murmur3Token {
                        value: 3209194243814406361,
                    },
                    Murmur3Token {
                        value: 354684427944800207,
                    },
                    Murmur3Token {
                        value: 3563308276687622173,
                    },
                    Murmur3Token {
                        value: 3678064045462153538,
                    },
                    Murmur3Token {
                        value: 3761786819611303930,
                    },
                    Murmur3Token {
                        value: 3809649099704095915,
                    },
                    Murmur3Token {
                        value: 3904266880436901240,
                    },
                    Murmur3Token {
                        value: 3992780822468066397,
                    },
                    Murmur3Token {
                        value: 4144941098052189328,
                    },
                    Murmur3Token {
                        value: 4260123414175177516,
                    },
                    Murmur3Token {
                        value: 4303247167997899575,
                    },
                    Murmur3Token {
                        value: 470542395494563736,
                    },
                    Murmur3Token {
                        value: 4783859305837265406,
                    },
                    Murmur3Token {
                        value: 5076354996380960369,
                    },
                    Murmur3Token {
                        value: 5113528771775118115,
                    },
                    Murmur3Token {
                        value: 5299136648209033882,
                    },
                    Murmur3Token {
                        value: 531780339972545620,
                    },
                    Murmur3Token {
                        value: 5408351714367684417,
                    },
                    Murmur3Token {
                        value: 5792528445104973285,
                    },
                    Murmur3Token {
                        value: 5915844158608622715,
                    },
                    Murmur3Token {
                        value: 5972435795639927454,
                    },
                    Murmur3Token {
                        value: 6080905191860729956,
                    },
                    Murmur3Token {
                        value: 6563036755392266867,
                    },
                    Murmur3Token {
                        value: 6775055280217922444,
                    },
                    Murmur3Token {
                        value: 678095654549637784,
                    },
                    Murmur3Token {
                        value: 7040881611482063284,
                    },
                    Murmur3Token {
                        value: 7247101963345143463,
                    },
                    Murmur3Token {
                        value: 7448378400736079471,
                    },
                    Murmur3Token {
                        value: 7633898995541126752,
                    },
                    Murmur3Token {
                        value: 7773193485876466178,
                    },
                    Murmur3Token {
                        value: 781147752169843136,
                    },
                    Murmur3Token {
                        value: 7852772505971348600,
                    },
                    Murmur3Token {
                        value: 8028504102198789777,
                    },
                    Murmur3Token {
                        value: 8155834783923885216,
                    },
                    Murmur3Token {
                        value: 8360061172013177645,
                    },
                    Murmur3Token {
                        value: 8409254673007286793,
                    },
                    Murmur3Token {
                        value: 8662428246360769831,
                    },
                    Murmur3Token {
                        value: 8751131363440998549,
                    },
                    Murmur3Token {
                        value: 8879907527462430703,
                    },
                    Murmur3Token {
                        value: 8968217468486829651,
                    },
                    Murmur3Token {
                        value: 9145854446267185732,
                    },
                    Murmur3Token {
                        value: 919976358498925733,
                    },
                ],
                uuid!("2dd022d6-2937-4754-89d6-02d2933a8f7f"),
            ),
        ]
    }
}
