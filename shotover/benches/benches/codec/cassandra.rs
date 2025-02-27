use bytes::BytesMut;
use cassandra_protocol::frame::Version;
use cassandra_protocol::frame::message_result::{
    ColSpec, ColType, ColTypeOption, ColTypeOptionValue, RowsMetadata, RowsMetadataFlags, TableSpec,
};
use criterion::{BatchSize, Criterion, criterion_group};
use shotover::codec::cassandra::CassandraCodecBuilder;
use shotover::codec::{CodecBuilder, Direction};
use shotover::frame::{
    CassandraFrame, CassandraOperation, CassandraResult, Frame,
    cassandra::{Tracing, parse_statement_single},
    value::{GenericValue, IntSize},
};
use shotover::message::Message;
use tokio_util::codec::{Decoder, Encoder};

fn criterion_benchmark(c: &mut Criterion) {
    crate::init();
    let mut group = c.benchmark_group("cassandra_codec");
    group.noise_threshold(0.2);

    {
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 1,
            tracing: Tracing::Request(false),
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single("SELECT * FROM system.local;")),
                params: Box::default(),
            },
        }))];

        let (_, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("NONE".to_string(), Version::V4);

        let mut bytes = BytesMut::new();
        group.bench_function("encode_system.local_query_v4_no_compression", |b| {
            b.iter_batched(
                || messages.clone(),
                |messages| {
                    bytes.clear();
                    encoder.encode(messages, &mut bytes).unwrap();
                },
                BatchSize::SmallInput,
            )
        });

        let (mut decoder, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("NONE".to_string(), Version::V4);

        group.bench_function("decode_system.local_query_v4_no_compression", |b| {
            b.iter_batched(
                || {
                    let mut bytes = BytesMut::new();
                    encoder.encode(messages.clone(), &mut bytes).unwrap();
                    bytes
                },
                |mut bytes| decoder.decode(&mut bytes).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }

    {
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 1,
            tracing: Tracing::Request(false),
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single("SELECT * FROM system.local;")),
                params: Box::default(),
            },
        }))];

        let (_, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("NONE".to_string(), Version::V4);

        let mut bytes = BytesMut::new();
        group.bench_function("encode_system.local_query_v4_lz4_compression", |b| {
            b.iter_batched(
                || messages.clone(),
                |messages| {
                    bytes.clear();
                    encoder.encode(messages, &mut bytes).unwrap();
                },
                BatchSize::SmallInput,
            )
        });

        let (mut decoder, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("NONE".to_string(), Version::V4);

        group.bench_function("decode_system.local_query_v4_lz4_compression", |b| {
            b.iter_batched(
                || {
                    let mut bytes = BytesMut::new();
                    encoder.encode(messages.clone(), &mut bytes).unwrap();
                    bytes
                },
                |mut bytes| decoder.decode(&mut bytes).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }

    {
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing: Tracing::Response(None),
            warnings: vec![],
            operation: CassandraOperation::Result(peers_v2_result()),
        }))];

        let (_, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("NONE".to_string(), Version::V5);

        let mut bytes = BytesMut::new();
        group.bench_function("encode_system.local_result_v4_no_compression", |b| {
            b.iter_batched(
                || messages.clone(),
                |messages| {
                    bytes.clear();
                    encoder.encode(messages, &mut bytes).unwrap();
                },
                BatchSize::SmallInput,
            )
        });

        let (mut decoder, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("NONE".to_string(), Version::V5);

        group.bench_function("decode_system.local_result_v4_no_compression", |b| {
            b.iter_batched(
                || {
                    let mut bytes = BytesMut::new();
                    encoder.encode(messages.clone(), &mut bytes).unwrap();
                    bytes
                },
                |mut bytes| decoder.decode(&mut bytes).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }

    {
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            stream_id: 0,
            tracing: Tracing::Response(None),
            warnings: vec![],
            operation: CassandraOperation::Result(peers_v2_result()),
        }))];

        let (_, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("LZ4".to_string(), Version::V5);

        let mut bytes = BytesMut::new();
        group.bench_function("encode_system.local_result_v4_lz4_compression", |b| {
            b.iter_batched(
                || messages.clone(),
                |messages| {
                    bytes.clear();
                    encoder.encode(messages, &mut bytes).unwrap();
                },
                BatchSize::SmallInput,
            )
        });

        let (mut decoder, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("LZ4".to_string(), Version::V5);

        group.bench_function("decode_system.local_result_v4_lz4_compression", |b| {
            b.iter_batched(
                || {
                    let mut bytes = BytesMut::new();
                    encoder.encode(messages.clone(), &mut bytes).unwrap();
                    bytes
                },
                |mut bytes| decoder.decode(&mut bytes).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }

    {
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V5,
            stream_id: 1,
            tracing: Tracing::Request(false),
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single("SELECT * FROM system.local;")),
                params: Box::default(),
            },
        }))];

        let (_, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("NONE".to_string(), Version::V5);

        let mut bytes = BytesMut::new();
        group.bench_function("encode_system.local_query_v5_no_compression", |b| {
            b.iter_batched(
                || messages.clone(),
                |messages| {
                    bytes.clear();
                    encoder.encode(messages, &mut bytes).unwrap();
                },
                BatchSize::SmallInput,
            )
        });

        let (mut decoder, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("LZ4".to_string(), Version::V5);

        group.bench_function("decode_system.local_query_v5_no_compression", |b| {
            b.iter_batched(
                || {
                    let mut bytes = BytesMut::new();
                    encoder.encode(messages.clone(), &mut bytes).unwrap();
                    bytes
                },
                |mut bytes| decoder.decode(&mut bytes).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }

    {
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V5,
            stream_id: 1,
            tracing: Tracing::Request(false),
            warnings: vec![],
            operation: CassandraOperation::Query {
                query: Box::new(parse_statement_single("SELECT * FROM system.local;")),
                params: Box::default(),
            },
        }))];

        let (_, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("LZ4".to_string(), Version::V5);

        let mut bytes = BytesMut::new();
        group.bench_function("encode_system.local_query_v5_lz4_compression", |b| {
            b.iter_batched(
                || messages.clone(),
                |messages| {
                    bytes.clear();
                    encoder.encode(messages, &mut bytes).unwrap();
                },
                BatchSize::SmallInput,
            )
        });

        let (mut decoder, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("LZ4".to_string(), Version::V5);

        group.bench_function("decode_system.local_query_v5_lz4_compression", |b| {
            b.iter_batched(
                || {
                    let mut bytes = BytesMut::new();
                    encoder.encode(messages.clone(), &mut bytes).unwrap();
                    bytes
                },
                |mut bytes| decoder.decode(&mut bytes).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }

    {
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V5,
            stream_id: 0,
            tracing: Tracing::Response(None),
            warnings: vec![],
            operation: CassandraOperation::Result(peers_v2_result()),
        }))];

        let (_, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("NONE".to_string(), Version::V5);

        // This bench is disabled because it is incredibly noisy.
        // The noisiness actually indicates a real problem, which is why this bench is commented out instead of removed.
        // The cassandra frame requires many allocations, consider the realistic value returned by peers_v2_results:
        // there are hundreds of allocations required due to all those individually allocated strings.
        // Lots of allocations results in noisy performance because allocating and deallocating allocations can be:
        // * fast - there is memory in the heap ready to go
        // * or slow - we need to ask the OS for more memory or ask the OS to take back some memory or ...
        //
        // However fixing this problem properly by reducing allocations would require rewriting cassandra-protocol
        // which is months of development time and improving cassandra performance just isnt a priority right now.
        // So instead of fixing it, we just commented it out to stop giving us false positives in CI.
        //
        //let mut bytes = BytesMut::new();
        // group.bench_function("encode_system.local_result_v5_no_compression", |b| {
        //     b.iter_batched(
        //         || messages.clone(),
        //         |messages| {
        //             bytes.clear();
        //             encoder.encode(messages, &mut bytes).unwrap();
        //         },
        //         BatchSize::SmallInput,
        //     )
        // });

        let (mut decoder, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("NONE".to_string(), Version::V5);

        group.bench_function("decode_system.local_result_v5_no_compression", |b| {
            b.iter_batched(
                || {
                    let mut bytes = BytesMut::new();
                    encoder.encode(messages.clone(), &mut bytes).unwrap();
                    bytes
                },
                |mut bytes| decoder.decode(&mut bytes).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }

    {
        let messages = vec![Message::from_frame(Frame::Cassandra(CassandraFrame {
            version: Version::V5,
            stream_id: 0,
            tracing: Tracing::Response(None),
            warnings: vec![],
            operation: CassandraOperation::Result(peers_v2_result()),
        }))];

        let (_, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("LZ4".to_string(), Version::V5);

        let mut bytes = BytesMut::new();
        group.bench_function("encode_system.local_result_v5_lz4_compression", |b| {
            b.iter_batched(
                || messages.clone(),
                |messages| {
                    bytes.clear();
                    encoder.encode(messages, &mut bytes).unwrap();
                },
                BatchSize::SmallInput,
            )
        });

        let (mut decoder, mut encoder) =
            CassandraCodecBuilder::new(Direction::Sink, "cassandra".to_owned()).build();

        encoder.set_startup_state_ext("LZ4".to_string(), Version::V5);

        group.bench_function("decode_system.local_result_v5_lz4_compression", |b| {
            b.iter_batched(
                || {
                    let mut bytes = BytesMut::new();
                    encoder.encode(messages.clone(), &mut bytes).unwrap();
                    bytes
                },
                |mut bytes| decoder.decode(&mut bytes).unwrap(),
                BatchSize::SmallInput,
            )
        });
    }
}

criterion_group!(benches, criterion_benchmark);

fn peers_v2_result() -> CassandraResult {
    CassandraResult::Rows {
        rows: vec![vec![
            GenericValue::Varchar("local".into()),
            GenericValue::Varchar("COMPLETED".into()),
            GenericValue::Inet("127.0.0.1".parse().unwrap()),
            GenericValue::Integer(7000, IntSize::I32),
            GenericValue::Varchar("TestCluster".into()),
            GenericValue::Varchar("3.4.5".into()),
            GenericValue::Varchar("datacenter1".into()),
            GenericValue::Integer(1662429909, IntSize::I32),
            GenericValue::Uuid("2dd022d6-2937-4754-89d6-02d2933a8f7a".parse().unwrap()),
            GenericValue::Inet("127.0.0.1".parse().unwrap()),
            GenericValue::Integer(7000, IntSize::I32),
            GenericValue::Varchar("5".into()),
            GenericValue::Varchar("org.apache.cassandra.dht.Murmur3Partitioner".into()),
            GenericValue::Varchar("rack1".into()),
            GenericValue::Varchar("4.0.6".into()),
            GenericValue::Inet("0.0.0.0".parse().unwrap()),
            GenericValue::Integer(9042, IntSize::I32),
            GenericValue::Uuid("397ea5ef-1dfa-4760-827c-71b130deeff1".parse().unwrap()),
            GenericValue::List(vec![
                GenericValue::Varchar("-1053286887251221439".into()),
                GenericValue::Varchar("-106570420317937252".into()),
                GenericValue::Varchar("-1118500511339798278".into()),
                GenericValue::Varchar("-1169241438815364676".into()),
                GenericValue::Varchar("-1233774346411700013".into()),
                GenericValue::Varchar("-1283007285177402315".into()),
                GenericValue::Varchar("-1326494175141678507".into()),
                GenericValue::Varchar("-1391881896845523737".into()),
                GenericValue::Varchar("-1407090101949198040".into()),
                GenericValue::Varchar("-1453855538082496644".into()),
                GenericValue::Varchar("-1515967539091591462".into()),
                GenericValue::Varchar("-1533489865785121542".into()),
                GenericValue::Varchar("-1576157525159793283".into()),
                GenericValue::Varchar("-162157776874719171".into()),
                GenericValue::Varchar("-1643837294794869601".into()),
                GenericValue::Varchar("-1680164836710734319".into()),
                GenericValue::Varchar("-1732769176205285547".into()),
                GenericValue::Varchar("-1802660587057735092".into()),
                GenericValue::Varchar("-1841456610901604190".into()),
                GenericValue::Varchar("-1879340784789368908".into()),
                GenericValue::Varchar("-1930662097533829005".into()),
                GenericValue::Varchar("-193234807999467820".into()),
                GenericValue::Varchar("-1952312776363345492".into()),
                GenericValue::Varchar("-2010595959477605104".into()),
                GenericValue::Varchar("-2047162668796828666".into()),
                GenericValue::Varchar("-2103524200192330661".into()),
                GenericValue::Varchar("-2136351916082225294".into()),
                GenericValue::Varchar("-2211818113880839641".into()),
                GenericValue::Varchar("-2253026203848089498".into()),
                GenericValue::Varchar("-2306232275618633861".into()),
                GenericValue::Varchar("-2366762723148986512".into()),
                GenericValue::Varchar("-2398213000465113021".into()),
                GenericValue::Varchar("-2451513595470736998".into()),
                GenericValue::Varchar("-245424348530919796".into()),
                GenericValue::Varchar("-2485765597152082750".into()),
                GenericValue::Varchar("-2541024644192793504".into()),
                GenericValue::Varchar("-2548126194762290937".into()),
                GenericValue::Varchar("-2584465729882569296".into()),
                GenericValue::Varchar("-2629893092487733861".into()),
                GenericValue::Varchar("-2653555520877023374".into()),
                GenericValue::Varchar("-2695950381051029246".into()),
                GenericValue::Varchar("-2759537644158335976".into()),
                GenericValue::Varchar("-2800328130355888275".into()),
                GenericValue::Varchar("-2843129825641154811".into()),
                GenericValue::Varchar("-2904470691585836218".into()),
                GenericValue::Varchar("-2967240337412998128".into()),
                GenericValue::Varchar("-300828578727447484".into()),
                GenericValue::Varchar("-3009522390354666974".into()),
                GenericValue::Varchar("-3069475490760330304".into()),
                GenericValue::Varchar("-3088401570202593613".into()),
                GenericValue::Varchar("-3138589667889054545".into()),
                GenericValue::Varchar("-3207852532422258106".into()),
                GenericValue::Varchar("-3240157743632548808".into()),
                GenericValue::Varchar("-3301648437477540995".into()),
                GenericValue::Varchar("-3354106246635327873".into()),
                GenericValue::Varchar("-3388016705890739503".into()),
                GenericValue::Varchar("-3444922072378403206".into()),
                GenericValue::Varchar("-3495453336869229616".into()),
                GenericValue::Varchar("-353168487411060067".into()),
                GenericValue::Varchar("-3532270068745407705".into()),
                GenericValue::Varchar("-3580206458269889837".into()),
                GenericValue::Varchar("-3658562915663499950".into()),
                GenericValue::Varchar("-3666640389151057780".into()),
                GenericValue::Varchar("-3700619198255472071".into()),
                GenericValue::Varchar("-3754049964760558007".into()),
                GenericValue::Varchar("-3807585812801555352".into()),
                GenericValue::Varchar("-3848675294420850973".into()),
                GenericValue::Varchar("-385440080170531058".into()),
                GenericValue::Varchar("-3910605082546569993".into()),
                GenericValue::Varchar("-3955281892282785998".into()),
                GenericValue::Varchar("-4021126431389330202".into()),
                GenericValue::Varchar("-4072929672183074707".into()),
                GenericValue::Varchar("-4138408827936160334".into()),
                GenericValue::Varchar("-4157900636303623518".into()),
                GenericValue::Varchar("-4223696570626851441".into()),
                GenericValue::Varchar("-4262936741388560065".into()),
                GenericValue::Varchar("-4329054442683003056".into()),
                GenericValue::Varchar("-4330220362242958762".into()),
                GenericValue::Varchar("-436644831030142346".into()),
                GenericValue::Varchar("-4370056871164132065".into()),
                GenericValue::Varchar("-4432538416242080842".into()),
                GenericValue::Varchar("-4452116165357195997".into()),
                GenericValue::Varchar("-447014253779697377".into()),
                GenericValue::Varchar("-4505128235357924458".into()),
                GenericValue::Varchar("-4571209218094809156".into()),
                GenericValue::Varchar("-4648120762324841346".into()),
                GenericValue::Varchar("-4717394893147059049".into()),
                GenericValue::Varchar("-4795241631068571608".into()),
                GenericValue::Varchar("-4847135636540662810".into()),
                GenericValue::Varchar("-4928100176324910967".into()),
                GenericValue::Varchar("-4979991867611160324".into()),
                GenericValue::Varchar("-5041141770579346068".into()),
                GenericValue::Varchar("-5120544580329844275".into()),
                GenericValue::Varchar("-5179090870774393369".into()),
                GenericValue::Varchar("-5230350850137708564".into()),
                GenericValue::Varchar("-527273302598393468".into()),
                GenericValue::Varchar("-5306754816121976370".into()),
                GenericValue::Varchar("-5343341207081001140".into()),
                GenericValue::Varchar("-5388881150707159950".into()),
                GenericValue::Varchar("-5428874231606218352".into()),
                GenericValue::Varchar("-5492497409816003103".into()),
                GenericValue::Varchar("-5553387924544763987".into()),
                GenericValue::Varchar("-5593809836937936630".into()),
                GenericValue::Varchar("-5651628658300363087".into()),
                GenericValue::Varchar("-5662262536212885409".into()),
                GenericValue::Varchar("-5707251483611981081".into()),
                GenericValue::Varchar("-5764048300972564294".into()),
                GenericValue::Varchar("-578583790895217637".into()),
                GenericValue::Varchar("-5803870505152483214".into()),
                GenericValue::Varchar("-5845946725096068838".into()),
                GenericValue::Varchar("-58673803315641386".into()),
                GenericValue::Varchar("-5914217934162231273".into()),
                GenericValue::Varchar("-5919462482734346233".into()),
                GenericValue::Varchar("-5961825585269426164".into()),
                GenericValue::Varchar("-6019810822063969717".into()),
                GenericValue::Varchar("-6073041226425096764".into()),
                GenericValue::Varchar("-6112331883326273396".into()),
                GenericValue::Varchar("-6173743445282830250".into()),
                GenericValue::Varchar("-6201042736901190677".into()),
                GenericValue::Varchar("-6251976154389366291".into()),
                GenericValue::Varchar("-6330896149498434702".into()),
                GenericValue::Varchar("-6406732555449586966".into()),
                GenericValue::Varchar("-6432477073634185852".into()),
                GenericValue::Varchar("-6491545169337203540".into()),
                GenericValue::Varchar("-652453200366894428".into()),
                GenericValue::Varchar("-6529330420810611401".into()),
                GenericValue::Varchar("-6576612914985995533".into()),
                GenericValue::Varchar("-657719536957549295".into()),
                GenericValue::Varchar("-6583794520508215363".into()),
                GenericValue::Varchar("-6618106006869208679".into()),
                GenericValue::Varchar("-6668593639832474693".into()),
                GenericValue::Varchar("-6701772576942435295".into()),
                GenericValue::Varchar("-6756268410124392265".into()),
                GenericValue::Varchar("-6818506834129652609".into()),
                GenericValue::Varchar("-6861511773295862196".into()),
                GenericValue::Varchar("-6923936160244385046".into()),
                GenericValue::Varchar("-6965815207939944022".into()),
                GenericValue::Varchar("-702143626612579554".into()),
                GenericValue::Varchar("-7033907288367203523".into()),
                GenericValue::Varchar("-7070708769723249947".into()),
                GenericValue::Varchar("-7146398783089308371".into()),
                GenericValue::Varchar("-7184575685466032188".into()),
                GenericValue::Varchar("-7237620976780359800".into()),
                GenericValue::Varchar("-7314104561578788957".into()),
                GenericValue::Varchar("-7339856130127691976".into()),
                GenericValue::Varchar("-7387341485156361661".into()),
                GenericValue::Varchar("-7451004336948210538".into()),
                GenericValue::Varchar("-7478233171789619778".into()),
                GenericValue::Varchar("-7540633815441714500".into()),
                GenericValue::Varchar("-7572029076844902667".into()),
                GenericValue::Varchar("-761803330890584664".into()),
                GenericValue::Varchar("-7618654367104839633".into()),
                GenericValue::Varchar("-7679836420743153652".into()),
                GenericValue::Varchar("-7715302711745764878".into()),
                GenericValue::Varchar("-7787172699037781843".into()),
                GenericValue::Varchar("-7850587097637251509".into()),
                GenericValue::Varchar("-7884679363341611299".into()),
                GenericValue::Varchar("-7937021028518419452".into()),
                GenericValue::Varchar("-7961083598894068837".into()),
                GenericValue::Varchar("-798559017799826755".into()),
                GenericValue::Varchar("-8012772684469989757".into()),
                GenericValue::Varchar("-8077966452168917024".into()),
                GenericValue::Varchar("-8131423367483532508".into()),
                GenericValue::Varchar("-8166946513735346444".into()),
                GenericValue::Varchar("-8225662531650147670".into()),
                GenericValue::Varchar("-8271684543548777051".into()),
                GenericValue::Varchar("-8346605655512010775".into()),
                GenericValue::Varchar("-8408789467303522006".into()),
                GenericValue::Varchar("-8451716476555525946".into()),
                GenericValue::Varchar("-850163940962482603".into()),
                GenericValue::Varchar("-8507017377751998651".into()),
                GenericValue::Varchar("-8548168288992657798".into()),
                GenericValue::Varchar("-8600601001610320434".into()),
                GenericValue::Varchar("-8606494085783673520".into()),
                GenericValue::Varchar("-8657080191709078624".into()),
                GenericValue::Varchar("-8722496804724557669".into()),
                GenericValue::Varchar("-8761230575244592800".into()),
                GenericValue::Varchar("-8816463672026944964".into()),
                GenericValue::Varchar("-8886426330656473835".into()),
                GenericValue::Varchar("-8918501401692203018".into()),
                GenericValue::Varchar("-8976273572946855876".into()),
                GenericValue::Varchar("-9054825905108122129".into()),
                GenericValue::Varchar("-9065622270435933282".into()),
                GenericValue::Varchar("-9124209862469428289".into()),
                GenericValue::Varchar("-9154952761677859578".into()),
                GenericValue::Varchar("-920361893982479193".into()),
                GenericValue::Varchar("-9205998296664244522".into()),
                GenericValue::Varchar("-959970210770346892".into()),
                GenericValue::Varchar("-999975303519652468".into()),
                GenericValue::Varchar("1030222895734812861".into()),
                GenericValue::Varchar("1063731083911270551".into()),
                GenericValue::Varchar("107818616681201012".into()),
                GenericValue::Varchar("112480003063738152".into()),
                GenericValue::Varchar("1136290717253665518".into()),
                GenericValue::Varchar("1181979069164768056".into()),
                GenericValue::Varchar("1222859986767344417".into()),
                GenericValue::Varchar("1282883050143318175".into()),
                GenericValue::Varchar("1326500357976020626".into()),
                GenericValue::Varchar("1365909947781525452".into()),
                GenericValue::Varchar("1434831827110874202".into()),
                GenericValue::Varchar("1484201229063580712".into()),
                GenericValue::Varchar("1510842995209025695".into()),
                GenericValue::Varchar("1571834839178975815".into()),
                GenericValue::Varchar("1614889509643212856".into()),
                GenericValue::Varchar("163265740678521809".into()),
                GenericValue::Varchar("1640487546879627807".into()),
                GenericValue::Varchar("1705297175215364486".into()),
                GenericValue::Varchar("1729355995174568165".into()),
                GenericValue::Varchar("1800360739872152532".into()),
                GenericValue::Varchar("1846448981948191415".into()),
                GenericValue::Varchar("1861776679721810".into()),
                GenericValue::Varchar("1903617916218375157".into()),
                GenericValue::Varchar("193489307933790068".into()),
                GenericValue::Varchar("1959361367861888547".into()),
                GenericValue::Varchar("1998979663237001201".into()),
                GenericValue::Varchar("2058562525486522027".into()),
                GenericValue::Varchar("2120723964562291790".into()),
                GenericValue::Varchar("2166856439175031009".into()),
                GenericValue::Varchar("2215192542268176877".into()),
                GenericValue::Varchar("2251250724552462384".into()),
                GenericValue::Varchar("2318067863004016178".into()),
                GenericValue::Varchar("2319225229995541919".into()),
                GenericValue::Varchar("2381846349184783573".into()),
                GenericValue::Varchar("2428924028465757481".into()),
                GenericValue::Varchar("2467783999193743362".into()),
                GenericValue::Varchar("247725071650470321".into()),
                GenericValue::Varchar("249254207978031467".into()),
                GenericValue::Varchar("2535823555480865323".into()),
                GenericValue::Varchar("2590215802656627353".into()),
                GenericValue::Varchar("2599741064624157916".into()),
                GenericValue::Varchar("2649739639682596335".into()),
                GenericValue::Varchar("2689083302891684496".into()),
                GenericValue::Varchar("2754413100275770208".into()),
                GenericValue::Varchar("2817679391525171433".into()),
                GenericValue::Varchar("2863290537418163630".into()),
                GenericValue::Varchar("2884512583568294858".into()),
                GenericValue::Varchar("2931722548687614195".into()),
                GenericValue::Varchar("2969093904822452449".into()),
                GenericValue::Varchar("3028385995134245917".into()),
                GenericValue::Varchar("3036606292955661657".into()),
                GenericValue::Varchar("304050136121377187".into()),
                GenericValue::Varchar("3110373498393863835".into()),
                GenericValue::Varchar("3161614849707931198".into()),
                GenericValue::Varchar("3217093752116140230".into()),
                GenericValue::Varchar("3258382241972158580".into()),
                GenericValue::Varchar("3314163569464468106".into()),
                GenericValue::Varchar("3350018745384882477".into()),
                GenericValue::Varchar("3425431416062890618".into()),
                GenericValue::Varchar("346993658581393029".into()),
                GenericValue::Varchar("3472333282539228366".into()),
                GenericValue::Varchar("3508577308476777005".into()),
                GenericValue::Varchar("3561734717669202147".into()),
                GenericValue::Varchar("359775556820791677".into()),
                GenericValue::Varchar("3599267619603735560".into()),
                GenericValue::Varchar("3617927439000467242".into()),
                GenericValue::Varchar("3691874056121208030".into()),
                GenericValue::Varchar("3739747260259335087".into()),
                GenericValue::Varchar("3792467575625853618".into()),
                GenericValue::Varchar("3833735808337219325".into()),
                GenericValue::Varchar("3901850305506109551".into()),
                GenericValue::Varchar("3947442046033373244".into()),
                GenericValue::Varchar("4024956290836441874".into()),
                GenericValue::Varchar("4077134487564787399".into()),
                GenericValue::Varchar("411297405619220081".into()),
                GenericValue::Varchar("4113992687741467801".into()),
                GenericValue::Varchar("41368531426448077".into()),
                GenericValue::Varchar("4163810219049424418".into()),
                GenericValue::Varchar("4216948767723528746".into()),
                GenericValue::Varchar("4249601410873011666".into()),
                GenericValue::Varchar("4306544265635989688".into()),
                GenericValue::Varchar("4346428801926007609".into()),
                GenericValue::Varchar("4378199256048562683".into()),
                GenericValue::Varchar("4432054265677051785".into()),
                GenericValue::Varchar("4472506620499555205".into()),
                GenericValue::Varchar("449683977510099902".into()),
                GenericValue::Varchar("4518105711017831993".into()),
                GenericValue::Varchar("4565243953134324620".into()),
                GenericValue::Varchar("4617374297948754701".into()),
                GenericValue::Varchar("4674073777645752410".into()),
                GenericValue::Varchar("4717892712775449163".into()),
                GenericValue::Varchar("4765114531994624473".into()),
                GenericValue::Varchar("4801100051415754824".into()),
                GenericValue::Varchar("4861907410776146213".into()),
                GenericValue::Varchar("4926306170851979261".into()),
                GenericValue::Varchar("4970365137677945421".into()),
                GenericValue::Varchar("4989527803867045424".into()),
                GenericValue::Varchar("5059864525358175145".into()),
                GenericValue::Varchar("5104185976087488498".into()),
                GenericValue::Varchar("5121537529234067814".into()),
                GenericValue::Varchar("516330674606803661".into()),
                GenericValue::Varchar("5194840708074843155".into()),
                GenericValue::Varchar("5242869897471595252".into()),
                GenericValue::Varchar("5289937103685716107".into()),
                GenericValue::Varchar("530719412048393152".into()),
                GenericValue::Varchar("5334111723278632223".into()),
                GenericValue::Varchar("5382392968492049698".into()),
                GenericValue::Varchar("5415132859097597143".into()),
                GenericValue::Varchar("5493240626134706089".into()),
                GenericValue::Varchar("5569869348558516421".into()),
                GenericValue::Varchar("5596880997343382298".into()),
                GenericValue::Varchar("5669346056590191291".into()),
                GenericValue::Varchar("5720532317166890482".into()),
                GenericValue::Varchar("5754581868430942384".into()),
                GenericValue::Varchar("579792355629466266".into()),
                GenericValue::Varchar("5829757778830179168".into()),
                GenericValue::Varchar("5885270149010574528".into()),
                GenericValue::Varchar("5935308853601875072".into()),
                GenericValue::Varchar("5975677814582726158".into()),
                GenericValue::Varchar("6044940066858657433".into()),
                GenericValue::Varchar("6116829621315553087".into()),
                GenericValue::Varchar("611817723703861719".into()),
                GenericValue::Varchar("6172113745083642653".into()),
                GenericValue::Varchar("6215279835448121105".into()),
                GenericValue::Varchar("6269360302604362873".into()),
                GenericValue::Varchar("6343801543522170786".into()),
                GenericValue::Varchar("6389190735304408098".into()),
                GenericValue::Varchar("6391104603929653462".into()),
                GenericValue::Varchar("6453087171424913833".into()),
                GenericValue::Varchar("6496527013599036291".into()),
                GenericValue::Varchar("6554245173561108665".into()),
                GenericValue::Varchar("6589605869362903591".into()),
                GenericValue::Varchar("6635729618900475440".into()),
                GenericValue::Varchar("6688822536995061684".into()),
                GenericValue::Varchar("6725359097393979403".into()),
                GenericValue::Varchar("6738164638561105034".into()),
                GenericValue::Varchar("676663160461110210".into()),
                GenericValue::Varchar("6812209884287209063".into()),
                GenericValue::Varchar("6862258872763400985".into()),
                GenericValue::Varchar("6870121703991519588".into()),
                GenericValue::Varchar("6919993169761775437".into()),
                GenericValue::Varchar("6959463942259046168".into()),
                GenericValue::Varchar("7029964651252881570".into()),
                GenericValue::Varchar("7093736484814701305".into()),
                GenericValue::Varchar("7142456145974986419".into()),
                GenericValue::Varchar("7154893222935656530".into()),
                GenericValue::Varchar("719147164499683752".into()),
                GenericValue::Varchar("7234510929056080953".into()),
                GenericValue::Varchar("7298766634501607589".into()),
                GenericValue::Varchar("7314851661046327745".into()),
                GenericValue::Varchar("7380554294986428671".into()),
                GenericValue::Varchar("7420095024217797677".into()),
                GenericValue::Varchar("7431995489075292870".into()),
                GenericValue::Varchar("7487018446356109884".into()),
                GenericValue::Varchar("7528762881339520252".into()),
                GenericValue::Varchar("7592568913833974580".into()),
                GenericValue::Varchar("7653965050391528958".into()),
                GenericValue::Varchar("7695812055430252290".into()),
                GenericValue::Varchar("7743886360708004089".into()),
                GenericValue::Varchar("774927302498132054".into()),
                GenericValue::Varchar("7806153317721812930".into()),
                GenericValue::Varchar("7845467284843755239".into()),
                GenericValue::Varchar("7869648258971097232".into()),
                GenericValue::Varchar("7948492852229030510".into()),
                GenericValue::Varchar("8002619989059359691".into()),
                GenericValue::Varchar("8010127899626696759".into()),
                GenericValue::Varchar("8058518190900093487".into()),
                GenericValue::Varchar("8094201397447483830".into()),
                GenericValue::Varchar("8156552612278220223".into()),
                GenericValue::Varchar("8217822685400734916".into()),
                GenericValue::Varchar("8256900951607843708".into()),
                GenericValue::Varchar("8334144973840653479".into()),
                GenericValue::Varchar("833805336720126826".into()),
                GenericValue::Varchar("8384373327108829473".into()),
                GenericValue::Varchar("8412315133369625646".into()),
                GenericValue::Varchar("8473863667935968391".into()),
                GenericValue::Varchar("8519982050240373338".into()),
                GenericValue::Varchar("8524734776041826854".into()),
                GenericValue::Varchar("8579659334710038418".into()),
                GenericValue::Varchar("8616809441293369281".into()),
                GenericValue::Varchar("8683866024526186839".into()),
                GenericValue::Varchar("8742887259866916877".into()),
                GenericValue::Varchar("8795515464445976787".into()),
                GenericValue::Varchar("882462547635745377".into()),
                GenericValue::Varchar("8835624592501686292".into()),
                GenericValue::Varchar("8869608618220213571".into()),
                GenericValue::Varchar("8940371677333861754".into()),
                GenericValue::Varchar("8988273352142810835".into()),
                GenericValue::Varchar("8997272563567796572".into()),
                GenericValue::Varchar("9046467394465522107".into()),
                GenericValue::Varchar("9081239282957691906".into()),
                GenericValue::Varchar("9132288050143507885".into()),
                GenericValue::Varchar("916274392732033795".into()),
                GenericValue::Varchar("9196371566731029617".into()),
                GenericValue::Varchar("972489258104233580".into()),
            ]),
            GenericValue::Null,
        ]],
        metadata: Box::new(RowsMetadata {
            flags: RowsMetadataFlags::GLOBAL_TABLE_SPACE,
            columns_count: 40,
            paging_state: None,
            new_metadata_id: None,
            global_table_spec: Some(TableSpec {
                ks_name: "system".into(),
                table_name: "local".into(),
            }),
            col_specs: vec![
                ColSpec {
                    table_spec: None,
                    name: "key".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "bootstrapped".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "broadcast_address".into(),
                    col_type: ColTypeOption {
                        id: ColType::Inet,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "broadcast_port".into(),
                    col_type: ColTypeOption {
                        id: ColType::Int,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "cluster_name".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "cql_version".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
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
                    name: "gossip_generation".into(),
                    col_type: ColTypeOption {
                        id: ColType::Int,
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
                    name: "listen_address".into(),
                    col_type: ColTypeOption {
                        id: ColType::Inet,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "listen_port".into(),
                    col_type: ColTypeOption {
                        id: ColType::Int,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "native_protocol_version".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
                        value: None,
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "partitioner".into(),
                    col_type: ColTypeOption {
                        id: ColType::Varchar,
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
                    name: "rpc_port".into(),
                    col_type: ColTypeOption {
                        id: ColType::Int,
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
                        value: Some(ColTypeOptionValue::CSet(Box::new(ColTypeOption {
                            id: ColType::Varchar,
                            value: None,
                        }))),
                    },
                },
                ColSpec {
                    table_spec: None,
                    name: "truncated_at".into(),
                    col_type: ColTypeOption {
                        id: ColType::Map,
                        value: Some(ColTypeOptionValue::CMap(
                            Box::new(ColTypeOption {
                                id: ColType::Uuid,
                                value: None,
                            }),
                            Box::new(ColTypeOption {
                                id: ColType::Blob,
                                value: None,
                            }),
                        )),
                    },
                },
            ],
        }),
    }
}
