use super::KafkaSinkCluster;
use crate::{
    frame::{
        Frame,
        kafka::{KafkaFrame, RequestBody},
    },
    message::Message,
};
use kafka_protocol::messages::{
    AddPartitionsToTxnRequest, BrokerId, ConsumerGroupDescribeRequest, DeleteGroupsRequest,
    DeleteRecordsRequest, DescribeGroupsRequest, DescribeLogDirsRequest, DescribeProducersRequest,
    DescribeTransactionsRequest, GroupId, ListGroupsRequest, ListOffsetsRequest,
    ListTransactionsRequest, OffsetFetchRequest, OffsetForLeaderEpochRequest, ProduceRequest,
    TopicName, TransactionalId, add_partitions_to_txn_request::AddPartitionsToTxnTransaction,
    delete_records_request::DeleteRecordsTopic, describe_producers_request::TopicRequest,
    list_offsets_request::ListOffsetsTopic, offset_fetch_request::OffsetFetchRequestGroup,
    offset_for_leader_epoch_request::OffsetForLeaderTopic, produce_request::TopicProduceData,
};
use std::collections::HashMap;

pub trait RequestSplitAndRouter {
    type SubRequests;
    type Request;
    fn get_request_frame(request: &mut Message) -> &mut Self::Request;
    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests>;
    fn reassemble(request: &mut Self::Request, item: Self::SubRequests);
}

pub struct ProduceRequestSplitAndRouter;

impl RequestSplitAndRouter for ProduceRequestSplitAndRouter {
    type Request = ProduceRequest;
    type SubRequests = HashMap<TopicName, TopicProduceData>;

    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests> {
        transform.split_produce_request_by_destination(request)
    }

    fn get_request_frame(request: &mut Message) -> &mut Self::Request {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::Produce(request),
                ..
            })) => request,
            _ => unreachable!(),
        }
    }

    fn reassemble(request: &mut Self::Request, item: Self::SubRequests) {
        request.topic_data = item.into_values().collect();
    }
}

pub struct AddPartitionsToTxnRequestSplitAndRouter;

impl RequestSplitAndRouter for AddPartitionsToTxnRequestSplitAndRouter {
    type Request = AddPartitionsToTxnRequest;
    type SubRequests = Vec<AddPartitionsToTxnTransaction>;

    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests> {
        transform.split_add_partition_to_txn_request_by_destination(request)
    }

    fn get_request_frame(request: &mut Message) -> &mut Self::Request {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::AddPartitionsToTxn(request),
                ..
            })) => request,
            _ => unreachable!(),
        }
    }

    fn reassemble(request: &mut Self::Request, item: Self::SubRequests) {
        request.transactions = item;
    }
}

pub struct ListOffsetsRequestSplitAndRouter;

impl RequestSplitAndRouter for ListOffsetsRequestSplitAndRouter {
    type Request = ListOffsetsRequest;
    type SubRequests = Vec<ListOffsetsTopic>;

    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests> {
        transform.split_list_offsets_request_by_destination(request)
    }

    fn get_request_frame(request: &mut Message) -> &mut Self::Request {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::ListOffsets(request),
                ..
            })) => request,
            _ => unreachable!(),
        }
    }

    fn reassemble(request: &mut Self::Request, item: Self::SubRequests) {
        request.topics = item;
    }
}

pub struct OffsetForLeaderEpochRequestSplitAndRouter;

impl RequestSplitAndRouter for OffsetForLeaderEpochRequestSplitAndRouter {
    type Request = OffsetForLeaderEpochRequest;
    type SubRequests = Vec<OffsetForLeaderTopic>;

    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests> {
        transform.split_offset_for_leader_epoch_request_by_destination(request)
    }

    fn get_request_frame(request: &mut Message) -> &mut Self::Request {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::OffsetForLeaderEpoch(request),
                ..
            })) => request,
            _ => unreachable!(),
        }
    }

    fn reassemble(request: &mut Self::Request, item: Self::SubRequests) {
        request.topics = item;
    }
}

pub struct DeleteRecordsRequestSplitAndRouter;

impl RequestSplitAndRouter for DeleteRecordsRequestSplitAndRouter {
    type Request = DeleteRecordsRequest;
    type SubRequests = Vec<DeleteRecordsTopic>;

    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests> {
        transform.split_delete_records_request_by_destination(request)
    }

    fn get_request_frame(request: &mut Message) -> &mut Self::Request {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::DeleteRecords(request),
                ..
            })) => request,
            _ => unreachable!(),
        }
    }

    fn reassemble(request: &mut Self::Request, item: Self::SubRequests) {
        request.topics = item;
    }
}

pub struct DescribeProducersRequestSplitAndRouter;

impl RequestSplitAndRouter for DescribeProducersRequestSplitAndRouter {
    type Request = DescribeProducersRequest;
    type SubRequests = Vec<TopicRequest>;

    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests> {
        transform.split_describe_producers_request_by_destination(request)
    }

    fn get_request_frame(request: &mut Message) -> &mut Self::Request {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::DescribeProducers(request),
                ..
            })) => request,
            _ => unreachable!(),
        }
    }

    fn reassemble(request: &mut Self::Request, item: Self::SubRequests) {
        request.topics = item;
    }
}

pub struct DeleteGroupsSplitAndRouter;

impl RequestSplitAndRouter for DeleteGroupsSplitAndRouter {
    type Request = DeleteGroupsRequest;
    type SubRequests = Vec<GroupId>;

    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests> {
        transform.split_delete_groups_request_by_destination(request)
    }

    fn get_request_frame(request: &mut Message) -> &mut Self::Request {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::DeleteGroups(request),
                ..
            })) => request,
            _ => unreachable!(),
        }
    }

    fn reassemble(request: &mut Self::Request, item: Self::SubRequests) {
        request.groups_names = item;
    }
}

pub struct ListGroupsSplitAndRouter;

impl RequestSplitAndRouter for ListGroupsSplitAndRouter {
    type Request = ListGroupsRequest;
    type SubRequests = ();

    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        _request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests> {
        transform.split_request_by_routing_to_all_brokers()
    }

    fn get_request_frame(request: &mut Message) -> &mut Self::Request {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::ListGroups(request),
                ..
            })) => request,
            _ => unreachable!(),
        }
    }

    fn reassemble(_request: &mut Self::Request, _item: Self::SubRequests) {
        // No need to reassemble, each ListGroups is an exact clone of the original
    }
}

pub struct ListTransactionsSplitAndRouter;

impl RequestSplitAndRouter for ListTransactionsSplitAndRouter {
    type Request = ListTransactionsRequest;
    type SubRequests = ();

    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        _request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests> {
        transform.split_request_by_routing_to_all_brokers()
    }

    fn get_request_frame(request: &mut Message) -> &mut Self::Request {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::ListTransactions(request),
                ..
            })) => request,
            _ => unreachable!(),
        }
    }

    fn reassemble(_request: &mut Self::Request, _item: Self::SubRequests) {
        // No need to reassemble, each ListTransactions is an exact clone of the original
    }
}

pub struct DescribeLogDirsSplitAndRouter;

impl RequestSplitAndRouter for DescribeLogDirsSplitAndRouter {
    type Request = DescribeLogDirsRequest;
    type SubRequests = ();

    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        _request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests> {
        transform.split_request_by_routing_to_all_brokers()
    }

    fn get_request_frame(request: &mut Message) -> &mut Self::Request {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::DescribeLogDirs(request),
                ..
            })) => request,
            _ => unreachable!(),
        }
    }

    fn reassemble(_request: &mut Self::Request, _item: Self::SubRequests) {
        // No need to reassemble, each DescribeLogDirs is an exact clone of the original
    }
}

pub struct DescribeTransactionsSplitAndRouter;

impl RequestSplitAndRouter for DescribeTransactionsSplitAndRouter {
    type Request = DescribeTransactionsRequest;
    type SubRequests = Vec<TransactionalId>;

    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests> {
        transform.split_describe_transactions_request_by_destination(request)
    }

    fn get_request_frame(request: &mut Message) -> &mut Self::Request {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::DescribeTransactions(request),
                ..
            })) => request,
            _ => unreachable!(),
        }
    }

    fn reassemble(request: &mut Self::Request, item: Self::SubRequests) {
        request.transactional_ids = item;
    }
}

pub struct OffsetFetchSplitAndRouter;

impl RequestSplitAndRouter for OffsetFetchSplitAndRouter {
    type Request = OffsetFetchRequest;
    type SubRequests = Vec<OffsetFetchRequestGroup>;

    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests> {
        transform.split_offset_fetch_request_by_destination(request)
    }

    fn get_request_frame(request: &mut Message) -> &mut Self::Request {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::OffsetFetch(request),
                ..
            })) => request,
            _ => unreachable!(),
        }
    }

    fn reassemble(request: &mut Self::Request, item: Self::SubRequests) {
        request.groups = item;
    }
}

pub struct DescribeGroupsSplitAndRouter;

impl RequestSplitAndRouter for DescribeGroupsSplitAndRouter {
    type Request = DescribeGroupsRequest;
    type SubRequests = Vec<GroupId>;

    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests> {
        transform.split_describe_groups_request_by_destination(request)
    }

    fn get_request_frame(request: &mut Message) -> &mut Self::Request {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::DescribeGroups(request),
                ..
            })) => request,
            _ => unreachable!(),
        }
    }

    fn reassemble(request: &mut Self::Request, item: Self::SubRequests) {
        request.groups = item;
    }
}

pub struct ConsumerGroupDescribeSplitAndRouter;

impl RequestSplitAndRouter for ConsumerGroupDescribeSplitAndRouter {
    type Request = ConsumerGroupDescribeRequest;
    type SubRequests = Vec<GroupId>;

    fn split_by_destination(
        transform: &mut KafkaSinkCluster,
        request: &mut Self::Request,
    ) -> HashMap<BrokerId, Self::SubRequests> {
        transform.split_consumer_group_describe_request_by_destination(request)
    }

    fn get_request_frame(request: &mut Message) -> &mut Self::Request {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::ConsumerGroupDescribe(request),
                ..
            })) => request,
            _ => unreachable!(),
        }
    }

    fn reassemble(request: &mut Self::Request, item: Self::SubRequests) {
        request.group_ids = item;
    }
}
