use super::KafkaSinkCluster;
use crate::{
    frame::{
        kafka::{KafkaFrame, RequestBody},
        Frame,
    },
    message::Message,
};
use kafka_protocol::messages::{
    add_partitions_to_txn_request::AddPartitionsToTxnTransaction,
    list_offsets_request::ListOffsetsTopic, offset_fetch_request::OffsetFetchRequestGroup,
    offset_for_leader_epoch_request::OffsetForLeaderTopic, produce_request::TopicProduceData,
    AddPartitionsToTxnRequest, BrokerId, DeleteGroupsRequest, GroupId, ListOffsetsRequest,
    OffsetFetchRequest, OffsetForLeaderEpochRequest, ProduceRequest, TopicName,
};
use std::collections::HashMap;

pub trait RequestSplitAndRouter {
    type SubRequests;
    type Request;
    fn get_request_frame(request: &mut Message) -> Option<&mut Self::Request>;
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

    fn get_request_frame(request: &mut Message) -> Option<&mut Self::Request> {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::Produce(request),
                ..
            })) => Some(request),
            _ => None,
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

    fn get_request_frame(request: &mut Message) -> Option<&mut Self::Request> {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::AddPartitionsToTxn(request),
                ..
            })) => Some(request),
            _ => None,
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

    fn get_request_frame(request: &mut Message) -> Option<&mut Self::Request> {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::ListOffsets(request),
                ..
            })) => Some(request),
            _ => None,
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

    fn get_request_frame(request: &mut Message) -> Option<&mut Self::Request> {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::OffsetForLeaderEpoch(request),
                ..
            })) => Some(request),
            _ => None,
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

    fn get_request_frame(request: &mut Message) -> Option<&mut Self::Request> {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::DeleteGroups(request),
                ..
            })) => Some(request),
            _ => None,
        }
    }

    fn reassemble(request: &mut Self::Request, item: Self::SubRequests) {
        request.groups_names = item;
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

    fn get_request_frame(request: &mut Message) -> Option<&mut Self::Request> {
        match request.frame() {
            Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::OffsetFetch(request),
                ..
            })) => Some(request),
            _ => None,
        }
    }

    fn reassemble(request: &mut Self::Request, item: Self::SubRequests) {
        request.groups = item;
    }
}
