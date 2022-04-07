use crate::codec::util::packet_parse::{OwnedPcapPacket, PacketHeader, PacketParse, ParsedPacket};
use anyhow::Result;
use pcap::{Active, Capture, Device};
use rayon::prelude::*;
use std::net::IpAddr;
use std::path::Path;

#[derive(Default)]
pub struct PacketCapture {
    err_count: u64,
}

impl PacketCapture {
    pub fn list_devices() {
        let devices: Vec<String> = Device::list()
            .unwrap()
            .iter()
            .map(|val| val.name.clone())
            .collect();
        println!("All Interfaces : ");
        devices.iter().for_each(|val| println!("* {val}"));
    }

    fn print_err(&mut self, err: String) {
        self.err_count += 1;
        eprintln!("ERROR {} : {}", self.err_count, err);
    }

    pub fn save_to_file(&mut self, mut cap_handle: Capture<Active>, file_name: &str) {
        match cap_handle.savefile(&file_name) {
            Ok(mut file) => {
                while let Ok(packet) = cap_handle.next() {
                    file.write(&packet);
                }
            }
            Err(err) => {
                self.print_err(err.to_string());
            }
        }
    }

    pub fn print_to_console(&mut self, mut cap_handle: Capture<Active>) {
        self.print_headers();

        while let Ok(packet) = cap_handle.next() {
            let packet = OwnedPcapPacket::from(packet);
            let packet_parse = PacketParse::default();
            let parsed_packet = packet_parse.parse_packet(&packet);
            match parsed_packet {
                Ok(parsed_packet) => {
                    self.print_packet(&parsed_packet);
                }
                Err(err) => {
                    self.print_err(err.to_string());
                }
            }
        }
    }

    fn print_headers(&self) {
        println!(
            "{0: <25} | {1: <15} | {2: <25} | {3: <15} | {4: <15} | {5: <15} | {6: <35} |",
            "Source IP", "Source Port", "Dest IP", "Dest Port", "Protocol", "Length", "Timestamp"
        );
        println!("{:-^1$}", "-", 165,);
    }

    fn get_packet_meta(&self, parsed_packet: &ParsedPacket) -> (String, String, String, String) {
        let mut src_addr = "".to_string();
        let mut dst_addr = "".to_string();
        let mut src_port = "".to_string();
        let mut dst_port = "".to_string();

        parsed_packet.headers.iter().for_each(|pack| {
            match pack {
                PacketHeader::Tcp(packet) => {
                    src_port = packet.source_port.to_string();
                    dst_port = packet.dest_port.to_string();
                }
                PacketHeader::Udp(packet) => {
                    src_port = packet.source_port.to_string();
                    dst_port = packet.dest_port.to_string();
                }
                PacketHeader::Ipv4(packet) => {
                    src_addr = IpAddr::V4(packet.source_addr).to_string();
                    dst_addr = IpAddr::V4(packet.dest_addr).to_string();
                }
                PacketHeader::Ipv6(packet) => {
                    src_addr = IpAddr::V6(packet.source_addr).to_string();
                    dst_addr = IpAddr::V6(packet.dest_addr).to_string();
                }
                PacketHeader::Arp(packet) => {
                    src_addr = packet.src_addr.to_string();
                    dst_addr = packet.dest_addr.to_string();
                }
                _ => {}
            };
        });

        (src_addr, src_port, dst_addr, dst_port)
    }

    pub fn print_packet(&self, parsed_packet: &ParsedPacket) {
        let (src_addr, src_port, dst_addr, dst_port) = self.get_packet_meta(parsed_packet);
        let protocol = &parsed_packet.headers[0].to_string();
        let length = &parsed_packet.len;
        let ts = &parsed_packet.timestamp;
        println!(
            "{0: <25} | {1: <15} | {2: <25} | {3: <15} | {4: <15} | {5: <15} | {6: <35}",
            src_addr, src_port, dst_addr, dst_port, protocol, length, ts
        );
    }

    pub fn get_packet_details(
        &self,
        parsed_packet: &ParsedPacket,
    ) -> (String, String, String, String, String, u32, String) {
        let (src_addr, src_port, dst_addr, dst_port) = self.get_packet_meta(parsed_packet);
        let protocol = parsed_packet.headers[0].to_string();
        let length = parsed_packet.len;
        let ts = parsed_packet.timestamp.clone();
        (src_addr, src_port, dst_addr, dst_port, protocol, length, ts)
    }

    pub fn parse_from_file(
        &mut self,
        file_name: &Path,
        filter: Option<String>,
    ) -> Vec<Result<ParsedPacket, String>> {
        let mut cap_handle = Capture::from_file(file_name).unwrap();

        if let Some(filter) = filter {
            cap_handle
                .filter(&filter, false)
                .expect("Filters invalid, please check the documentation.");
        }

        let packets: Vec<_> =
            std::iter::from_fn(move || cap_handle.next().ok().map(OwnedPcapPacket::from)).collect();

        packets
            .par_iter()
            .map(|packet| {
                let packet_parse = PacketParse::default();
                packet_parse.parse_packet(packet)
            })
            .collect()
    }
}
