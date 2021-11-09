FROM rust:latest as builder

WORKDIR /shotover-proxy

COPY ./ ./

RUN cargo build --release

FROM debian:bullseye-slim

COPY --from=builder /shotover-proxy/target/release/shotover-proxy /shotover-proxy

ENTRYPOINT ["./shotover-proxy"]
