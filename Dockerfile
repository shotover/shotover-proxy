FROM rust:bookworm as builder

WORKDIR /shotover-proxy

COPY ./ ./

RUN cargo build -p shotover-proxy --release

FROM debian:bookworm-slim

COPY --from=builder /shotover-proxy/target/release/shotover-proxy /shotover-proxy

ENTRYPOINT ["./shotover-proxy"]
