FROM rust:latest as builder

WORKDIR ./shotover-proxy

COPY ./ ./

# Clean out anything already built in the target dir
RUN rm -rf target

RUN cargo build

FROM debian:bullseye-slim

RUN apt-get update -y
    \ && apt-get install glibc-doc -y
    \ && rm -rf /var/lib/apt/lists/*

COPY --from=builder /shotover-proxy/target/debug/shotover-proxy /shotover-proxy

ENTRYPOINT ["./shotover-proxy"]
