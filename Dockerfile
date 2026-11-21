# Build the native pipeline, then ship only the binary.
FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY engine/ ./
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/shopping /usr/local/bin/shopping
USER 1000:1000
CMD ["/usr/local/bin/shopping"]
