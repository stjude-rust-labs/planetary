#################
# Builder Image #
#################

FROM alpine:latest AS builder
ARG TARGETPLATFORM

# Install the necessary packages and Rust.
RUN apk add --update curl clang openssl-libs-static libpq-dev
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile minimal

# Add `cargo` to the path.
ENV PATH=/root/.cargo/bin:$PATH

# Set the working directory.
WORKDIR /app

# Add the files needed to build the `planetary` and `transporter` binaries.
COPY ./Cargo.toml ./Cargo.lock ./
COPY ./cloud ./cloud
COPY ./planetary-db ./planetary-db
COPY ./planetary ./planetary
COPY ./planetary-transporter ./planetary-transporter

# Build the tool in release mode.
RUN PQ_LIB_STATIC=1 RUSTFLAGS="-lpgcommon -lpgport -lpq -lssl -lcrypto" cargo build --release

# Remove debug symbols, if present.
RUN strip target/release/planetary
RUN strip target/release/transporter

################################
# Production Transporter Image #
################################

FROM alpine:latest AS transporter

# Set the working directory.
WORKDIR /app

# Copy the binary from the builder.
COPY --from=builder /app/target/release/transporter /usr/local/bin

# Set the entrypoint to the built binary.
ENTRYPOINT ["/usr/local/bin/transporter"]

# Set the default arguments.
CMD []

##############################
# Production Planetary Image #
##############################

FROM alpine:latest AS planetary

# Set the working directory.
WORKDIR /app

# Copy the binary from the builder.
COPY --from=builder /app/target/release/planetary /usr/local/bin

# Expose the default server port.
EXPOSE 6492

# Set the entrypoint to the built binary.
ENTRYPOINT ["/usr/local/bin/planetary"]

# Set the default arguments.
CMD ["-v"]
