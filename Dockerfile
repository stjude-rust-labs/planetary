#################
# Builder Image #
#################

FROM alpine:latest AS builder

# Install the necessary packages and Rust.
RUN apk add --update curl clang openssl-dev pkgconfig
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile minimal

# Add `cargo` to the path.
ENV PATH=/root/.cargo/bin:$PATH

# Set the working directory.
WORKDIR /app

# Add the files needed to build the `planetary` binary.
COPY ./Cargo.toml ./Cargo.lock ./
COPY ./planetary ./planetary

# Build the tool in release mode.
RUN cargo build --release

# Remove debug symbols, if present.
RUN strip target/release/planetary

####################
# Production Image #
####################

FROM alpine:latest

# Set the working directory.
WORKDIR /app

# Copy the binary from the builder.
COPY --from=builder /app/target/release/planetary /usr/local/bin

# Expose the default server port.
EXPOSE 6492

# Set the entrypoint to the built binary.
ENTRYPOINT ["/usr/local/bin/planetary"]

# Set the default arguments.
CMD ["-vvv", "--server-version", "1.0.0"]
