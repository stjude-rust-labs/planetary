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
COPY ./api ./api
COPY ./db ./db
COPY ./monitor ./monitor
COPY ./orchestrator ./orchestrator
COPY ./server ./server
COPY ./transporter ./transporter

# Build the tool in release mode.
RUN PQ_LIB_STATIC=1 RUSTFLAGS="-lpgcommon -lpgport -lpq -lssl -lcrypto" cargo build --release

# Remove debug symbols, if present.
RUN strip target/release/planetary-api
RUN strip target/release/planetary-monitor
RUN strip target/release/planetary-orchestrator
RUN strip target/release/planetary-transporter

###############################
# Planetary API Service Image #
###############################

FROM alpine:latest AS planetary-api

# Set the working directory.
WORKDIR /app

# Copy the binary from the builder.
COPY --from=builder /app/target/release/planetary-api /usr/local/bin

# Expose the default server port.
EXPOSE 8080

# Set the entrypoint to the built binary.
ENTRYPOINT ["/usr/local/bin/planetary-api"]

# Set the default arguments.
CMD []

###################################
# Planetary Monitor Service Image #
###################################

FROM alpine:latest AS planetary-monitor

# Set the working directory.
WORKDIR /app

# Copy the binary from the builder.
COPY --from=builder /app/target/release/planetary-monitor /usr/local/bin

# Expose the default server port.
EXPOSE 8080

# Set the entrypoint to the built binary.
ENTRYPOINT ["/usr/local/bin/planetary-monitor"]

# Set the default arguments.
CMD []

########################################
# Planetary Orchestrator Service Image #
########################################

FROM alpine:latest AS planetary-orchestrator

# Set the working directory.
WORKDIR /app

# Copy the binary from the builder.
COPY --from=builder /app/target/release/planetary-orchestrator /usr/local/bin

# Expose the default server port.
EXPOSE 8080

# Set the entrypoint to the built binary.
ENTRYPOINT ["/usr/local/bin/planetary-orchestrator"]

# Set the default arguments.
CMD []

################################
# Production Transporter Image #
################################

FROM alpine:latest AS planetary-transporter

# Set the working directory.
WORKDIR /app

# Copy the binary from the builder.
COPY --from=builder /app/target/release/planetary-transporter /usr/local/bin

# Set the entrypoint to the built binary.
ENTRYPOINT ["/usr/local/bin/planetary-transporter"]

# Set the default arguments.
CMD []

#####################################
# Database Migration Image (Diesel) #
#####################################

FROM ubuntu:latest AS planetary-migration

# Install necessary packages for diesel CLI and PostgreSQL.
RUN apt-get update && apt-get install -y curl libpq-dev pkg-config build-essential libssl-dev postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Create and switch to `diesel` user.
RUN useradd -m -u 1001 -s /bin/bash diesel
USER diesel
WORKDIR /home/diesel

# Install Rust.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile minimal

# Add `cargo` to the path.
ENV PATH=/home/diesel/.cargo/bin:$PATH

# Install the `diesel` CLI.
RUN cargo install diesel_cli --no-default-features --features postgres

# Set the working directory.
WORKDIR /app

# Copy the database crate and diesel config.
COPY --chown=diesel:diesel ./db ./db
COPY --chown=diesel:diesel ./diesel.toml ./diesel.toml

# Set the entrypoint.
ENTRYPOINT ["/home/diesel/.cargo/bin/diesel"]
CMD []