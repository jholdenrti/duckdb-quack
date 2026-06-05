# Multi-arch Dockerfile for building DuckDB ducklake extension
# Supports linux_arm64 and linux_amd64

FROM ubuntu:22.04 AS builder

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies (including vcpkg requirements)
RUN apt-get update && apt-get install -y \
    build-essential \
    clang \
    cmake \
    ninja-build \
    git \
    python3 \
    ccache \
    curl \
    zip \
    unzip \
    tar \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

ENV CC=clang
ENV CXX=clang++

# Install vcpkg
RUN git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg && \
    /opt/vcpkg/bootstrap-vcpkg.sh
ENV VCPKG_ROOT=/opt/vcpkg
ENV VCPKG_TOOLCHAIN_PATH=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake

# Set up working directory
WORKDIR /workspace

# Copy the source code
COPY . .

# Build the extension in release mode using Ninja for faster builds
ARG OVERRIDE_GIT_DESCRIBE=v1.5.3
RUN OVERRIDE_GIT_DESCRIBE=${OVERRIDE_GIT_DESCRIBE} GEN=ninja make release

# The extension will be in build/release/extension/ducklake/
# Create a minimal output stage
FROM scratch AS export
COPY --from=builder /workspace/build/release/extension/quack/quack.duckdb_extension /quack.duckdb_extension
