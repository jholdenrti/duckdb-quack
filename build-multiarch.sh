#!/bin/bash
set -e

# Build script for multi-arch Linux DuckDB quack extension
# Builds for linux_arm64 and linux_amd64

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build/artifacts"
EXTENSION_NAME="quack"

echo "=== Multi-arch DuckDB Extension Builder ==="
echo "Building ${EXTENSION_NAME} for linux_arm64 and linux_amd64"
echo ""

# Create output directory
mkdir -p "${BUILD_DIR}"

# Ensure buildx is available and create a builder if needed
if ! docker buildx inspect multiarch-builder >/dev/null 2>&1; then
	echo "Creating docker buildx builder..."
	docker buildx create --name multiarch-builder --use
else
	docker buildx use multiarch-builder
fi

# Function to build for a specific architecture
build_arch() {
	local arch=$1
	local platform=$2
	local suffix=$3

	echo ""
	echo "=== Building for ${arch} (${platform}) ==="

	# Build and export the extension
	docker buildx build \
		--platform "${platform}" \
		--target export \
		--output "type=local,dest=${BUILD_DIR}/tmp_${arch}" \
		--file "${SCRIPT_DIR}/Dockerfile" \
		"${SCRIPT_DIR}"

	# Move and rename the extension with arch suffix
	mv "${BUILD_DIR}/tmp_${arch}/${EXTENSION_NAME}.duckdb_extension" \
		"${BUILD_DIR}/${EXTENSION_NAME}.${suffix}.duckdb_extension"

	# Clean up temp directory
	rmdir "${BUILD_DIR}/tmp_${arch}"

	echo "Built: ${BUILD_DIR}/${EXTENSION_NAME}.${suffix}.duckdb_extension"
}

# Build for both architectures
build_arch "arm64" "linux/arm64" "linux_arm64"
#build_arch "amd64" "linux/amd64" "linux_amd64"

echo ""
echo "=== Build Complete ==="
echo "Extensions are available in: ${BUILD_DIR}"
ls -la "${BUILD_DIR}"
