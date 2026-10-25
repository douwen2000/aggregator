#!/usr/bin/env bash
# Selects which service binary to run based on the first argument.
set -e

case "$1" in
    aggregator)
        exec /app/aggregator_server --feeds sim
        ;;
    bbo)
        exec /app/bbo_publisher --target aggregator:50051
        ;;
    volume)
        exec /app/volume_bands_publisher --target aggregator:50051
        ;;
    price)
        exec /app/price_bands_publisher --target aggregator:50051
        ;;
    *)
        echo "unknown service: $1 (expected aggregator|bbo|volume|price)" >&2
        exit 1
        ;;
esac
