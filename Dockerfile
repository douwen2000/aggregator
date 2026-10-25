# Build the aggregator + clients. Uses the official gRPC C++ image which ships
# a matching protoc / grpc_cpp_plugin and working CMake configs.
FROM grpc/cpp:1.62.0 AS build

WORKDIR /src
COPY . /src

RUN mkdir -p /src/build && cd /src/build \
    && cmake -DCMAKE_BUILD_TYPE=Release .. \
    && make -j"$(nproc)"

# Runtime image just needs the gRPC/stdlib libs (already present in base image).
FROM grpc/cpp:1.62.0

WORKDIR /app
COPY --from=build /src/build/aggregator_server /app/aggregator_server
COPY --from=build /src/build/bbo_publisher        /app/bbo_publisher
COPY --from=build /src/build/volume_bands_publisher /app/volume_bands_publisher
COPY --from=build /src/build/price_bands_publisher  /app/price_bands_publisher
COPY docker_entrypoint.sh /app/docker_entrypoint.sh
RUN chmod +x /app/docker_entrypoint.sh

ENTRYPOINT ["/app/docker_entrypoint.sh"]
# Default; overridden per-service in docker-compose.yml
CMD ["aggregator"]
