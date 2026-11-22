# Stage 1: build a fully static binary (build has network; run does not need any).
FROM gcc:13-bookworm AS build
WORKDIR /src
COPY src/main.cpp .
RUN g++ -O3 -march=x86-64-v3 -std=c++17 -pthread -static -s -o /shopsesh main.cpp

# Stage 2: tiny runtime. The binary is static, so nothing else is needed.
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /shopsesh /app/shopsesh
ENV HOME=/scratch
USER 1000
CMD ["/app/shopsesh"]
