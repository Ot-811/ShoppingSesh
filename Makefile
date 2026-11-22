IMAGE ?= shopsesh:latest
CPUS ?= 8
MEM ?= 24g
.PHONY: build local fixtures verify
build:
	docker buildx build --platform linux/amd64 --load -t $(IMAGE) .
# native build + the 36 fixtures, using the pure-python Parquet reader in tools/ (no pip needed)
local:
	g++ -O2 -std=c++17 -pthread -o /tmp/ss src/main.cpp
fixtures: local
	BIN=/tmp/ss sh tools/run_fixtures.sh tools/run_cpp.sh
# randomized differential test vs the slow literal reference: make verify SEED=3
SEED ?= 1
verify: local
	python3 tools/gen.py /tmp/v$(SEED) $(SEED) 30000 8000
	python3 tools/reference.py /tmp/v$(SEED) /tmp/v$(SEED).ref
	BIN=/tmp/ss tools/run_cpp.sh /tmp/v$(SEED) /tmp/v$(SEED).cpp
	python3 tools/cmpcsv.py /tmp/v$(SEED).cpp /tmp/v$(SEED).ref && echo MATCH
