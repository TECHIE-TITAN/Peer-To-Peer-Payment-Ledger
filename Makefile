.PHONY: proto test test-raft fuzz-raft lint format

proto:
	python -m grpc_tools.protoc -Iledgernet/gen=proto --python_out=. --grpc_python_out=. --pyi_out=. proto/ledgernet.proto

test:
	pytest -q

test-raft:
	pytest -q tests/raft

# Long randomized Raft runs (hundreds of seeds); not part of the quick suite.
fuzz-raft:
	LEDGERNET_FUZZ=1 pytest -q tests/raft -m fuzz

lint:
	ruff check .
	black --check .

format:
	black .
	ruff check --fix .
