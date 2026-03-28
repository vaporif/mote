default:
    @just --list

check: clippy test check-fmt lint

lint: lint-toml check-typos check-nix-fmt lint-actions

fmt: fmt-rust fmt-toml fmt-nix

build *args:
    cargo build --workspace {{args}}

clippy:
    cargo clippy --workspace -- -D warnings

test:
    cargo nextest run --workspace

check-fmt:
    cargo fmt --all -- --check

fmt-rust:
    cargo fmt --all

lint-toml:
    taplo check

fmt-toml:
    taplo fmt

check-nix-fmt:
    alejandra --check flake.nix

fmt-nix:
    alejandra flake.nix

check-typos:
    typos

lint-actions:
    actionlint

run-eth *args:
    cargo run -p glint-node-eth -- node --chain etc/genesis.json {{args}}

run-op *args:
    cargo run -p glint-node-op -- node --chain etc/genesis.json {{args}}

run-sidecar *args:
    cargo run -p glint-db-sidecar -- {{args}}

e2e:
    docker build -f docker/Dockerfile.node -t eth-glint:latest .
    docker build -f docker/Dockerfile.sidecar -t glint-db-sidecar:latest .
    cargo nextest run -p glint-e2e --run-ignored all
