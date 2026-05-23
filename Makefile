BIN=bin/scraper
PID_FILE=.run.pid
CONFIG?=./config.yaml
LOG_CONFIG?=./log-config.json

.PHONY: build dev run start stop test clean ensure-db

# Create exchange-year market-data databases (if missing) + apply migrations.
# Defaults to current-year binance/okx. Override with SCRAPER_DBS or
# SCRAPER_EXCHANGES + SCRAPER_YEARS.
# Idempotent: safe to rerun. Honors the same PG* env vars as other services.
ensure-db:
	go run ./cmd/ensure-scraper-db

# scraper 读 ./config.yaml 和 ./log-config.json (硬编码路径),
# 因此所有 run/dev/start 目标都要求 cwd = scraper 目录。
# 根 Makefile 通过 `make -C scraper <target>` 正确地切到这个目录再执行。

build:
	mkdir -p bin
	go build -o $(BIN) ./cmd/scraper

dev:
	go run ./cmd/scraper -config $(CONFIG) -log-config $(LOG_CONFIG)

run: dev

start: build
	mkdir -p logs
	python3 -c 'import subprocess; out=open("logs/scraper.out","ab",buffering=0); p=subprocess.Popen(["./$(BIN)","-config","$(CONFIG)","-log-config","$(LOG_CONFIG)"], stdout=out, stderr=subprocess.STDOUT, start_new_session=True, close_fds=True); open("$(PID_FILE)","w").write(str(p.pid)+"\n")'
	@echo "✓ scraper started (pid=$$(cat $(PID_FILE))), logs at scraper/logs/scraper.out"

stop:
	@if [ -f $(PID_FILE) ]; then kill $$(cat $(PID_FILE)) 2>/dev/null || true; rm -f $(PID_FILE); echo "✓ scraper stopped"; else echo "(no $(PID_FILE), nothing to stop)"; fi

test:
	go test ./...

clean:
	rm -rf bin $(PID_FILE)
