# FILE: Makefile
.PHONY: up upk down test fmt lint
up:   ; docker compose up -d --build
upk:  ; docker compose -f docker-compose.kafka.yml up -d --build
down: ; docker compose down -v || true; docker compose -f docker-compose.kafka.yml down -v || true