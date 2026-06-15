.PHONY: build
build:
	docker build . -t ghcr.io/benbanerjeerichards/energy-sync
	docker push ghcr.io/benbanerjeerichards/energy-sync
