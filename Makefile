CARGO=cargo
export SODIUM_DISABLE_PIE=1

.PHONY: build
build:
	$(CARGO) build

.PHONY: release
release:
	$(CARGO) build --release

.PHONY: clean
clean:
	$(CARGO) clean.


.PHONY: run
run:
	$(CARGO) run