.PHONY: fmt

fmt:
	@go fmt ./...
	@npx prettier --write --print-width 150 "*.{md,yml,yaml}" "**/*.{md,yml,yaml}" --no-error-on-unmatched-pattern
