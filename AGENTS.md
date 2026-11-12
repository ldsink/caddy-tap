# Project Guidelines

`caddy-tap` is HTTP capture middleware for Caddy v2 and uses Go 1.26.

## Documentation

- `README.md` is the default English README, and `README.zh-CN.md` is the Simplified Chinese version. When updating either version, update the other in the same change. Keep feature descriptions, configuration options, defaults, examples, and limitations consistent. Preserve the language switch links at the top of both files.

## Testing

- Add unit tests for every new feature or behavior change. When fixing a bug, add a regression test that reproduces the original issue.
- Cover normal behavior, edge cases, and relevant failure paths. Depending on the change, consider invalid input, I/O failures, request cancellation, concurrency, and resource cleanup. Verify both the result and the state after a failure.
- Aim for high test coverage. Run `go test -cover ./...` and review uncovered branches affected by the change. For changes involving concurrency or shared state, run `go test -race ./...`.

## Commits

- Create a separate commit for each completed, self-contained change.
- Before every commit, run `make fmt` to format Go, Markdown, and YAML files. Include the formatting changes relevant to your work in the same commit.
