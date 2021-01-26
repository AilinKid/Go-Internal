.PHONY: fmt

FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1 } }'
FILES     := $$(find $$($(PACKAGE_DIRECTORIES)) -name "*.go")

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)