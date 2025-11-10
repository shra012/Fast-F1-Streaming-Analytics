.PHONY: help package upload clean

DIST_DIR ?= dist
PACKAGE_ZIP ?= $(DIST_DIR)/spark_package.zip

help:
	@echo "Spark packaging targets:"
	@echo "  make package   - Build dist/spark_package.zip containing the spark package"
	@echo "  make upload    - Upload package and entrypoints to S3 (requires S3_PREFIX env)"
	@echo "  make clean     - Remove dist artifacts"

$(DIST_DIR):
	mkdir -p $(DIST_DIR)

package: $(DIST_DIR)
	cd .. && zip -qr spark/$(PACKAGE_ZIP) spark -x 'spark/dist/*' 'spark/__pycache__/*' 'spark/*.pyc'
	@echo "Created $(PACKAGE_ZIP)"

upload: package
	@test -n "$(S3_PREFIX)" || (echo "Set S3_PREFIX (e.g. s3://fastf1-artifacts/spark) before running upload"; exit 1)
	aws s3 cp $(PACKAGE_ZIP) $(S3_PREFIX)/spark_package.zip
	aws s3 cp bronze_stream.py $(S3_PREFIX)/bronze_stream.py
	@if [ -f gold_stream.py ]; then \
		aws s3 cp gold_stream.py $(S3_PREFIX)/gold_stream.py; \
		echo "Uploaded gold_stream.py"; \
	else \
		echo "Note: gold_stream.py not found (not yet implemented)"; \
	fi

clean:
	rm -rf $(DIST_DIR)
