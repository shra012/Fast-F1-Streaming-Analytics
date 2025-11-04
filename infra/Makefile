.PHONY: help preflight preflight-fresh init plan apply destroy fmt validate lint clean output switch-account

# Default target
help:
	@echo "F1 Streaming Graph Infrastructure - Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  preflight        - Discover existing AWS resources and generate config"
	@echo "  preflight-fresh  - Clean cached discovery and run fresh preflight (use after switching AWS accounts)"
	@echo "  switch-account   - Clean all cached data for account switch"
	@echo "  init             - Initialize Terraform"
	@echo "  plan             - Show Terraform execution plan"
	@echo "  apply            - Apply Terraform changes"
	@echo "  destroy          - Destroy Terraform-managed infrastructure"
	@echo "  fmt              - Format Terraform files"
	@echo "  validate         - Validate Terraform configuration"
	@echo "  lint             - Run tflint on Terraform files"
	@echo "  output           - Show Terraform outputs"
	@echo "  clean            - Remove generated files"
	@echo ""
	@echo "Quick start:"
	@echo "  make preflight && terraform init && terraform apply"
	@echo ""
	@echo "After switching AWS accounts:"
	@echo "  make switch-account && make preflight"

# Run AWS resource discovery
preflight:
	@echo "Discovering existing AWS resources..."
	@echo "This will update terraform.tfvars with VPC ID, VPC CIDR, and minimal EMR settings"
	python3 preflight.py --prefer-existing --use-awscli --update-tfvars
	@echo "Discovery complete. Review generated.auto.tfvars.json and terraform.tfvars"

# Fresh preflight (removes cached discovery first)
preflight-fresh:
	@echo "Cleaning cached discovery data..."
	rm -f generated.auto.tfvars.json
	@echo "Running fresh AWS resource discovery..."
	$(MAKE) preflight

# Clean everything for account switch
switch-account:
	@echo "================================================"
	@echo "Preparing for AWS Account Switch"
	@echo "================================================"
	@echo "Current AWS Account:"
	@aws sts get-caller-identity --query 'Account' --output text 2>/dev/null || echo "Not authenticated"
	@echo ""
	@echo "Cleaning cached resources from previous account..."
	rm -f generated.auto.tfvars.json
	rm -f terraform.tfstate terraform.tfstate.backup
	rm -f .terraform/terraform.tfstate 2>/dev/null || true
	rm -f plan.tfplan 2>/dev/null || true
	@echo "✓ Clean complete!"
	@echo ""
	@echo "Running preflight to discover resources in current account..."
	@$(MAKE) preflight
	@echo ""
	@echo "Re-initializing Terraform for new account..."
	terraform init -reconfigure
	@echo ""
	@echo "================================================"
	@echo "✓ Account switch complete!"
	@echo "Next step: terraform plan"
	@echo "================================================"

# Initialize Terraform
init:
	@echo "Initializing Terraform..."
	terraform init

# Show execution plan
plan: preflight
	@echo "Generating Terraform plan..."
	terraform plan

# Apply changes
apply: preflight
	@echo "Applying Terraform changes..."
	terraform apply && ../scripts/sync_msk_env.sh

# Destroy infrastructure
destroy:
	@echo "Destroying infrastructure..."
	@echo "WARNING: This will destroy all Terraform-managed resources!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		terraform destroy; \
	else \
		echo "Destroy cancelled."; \
	fi

# Format Terraform files
fmt:
	@echo "Formatting Terraform files..."
	terraform fmt -recursive

# Validate Terraform configuration
validate:
	@echo "Validating Terraform configuration..."
	terraform validate

# Run tflint
lint:
	@echo "Running tflint..."
	@if command -v tflint >/dev/null 2>&1; then \
		tflint --init; \
		tflint; \
	else \
		echo "WARNING: tflint not found. Install with: brew install tflint"; \
		exit 1; \
	fi

# Show outputs
output:
	@echo "Terraform outputs:"
	@terraform output

# Clean generated files
clean:
	@echo "Cleaning generated files..."
	rm -f generated.auto.tfvars.json
	rm -f .terraform.lock.hcl
	rm -rf .terraform/
	@echo "Clean complete"
