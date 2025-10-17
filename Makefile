# Makefile for complete project setup (Python + Astro)

# --- Configuration ---
PYTHON_VERSION = 3.13.3
VENV_NAME = .venv
PIP = $(VENV_NAME)/bin/pip

# Phony targets aren't actual files, they are just commands.
.PHONY: setup create-venv install-python-deps install-astro-env help

# --- Main Command ---
# This single command runs all setup steps in the correct order.
setup: create-venv install-python-deps install-astro-env
	@echo "\n✅ All setup steps completed successfully!"
	@echo "Activate the Python environment with: source $(VENV_NAME)/bin/activate"
	@echo "Create your Astro project with: npm create astro@latest"

# --- Python Setup Steps ---
create-venv:
	@echo "--- 1. Setting up Python $(PYTHON_VERSION) virtual environment ---"
	@if ! command -v brew &> /dev/null; then \
		echo "Error: Homebrew is not installed. Please install it first from https://brew.sh/"; \
		exit 1; \
	fi
	@if ! command -v pyenv &> /dev/null; then \
		echo "--> pyenv not found. Installing via Homebrew..."; \
		brew install pyenv; \
	else \
		echo "--> pyenv is already installed."; \
	fi
	@echo "--> Installing Python $(PYTHON_VERSION) using pyenv (if not already installed)..."
	pyenv install $(PYTHON_VERSION) --skip-existing
	@echo "--> Creating virtual environment '$(VENV_NAME)'..."
	"$(shell pyenv root)/versions/$(PYTHON_VERSION)/bin/python" -m venv $(VENV_NAME)
	@echo "--- Virtual environment created successfully. ---"

install-python-deps:
	@echo "--- 2. Installing Python dependencies from requirements.txt ---"
	@if [ ! -f requirements.txt ]; then \
		echo "Error: requirements.txt not found."; \
		exit 1; \
	fi
	$(PIP) install -r requirements.txt
	@echo "--- Python dependencies installed. ---"

# --- Astro Setup Step ---
install-astro-env:
	@echo "--- 3. Setting up Astro development environment ---"
	@echo "--> Installing nvm (Node Version Manager) via Homebrew..."
	brew install nvm
	@echo "--> Installing latest LTS version of Node.js..."
	. "/opt/homebrew/opt/nvm/nvm.sh" && nvm install --lts
	@echo "--- Astro environment setup complete. ---"

# --- Help Command ---
help:
	@echo "Available commands:"
	@echo "  make setup    - Runs all setup tasks for Python and Astro."
	@echo "  make help     - Shows this help message."