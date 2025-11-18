#!/bin/bash
################################################################################
# ETF Flow Sync - Mac Run Script
#
# This script runs the ETF flow synchronization process
#
# Usage:
#   ./run.sh                    # Run the sync script
#   ./run.sh --install          # Install dependencies first
################################################################################

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored messages
print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# Function to check if Python 3 is installed
check_python() {
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is not installed"
        print_info "Please install Python 3 from https://www.python.org/downloads/"
        exit 1
    fi

    PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
    print_success "Python $PYTHON_VERSION found"
}

# Function to install dependencies
install_dependencies() {
    print_info "Installing dependencies..."

    if [ ! -f "requirements.txt" ]; then
        print_error "requirements.txt not found"
        exit 1
    fi

    python3 -m pip install --upgrade pip
    python3 -m pip install -r requirements.txt

    if [ $? -eq 0 ]; then
        print_success "Dependencies installed successfully"
    else
        print_error "Failed to install dependencies"
        exit 1
    fi
}

# Function to run the sync script
run_sync() {
    print_info "Starting ETF flow synchronization..."
    echo ""

    # Run the Python script
    python3 sync_etf_flows.py

    EXIT_CODE=$?
    echo ""

    if [ $EXIT_CODE -eq 0 ]; then
        print_success "Synchronization completed successfully!"
    else
        print_error "Synchronization failed with exit code $EXIT_CODE"
        exit $EXIT_CODE
    fi
}

# Main script
echo "════════════════════════════════════════════════════════════════"
echo "  ETF FLOW SYNCHRONIZATION - MAC RUN SCRIPT"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Check for --install flag
if [ "$1" = "--install" ]; then
    check_python
    install_dependencies
    echo ""
    print_info "Installation complete. Run ./run.sh to start synchronization."
    exit 0
fi

# Normal run
check_python

# Check if dependencies are installed
if ! python3 -c "import pandas; import openpyxl" 2>/dev/null; then
    print_warning "Dependencies not installed"
    print_info "Run: ./run.sh --install"
    echo ""
    read -p "Install dependencies now? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        install_dependencies
        echo ""
    else
        print_error "Cannot proceed without dependencies"
        exit 1
    fi
fi

# Run the synchronization
run_sync

# Keep terminal open for a moment
echo ""
print_info "Press any key to exit..."
read -n 1 -s
