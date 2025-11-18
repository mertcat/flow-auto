#!/bin/bash
################################################################################
# ETF Flow Sync - Double-Click Version for Mac
#
# This file can be double-clicked from Finder to run the sync script
################################################################################

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Run the main script
./run.sh

# Keep terminal open
echo ""
echo "Press Enter to close this window..."
read
