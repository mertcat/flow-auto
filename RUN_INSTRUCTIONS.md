# How to Run the ETF Flow Sync Script on Mac

## Quick Start

You have **3 ways** to run the script:

---

## Option 1: Double-Click (Easiest)

1. **Double-click** `run.command` in Finder
2. The script will automatically run in Terminal
3. That's it!

> **Note:** The first time you double-click, macOS might ask for permission. Right-click → Open if needed.

---

## Option 2: Terminal (Recommended)

1. **Open Terminal**
2. Navigate to the project folder:
   ```bash
   cd /Users/mertcat/PycharmProjects/flow-auto
   ```
3. Run the script:
   ```bash
   ./run.sh
   ```

---

## Option 3: Direct Python

If you prefer to run Python directly:
```bash
python3 sync_etf_flows.py
```

---

## First Time Setup

If you haven't installed dependencies yet:

```bash
./run.sh --install
```

This will install:
- pandas
- openpyxl
- Any other required packages

---

## What the Script Does

1. ✅ Reads Excel files from `Source/` directory
2. ✅ Updates flow data in `Destination/` directory
3. ✅ Calculates Adjusted Total Flow with multipliers:
   - Long positions (3x L) → multiply by 3
   - Short positions (3x S) → multiply by -3
4. ✅ Updates statistics tables:
   - LAST DAY
   - LAST 5 DAYS
   - LAST 20 DAYS
5. ✅ Saves all changes to the Excel files

---

## Troubleshooting

### "Permission Denied" Error
```bash
chmod +x run.sh run.command
```

### "Python 3 not found"
Install Python 3 from: https://www.python.org/downloads/

### "Module not found" Error
Run the install command:
```bash
./run.sh --install
```

### Excel File is Open
Close the Excel file before running the script.

---

## Scheduling Automatic Runs

To run the script automatically every day:

1. Open **Automator** (Applications → Automator)
2. Create a new **Calendar Alarm**
3. Add **Run Shell Script** action:
   ```bash
   cd /Users/mertcat/PycharmProjects/flow-auto
   ./run.sh > logs/sync_$(date +\%Y\%m\%d_\%H\%M\%S).log 2>&1
   ```
4. Save and set the schedule in Calendar

---

## Need Help?

- Check the log output for error messages
- Ensure Source and Destination directories exist
- Verify Excel files are in the correct directories
- Make sure Excel files are closed before running

---

**Last Updated:** 2025-11-18
