 ### ETF Flow Automation Script

1. Project Goal

This script automates the daily task of synchronizing ETF flow data.

It reads a daily data export from Bloomberg (ETF1_uf4xn3oe.xlsx - Worksheet.csv) and automatically updates one or more master tracking spreadsheets (e.g., Flows-2.xlsx - S&P 500 ETF.csv).

The script is designed to be run multiple times per day. It will overwrite any existing data for the current day, ensuring your files are always up-to-date with the latest ETF1 file, but it will never create duplicate daily entries.

2. How It Works

The script performs the following steps:

Load Source Data: It reads the SOURCE_FILE (your ETF1...csv) into memory.

Create Lookup Map: It processes the source file to find the two most important columns: Ticker and  (M USD) (which contains the 1-Day Flow data). It creates a fast lookup map (a Python dictionary) for all tickers, e.g., {"SPY US": -1195.34, "IVV US": 250.0, ...}.

Define Mappings: It uses a user-defined mapping dictionary (e.g., SP500_MAPPING) to know which column in the destination file corresponds to which ticker in the source file.

Load Destination Data: It reads the DESTINATION_FILE (your Flows-2...csv) into memory.

Get Today's Date: It gets the current date (e.g., 2025-11-17).

Build New Data: It builds a data row for today by looking up each ticker from the mapping and finding its flow value in the map.

"Upsert" Logic:

If a row for today's date already exists: The script finds that row and overwrites the flow data in the mapped columns. All other data in the row (like SPx VWAP) is left untouched.

If no row for today's date exists: The script appends the new data as a brand new row at the end of the file.

Save File: It saves the updated data back to the DESTINATION_FILE as a CSV.

3. Requirements & Setup

Before you can run the script, you need two things:

Python 3: If you don't have it, you can download it from python.org.

Pandas Library: This is the library used to read and write spreadsheet data. You can install it by opening a terminal or command prompt and running:

pip install pandas


4. File Structure

For the script to work, all your files must be in the same folder:

/Your-Project-Folder/
  |
  |-- update_sp500_flows.py       (The Python script)
  |-- ETF1_uf4xn3oe.xlsx - Worksheet.csv (Your daily Bloomberg export)
  |-- Flows-2.xlsx - S&P 500 ETF.csv (Your S&P 500 tracking file)
  |-- Flows-2.xlsx - Nasdaq 100 ETF.csv (Your Nasdaq tracking file)
  |-- ... (and so on)


5. How to Use (Daily Workflow)

Download: Download your new daily data file from Bloomberg.

Save: Save it in your project folder, overwriting the old ETF1_uf4xn3oe.xlsx - Worksheet.csv file. The name must match exactly.

Run: Open a terminal (macOS/Linux) or Command Prompt (Windows), navigate to your project folder, and run the script:

python update_sp500_flows.py


Done: The script will print its progress (e.g., "Successfully overwrote data..."). Your Flows-2.xlsx - S&P 500 ETF.csv file is now updated.

6. How to Configure the Script

This is the most important part. You must tell the script what to update. Open the update_sp500_flows.py file in a text editor.

a. File Paths

At the top of the script, make sure these variables match your file names exactly.

SOURCE_FILE = "ETF1_uf4xn3oe.xlsx - Worksheet.csv"
DESTINATION_FILE = "Flows-2.xlsx - S&P 500 ETF.csv"


b. The Mapping

The "brain" of the script is the SP500_MAPPING dictionary.

Key (left side): The exact column name from your DESTINATION_FILE (Flows-2...csv).

Value (right side): The exact ticker name from your SOURCE_FILE (ETF1...csv).

CRITICAL: Column names must be exact, including spaces! SPY  is not the same as SPY.

SP500_MAPPING = {
    # "Column Name in Flows-2...csv": "Ticker in ETF1...csv"
    "IVV ": "IVV US",
    "SPY ": "SPY US",
    "UPRO (3x L)": "UPRO US",
    "SPXL (3x L)": "SPXL US",
    "SPXS (3x S)": "SPXS US",
    "SPXU (3x S)": "SPXU US",
}


If your ETF1...csv file uses a different ticker (e.g., SPY Equity instead of SPY US), you must update the right side of the mapping.

7. How to Add More Sheets (e.g., Nasdaq)

To update more files (like your Nasdaq 100 sheet) at the same time, follow these steps:

Step 1. Define a new File Path and Mapping:
Add these to the configuration section of your script.

# --- Configuration ---
SOURCE_FILE = "ETF1_uf4xn3oe.xlsx - Worksheet.csv"

# --- S&P 500 Config ---
SP500_DEST_FILE = "Flows-2.xlsx - S&P 500 ETF.csv"
SP500_MAPPING = {
    "IVV ": "IVV US",
    "SPY ": "SPY US",
    # ... etc
}

# --- NASDAQ Config (NEW) ---
NASDAQ_DEST_FILE = "Flows-2.xlsx - Nasdaq 100 ETF.csv"
NASDAQ_MAPPING = {
    # "Column Name in Nasdaq...csv": "Ticker in ETF1...csv"
    "QQQ": "QQQ US",
    "QQQM": "QQQM US",
    "TQQQ (3x L)": "TQQQ US",
    "SQQQ (3x S)": "SQQQ US",
}


Step 2. Update the main "Run" section:
Go to the very bottom of the script (if __name__ == "__main__":) and add one more line to call the update function for your new file.

Before:

if __name__ == "__main__":
    print("--- Starting ETF Flow Update (Overwrite/Append) ---")
    flow_lookup_map = create_flow_lookup(SOURCE_FILE)
    
    if flow_lookup_map:
        # Step 2: Update the S&P 500 file
        update_destination_file(SP500_DEST_FILE, SP500_MAPPING, flow_lookup_map)
        
    print("--- Update Process Finished ---")


After (add the new line):

if __name__ == "__main__":
    print("--- Starting ETF Flow Update (Overwrite/Append) ---")
    flow_lookup_map = create_flow_lookup(SOURCE_FILE)
    
    if flow_lookup_map:
        # Step 2: Update the S&P 500 file
        update_destination_file(SP500_DEST_FILE, SP500_MAPPING, flow_lookup_map)
        
        # Step 3: Update the Nasdaq file (NEW LINE)
        update_destination_file(NASDAQ_DEST_FILE, NASDAQ_MAPPING, flow_lookup_map)
        
    print("--- Update Process Finished ---")


Now, when you run the script, it will update both the S&P 500 and Nasdaq files in a single step. You can repeat this for all your files (Bonds, Gold, IBIT, etc.).