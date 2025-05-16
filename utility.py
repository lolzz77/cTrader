import json
import configparser
import re
import json
import csv
import os
import time

gSymbolData = None
gConfigData = None

def read_symbol_id(symbol_id_to_search, account_type, to_print=False):
    """
    account_type = demo or live
    """
    global gSymbolData
    if gSymbolData is None:
        filename = "symbolList_" + account_type + ".json"
        with open(filename, "r", encoding="utf-8") as json_file:
            gSymbolData = json.load(json_file)  # Load JSON into a dictionary

    # Search for key and output the value
    result = next((item for item in gSymbolData if item.get("symbolId") == symbol_id_to_search), None)

    # Print the result
    if result:
        if to_print:
            print("symbolId:{} symbolName:{}".format(result["symbolId"], result["symbolName"]))
    else:
        print("Key not found in the JSON file.")

    return result

def read_config_file(reload=False):
    global gConfigData
    if (gConfigData is not None) and (reload == False):
        return

    # Initialize the parser
    config = configparser.ConfigParser()

    # Preserve the original case of keys (eg: UPPERCASE, lowercase)
    config.optionxform = str

    # Read the config.ini file
    config.read("config.ini")

    # Convert the configuration into a dictionary
    gConfigData = {key: value for section in config.sections() for key, value in config[section].items()}


def convert_txt_to_json(txt_path, account_type):
    """
    account_type = demo or live
    """
    filename_txt = txt_path
    with open(filename_txt, "r", encoding="utf-8") as file:
        content = file.read()

    # Regular expression pattern to find "symbolId" followed by "symbolName"
    pattern = re.findall(r"symbolId:\s*(\d+)\s*symbolName:\s*\"(.*?)\"", content)

    # Convert extracted data into a structured list
    symbols = [{"symbolId": int(symbol_id), "symbolName": symbol_name} for symbol_id, symbol_name in pattern]

    # Print the result in nicely formatted JSON
    # json_output = json.dumps(symbols, indent=4)
    # print(json_output)

    filename_json = "symbolList_" + account_type + ".json"
    with open(filename_json, "w", encoding="utf-8") as json_file:
        json.dump(symbols, json_file, indent=4)

    print("Data successfully written to symbolist.json!")


def write_csv(data):
    filename = "spread.csv"
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        header = [
            ["symbolId", "symbol", "bid", "ask", "timestamp"]
        ]

        with open(filename, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(header)

    with open(filename, "a", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(data)

    print("Data Written!")
    



class Timer:
    def __init__(self, timeout):
        self.timeout = timeout  # Duration before reset
        self.start_time = time.time()  # Start timer

    def timer_expired(self):
        current_time = time.time()
        if current_time - self.start_time >= self.timeout:
            self.start_time = current_time  # Reset timer
            return True  # Timer expired and reset
        print(f"Timer left {self.timeout - (current_time - self.start_time)}")
        return False  # Timer still running

