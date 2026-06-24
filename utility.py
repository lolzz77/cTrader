import json
import configparser
from commentedconfigparser import CommentedConfigParser
import re
from globalpy import GlobalVar, SymbolJsonUpdate
import os
import json
import csv
from enum import Enum

class SymbolJsonUpdate(Enum):
    NO_UPDATE = 1
    HAS_UPDATE = 2

    @classmethod
    def getName(cls, value):
        for key in cls:
            if key.value == value:
                return key.name
        return None

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

def read_symbol_file(account_type, to_print=False):
    """
    account_type = demo or live
    """
    if GlobalVar.g_Symbol_Data_ID_As_Key is not None:
        return

    filename = GlobalVar.SYMBOL_LIST_JSON_FILENAME + account_type + ".json"
    with open(filename, "r", encoding="utf-8") as json_file:
        content = json.load(json_file)  # Load JSON into a dictionary
    # Convert (str) key into (int) key
    GlobalVar.g_Symbol_Data_ID_As_Key = {int(k): v for k, v in content.items()}
    # Swap keys and values
    GlobalVar.g_Symbol_Data_Name_As_Key = {v: k for k, v in GlobalVar.g_Symbol_Data_ID_As_Key.items()}

def read_config_file(reload=False):
    if (GlobalVar.g_Config_Data is not None) and (reload == False):
        return

    # Initialize the parser
    config = configparser.ConfigParser()

    # Preserve the original case of keys (eg: UPPERCASE, lowercase)
    config.optionxform = str

    # Read the config.ini file
    config.read(GlobalVar.CONFIG_FILENAME)

    # Convert the configuration into a dictionary
    GlobalVar.g_Config_Data = {key: value for section in config.sections() for key, value in config[section].items()}

def write_config_file(section, key, value):
    """
    [Biography]
    Name = Ali

    section = Biography
    Key = Name
    Value = Ali
    """
    config = CommentedConfigParser()
    config.optionxform = str  # Preserve case sensitivity
    config.read(GlobalVar.CONFIG_FILENAME)

    existing_value = None

    # Check if the section and key exist before updating
    if config.has_section(section) and config.has_option(section, key):
        existing_value = config.get(section, key)

        # If value is the same, exit function without writing
        if existing_value == str(value):
            # Let's ignore this for now
            # print(f"No update needed: [{key}] is already {value}")
            return

    # Update the value only if different
    config.set(str(section), str(key), str(value))

    # Save the file while keeping comments
    with open(GlobalVar.CONFIG_FILENAME, "w") as file:
        config.write(file)

    print(f"Have New Update:")
    print(f"Before: [{section}] {key} = {existing_value}")
    print(f"After : [{section}] {key} = {value}")

def convert_txt_to_json(txt_path, account_type):
    """
    account_type = demo or live
    Read TXT file
    Convert to JSON
    Before write, compare data same or not
    Write into JSON
    """
    with open(txt_path, "r", encoding="utf-8") as file:
        content = file.read()

    # Regular expression pattern to find "symbolId" followed by "symbolName"
    pattern = re.findall(r"symbolId:\s*(\d+)\s*symbolName:\s*\"(.*?)\"", content)

    # Convert extracted data into a structured dict
    symbols_new_ID_first_dict = {int(symbol_id): symbol_name for symbol_id, symbol_name in pattern}

    # Check if data is same or not, read the existing JSON before writing it
    filename_ID_first_json = GlobalVar.SYMBOL_LIST_JSON_FILENAME + account_type + ".json"
    symbols_old_dict = {}
    with open(filename_ID_first_json, "r", encoding="utf-8") as json_file:
        data_dict = json.load(json_file)

    # Convert (str) key, into (int) key
    symbols_old_dict = {int(k): v for k, v in data_dict.items()}

    # Same data, nothing to update, return
    if symbols_new_ID_first_dict == symbols_old_dict:
        print(f"Symbols ID is up to date")
        return SymbolJsonUpdate.NO_UPDATE

    # Update the JSON file
    with open(filename_ID_first_json, "w", encoding="utf-8") as json_file:
        json.dump(symbols_new_ID_first_dict, json_file, indent=4)

    # Swap keys and values
    GlobalVar.g_Symbol_Data_Name_As_Key = {v: k for k, v in symbols_new_ID_first_dict.items()}

    print(f"Symbol ID has new update! Remember to manually run UpdateSymbolDetail until Auto is implemented!")
    return SymbolJsonUpdate.HAS_UPDATE

def create_record_file(forceCreate = False):
    """
    Check if file exists, if not, create
    """
    if forceCreate:
        os.remove(GlobalVar.RECORD_FILENAME)

    if not os.path.exists(GlobalVar.RECORD_FILENAME):
        with open(GlobalVar.RECORD_FILENAME, 'w') as f:
            f.write('[HEADER]\n')

def read_record_file():
    """
    """
    # Initialize the parser
    config = configparser.ConfigParser()

    # Preserve the original case of keys (eg: UPPERCASE, lowercase)
    config.optionxform = str

    # Read the config.ini file
    config.read(GlobalVar.RECORD_FILENAME)

    GlobalVar.g_Record_Data = config

def write_record_file():
    """
    """
    with open(GlobalVar.RECORD_FILENAME, 'w') as configfile:
        GlobalVar.g_Record_Data.write(configfile)

def populate_favourite_symbol():
    """
    Populate g_favourite_symbol, read from favourite.txt
    In favourite.txt, input symbol name as shown in cTrader
    """

    if GlobalVar.g_Symbol_Data_Name_As_Key is None:
        print(f"populate_favourite_symbol: g_Symbol_Data_Name_As_Key is None, exit.")
        return

    # Always reset it when you re-populate it
    GlobalVar.g_favourite_symbol = {}
    with open("favourite.txt", "r", encoding="utf-8") as f:
        lines = f.readlines()

    for l in lines:
        if l == '\n': # Skip newline, like, end of txt you have a newline right?
            continue
        l = l.strip() # Strip the newline at the end
        id = GlobalVar.g_Symbol_Data_Name_As_Key.get(str(l))
        GlobalVar.g_favourite_symbol[str(l)] = id

def write_csv_spread(data):
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
