import json
import configparser
from commentedconfigparser import CommentedConfigParser
import re
import json
from enum import Enum

gSymbolData = None # Hold symbolList_demo/live.json data
gSymbolDataSwap = None # Swap the key & value, so i can search wtih symbolName, get their ID
gConfigData = None # Hold config.ini data

SYMBOL_LIST_JSON_FILENAME = "symbolList_"
CONFIG_FILENAME = "config.ini"

class SymbolJsonUpdate(Enum):
    NO_UPDATE = 1
    HAS_UPDATE = 2

    @classmethod
    def getName(cls, value):
        for key in cls:
            if key.value == value:
                return key.name
        return None

def read_symbol_file(account_type, to_print=False):
    """
    account_type = demo or live
    """
    global gSymbolData
    global gSymbolDataSwap

    if gSymbolData is not None:
        return

    filename = SYMBOL_LIST_JSON_FILENAME + account_type + ".json"
    with open(filename, "r", encoding="utf-8") as json_file:
        content = json.load(json_file)  # Load JSON into a dictionary
    # Convert (str) key into (int) key
    gSymbolData = {int(k): v for k, v in content.items()}
    # Swap keys and values
    gSymbolDataSwap = {v: k for k, v in gSymbolData.items()}

def read_config_file(reload=False):
    global gConfigData
    if (gConfigData is not None) and (reload == False):
        return

    # Initialize the parser
    config = configparser.ConfigParser()

    # Preserve the original case of keys (eg: UPPERCASE, lowercase)
    config.optionxform = str

    # Read the config.ini file
    config.read(CONFIG_FILENAME)

    # Convert the configuration into a dictionary
    gConfigData = {key: value for section in config.sections() for key, value in config[section].items()}

def write_config_file(section, key, value):
    """
    [Biography]
    Name = Ali
    
    section = Biography
    Key = Name
    Value = Ali
    """
    global gConfigData

    config = CommentedConfigParser()
    config.read(CONFIG_FILENAME)

    existing_value = None

    # Check if the section and key exist before updating
    if config.has_section(section) and config.has_option(section, key):
        existing_value = config.get(section, key)

        # If value is the same, exit function without writing
        if existing_value == str(value):
            print(f"No update needed: [{section}] {key} is already {value}")
            return

    # Update the value only if different
    config.set(str(section), str(key), str(value))

    # Save the file while keeping comments
    with open(CONFIG_FILENAME, "w") as file:
        config.write(file)

    print(f"Updated:")
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
    global gSymbolDataSwap

    with open(txt_path, "r", encoding="utf-8") as file:
        content = file.read()

    # Regular expression pattern to find "symbolId" followed by "symbolName"
    pattern = re.findall(r"symbolId:\s*(\d+)\s*symbolName:\s*\"(.*?)\"", content)

    # Convert extracted data into a structured dict
    symbols_new_ID_first_dict = {int(symbol_id): symbol_name for symbol_id, symbol_name in pattern}

    # Check if data is same or not, read the existing JSON before writing it
    filename_ID_first_json = SYMBOL_LIST_JSON_FILENAME + account_type + ".json"
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
    gSymbolDataSwap = {v: k for k, v in symbols_new_ID_first_dict.items()}

    print("Data successfully written to symbolist.json!")
    return SymbolJsonUpdate.HAS_UPDATE
