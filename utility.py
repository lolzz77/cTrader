import json
import configparser
import re
import json
from enum import Enum

gSymbolData = None
gConfigData = None

SYMBOL_LIST_JSON_FILENAME = "symbolList_"

class SymbolJsonUpdate(Enum):
    NO_UPDATE = 1
    HAS_UPDATE = 2

    @classmethod
    def getName(cls, value):
        for key in cls:
            if key.value == value:
                return key.name
        return None

def read_symbol_id(symbol_id_to_search, account_type, to_print=False):
    """
    account_type = demo or live
    """
    global gSymbolData
    if gSymbolData is None:
        filename = SYMBOL_LIST_JSON_FILENAME + account_type + ".json"
        with open(filename, "r", encoding="utf-8") as json_file:
            content = json.load(json_file)  # Load JSON into a dictionary

    # Convert (str) key into (int) key
    gSymbolData = {int(k): v for k, v in content.items()}

    return gSymbolData[symbol_id_to_search]

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
    symbols_new_ID_first_dict = {int(symbol_id): symbol_name for symbol_id, symbol_name in pattern}

    # Check if data is same or not
    filename_ID_first_json = SYMBOL_LIST_JSON_FILENAME + account_type + ".json"
    # Read existing JSON file
    symbols_old_dict = {}
    with open(filename_ID_first_json, "r", encoding="utf-8") as json_file:
        data_dict = json.load(json_file)

    # Convert (str) key, into (int) key
    symbols_old_dict = {int(k): v for k, v in data_dict.items()}


    if symbols_new_ID_first_dict == symbols_old_dict:
        print(f"Symbols ID is up to date")
        return SymbolJsonUpdate.NO_UPDATE, None

    # Update the JSON file
    with open(filename_ID_first_json, "w", encoding="utf-8") as json_file:
        json.dump(symbols_new_ID_first_dict, json_file, indent=4)

    # Swap keys and values
    symbols_old_NAME_first_dict = {v: k for k, v in symbols_old_dict.items()}

    print("Data successfully written to symbolist.json!")
    return SymbolJsonUpdate.HAS_UPDATE, symbols_old_NAME_first_dict
