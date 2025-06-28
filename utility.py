import json
import configparser
from commentedconfigparser import CommentedConfigParser
import re
from globalpy import GlobalVar, SymbolJsonUpdate
import os

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
            print(f"No update needed: [{key}] is already {value}")
            return

    # Update the value only if different
    config.set(str(section), str(key), str(value))

    # Save the file while keeping comments
    with open(GlobalVar.CONFIG_FILENAME, "w") as file:
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

def populate_favourite_symbol():
    """
    Populate g_favourite_symbol
    """
    GlobalVar.g_favourite_symbol = []
    for key, value in GlobalVar.g_Config_Data.items():
        if key.startswith("SPREAD_"):
            # Append what's after "SPREAD_", that's their symbol name
            GlobalVar.g_favourite_symbol.append(key[len("SPREAD_"):])
