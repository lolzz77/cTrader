import os
import pytz
from enum import Enum

class GlobalVar():
    """
    When this file is included
    This line APP_CLIENT_ID = os.getenv('') will run
    """
    APP_CLIENT_ID       = os.getenv('APP_CLIENT_ID')
    APP_CLIENT_SECRET   = os.getenv('APP_CLIENT_SECRET')
    ACCESS_TOKEN        = os.getenv('ACCESS_TOKEN')
    ACCOUNT_TYPE        = os.getenv('ACCOUNT_TYPE')
    CURRENT_CTIDTRADERACCOUNTID = int(os.getenv('CURRENT_ACCOUNT_ID'))
    g_print_heartbeat   = False # Enable print heartbeat message
    g_my_timezone        = pytz.timezone("Asia/Singapore")

    # This helps me keep track what is the last time_checks i checked
    # If already done, then can skip the market open/close checking shit
    g_time_checks_record    = { "None" : -1 }

    # Input symbol name as shown in cTrader into facourite.txt
    # This will get the symbol name, and append the symbol ID to it
    g_favourite_symbol      = {}

    """
    For command processing
    My rules, the list index contains the following
    [0] - Function name, execute this function
    [1] - parameters to pass to function specified [0]
    [2] - The payload ENUM, this is for handling function that sends requests,
        - It will make sure after running that function specified in [0], handle the incoming message first.
    [3] - For debugging [2] purposes, this holds the comment to tell me the
        - one whom trigger this task to keep waiting for server reply is triggered by whom
        - to the server. If this is set, [0] shall be None
    """
    g_task_queue    = []

    # For those task that sends request to server
    # The server returns data, this holds the data
    g_data_dict     = {}

    # To tell whether can I start user command now
    # Usually i start after connection tasks are finished
    START_USER_COMMAND      = False

    # For user input handling
    # If new print has printed onto console
    # Then ask user to retype their shit
    NEW_PRINT_HAS_HAPPENED  = False

    # To indicate whether is it safe to clear the record.ini records
    # Becos, if cleared, those pending order with lotsize 100 cannot be restored
    CLEAR_RECORD_INI_FILE   = False

    # To control monitor script
    START_MONITOR           = False

    # For my conveniences of `set 1`, `set 2`, set accounts by just typing 1 num
    g_auth_acc                  = []

    g_Symbol_Data_ID_As_Key     = None # eg: {41: "XAUUSD"}
    g_Symbol_Data_Name_As_Key   = None # eg: {"XAUUSD": 41}
    g_Config_Data               = None # Hold config.ini data
    g_Record_Data               = None # Hold record.txt, list of pending orders' lotsize

    SYMBOL_LIST_JSON_FILENAME   = "symbolList_"
    CONFIG_FILENAME             = "config.ini"
    RECORD_FILENAME             = "record.ini"
    GENERATED_PATH              = "./generated/"

class SymbolJsonUpdate(Enum):
    NO_UPDATE = 1
    HAS_UPDATE = 2

    @classmethod
    def getName(cls, value):
        for key in cls:
            if key.value == value:
                return key.name
        return None

class StopLossTakeProfit(Enum):
    """
    # In an order, it has relative stop loss or absolute stop loss
    # You have to choose one side
    """
    RELATIVE = 1
    ABSOLUTE = 2

    @classmethod
    def getName(cls, value):
        for key in cls:
            if key.value == value:
                return key.name
        return None