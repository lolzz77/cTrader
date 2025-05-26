# cTrader

# Version
1. Python - 3.10.12

# Quick Start
1. pip3 install -r requirements.txt
2. Create .env file
3. Put the following
```
APP_CLIENT_ID="xxx"
APP_CLIENT_SECRET="xxx"
ACCESS_TOKEN="xxx"
REFRESH_TOKEN="xxx"
CURRENT_ACCOUNT_ID="xxx"
ACCOUNT_TYPE="demo"
```
4. To get the value, go to https://openapi.ctrader.com/apps
5. Click "Credentails"
6. Click "Sandbox"
7. You know how to get the values
8. After that, to start the script, run `python3 main.py`

# Note
1. The heartbeat thing
2. Go to /usr/local/lib/python3.10/dist-packages/ctrader_open_api/tcpProtocol.py
3. Look for `def heartbeat(self):`
4. This sends heartbeat to server
5. And look for `def stringReceived(self, data):`
6. This is triggered everytime you receives message from server

# Termux known issue
1. When you type `sub 41`, it will become `sub 4` and sends command
2. You have to type 1 more char behind, for termux

# Branch
1. `main` - Set entry with spread, set expiry, set lotsize to 0.01
2. `lotsize` - Set lotsize of your desired amount
3. `tpp` - monitor your running position and take partial profit
4. `spread` - monitor spread of asset you desired