import time
import ccxt
import json
from termcolor import cprint
import random
import csv
from locale import atof
import os
import yaml
import sys
import random
import datetime
import base64
import hmac
import hashlib
import requests
import okx.Funding as Funding

def stub_withdraw(address, amount_to_withdrawal, symbolWithdraw, network, exchange):
    cprint(f">>> Stub withdraw ok: {exchange} | {address} | {amount_to_withdrawal} | {symbolWithdraw} | {network}  ", "green")
    transaction_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    write_to_csv(success_file_path, [transaction_time, exchange, network, symbol_withdraw, address, amount_to_withdrawal, "success"])
    try:
        test = 1/0
    except Exception as error:
        write_to_csv(error_file_path, [ transaction_time, exchange, network, symbol_withdraw, address, amount_to_withdrawal, type(error).__name__, str(error)])

def write_to_csv(filename, data):
    with open(filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(data)

def write_csv_header():
    success_header = ["Transaction Time", "Exchange", "Network", "Symbol Withdraw", "Address", "Amount to Withdrawal", "Status"]
    error_header = ["Transaction Time", "Exchange", "Network", "Symbol Withdraw", "Address", "Amount to Withdrawal", "Error Type", "Error Message"]

    with open(success_file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(success_header)

    with open(error_file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(error_header)

def binance_withdraw(address, amount_to_withdrawal, symbol_withdraw, network, exchange):
    transaction_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        account_binance = ccxt.binance({
            'apiKey': API_KEY_BINANCE,
            'secret': API_SECRET_BINANCE,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            }
        })

        #info = account_bybit.fetch_currencies()
        #with open(os.path.join(os.path.dirname(__file__), 'account_binance_info.txt'), 'w') as f:
        #        json.dump(info, f, indent=4)    
        
        account_binance.withdraw(
            code    = symbol_withdraw,
            amount  = amount_to_withdrawal,
            address = address,
            tag     = None,
            params  = {
                "network": network
            }
        )
        cprint(f">>> Succesfull (binance) | {address} | {amount_to_withdrawal}", "green")
        write_to_csv(success_file_path, [transaction_time, exchange, network, symbol_withdraw, address, amount_to_withdrawal, "success"])
    except Exception as error:
        cprint(f">>> Error (binance) | {address} | {type(error).__name__}: {error}", "red")
        write_to_csv(error_file_path, [ transaction_time, exchange, network, symbol_withdraw, address, amount_to_withdrawal, type(error).__name__, str(error)])


def bybit_withdraw(address, amount_to_withdrawal, symbol_withdraw, network, exchange):
    transaction_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        account_bybit = ccxt.bybit({
            'apiKey': API_BYBIT_KEY,
            'secret': API_BYBIT_SECRET,
            'enableRateLimit': True,
            'options': { "defaultType": "spot", "recvWindow": 10000*1000, 'adjustForTimeDifference': True, },
        })

        #account_bybit.verbose = True

        #info = account_bybit.fetch_currencies()
        #with open(os.path.join(os.path.dirname(__file__), 'account_bybit_info.txt'), 'w') as f:
        #        json.dump(info, f, indent=4)    
        
        #bybit_time = account_bybit.fetch_time()
        #my_time = account_bybit.milliseconds()
        
        #account_bybit.options['customTimestamp'] = bybit_time
    
        account_bybit.withdraw(
            code    = symbol_withdraw,
            amount  = amount_to_withdrawal,
            address = address,
            tag     = None,
            params  = {
                "network": network
            }
        )
        cprint(f">>> Succesfull (bybit) | {address} | {amount_to_withdrawal}", "green")
        write_to_csv(success_file_path, [transaction_time, exchange, network, symbol_withdraw, address, amount_to_withdrawal, "success"])
    except Exception as error:
        cprint(f">>> Error (bybit) | {address} | {type(error).__name__}: {error}", "red")
        write_to_csv(error_file_path, [ transaction_time, exchange, network, symbol_withdraw, address, amount_to_withdrawal, type(error).__name__, str(error)])
        

def okex_withdraw(address, amount_to_withdrawal, symbol_withdraw, network, exchange):
    cprint(f">>> Withdraw new okex: {exchange} | {address} | {amount_to_withdrawal} | {symbol_withdraw} | {network}  ", "white")
    transaction_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    flag = "0" 
    fundingAPI = Funding.FundingAPI(API_KEY_OKX, API_SECRET_OKX, API_PASSPHRASE_OKX, False, flag, debug = False)

    def get_min_fee():
        result = fundingAPI.get_currencies()
        data = result['data']
        for item in data:
            if item['chain'] == "ETH-ERC20":
                return item['minFee']
    

    try:
        minfee = get_min_fee()
        result = fundingAPI.withdrawal(ccy=symbol_withdraw,amt=amount_to_withdrawal,dest='4',toAddr=address,fee=minfee, chain=network)
        print(result)
        #if response.status_code == 200:
        #    cprint(f">>> Successful (okx) | {address} | {amount_to_withdrawal}", "green")
        #    write_to_csv(success_file_path, [transaction_time, exchange, network, symbol_withdraw, address, amount_to_withdrawal, "success"])
        #else:
        #    error_message = response.json().get('msg', 'Unknown error')
        #    raise Exception(f"HTTP {response.status_code} {error_message}")

    except Exception as error:
        cprint(f">>> Error (okx) | {address} | {type(error).__name__}: {str(error)}", "red")
        write_to_csv(error_file_path, [transaction_time, exchange, network, symbol_withdraw, address, amount_to_withdrawal, type(error).__name__, str(error)])



network_mappings = {
    "binance": {
        "арби": "ARBITRUM",
        "опти": "OPTIMISM",
        "бейс": "BASE",
        "сол": "SOL",
        "bsc": "BEP20",
        "матик": "MATIC",
        "function": binance_withdraw
    },
    "okex": {
        "арби": "ARBONE",
        "опти": "OPTIMISM",
        "бейс": "Base",
        "сол": "SOL",
        "bsc": "BEP20",
        "матик": "MATIC",
        "ерц20": "ETH-ERC20",
        "function": okex_withdraw
    },
    "bybit": {
        "арби": "ARBI", 
        "опти": "OP", 
        "бейс": "BASE", 
        "сол": "SOL", 
        "function": bybit_withdraw
    }
}

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    logs_dir = os.path.join(os.path.dirname(__file__), 'logs')
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    success_file_path = os.path.join(logs_dir, f"success_{start_time.strftime('%Y-%m-%d_%H-%M-%S')}.csv")
    error_file_path = os.path.join(logs_dir, f"error_{start_time.strftime('%Y-%m-%d_%H-%M-%S')}.csv")


    wallets_file_path = os.path.join(os.path.dirname(__file__), 'wallets.txt')
    cred_file_path = os.path.join(os.path.dirname(__file__), 'settings.txt')

    # Проверка наличия файлов
    if not os.path.exists(wallets_file_path):
        raise FileNotFoundError(f"Wallets file '{wallets_file_path}' is missing.")
    if not os.path.exists(cred_file_path):
        raise FileNotFoundError(f"Settings file '{cred_file_path}' is missing.")

    # Открываем файл
    with open(cred_file_path, 'r') as file:
        data = yaml.safe_load(file)

    # Проверка наличия переменных в файле и их пустые значения
    required_variables = ['API_KEY_BINANCE', 'API_SECRET_BINANCE', 'API_KEY_OKX', 'API_SECRET_OKX', 'API_PASSPHRASE_OKX', 'API_BYBIT_KEY', 'API_BYBIT_SECRET', 'time_sleep_low', 'time_sleep_max']
    missing_variables = [var for var in required_variables if var not in data or not data[var]]

    if missing_variables:
        raise ValueError(f"Missing or empty required variables in settings file: {', '.join(missing_variables)}")

    # Получаем значения переменных
    API_KEY_BINANCE = data['API_KEY_BINANCE']
    API_SECRET_BINANCE = data['API_SECRET_BINANCE']
    API_KEY_OKX = data['API_KEY_OKX']
    API_SECRET_OKX = data['API_SECRET_OKX']
    API_PASSPHRASE_OKX = data['API_PASSPHRASE_OKX']
    API_BYBIT_KEY = data['API_BYBIT_KEY']
    API_BYBIT_SECRET = data['API_BYBIT_SECRET']
    time_sleep_low = data['time_sleep_low']
    time_sleep_max = data['time_sleep_max']
    test_mode = data['test_mode']

    cprint(f"Start. python_ver: {sys.version}, ccxt_ver: {ccxt.__version__}", "white")
    if random.randint(1, 100) <= 20:
        selected_message_encoded = random.choice([
            '0JLRgNC10LzRjyDRgdC80L7RgtGA0LXRgtGMINC40L3RgdGC0LDQs9GA0LDQvA==',
            '0JjQtNC4INC/0L7Qs9C70LDQtNGMINC60L7RiNC60YMs INC+0L3QsCDRg9C20LUg0YHQutGD0YfQsNC10YI=',
            '0J3QtSDQsdGD0LTRjCDQttC+0L/QvtC5Lg==',
            '0KHQtNC10LvQsNC5INGH0LDQuSDQvdCw0LrQvtC90LXRhiwg0YDQsNCx0L7RgtCw0YLRjCDQvdCwINGB0YPRhdGD0Y4g4oCU INC/0L7Qu9C90YvQuSDQv9C40LfQtNC10YYu',
            '0J/QvtGA0LAg0LIg0YHQv9C+0YDRgtC30LDQuy4uLiDRjdGN0Y0s INGF0L7RgtGP INCx0Ysg0LTQviDRhdC+0LvQvtC00LjQu9GM0L3QuNC60LAg0Lgg0L7QsdGA0LDRgtC90L4u IA==',
            '0J/QvtGB0LzQvtGC0YDQuCDQsiDQvtC60L3Qviwg0LLQtNGA0YPQsyDRgtCw0Lwg0YfRgtC+LdGC0L4g0LjQt9C80LXQvdC40LvQvtGB0Ywg0LfQsCDQv9C+0YHQu9C10LTQvdC40LUgMTAg0YfQsNGB0L7Qsi4=',
            '0J3QtSDQs9C+0YDQsdC40YHRjCE='])
        selected_message = base64.b64decode(selected_message_encoded).decode("utf-8")
        print(selected_message)

    write_csv_header()

    with open(wallets_file_path, 'r', encoding='utf-8', newline='') as tsvfile:
        reader = csv.reader(tsvfile, delimiter='\t')
        total_wallets = sum(1 for row in reader)
        tsvfile.seek(0)

        for idx, row in enumerate(reader, start=1):
            if any(not field for field in row):
                cprint(f'Skipping row {idx} due to empty field(s)', 'yellow')
                continue
            wallet = row[0]
            amount_str = row[1].replace(',', '.')
            amount = round(float(amount_str), 8) if amount_str else 0.0
            symbol_withdraw = row[2]
            network = ""
            
            exchange = row[4].lower()
            exchange_data = network_mappings.get(exchange, {})
            
            network_key = row[3].lower()
            network = exchange_data.get(network_key)


            if exchange not in network_mappings:
                cprint(f'Unsupported exchange: "{exchange}". Supported exchanges are: {", ".join(network_mappings.keys())}', "red")
                quit()

            exchange_data = network_mappings[exchange]
            if network_key not in exchange_data:
                cprint(f'Unsupported network: "{network_key}" for exchange {exchange}. Supported networks are: {", ".join(exchange_data.keys())}', "red")
                quit()

            network = exchange_data[network_key]
            cprint(f'Withdrawing by {exchange}: {amount} {symbol_withdraw} -> {wallet}/{network}, wallet {idx} of {total_wallets}', 'white')
            
            
            if test_mode == "yes":
                withdraw_function = stub_withdraw
            else:
                withdraw_function = exchange_data.get("function")

            withdraw_function(wallet, amount, symbol_withdraw, network, exchange)
            
            
            sleep_time = random.randint(time_sleep_low, time_sleep_max)
            cprint(f'Sleeping for {sleep_time} seconds...', 'white')
            time.sleep(sleep_time)

