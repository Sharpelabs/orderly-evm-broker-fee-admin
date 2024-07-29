import time
from controllers.api import (
    get_broker_users_fees,
    set_broker_default_rate,
    get_broker_users_volumes,
    get_broker_default_rate,
    get_tier,
    set_broker_user_fee,
)
from utils.mylogging import setup_logging
from utils.myconfig import ConfigLoader
import scheduler
from utils.pd import BrokerFee
from decimal import Decimal, getcontext
from web3 import Web3
import os
import requests
from dotenv import load_dotenv
load_dotenv()

alchemy_api_key = os.getenv("ALCHEMY_KEY")

def get_nft_owners_for_collection():
    contract_address = '0x026224A2940bFE258D0dbE947919B62fE321F042'
    base_url = "https://eth-mainnet.alchemyapi.io/v2/"  # Replace with your Alchemy URL
    url = f"{base_url}{alchemy_api_key}/getOwnersForCollection"

    params = {
        'contractAddress': contract_address
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors

        data = response.json()
        print(data)
        print(len(data['ownerAddresses']))
        # # Check if response contains valid data
        if 'ownerAddresses' in data:
            return data['ownerAddresses']
        else:
            raise Exception("Invalid response structure from Alchemy API")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {str(e)}")
        return []

def get_perp_addresses_with_lobsternft():
    broker_id='sharpe_ai'
    base_url = "https://api-evm.orderly.org/v1/get_account"

    addresses = get_nft_owners_for_collection()
    
    results = []
    for address in addresses:
        try:
            url = f"{base_url}?broker_id={broker_id}&address={address}"
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors

            data = response.json()
            print(f"Orderly API response for address {address}:", data)
            if(data['success'] == True):
              results.append({
                  'address': address,
                  'accountId': data['data']["account_id"]
              })

        except requests.exceptions.RequestException as e:
            print(f"An error occurred while checking address {address}: {str(e)}")

    return results

def is_address_in_array(address_to_check, array_of_results):
    # Convert the address to lowercase for case-insensitive comparison
    address_to_check_lower = address_to_check.lower()

    # Iterate through the array of results
    for result in array_of_results:
        # Convert each address in the array to lowercase and compare
        if result['address'].lower() == address_to_check_lower:
            return True

    # Address not found in the array
    return False

logger = setup_logging()
config = ConfigLoader.load_config()


def init_broker_fees():
    # 每次启动，将当前Broker所有用户费率配置情况更新到本地数据库
    _count = 1
    broker_fee = BrokerFee(_type="broker_user_fee")
    while True:
        data = get_broker_users_fees(_count)
        if not data or not data.get("data") or not data["data"].get("rows"):
            break
        if data:
            for _data in data["data"]["rows"]:
                print(_data)
                broker_fee.create_update_user_fee_data(_data, delete_flag=True)
        _count += 1
        time.sleep(2)


def fetch_broker_default_rate():
    get_broker_default_rate()


def update_broker_default_fee(maker_fee, taker_fee):
    url = "/v1/broker/fee_rate/default"
    data = None
    try:
        _data = get_broker_default_rate()
        if _data:
            logger.info(
                f"Modifying Broker Default Fees:  Maker Fee {_data['data']['maker_fee_rate']}->{maker_fee},Taker Fee {_data['data']['taker_fee_rate']}->{taker_fee}"
            )
        set_broker_default_rate(maker_fee, taker_fee)
    except Exception as e:
        logger.error(f"Get Broker Default Fee URL Failed: {url} - {e}")


def update_user_special_rate(account_id, maker_fee, taker_fee):
    _whitelists = config["rate"]["special_rate_whitelists"]
    if "special_rate_whitelists" in config["rate"] and isinstance(
        config["rate"]["special_rate_whitelists"], list
    ):
        if account_id not in _whitelists:
            _whitelists.append(f"{account_id}")
    else:
        logger.info(f"Key '{config['rate']}' not found or is not a list.")
    _data = [
        {
            "account_id": account_id,
            "futures_maker_fee_rate": maker_fee,
            "futures_taker_fee_rate": taker_fee,
        }
    ]
    _ok_count, _fail_count = set_broker_user_fee(_data)
    if _ok_count == 1:
        ConfigLoader.save_config(config)
    logger.info(
        f"Update User's Special Rate: Account ID = {account_id}, Taker Fee = {taker_fee}, Maker Fee = {maker_fee}"
    )


def update_user_rate_base_volume():
    logger.info("Broker user rate update started")
    _count = 1
    user_fee = BrokerFee(_type="broker_user_fee")
    special_rate_whitelists = config["rate"]["special_rate_whitelists"]
    data = []
    lobster_users = get_perp_addresses_with_lobsternft()
    is_lobster_fee_updated = {}
    lobster_maker_fee = 0.00024
    lobster_taker_fee = 0.00054
    while True:
        _data = get_broker_users_volumes(_count)
        if not _data or not _data.get("data") or not _data["data"].get("rows"):
            break
        if _data:
            for _da in _data["data"]["rows"]:
                _user_fee = get_tier(_da["perp_volume"])
                _account_id = _da["account_id"]
                _address = _da["address"]
                if _account_id not in special_rate_whitelists:
                    _new_futures_maker_fee_rate = Decimal(
                        _user_fee["futures_maker_fee_rate"]
                    )
                    _new_futures_taker_fee_rate = Decimal(
                        _user_fee["futures_taker_fee_rate"]
                    )
                    old_user_fee = user_fee.pd.query_data(_account_id)
                    # Lobster NFT logic
                    is_nft_holder = is_address_in_array(_address, lobster_users)
                    if not old_user_fee.empty:
                        _old_futures_maker_fee_rate = Decimal(
                            old_user_fee.futures_maker_fee_rate.values[0]
                        )
                        _old_futures_taker_fee_rate = Decimal(
                            old_user_fee.futures_taker_fee_rate.values[0]
                        )
                        try:
                            if (
                                _new_futures_maker_fee_rate
                                != _old_futures_maker_fee_rate
                                or _new_futures_taker_fee_rate
                                != _old_futures_taker_fee_rate
                            ):
                                maker_fee_rate = _new_futures_maker_fee_rate
                                taker_fee_rate = _new_futures_taker_fee_rate
                                logger.info(
                                    f"{_account_id} - New Maker Fee Rate: {maker_fee_rate}, Smaller Taker Fee Rate: {taker_fee_rate}"
                                )
                                _ret = {}

                                if is_nft_holder and maker_fee_rate > lobster_maker_fee and taker_fee_rate > lobster_taker_fee:
                                    is_lobster_fee_updated[_address.lower()] = {
                                        "updated": True 
                                    }
                                    _ret = {
                                        "account_id": _account_id,
                                        "futures_maker_fee_rate": lobster_maker_fee,
                                        "futures_taker_fee_rate": lobster_taker_fee,
                                        "address": _address,
                                    }
                                else:
                                    _ret = {
                                        "account_id": _account_id,
                                        "futures_maker_fee_rate": maker_fee_rate,
                                        "futures_taker_fee_rate": taker_fee_rate,
                                        "address": _address,
                                    }
                                
                                data.append(_ret)
                                user_fee.create_update_user_fee_data(_ret)
                            status = True
                        except:
                            status = False
                            print(
                                f"New rates are not smaller than old rates: {_account_id}"
                            )
                    else:
                        _ret = {}

                        if is_nft_holder and maker_fee_rate > lobster_maker_fee and taker_fee_rate > lobster_taker_fee:
                            is_lobster_fee_updated[_address.lower()] = {
                                "updated": True 
                            }
                            _ret = {
                                "account_id": _account_id,
                                "futures_maker_fee_rate": lobster_maker_fee,
                                "futures_taker_fee_rate": lobster_taker_fee,
                                "address": _address,
                            }
                        else:
                            _ret = {
                                "account_id": _account_id,
                                "futures_maker_fee_rate": maker_fee_rate,
                                "futures_taker_fee_rate": taker_fee_rate,
                                "address": _address,
                            }
                        data.append(_ret)
                        user_fee.create_update_user_fee_data(_ret)
        _count += 1
        time.sleep(2)
    
    for item in lobster_users:
        if (item["address"].lower() in is_lobster_fee_updated) == False:
            _ret = {
                "account_id": item["accountId"],
                "futures_maker_fee_rate": lobster_maker_fee,
                "futures_taker_fee_rate": lobster_taker_fee,
                "address": item["address"],
            }
            data.append(_ret)
            

    # 5.request the batch fee interface: data
    set_broker_user_fee(data)
    logger.info("Broker user rate update completed")


def update_rate_base_volume():
    logger.info(
        "========================Orderly EVM Broker Fee Admin Startup========================"
    )
    init_broker_fees()
    update_user_rate_base_volume()
