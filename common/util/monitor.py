import time
import logging
from .azureblob import AzureBlob
from .azuretable import AzureTable

logger = logging.getLogger(__name__)

thumbprint = str(time.time())

def init_monitor(config): 
    azure_table = AzureTable(config.az_storage_account, config.az_storage_account_key)
    if not azure_table.exists_table(config.az_tsana_moniter_table):
        azure_table.create_table(config.az_tsana_moniter_table)
    tk = time.time()
    azure_table.insert_or_replace_entity(config.az_tsana_moniter_table, config.tsana_app_name, 
                        thumbprint, 
                        ping = tk)

def run_monitor(config): 
    azure_table = AzureTable(config.az_storage_account, config.az_storage_account_key)
    if not azure_table.exists_table(config.az_tsana_moniter_table):
        return

    tk = time.time()
    azure_table.insert_or_replace_entity(config.az_tsana_moniter_table, config.tsana_app_name, 
                        thumbprint, 
                        ping = tk)    

def stop_monitor(config):
    logger.info('Monitor exit! ')

    try: 
        azure_table = AzureTable(config.az_storage_account, config.az_storage_account_key)
        if not azure_table.exists_table(config.az_tsana_moniter_table):
            return
        azure_table.delete_entity(config.az_tsana_moniter_table, config.tsana_app_name, 
                            thumbprint)       
    except:
        pass
