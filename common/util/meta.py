import time

from .azureblob import AzureBlob
from .azuretable import AzureTable
from .constant import STATUS_SUCCESS, STATUS_FAIL
from .constant import ModelState

from telemetry import log

from .monitor import thumbprint

def insert_meta(config, subscription, model_key, meta):
    azure_table = AzureTable(config.az_storage_account, config.az_storage_account_key)
    if not azure_table.exists_table(config.az_tsana_meta_table):
        azure_table.create_table(config.az_tsana_meta_table)
    azure_table.insert_or_replace_entity(config.az_tsana_meta_table, subscription, 
            model_key, 
            group_id=meta['groupId'], 
            app_id=meta['instance']['appId'], 
            app_name=meta['instance']['appName'], 
            series_set=str(meta['seriesSets']), 
            inst_name=meta['instance']['instanceName'], 
            inst_id=meta['instance']['instanceId'], 
            para=str(meta['instance']['params']),
            state=ModelState.Training.name,
            timekey=time.time())

# Get a model entity from meta
# Parameters: 
#   config: a dict object which should include AZ_STORAGE_ACCOUNT, AZ_STORAGE_ACCOUNT_KEY, AZ_META_TABLE
#   subscription: a subscription is a name to differenciate a user, could be used for Authorization
#   model_key: The UUID for the model created
# Return: 
#   meta: a Dict object which includes all the column of an model entity
def get_meta(config, subscription, model_key):
    try: 
        azure_table = AzureTable(config.az_storage_account, config.az_storage_account_key)
        if not azure_table.exists_table(config.az_tsana_meta_table):
            raise Exception('Meta table not exists')

        entity = azure_table.get_entity(config.az_tsana_meta_table, subscription, model_key)
        return entity
    except Exception as e: 
        log.error("Get entity error from %s with model_key %s and subscription %s, exception: %s." % (config.az_tsana_meta_table, model_key, subscription, str(e)))
        return None  

# Update a model entity
# Parameters: 
#   config: a dict object which should include AZ_STORAGE_ACCOUNT, AZ_STORAGE_ACCOUNT_KEY, THUMBPRINT, AZ_META_TABLE
#   subscription: a subscription is a name to differenciate a user, could be used for Authorization
#   model_key: The UUID for the model created
#   state: model state
# Return:
#   result: STATUS_SUCCESS / STATUS_FAIL
#   message: description for the result 
def update_state(config, subscription, model_key, state:ModelState=None, context:str=None, last_error:str=None): 
    azure_table = AzureTable(config.az_storage_account, config.az_storage_account_key)
    meta = get_meta(config, subscription, model_key)
    if meta == None or meta['state'] == ModelState.Deleted.name:
        return STATUS_FAIL, 'Model is not found!'

    if state is not None:
        meta['state'] = state.name

    if context is not None:
        meta['context'] = context

    if last_error is not None:
        meta['last_error'] = last_error

    meta['timekey'] = time.time()
    etag = azure_table.insert_or_replace_entity2(config.az_tsana_meta_table, meta)

    log.info("Insert or replace %s to table %s, result: %s." % (model_key, config.az_tsana_meta_table, etag))

    return STATUS_SUCCESS, ''

def get_model_list(config, subscription):
    models = []
    azure_table = AzureTable(config.az_storage_account, config.az_storage_account_key)
    if not azure_table.exists_table(config.az_tsana_meta_table):
        return models
        
    entities = azure_table.get_entities(config.az_tsana_meta_table, subscription)
    
    for entity in entities.items:
        models.append(dict(modelId=entity['RowKey'],
            state=entity['state'] if 'state' in entity else 'UNKNOWN',
            timekey=entity['timekey']))
    return models

# Make sure there is no a dead process is owning the training
# Parameters: 
#   config: a dict object which should include AZ_STORAGE_ACCOUNT, AZ_STORAGE_ACCOUNT_KEY, THUMBPRINT, 
#           AZ_META_TABLE, AZ_MONITOR_TABLE, TSANA_APP_NAME, TRAINING_OWNER_LIFE
#   subscription: a subscription is a name to differenciate a user, could be used for Authorization
#   model_key: The UUID for the model created
#   entity: model entity
# Return:
#   entity: a entity with a correct state
def clear_state_when_necessary(config, subscription, model_key, entity):
    if entity['state'] == ModelState.Training.name:
        azure_table = AzureTable(config.az_storage_account, config.az_storage_account_key)
        if not azure_table.exists_table(config.az_tsana_moniter_table):
            return entity
        
        # Find the training owner in the monitor table and make sure it is alive
        try: 
            monitor_entity = azure_table.get_entity(config.az_tsana_moniter_table, config.tsana_app_name, thumbprint)
        except: 
            monitor_entity = None
        now = time.time()
        
        # Problem is server time sync
        if monitor_entity is None or (now - float(monitor_entity['ping']) > config.training_owner_life): 
            # The owner is dead, then
            # Fix the state
            entity['state'] = ModelState.Failed.name
            update_state(config, subscription, model_key, ModelState.Failed, None, 'Training job dead')
                
    return entity