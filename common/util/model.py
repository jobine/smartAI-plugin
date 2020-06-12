import time
from time import gmtime, strftime
import traceback
from werkzeug.utils import secure_filename
import re
import os
from os.path import basename
import sys
import logging
import shutil
import zipfile
import json

from .azureblob import AzureBlob
from .azuretable import AzureTable
from .timeutil import get_time_offset, str_to_dt, dt_to_str
from .constant import TIMESTAMP, VALUE
from .constant import STATUS_SUCCESS, STATUS_FAIL

logger = logging.getLogger(__name__)
# Copy the model file to local prd directory, zip and upload to AzureBlob
# The training output and prd directory is calculated by config with subscription/model_key
# Parameters:
#   config: a dict object which should include MODEL_TMP_DIR, TSANA_APP_NAME, AZ_BLOB_CONNECTION
#   subscription: the name of the user
#   model_key: UUID for the model
# Return:
#   result: STATE_SUCCESS / STATE_FAIL
#   message: description of the result
def copy_tree_and_zip_and_update_remote(config, subscription, model_key, timekey):
    try:
        src = os.path.join(config.model_temp_dir, subscription + '_' + model_key + '_' + str(timekey))
        dest = model_dir = os.path.join(config.model_temp_dir, subscription + '_' + model_key)
        os.makedirs(dest, exist_ok=True)
        
        zip_file = os.path.join(dest, "model.zip")
        with zipfile.ZipFile(zip_file, "w") as zf:
            src_files = os.listdir(src)
            tkfile = os.path.join(dest, "timekey.txt")
            with open(tkfile, 'w') as timekey_file:
                timekey_file.write(str(timekey))

            file_exist = False
            for file_name in src_files:
                full_file_name = os.path.join(src, file_name)
                if os.path.isfile(full_file_name):
                    shutil.copy(full_file_name, dest)
                    full_dest_name = os.path.join(dest, file_name)
                    zf.write(full_dest_name, basename(full_dest_name))
                else: 
                    dest_full = os.path.join(dest, file_name)
                    shutil.rmtree(dest_full, ignore_errors = True)
                    shutil.copytree(full_file_name, dest_full)
                    for root, dirs, files in os.walk(dest_full):
                        #NOTE: ignore empty directories
                        for fn in files:
                            absfn = os.path.join(root, fn)
                            print(absfn)
                            zfn = absfn[len(dest)+len(os.sep):]
                            zf.write(absfn, zfn)
                    # zf.write(full_file_name, basename(full_file_name))

            file_exist = True
            zf.write(tkfile, basename(tkfile))
            zf.close()

        if file_exist is True: 
            # Write timekey file

            container_name = config.tsana_app_name
            azure_blob = AzureBlob(config.az_tsana_model_blob_connection)
            print("------")
            print(zip_file)
            print(container_name)
            print(subscription + '_' + model_key)
            azure_blob.create_container(container_name)

            with open(zip_file, "rb") as data:
                azure_blob.upload_blob(container_name, subscription + '_' + model_key, data)
            return STATUS_SUCCESS, ''
        else:
            shutil.rmtree(tkfile, ignore_errors = True)
            shutil.rmtree(zip_file, ignore_errors = True)
            return STATUS_FAIL, 'No model file is found! '
    except Exception as e:
        logger.exception("-----Exception-----")
        shutil.rmtree(tkfile, ignore_errors = True)
        shutil.rmtree(zip_file, ignore_errors = True)
        
        return STATUS_FAIL, str(e)


# Check if local prd directory contains up-to-date model, otherwise, try to download from blob
# Parameters:
#   config: a dict object which should include MODEL_TMP_DIR, TSANA_APP_NAME, AZ_BLOB_CONNECTION
#   subscription: the name of the user
#   model_key: UUID for the model
# Return:
#   result: STATE_SUCCESS / STATE_FAIL
#   message: description of the result

def prepare_model(config, subscription, model_key, timekey, force = False): 
    prd_dir = os.path.join(config.model_temp_dir, subscription + '_' + model_key)
    tkfile = os.path.join(prd_dir, "timekey.txt")
    tk = ''
    try: 
        try: 
            with open(tkfile, 'r') as tk_file: 
                tk = tk_file.read()
        except:
            logger.info("No timekey file is found.")
            pass

        if tk == str(timekey) and force is not True: 
            # up to data
            return STATUS_SUCCESS, ''
        else: 
            # download from blob
            container_name = config.tsana_app_name
            azure_blob = AzureBlob(config.az_tsana_model_blob_connection)
            azure_blob.create_container(container_name)
            model_name = subscription + '_' + model_key
            try:
                shutil.rmtree(prd_dir)
            except: 
                pass
            os.makedirs(prd_dir, exist_ok=True)
            namelist = azure_blob.list_blob(container_name)
            if model_name in azure_blob.list_blob(container_name):
                logger.info("Download model %s from Azure." % model_key)
                zip_file = os.path.join(prd_dir, "model.zip")
                azure_blob.download_blob(container_name, model_name, zip_file)
                with zipfile.ZipFile(zip_file) as zf:
                    zf.extractall(path = prd_dir)
                    zf.close()
                return STATUS_SUCCESS, ''
            else:
                return STATUS_FAIL, 'There is no valid model'         
    except Exception as e: 
        logger.exception("-----Exception-----")
        return STATUS_FAIL, str(e)

