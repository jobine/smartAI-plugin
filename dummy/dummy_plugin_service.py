import os
import json
from flask import jsonify, make_response
import uuid
from common.plugin_service import PluginService

class DummyPluginService(PluginService):

    def __init__(self):
        super().__init__()