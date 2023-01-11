"""Stream class for tap-diabolocom."""

import base64
from typing import Dict, Optional, Any, Iterable
from pathlib import Path
from functools import cached_property
import requests
import time
import json
import urllib.parse
import singer

LOGGER = singer.get_logger()

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class DiabolocomApi(object):
    
    diabolocom_streams=["users"]

    def __init__(
        self,
        api_key,
        url_base= "https://public-fr6.engage.diabolocom.com/api/v1",
        retry=0,
    ):
        self.api_key = api_key,
        self.url_base = url_base,
        self.retry = retry


    def get_sync_endpoints(self, stream, api_key, path, parameters={}):
        current_retry = 0
        page=1
        headers = {
            "Content-Type": "application/json",
            "Private-Token": f"{api_key}",
        }
        next_pages = True
        try:
            while next_pages:
                if stream == "users":
                    url = f"https://public-fr6.engage.diabolocom.com/api/v1{path}?page={page}"
                response = requests.get(url, headers=headers, timeout=60)

                if response.status_code != 200:
                    if current_retry < self.retry:
                        LOGGER.warning(
                            f"Unexpected response status_code {response.status_code} i need to sleep 60s before retry {current_retry}/{self.retry}"
                        )
                        time.sleep(60)
                        current_retry = current_retry + 1
                    else:
                        raise RuntimeError(
                            f"Too many retry, last response status_code {response.status_code} : {response.content}"
                        )
                else:
                    records = json.loads(response.content.decode("utf-8"))
                    page+=1
                    for record in [records["users"]]:
                        LOGGER.info(f"the record to be sent -  {record}")
                        yield record
                    if not records["next"]: 
                        next_pages = False
                
        
        except Exception as e:
                if current_retry < self.retry:
                    LOGGER.warning(
                        f"I need to sleep 60 s, Because last get call to {url} raised exception : {e}"
                    )
                    time.sleep(60)
                    current_retry = current_retry + 1
                else:
                    raise e


