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


    def get_sync_endpoints(self, stream, path, parameters={}):
        current_retry = 0
        page=0
        headers = {
            "accept": "application/json",
            "authorization": f"Basic {self.api_key}",
        }

        if stream == "users":
            url = f"https://public-fr6.engage.diabolocom.com/api/v1{path}?page="

        next_pages = True
        try:
            while next_pages:
                url=f"{url}{page}"
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
                    if not [records["next"]]:
                        next_pages = False
                    if stream == "users":
                        records = [records["users"]]
                    for record in records:
                        LOGGER.info(f"the record to be sent -  {record}")
                        yield record
                    page+=1
                
        
        except Exception as e:
                if current_retry < self.retry:
                    LOGGER.warning(
                        f"I need to sleep 60 s, Because last get call to {url} raised exception : {e}"
                    )
                    time.sleep(60)
                    current_retry = current_retry + 1
                else:
                    raise e


