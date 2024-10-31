import json
import os
import time
from os.path import dirname, exists
from typing import Set, Dict
from pydantic import BaseSettings


class IPManager:
    def __init__(self):
        self.whitelist: Set[str] = set()
        self.blocklist: Set[str] = set()
        self.block_endpoints: Set[str] = set()
        self.cache_duration: int = Settings().CACHE_DURATION
        self.last_update: float = 0
        self.config: Dict = {}
        self.path = f'{dirname(os.path.realpath(__file__))}/{Settings().CONFIG_FILE}'
        self.ensure_config_exists()

    def update_config(self):
        current_time = time.time()
        if current_time - self.last_update > self.cache_duration:
            if exists(self.path):
                with open(Settings().CONFIG_FILE, 'r') as config_file:
                    self.config = json.load(config_file)
                    self.whitelist = set(self.config.get('whitelist', []))
                    self.blocklist = set(self.config.get('blocklist', []))
                    self.block_endpoints = set(self.config.get('block_endpoints', []))
                    self.cache_duration = self.config.get('cache_duration', Settings().CACHE_DURATION)
            self.last_update = current_time

    def ensure_config_exists(self):
        if not exists(self.path):
            default_config = {
                "whitelist": [],
                "blocklist": [],
                "block_endpoints": [],
                "cache_duration": Settings().CACHE_DURATION
            }
            with open(self.path, 'w') as config_file:
                json.dump(default_config, config_file, indent=4)

    def is_ip_allowed(self, ip: str) -> bool:
        self.update_config()
        return ip in self.whitelist or (ip not in self.blocklist and not self.whitelist)

    def is_ip_whitelisted(self, ip: str) -> bool:
        self.update_config()
        return ip in self.whitelist

    def is_endpoint_blocked(self, endpoint: str):
        return endpoint in self.block_endpoints


class Settings(BaseSettings):
    CONFIG_FILE: str = "ip_config.json"
    CACHE_DURATION: int = 300  # 5 minutes default
