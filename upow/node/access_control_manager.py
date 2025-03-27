import json
import os
import time
from os.path import dirname, exists
from typing import Set, Dict


class AccessControlManager:
    def __init__(self):
        self.whitelist: Set[str] = set()
        self.blocklist: Set[str] = set()
        self.block_endpoints: Set[str] = set()
        self.rate_limits: Dict[str, str] = {}
        self.cache_duration: int = Settings().CACHE_DURATION
        self.last_update: float = 0
        self.config: Dict = {}
        self.stop_sync: bool = False
        self.path = f'{dirname(os.path.realpath(__file__))}/{Settings().CONFIG_FILE}'
        self.ensure_config_exists()

    def update_config(self):
        """ Load the latest config if cache duration has expired. """
        current_time = time.time()
        if current_time - self.last_update > self.cache_duration:
            if exists(self.path):
                with open(self.path, 'r') as config_file:
                    self.config = json.load(config_file)
                    self.whitelist = set(self.config.get('whitelist', []))
                    self.blocklist = set(self.config.get('blocklist', []))
                    self.block_endpoints = set(self.config.get('block_endpoints', []))
                    self.rate_limits = self.config.get('rate_limits', {})
                    self.cache_duration = self.config.get('cache_duration', Settings().CACHE_DURATION)
                    self.stop_sync = self.config.get('stop_sync', False)
            self.last_update = current_time

    def ensure_config_exists(self):
        """ Ensure the config file exists with default values. """
        default_config = {
            "whitelist": [],
            "blocklist": [],
            "block_endpoints": [],
            "rate_limits": {},
            "stop_sync": False,
            "cache_duration": Settings().CACHE_DURATION
        }
        # if not exists(self.path):
        #     with open(self.path, 'w') as config_file:
        #         json.dump(default_config, config_file, indent=4)

        try:
            if exists(self.path):
                with open(self.path, 'r+') as config_file:
                    try:
                        config = json.load(config_file)
                    except json.JSONDecodeError:
                        config = {}  # Handle empty or invalid JSON

                    # Check for missing keys and add them with default values
                    modified = False
                    for key, default_value in default_config.items():
                        if key not in config:
                            config[key] = default_value
                            modified = True

                    if modified:
                        config_file.seek(0)  # Rewind to the beginning of the file
                        json.dump(config, config_file, indent=4)
                        config_file.truncate()  # remove the rest of the file, incase the new json is smaller.
            else:
                with open(self.path, 'w') as config_file:
                    json.dump(default_config, config_file, indent=4)

        except Exception as e:
            print(f"Error handling config file: {e}")

    def is_ip_allowed(self, ip: str) -> bool:
        """ Check if IP is allowed based on whitelist and blocklist. """
        self.update_config()
        return ip in self.whitelist or (ip not in self.blocklist and not self.whitelist)

    def is_ip_whitelisted(self, ip: str) -> bool:
        self.update_config()
        return ip in self.whitelist

    def is_endpoint_blocked(self, endpoint: str):
        return endpoint in self.block_endpoints

    def to_stop_sync(self):
        self.update_config()
        return self.stop_sync

    def get_rate_limit(self, endpoint: str) -> str:
        """ Get the rate limit for a given endpoint, default to 5/minute if not found. """
        self.update_config()
        return self.rate_limits.get(endpoint, "5/minute")  # Default limit


class Settings:
    CONFIG_FILE: str = "access_config.json"
    CACHE_DURATION: int = 300  # 5 minutes default
