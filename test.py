import config as config

CORE_URL = (
    getattr(config, "CORE_URL", "http://localhost:3006/")
    if hasattr(config, "CORE_URL") and config.CORE_URL
    else "http://localhost:3006/"
)

print("CORE_URL", CORE_URL)
