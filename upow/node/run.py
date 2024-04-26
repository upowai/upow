import uvicorn
from uvicorn.config import LOGGING_CONFIG

if __name__ == "__main__":
    LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
    uvicorn.run("main:app", host="0.0.0.0", port=3006, reload=True)