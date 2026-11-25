"""Local API entrypoint: `python main.py` (Docker runs uvicorn directly)."""

import uvicorn

if __name__ == "__main__":
    uvicorn.run("app.api:app", host="0.0.0.0", port=8000)
