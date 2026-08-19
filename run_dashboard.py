"""Dashboard process entrypoint: `python run_dashboard.py`. Runs behind
Caddy (see deploy/Caddyfile) for HTTPS in production; binds to localhost
only by default so it's never reachable except through the reverse proxy.
"""
import os

import uvicorn

if __name__ == "__main__":
    host = os.environ.get("DASHBOARD_HOST", "127.0.0.1")
    port = int(os.environ.get("DASHBOARD_PORT", "8000"))
    uvicorn.run("web.app:app", host=host, port=port, reload=False)
