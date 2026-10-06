import uvicorn

from fleet.api import create_app

app = create_app()


@app.get("/")
def read_root():
    return {"service": "fleet-mission-orchestrator", "status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
