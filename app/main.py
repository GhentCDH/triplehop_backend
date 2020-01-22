from fastapi import FastAPI

from app.api import entity
from app.db.core import db_connect, db_disconnect

app = FastAPI()

@app.on_event("startup")
async def startup():
    await db_connect(app)
    app.state.config = {}

@app.on_event("shutdown")
async def shutdown():
    await db_disconnect(app)

app.include_router(entity.router)
