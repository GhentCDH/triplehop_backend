from fastapi import FastAPI

from app.api.v1 import router as api_v1_router
from app.api.entity import router as entity_router
from app.db.core import db_connect, db_disconnect

app = FastAPI()


@app.on_event("startup")
async def startup():
    await db_connect(app)


@app.on_event("shutdown")
async def shutdown():
    await db_disconnect(app)

app.include_router(api_v1_router, prefix='/v1')
app.include_router(entity_router)
