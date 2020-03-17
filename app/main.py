from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.router.api.v1 import router as api_v1_router
from app.db.core import db_connect, db_disconnect

app = FastAPI()

# TODO: load from config
origins = [
    'http://local.crdb:3000',
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    await db_connect(app)


@app.on_event("shutdown")
async def shutdown():
    await db_disconnect(app)

app.include_router(api_v1_router, prefix='/v1')
