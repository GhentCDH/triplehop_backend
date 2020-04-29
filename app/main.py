from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import ALLOWED_ORIGINS
from app.db.core import db_connect, db_disconnect
from app.router.auth.v1 import router as router_auth_v1
from app.router.config.v1 import router as router_config_v1
from app.router.data.v1 import router as router_data_v1

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
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

app.include_router(router_auth_v1, prefix='/auth')
app.include_router(router_auth_v1, prefix='/auth/v1')
app.include_router(router_config_v1, prefix='/config')
app.include_router(router_config_v1, prefix='/config/v1')
app.include_router(router_data_v1, prefix='/data')
app.include_router(router_data_v1, prefix='/data/v1')
