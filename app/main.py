from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import ALLOWED_ORIGINS
from app.db.core import db_connect, db_disconnect
from app.router.api.v1 import router as api_v1_router
from app.router.auth.v1 import router as auth_v1_router

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

app.include_router(api_v1_router, prefix='/api')
app.include_router(api_v1_router, prefix='/api/v1')
app.include_router(auth_v1_router, prefix='/auth')
app.include_router(auth_v1_router, prefix='/auth/v1')
