from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi_jwt_auth import AuthJWT
from pydantic import BaseModel

from app.config import ALLOWED_ORIGINS, SECRET_KEY
from app.db.core import db_connect, db_disconnect
from app.es.core import es_connect, es_disconnect
from app.router.auth.v1 import router as router_auth_v1
from app.router.config.v1 import router as router_config_v1
from app.router.data.v1 import router as router_data_v1
from app.router.es.v1 import router as router_es_v1
from app.router.job.v1 import router as router_job_v1

app = FastAPI()


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    print(exc.errors())
    print(exc.body)
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": exc.errors(), "body": exc.body}),
    )


app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Settings(BaseModel):
    authjwt_secret_key: str = SECRET_KEY


@AuthJWT.load_config
def get_config():
    return Settings()


@app.on_event("startup")
async def startup():
    await db_connect(app)
    es_connect(app)


@app.on_event("shutdown")
async def shutdown():
    await db_disconnect(app)
    await es_disconnect(app)


app.include_router(router_auth_v1, prefix="/auth")
app.include_router(router_auth_v1, prefix="/auth/v1")
app.include_router(router_config_v1, prefix="/config")
app.include_router(router_config_v1, prefix="/config/v1")
app.include_router(router_data_v1, prefix="/data")
app.include_router(router_data_v1, prefix="/data/v1")
app.include_router(router_es_v1, prefix="/es")
app.include_router(router_es_v1, prefix="/es/v1")
app.include_router(router_job_v1, prefix="/job")
app.include_router(router_job_v1, prefix="/job/v1")
