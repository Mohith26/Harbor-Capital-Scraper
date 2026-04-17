"""Route aggregation."""
from fastapi import APIRouter
from web.routes.database import router as database_router
from web.routes.upload import router as upload_router
from web.routes.analytics import router as analytics_router
from web.routes.comp_finder import router as comp_finder_router
from web.routes.autocomplete import router as autocomplete_router

api_router = APIRouter()
api_router.include_router(database_router)
api_router.include_router(upload_router)
api_router.include_router(analytics_router)
api_router.include_router(comp_finder_router)
api_router.include_router(autocomplete_router)
