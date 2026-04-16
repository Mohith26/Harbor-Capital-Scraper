from fastapi import APIRouter
router = APIRouter(prefix="/database", tags=["database"])

@router.get("")
async def database_page():
    return {"page": "database", "status": "placeholder"}
