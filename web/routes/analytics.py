from fastapi import APIRouter
router = APIRouter(prefix="/analytics", tags=["analytics"])

@router.get("")
async def analytics_page():
    return {"page": "analytics", "status": "placeholder"}
