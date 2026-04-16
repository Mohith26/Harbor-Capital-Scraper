from fastapi import APIRouter
router = APIRouter(prefix="/finder", tags=["finder"])

@router.get("")
async def finder_page():
    return {"page": "finder", "status": "placeholder"}
