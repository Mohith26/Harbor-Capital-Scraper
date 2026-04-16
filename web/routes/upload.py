from fastapi import APIRouter
router = APIRouter(prefix="/upload", tags=["upload"])

@router.get("")
async def upload_page():
    return {"page": "upload", "status": "placeholder"}
