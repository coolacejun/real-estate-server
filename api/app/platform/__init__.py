"""Unified account, credit, mobile commerce, and canonical report APIs."""

from .routes import router
from .archive_routes import router as archive_router

router.include_router(archive_router)

__all__ = ["router"]
