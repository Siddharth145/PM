"""
Vercel entrypoint for FastAPI application.
This file ensures Vercel can find and deploy the FastAPI app.
"""
import sys
from pathlib import Path

# Add parent directory to path to import app module
sys.path.insert(0, str(Path(__file__).parent.parent))

from app import app

__all__ = ["app"]
