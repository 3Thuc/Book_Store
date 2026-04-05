"""
Image Search Router - HTTP endpoints for image-based book search
"""
import logging
from fastapi import APIRouter, File, UploadFile, Query
from fastapi.responses import JSONResponse
from typing import Optional

from search_app.search.image_search_service import get_image_search_service


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/books", tags=["image-search"])


@router.post("/search-by-image")
async def search_by_image(
    image: UploadFile = File(...),
    k: int = Query(20, ge=1, le=100, description="Number of results"),
    category: Optional[str] = Query(None, description="Filter by category"),
    in_stock: bool = Query(False, description="Filter by stock status"),
    min_price: Optional[int] = Query(None, description="Minimum price filter"),
    max_price: Optional[int] = Query(None, description="Maximum price filter")
):
    """
    Search for similar books by uploading an image
    
    **Parameters:**
    - `image`: Image file (JPG, PNG, WebP)
    - `k`: Number of results (default: 20, max: 100)
    - `category`: Optional category filter
    - `in_stock`: Filter for in-stock books only
    - `min_price`, `max_price`: Price range filter
    
    **Returns:**
    ```json
    {
      "success": true,
      "total": 20,
      "count": 20,
      "books": [
        {
          "book_id": "001",
          "title": "Harry Potter and the Philosopher's Stone",
          "author": "J.K. Rowling",
          "price": 150000,
          "category": "Fantasy",
          "image_url": "minio://...",
          "rating": 4.8,
          "in_stock": true,
          "similarity_score": 0.92,
          "similarity_percentage": 92.0
        },
        ...
      ],
      "took_ms": 515
    }
    ```
    
    **Workflow:**
    1. Upload image file
    2. Generate CLIP embedding (150ms)
    3. Query OpenSearch KNN index (250ms)
    4. Return top-k similar books with similarity scores
    """
    try:
        # Validate image file
        if not image.filename:
            return JSONResponse(
                status_code=400,
                content={"error": "No image provided"}
            )
        
        # Validate file extension
        allowed_extensions = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
        file_ext = f".{image.filename.split('.')[-1].lower()}"
        if file_ext not in allowed_extensions:
            return JSONResponse(
                status_code=400,
                content={"error": f"Invalid file type. Allowed: {', '.join(allowed_extensions)}"}
            )
        
        logger.info(f"📥 Received image: {image.filename} (size: {image.size} bytes)")
        
        # Read image bytes
        image_bytes = await image.read()
        
        # Validate image size (max 10MB)
        if len(image_bytes) > 10 * 1024 * 1024:
            return JSONResponse(
                status_code=400,
                content={"error": "Image file too large (max: 10MB)"}
            )
        
        # Prepare filters
        filters = {}
        if category:
            filters["category"] = category
        if in_stock:
            filters["in_stock"] = True
        if min_price is not None:
            filters["min_price"] = min_price
        if max_price is not None:
            filters["max_price"] = max_price
        
        # Search
        service = get_image_search_service()
        results = service.search_by_image(image_bytes, k=k, filters=filters)
        
        logger.info(f"✅ Image search completed: {len(results['books'])} results")
        
        return results
    
    except ValueError as e:
        logger.error(f"❌ Validation error: {str(e)}")
        return JSONResponse(
            status_code=400,
            content={"error": str(e)}
        )
    
    except Exception as e:
        logger.error(f"❌ Search error: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error during image search"}
        )


@router.get("/search-by-image/health")
async def health_check():
    """
    Health check endpoint for image search service
    
    **Returns:**
    ```json
    {
      "status": "healthy",
      "clip_model": "openai/clip-vit-base-patch32",
      "image_embedding_dimension": 512
    }
    ```
    """
    try:
        service = get_image_search_service()
        clip_service = service.clip_service
        
        return {
            "status": "healthy",
            "clip_model": clip_service.model_name,
            "device": clip_service.device,
            "image_embedding_dimension": 512
        }
    
    except Exception as e:
        logger.error(f"❌ Health check failed: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"status": "unhealthy", "error": str(e)}
        )
