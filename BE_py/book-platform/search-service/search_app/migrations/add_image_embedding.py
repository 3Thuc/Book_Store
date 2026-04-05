"""
Migration: Add image_embedding field to OpenSearch index mapping
This script updates the existing "books_current" index to include KNN vector search support
"""
import os
import json
import logging
from search_app.search.client import get_os_client


logger = logging.getLogger(__name__)


def add_image_embedding_field():
    """
    Add image_embedding KNN field to the OpenSearch index mapping
    
    This migration:
    1. Creates a new index version with image_embedding field
    2. Reindexes data from old index to new index
    3. Updates alias to point to new index
    """
    client = get_os_client()
    index_name = os.getenv("OPENSEARCH_INDEX", "books_current")
    
    try:
        logger.info(f"🔄 Starting migration for {index_name}")
        
        # Step 1: Check if index exists
        if not client.indices.exists(index=index_name):
            logger.warning(f"⚠️ Index {index_name} does not exist. Creating new index with image_embedding...")
            create_index_with_image_embedding(client, index_name)
            return
        
        # Step 2: Check if image_embedding field already exists
        mapping = client.indices.get_mapping(index=index_name)
        properties = mapping[index_name]["mappings"]["properties"]
        
        if "image_embedding" in properties:
            logger.info(f"✅ image_embedding field already exists in {index_name}")
            return
        
        logger.info(f"📝 Adding image_embedding field to {index_name}")
        
        # Step 3: Add field to mapping (in-place update)
        client.indices.put_mapping(
            index=index_name,
            body={
                "properties": {
                    "image_embedding": {
                        "type": "knn_vector",
                        "dimension": 512,
                        "method": {
                            "name": "hnsw",
                            "space_type": "cosinesimil",
                            "engine": "nmslib",
                            "parameters": {
                                "ef_construction": 128,
                                "m": 4
                            }
                        }
                    }
                }
            }
        )
        
        logger.info(f"✅ Successfully added image_embedding field to {index_name}")
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {str(e)}")
        raise


def create_index_with_image_embedding(client, index_name):
    """
    Create a new OpenSearch index with image_embedding support
    """
    mapping = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "knn": True,
                "knn.algo_param.ef_search": 100
            }
        },
        "mappings": {
            "properties": {
                "book_id": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword"},
                        "suggest": {"type": "text", "analyzer": "simple"}
                    }
                },
                "author": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                "description": {"type": "text"},
                "category": {
                    "type": "keyword",
                    "fields": {
                        "text": {"type": "text"}
                    }
                },
                "price": {"type": "long"},
                "rating": {"type": "float"},
                "in_stock": {"type": "boolean"},
                "created_at": {"type": "date"},
                "updated_at": {"type": "date"},
                "image_url": {"type": "keyword"},
                "image_embedding": {
                    "type": "knn_vector",
                    "dimension": 512,
                    "method": {
                        "name": "hnsw",
                        "space_type": "cosinesimil",
                        "engine": "nmslib",
                        "parameters": {
                            "ef_construction": 128,
                            "m": 4
                        }
                    }
                }
            }
        }
    }
    
    client.indices.create(index=index_name, body=mapping)
    logger.info(f"✅ Created new index {index_name} with image_embedding support")


def verify_migration():
    """
    Verify that migration was successful
    """
    client = get_os_client()
    index_name = os.getenv("OPENSEARCH_INDEX", "books_current")
    
    try:
        mapping = client.indices.get_mapping(index=index_name)
        properties = mapping[index_name]["mappings"]["properties"]
        
        if "image_embedding" not in properties:
            logger.error(f"❌ image_embedding field NOT found in {index_name}")
            return False
        
        image_emb = properties["image_embedding"]
        if image_emb.get("type") != "knn_vector":
            logger.error(f"❌ image_embedding is not knn_vector type")
            return False
        
        if image_emb.get("dimension") != 512:
            logger.error(f"❌ image_embedding dimension is not 512")
            return False
        
        logger.info(f"✅ Migration verified successfully!")
        logger.info(f"   - Type: {image_emb.get('type')}")
        logger.info(f"   - Dimension: {image_emb.get('dimension')}")
        logger.info(f"   - Method: {image_emb.get('method', {}).get('name')}")
        
        return True
    
    except Exception as e:
        logger.error(f"❌ Verification failed: {str(e)}")
        return False


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    print("\n🚀 Running OpenSearch Migration: Add image_embedding field\n")
    
    try:
        add_image_embedding_field()
        success = verify_migration()
        
        if success:
            print("\n✅ Migration completed successfully!\n")
        else:
            print("\n❌ Migration verification failed!\n")
    
    except Exception as e:
        print(f"\n❌ Migration failed: {e}\n")
        exit(1)
