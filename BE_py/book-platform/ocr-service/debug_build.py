"""
Chạy trực tiếp build_hash_index để xem lỗi ở đâu.
"""
import asyncio, sys, logging
sys.path.insert(0, '.')
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

for k in list(sys.modules.keys()):
    if 'similarity' in k or 'ocr_app' in k:
        del sys.modules[k]

async def main():
    try:
        from ocr_app.services.image_similarity_engine import build_hash_index, _fetch_book_images
        print("Test fetch books...")
        books = await _fetch_book_images()
        print(f"Fetched {len(books)} books.")
        
        print("Bắt đầu build_hash_index...")
        count = await build_hash_index(force=True)
        print(f"Hoàn thành: {count} books")
    except Exception as e:
        import traceback
        traceback.print_exc()

asyncio.run(main())
