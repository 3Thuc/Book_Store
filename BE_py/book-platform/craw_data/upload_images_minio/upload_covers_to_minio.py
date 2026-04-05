"""
upload_covers_to_minio.py – Upload toàn bộ ảnh bìa sách lên MinIO
==================================================================
Cấu trúc upload:
  Local:  covers/books/{book_id}/{book_id}.jpg
  MinIO:  covers/books/{book_id}/{book_id}.jpg   ← giữ nguyên path
          (Spring Boot lấy ảnh theo path này)

Yêu cầu:
  pip install minio

Cách chạy:
  python upload_covers_to_minio.py
  python upload_covers_to_minio.py --endpoint http://192.168.1.12:9000
  python upload_covers_to_minio.py --dry-run     # chỉ liệt kê, không upload
  python upload_covers_to_minio.py --skip-exist  # bỏ qua file đã tồn tại
"""

import argparse
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from minio import Minio
from minio.error import S3Error

# ─── CẤU HÌNH MẶC ĐỊNH ───────────────────────────────────────────────────────
MINIO_ENDPOINT  = "localhost:9000"    # không có http://
MINIO_ACCESS    = "admin"
MINIO_SECRET    = "admin123456789"
MINIO_BUCKET    = "bookstore"
MINIO_SECURE    = False              # True nếu dùng HTTPS

COVERS_DIR      = Path(r"E:\KLTN\12_22110190_22110243_XayDungWebsiteBanSachTichHopHeThongGoiY\covers\books")
EXTENSIONS      = {".jpg", ".jpeg", ".png", ".webp"}
# ─────────────────────────────────────────────────────────────────────────────


def collect_files() -> list[tuple[Path, str]]:
    """
    Thu thập tất cả ảnh và tính object_name tương ứng trên MinIO.
    Returns: [(local_path, minio_object_name)]
    """
    files = []
    for subdir in sorted(COVERS_DIR.iterdir()):
        if subdir.is_dir():
            for f in sorted(subdir.iterdir()):
                if f.suffix.lower() in EXTENSIONS:
                    # Object name: covers/books/{id}/{id}.jpg
                    object_name = f"covers/books/{subdir.name}/{f.name}"
                    files.append((f, object_name))
    return files


def ensure_bucket(client: Minio, bucket: str) -> None:
    """Tạo bucket nếu chưa tồn tại."""
    existing = [b.name for b in client.list_buckets()]
    if bucket not in existing:
        client.make_bucket(bucket)
        print(f"🆕 Đã tạo bucket: {bucket}")
    else:
        print(f"✅ Bucket '{bucket}' đã tồn tại.")


def object_exists(client: Minio, bucket: str, object_name: str) -> bool:
    """Kiểm tra object đã tồn tại trên MinIO chưa."""
    try:
        client.stat_object(bucket, object_name)
        return True
    except Exception:
        return False


def upload_file(
    client: Minio,
    bucket: str,
    local_path: Path,
    object_name: str,
    skip_exist: bool,
    dry_run: bool,
) -> tuple[str, str]:
    """
    Upload 1 file lên MinIO.
    Returns: (status, message)  status = "ok"|"skip"|"dry"|"error"
    """
    try:
        if dry_run:
            return "dry", f"[DRY] {object_name}"

        if skip_exist and object_exists(client, bucket, object_name):
            return "skip", f"⏭  {object_name}  [đã tồn tại]"

        content_type = mimetypes.guess_type(str(local_path))[0] or "image/jpeg"
        size = local_path.stat().st_size

        client.fput_object(
            bucket_name=bucket,
            object_name=object_name,
            file_path=str(local_path),
            content_type=content_type,
        )
        return "ok", f"✅ {object_name}  ({size / 1024:.1f} KB)"

    except Exception as e:
        return "error", f"❌ {object_name}: {e}"


def main():
    parser = argparse.ArgumentParser(description="Upload covers lên MinIO")
    parser.add_argument(
        "--endpoint", default=MINIO_ENDPOINT,
        help=f"MinIO endpoint không có http:// (default: {MINIO_ENDPOINT})"
    )
    parser.add_argument("--access",  default=MINIO_ACCESS,  help="Access key")
    parser.add_argument("--secret",  default=MINIO_SECRET,  help="Secret key")
    parser.add_argument("--bucket",  default=MINIO_BUCKET,  help="Bucket name")
    parser.add_argument("--secure",  action="store_true",   help="Dùng HTTPS")
    parser.add_argument("--dry-run", action="store_true",   help="Không upload, chỉ liệt kê")
    parser.add_argument("--skip-exist", action="store_true",
                        help="Bỏ qua file đã có trên MinIO (mặc định: ghi đè)")
    parser.add_argument("--workers", type=int, default=8,
                        help="Số luồng song song (default: 8)")
    args = parser.parse_args()

    # ── Kết nối MinIO ─────────────────────────────────────────
    client = Minio(
        endpoint=args.endpoint,
        access_key=args.access,
        secret_key=args.secret,
        secure=args.secure,
    )
    print(f"🔗 Kết nối: http{'s' if args.secure else ''}://{args.endpoint}")
    print(f"🪣 Bucket : {args.bucket}")

    if not args.dry_run:
        ensure_bucket(client, args.bucket)

    # ── Thu thập files ─────────────────────────────────────────
    files = collect_files()
    print(f"📸 Tìm thấy {len(files)} ảnh trong {COVERS_DIR}")
    if args.dry_run:
        print("⚠️  DRY RUN – không upload")
    if args.skip_exist:
        print("⏩ Bỏ qua file đã tồn tại trên MinIO")
    print()

    # ── Upload song song ───────────────────────────────────────
    counts = {"ok": 0, "skip": 0, "dry": 0, "error": 0}

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(
                upload_file, client, args.bucket,
                local, obj, args.skip_exist, args.dry_run
            ): obj
            for local, obj in files
        }
        for i, future in enumerate(as_completed(futures), 1):
            status, msg = future.result()
            counts[status] += 1
            print(f"[{i:>4}/{len(files)}] {msg}")

    # ── Tóm tắt ───────────────────────────────────────────────
    print(f"\n{'─'*55}")
    if args.dry_run:
        print(f"[DRY] Sẽ upload: {counts['dry']} ảnh")
    else:
        print(f"✅ Upload thành công: {counts['ok']}")
        print(f"⏭  Bỏ qua:           {counts['skip']}")
        print(f"❌ Lỗi:               {counts['error']}")
        if counts['error'] == 0:
            print(f"\n🎉 Hoàn tất! {counts['ok']} ảnh đã được lưu vào MinIO bucket '{args.bucket}'")


if __name__ == "__main__":
    main()
