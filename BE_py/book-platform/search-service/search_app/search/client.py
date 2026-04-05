import os
from dotenv import load_dotenv
from opensearchpy import OpenSearch

load_dotenv()

_OS_CLIENT: OpenSearch | None = None


def get_os_client() -> OpenSearch:
    """Module-level singleton – khởi tạo 1 lần, tái sử dụng xuyên suốt app.
    Thay vì tạo mới mỗi request → tiết kiệm TCP connection overhead.
    """
    global _OS_CLIENT
    if _OS_CLIENT is None:
        host    = os.getenv("OPENSEARCH_HOST", "localhost")
        port    = int(os.getenv("OPENSEARCH_PORT", "9200"))
        user    = os.getenv("OPENSEARCH_USER", "admin")
        pwd     = os.getenv("OPENSEARCH_PASSWORD", "")
        use_ssl = os.getenv("OPENSEARCH_USE_SSL", "true").lower() == "true"
        verify  = os.getenv("OPENSEARCH_VERIFY_CERTS", "false").lower() == "true"

        _OS_CLIENT = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=(user, pwd),
            use_ssl=use_ssl,
            verify_certs=verify,
            ssl_show_warn=False,
            # Giữ tối đa 10 kết nối TCP song song trong pool
            maxsize=10,
        )
    return _OS_CLIENT


def reset_os_client() -> None:
    """Buộc tạo lại client (khi .env thay đổi hoặc connection bị đứt)."""
    global _OS_CLIENT
    _OS_CLIENT = None
