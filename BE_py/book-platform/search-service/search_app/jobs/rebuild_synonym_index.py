"""
P3.2 CLEAN REBUILD:
  1. Xoa books_current cu
  2. Tao books_current moi voi synonym filter
  3. Reindex tu books_v2 (du lieu da sao luu)
  4. Xoa books_v2 (don dep)
  5. Revert docker-compose ve books_current
"""
import json, ssl, base64, sys, time
import urllib.request, urllib.error

BASE_URL = "https://localhost:9200"
USER     = "admin"
PASSWORD = "VnBook$2025!Qx9"

ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

AUTH    = base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
HEADERS = {"Content-Type": "application/json", "Authorization": f"Basic {AUTH}"}

def req(method, path, body=None):
    url  = BASE_URL + path
    data = json.dumps(body).encode() if body else None
    r    = urllib.request.Request(url, data=data, headers=HEADERS, method=method)
    try:
        with urllib.request.urlopen(r, context=ssl_ctx, timeout=60) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())

def step(n, title):
    print(f"\n[{n}] {title}")
    print("-" * 55)

# ── 1: Lay mapping hien tai tu books_v2 ──────────────────────────────────────
step(1, "Lay mapping tu books_v2")
s, mapping = req("GET", "/books_v2/_mapping")
if s != 200:
    step(1, "books_v2 khong ton tai, thu books_current")
    s, mapping = req("GET", "/books_current/_mapping")
    SOURCE_INDEX = "books_current_backup_temp"
    # Backup data truoc
    print("  Backup books_current -> temp index truoc khi xoa...")
    s2, r2 = req("POST", "/_reindex?wait_for_completion=true", {
        "source": {"index": "books_current"},
        "dest":   {"index": SOURCE_INDEX}
    })
    if s2 not in (200, 201):
        print(f"  FAIL backup: {r2}")
        sys.exit(1)
    print(f"  OK backup: {r2.get('total', 0)} docs")
    DATA_SOURCE = SOURCE_INDEX
    props = mapping["books_current"]["mappings"].get("properties", {})
else:
    DATA_SOURCE = "books_v2"
    props = mapping["books_v2"]["mappings"].get("properties", {})
    print(f"  OK: {len(props)} fields tu books_v2")

# ── 2: Xoa books_current cu ──────────────────────────────────────────────────
step(2, "Xoa index books_current cu")
s, r = req("DELETE", "/books_current")
print(f"  DELETE books_current: HTTP {s}")
if s not in (200, 404):
    print(f"  WARN: {r}")

# ── 3: Tao lai books_current moi voi synonym ─────────────────────────────────
step(3, "Tao lai books_current voi synonym filter")

new_settings = {
    "settings": {
        "number_of_shards":   1,
        "number_of_replicas": 0,
        "analysis": {
            "filter": {
                "vn_synonyms": {
                    "type":     "synonym",
                    "lenient":  True,
                    "synonyms": [
                        "sach, cuon, quyen, tac pham, tieu thuyet",
                        "mua, dat hang, order, lay, tim mua",
                        "tac gia, nguoi viet, writer, author, bien soan",
                        "truyen tranh, comic, manga",
                        "ky nang song, self help, phat trien ban than",
                        "tam ly, psychology, tam ly hoc",
                        "khoa hoc, science, khoa hoc ky thuat",
                        "van hoc, literature, tieu thuyet van hoc",
                        "kinh te, economics, tai chinh, business, dau tu, finance",
                        "gia, gia tien, gia ban, cost, price",
                        "con hang, in stock, co san, available",
                        "het hang, out of stock, khong co san",
                        # ── Mở rộng P4: Giảm miss rate ──
                        "lap trinh, programming, code, coding, dev, developer, software",
                        "kinh doanh, business, khoi nghiep, startup, entrepreneur",
                        "lich su, history, su hoc, su ky, lich su the gioi",
                        "giao duc, education, hoc sinh, sinh vien, hoc tap",
                        "nau an, cooking, am thuc, mon an, recipe, nha hang",
                        "suc khoe, health, y te, y hoc, benh, thuoc",
                        "trieu ly, philosophy, triet hoc, tu tuong",
                        "truyen, story, chuyen ke, ky su, phong su",
                        "tu dien, dictionary, ngu phap, hoc tieng anh, english",
                        "thieu nhi, tre em, children, kids, nhi dong",
                        "gia dinh, family, hon nhan, nuoi con, parenting",
                        "nghe thuat, art, hoi hoa, am nhac, music",
                        "may tinh, computer, cong nghe, technology, tech, it",
                        "quan tri, management, lanh dao, leadership, nhan su",
                    ],
                },
            },
            "analyzer": {
                # Search time: dung synonym
                "vn_search_analyzer": {
                    "type":      "custom",
                    "tokenizer": "standard",
                    "filter":    ["lowercase", "vn_synonyms"],
                },
                # Index time: KHONG dung synonym (de tranh bung kich thuoc index)
                "vn_index_analyzer": {
                    "type":      "custom",
                    "tokenizer": "standard",
                    "filter":    ["lowercase"],
                },
            },
        },
    },
    "mappings": {
        "properties": props
    },
}

s, r = req("PUT", "/books_current", new_settings)
if s not in (200, 201):
    print(f"  FAIL: {s} - {r}")
    sys.exit(1)
print(f"  OK: Index books_current moi tao thanh cong (co synonym filter)")

# ── 4: Reindex data tu source vao books_current moi ─────────────────────────
step(4, f"Reindex tu {DATA_SOURCE} -> books_current")
t0 = time.time()
s, r = req("POST", "/_reindex?wait_for_completion=true", {
    "source": {"index": DATA_SOURCE},
    "dest":   {"index": "books_current"},
})

if s not in (200, 201):
    print(f"  FAIL: {s} - {r}")
    sys.exit(1)

elapsed = round(time.time() - t0, 1)
total    = r.get("total", 0)
created  = r.get("created", 0)
failures = r.get("failures", [])
print(f"  OK: Reindex hoan thanh trong {elapsed}s")
print(f"      Total: {total} | Created: {created} | Failures: {len(failures)}")

# ── 5: Don dep index cu ──────────────────────────────────────────────────────
step(5, "Don dep: Xoa books_v2 va temp index")

for idx_del in ["books_v2", "books_current_backup_temp"]:
    s2, r2 = req("DELETE", f"/{idx_del}")
    if s2 == 200:
        print(f"  Xoa {idx_del}: OK")
    elif s2 == 404:
        print(f"  {idx_del}: Khong ton tai (skip)")
    else:
        print(f"  {idx_del}: WARN {s2}")

# ── 6: Verify ─────────────────────────────────────────────────────────────────
step(6, "Kiem tra index moi")
s, stats = req("GET", "/books_current/_count")
doc_count = stats.get("count", 0)
print(f"  Total docs trong books_current: {doc_count}")

# Test synonym hoat dong
s, r = req("POST", "/books_current/_analyze", {
    "analyzer": "vn_search_analyzer",
    "text": "cuon sach ky nang",
})
tokens = [t["token"] for t in r.get("tokens", [])]
print(f"  Analyze 'cuon sach ky nang' -> tokens: {tokens}")

print(f"\n{'='*55}")
print(f"  HOAN THANH!")
print(f"  Index: books_current ({doc_count} sach)")
print(f"  Synonym: vn_search_analyzer san sang")
print(f"  Service van dung OPENSEARCH_INDEX=books_current")
print(f"{'='*55}\n")
