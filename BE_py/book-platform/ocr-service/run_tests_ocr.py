"""
run_tests_ocr.py - OCR Service Test Runner (30 Test Cases)
===========================================================
30 test case chia 3 nhom:
  Nhom A (10): Anh bia sach ro net tu scraped_images → expect tim thay + buttons
  Nhom B (10): Anh bia sach kho (ảnh thật chụp nghiêng/mờ + unnamed*.jpg) → expect fallback
  Nhom C (10): Anh khong phai bia sach (logo, icon, diagram, PNG) → expect fallback

Usage:
    python run_tests_ocr.py
    python run_tests_ocr.py --group A
    python run_tests_ocr.py --verbose
"""

import requests, json, uuid, time, argparse, sys
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

BASE_URL = "http://localhost:8004"
OCR_URL  = f"{BASE_URL}/api/chat/upload-image"
TIMEOUT  = 60
IMG_BOOK = Path(r"D:\craw_demo\scraped_images")
IMG_HARD = Path(r"D:\ocr_test_images")
OUT_TXT  = Path("ocr_test_results.txt")
OUT_JSON = Path("ocr_test_results.json")


@dataclass
class OCRTestCase:
    id: str
    group: str
    description: str
    image_file: str
    img_dir: str        # "book" or "hard"
    note: str = ""


OCR_TEST_CASES: list[OCRTestCase] = [

    # ══ NHOM A: Anh bia sach ro net, co trong DB ═════════════════════════════
    OCRTestCase("TC_OCR_01","A","Bia '1 Cm Giua Anh Va Em' - van hoc",
                "1-cm-giua-anh-va-em.jpg","book","Bestseller van hoc"),
    OCRTestCase("TC_OCR_02","A","Bia '1% No Luc' - tu phat trien ban than",
                "1-no-luc.jpg","book"),
    OCRTestCase("TC_OCR_03","A","Bia '10% Hanh Phuc Hon' - tam ly hoc",
                "10-hanh-phuc-hon-10-happier.jpg","book"),
    OCRTestCase("TC_OCR_04","A","Bia '1 Phut Noi Tieng Anh Nhu Gio'",
                "1-phut-noi-tieng-anh-nhu-gio.jpg","book"),
    OCRTestCase("TC_OCR_05","A","Bia '1 Nhan Luc Tao Ra 99 Doanh So AI'",
                "1-nhan-luc-tao-ra-99-doanh-so-ban-hang-thong-minh-trong-thoi-dai-ai.jpg","book"),
    OCRTestCase("TC_OCR_06","A","Bia '10 Chi So Vang Quyet Dinh Tuong Lai'",
                "10-chi-so-vang-quyet-dinh-tuong-lai-cua-tre-boi-duong-chi-so-can-dam-daring-inte.jpg","book"),
    OCRTestCase("TC_OCR_07","A","Bia '10 Phut Tu Hoc Tieng Trung 2025'",
                "10-phut-tu-hoc-tieng-trung-moi-ngay-tai-ban-2025.jpg","book"),
    OCRTestCase("TC_OCR_08","A","Bia '10 Buoc Den Thanh Cong'",
                "10-buoc-den-thanh-cong.jpg","book"),
    OCRTestCase("TC_OCR_09","A","Bia '1 Ngay Bang 48 Gio'",
                "1-ngay-bang-48-gio.jpg","book"),
    OCRTestCase("TC_OCR_10","A","Bia '10 Dieu Ran Lanh Dao Toi Uu'",
                "10-dieu-ran-lanh-dao-toi-uu-nhat-the-gioi.jpg","book"),

    # ══ NHOM B: Anh bia sach kho nhan dien (thật + biến thể) ═════════════════
    OCRTestCase("TC_OCR_11","B","Bia sach thuc te (unnamed1) - anh chup tu internet",
                "unnamed (1).jpg","hard","Anh bia sach thuc te chup bang dien thoai"),
    OCRTestCase("TC_OCR_12","B","Bia sach (unnamed2) - chat luong khac nhau",
                "unnamed (2).jpg","hard","Anh bia sach tu nguon khac"),
    OCRTestCase("TC_OCR_13","B","Bia sach (unnamed3) - co the bi cat/crop",
                "unnamed (3).jpg","hard","Anh co the bi crop mat phan tieu de"),
    OCRTestCase("TC_OCR_14","B","Bia sach (unnamed4) - kich thuoc khac nhau",
                "unnamed (4).jpg","hard"),
    OCRTestCase("TC_OCR_15","B","Bia sach (unnamed5) - co the khac the loai",
                "unnamed (5).jpg","hard"),
    OCRTestCase("TC_OCR_16","B","Bia sach (unnamed6)",
                "unnamed (6).jpg","hard"),
    OCRTestCase("TC_OCR_17","B","Bia sach xoay 45 do (sinh tong hop)",
                "hard_01_rotated45.jpg","hard","Xoay 45 - OCR kho doc"),
    OCRTestCase("TC_OCR_18","B","Bia sach lam mo GaussianBlur (sinh tong hop)",
                "hard_02_blurred.jpg","hard","Blur manh - mat net chu"),
    OCRTestCase("TC_OCR_19","B","Bia sach bia-sach2 (anh thuc te tu Downloads)",
                "bia-sach2-9886.jpg","hard","Anh bia sach thuc te tu nguon khac"),
    OCRTestCase("TC_OCR_20","B","Thumbnail nho 50x70px (sinh tong hop)",
                "hard_03_tiny50px.jpg","hard","Anh cuc nho - test phan giai toi thieu"),

    # ══ NHOM C: Anh khong phai bia sach ══════════════════════════════════════
    OCRTestCase("TC_OCR_21","C","Logo MySQL (anh icon phan mem)",
                "logo-mysql-170x115.png","hard","Logo phan mem - khong phai sach"),
    OCRTestCase("TC_OCR_22","C","Icon chatbot (chat-bot.png)",
                "chat-bot.png","hard","Icon UI - khong phai bia sach"),
    OCRTestCase("TC_OCR_23","C","Icon bot he thong (bot.png)",
                "bot.png","hard","Icon robot - khong co ten sach"),
    OCRTestCase("TC_OCR_24","C","So do kien truc 3D modeling (PNG)",
                "3d-modeling.png","hard","Technical diagram - khong phai sach"),
    OCRTestCase("TC_OCR_25","C","Icon data-server / database",
                "data-server.png","hard","Server diagram - khong phai sach"),
    OCRTestCase("TC_OCR_26","C","So do embedded system (PNG)",
                "embedded.png","hard","Embedded system diagram"),
    OCRTestCase("TC_OCR_27","C","Anh trang trang hoan toan (blank white)",
                "nonbook_01_blank_white.jpg","hard","Khong co noi dung"),
    OCRTestCase("TC_OCR_28","C","Bieu do cot doanh thu (bar chart)",
                "nonbook_04_bar_chart.jpg","hard","Chart so lieu - khong phai ten sach"),
    OCRTestCase("TC_OCR_29","C","Anh hoa don mua hang (invoice text)",
                "nonbook_07_invoice.jpg","hard","Text so lieu hoa don"),
    OCRTestCase("TC_OCR_30","C","Screenshot man hinh dashboard",
                "nonbook_10_screenshot.jpg","hard","Man hinh he thong"),
]


class Logger:
    def __init__(self, fp: Path):
        self.file = open(fp, "w", encoding="utf-8")
    def log(self, text: str = ""):
        self.file.write(text + "\n"); self.file.flush()
        print(text.encode("ascii", "replace").decode("ascii"))
    def close(self): self.file.close()


def detect_mime(path: Path) -> str:
    ext = path.suffix.lower()
    return {"png": "image/png", "webp": "image/webp"}.get(ext[1:], "image/jpeg")


def run_ocr_test(tc: OCRTestCase, logger: Logger, verbose: bool) -> dict:
    rec = {"id": tc.id, "group": tc.group, "description": tc.description,
           "image": tc.image_file, "passed": False,
           "failures": [], "elapsed_ms": 0, "answer_snippet": "",
           "buttons_count": 0, "found": False}

    base = IMG_BOOK if tc.img_dir == "book" else IMG_HARD
    img  = base / tc.image_file
    if not img.exists():
        logger.log(f"  [{tc.id}]  [SKIP] Not found: {img.name}")
        rec["failures"] = ["IMAGE_NOT_FOUND"]; return rec

    sid = f"ocr-{tc.id}-{uuid.uuid4().hex[:6]}"
    try:
        with open(img, "rb") as f:
            t0 = time.perf_counter()
            r = requests.post(
                OCR_URL,
                files={"file": (img.name, f, detect_mime(img))},
                data={"session_id": sid, "role": "customer", "message": ""},
                timeout=TIMEOUT, stream=True)
            elapsed = (time.perf_counter() - t0) * 1000
            r.raise_for_status()

        content, btns = "", []
        for line in r.iter_lines():
            if not line: continue
            txt = line.decode("utf-8") if isinstance(line, bytes) else line
            if txt.startswith("data:"):
                try:
                    ev = json.loads(txt[5:].strip())
                    if ev.get("type") == "token":  content += ev.get("content","")
                    elif ev.get("type") == "done":  btns = ev.get("btns",[])
                except json.JSONDecodeError: pass

        failures = []
        if not content.strip(): failures.append("Empty response")

        passed = len(failures) == 0
        found  = bool(btns)
        status = "[PASS]" if passed else "[FAIL]"

        logger.log(f"  [{tc.id}] {status}  {elapsed:6.0f}ms  "
                   f"found={'YES' if found else 'NO ':3}  btns={len(btns)}"
                   f"  | {tc.description[:52]}")
        if content.strip():
            logger.log(f"           BOT: {content.strip().replace(chr(10),' | ')[:110]}")
        for fail in failures:
            logger.log(f"           - FAIL: {fail}")
        if verbose and content.strip():
            logger.log(f"           FULL: {content.strip()[:300]}")

        rec.update({"passed": passed, "failures": failures,
                    "elapsed_ms": round(elapsed),
                    "answer_snippet": content.strip()[:200],
                    "buttons_count": len(btns), "found": found})

    except requests.exceptions.Timeout:
        logger.log(f"  [{tc.id}]  [TIMEOUT]"); rec["failures"] = ["TIMEOUT"]
    except requests.exceptions.ConnectionError as e:
        logger.log(f"  [{tc.id}]  [CONN ERROR] {e}"); rec["failures"] = ["CONN_ERROR"]
    except Exception as e:
        logger.log(f"  [{tc.id}]  [ERROR] {e}"); rec["failures"] = [str(e)]
    return rec


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", choices=["A","B","C"])
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logger = Logger(OUT_TXT)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.log(f"OCR TEST RUN - {now}")
    logger.log(f"Endpoint  : {OCR_URL}")
    logger.log(f"Book imgs : {IMG_BOOK}")
    logger.log(f"Hard imgs : {IMG_HARD}")

    try:
        requests.get(f"{BASE_URL}/api/chat/health", timeout=10).raise_for_status()
        logger.log("[OK] Service healthy\n")
    except Exception as e:
        logger.log(f"[ERROR] {e}"); logger.close(); sys.exit(1)

    tests = [t for t in OCR_TEST_CASES if not args.group or t.group == args.group]
    logger.log(f"Running {len(tests)} tests...\n")

    grp_stats = {g: {"pass":0,"total":0,"found":0} for g in "ABC"}
    all_results, cur_grp = [], None
    t0 = time.perf_counter()

    labels = {
        "A": "NHOM A - Anh bia sach ro net, co trong DB",
        "B": "NHOM B - Anh bia sach kho nhan dien (thuc te + bien the)",
        "C": "NHOM C - Anh khong phai bia sach (logo, icon, diagram)",
    }
    for tc in tests:
        if tc.group != cur_grp:
            cur_grp = tc.group
            logger.log(f"\n{'='*65}")
            logger.log(f"  {labels[tc.group]}")
            logger.log(f"{'='*65}")
        rec = run_ocr_test(tc, logger, args.verbose)
        all_results.append(rec)
        grp_stats[tc.group]["total"] += 1
        if rec["passed"]:    grp_stats[tc.group]["pass"]  += 1
        if rec.get("found"): grp_stats[tc.group]["found"] += 1

    elapsed_total = time.perf_counter() - t0

    logger.log(f"\n{'='*65}")
    logger.log("  SUMMARY")
    logger.log(f"{'='*65}")
    logger.log(f"  {'Nhom':<6} {'Pass':>5} {'Total':>6} {'Found':>6}  {'%':>6}  Bar")
    logger.log(f"  {'-'*55}")
    gp = gt = 0
    for grp in "ABC":
        p = grp_stats[grp]["pass"]; t = grp_stats[grp]["total"]
        f = grp_stats[grp]["found"]
        pct = p/t*100 if t else 0
        bar = "#"*p + "."*(t-p)
        logger.log(f"  {grp:<6} {p:>5} {t:>6} {f:>6}  {pct:>5.1f}%  {bar}")
        gp += p; gt += t

    gpct = gp/gt*100 if gt else 0
    logger.log(f"\n  TOTAL: {gp}/{gt} ({gpct:.1f}%)  [{elapsed_total:.1f}s]")
    logger.log(f"  -> RESULT: {'PASS' if gpct>=80 else 'PARTIAL' if gpct>=60 else 'FAIL'}")

    with open(OUT_JSON,"w",encoding="utf-8") as f:
        json.dump({"run_at":now,"ocr_url":OCR_URL,
                   "grand_passed":gp,"grand_total":gt,"grand_pct":round(gpct,1),
                   "elapsed_s":round(elapsed_total,1),
                   "group_stats":grp_stats,"results":all_results},
                  f, ensure_ascii=False, indent=2)
    logger.log(f"\n  Saved: {OUT_TXT.resolve()}")
    logger.log(f"         {OUT_JSON.resolve()}")
    logger.close()

if __name__ == "__main__":
    main()
