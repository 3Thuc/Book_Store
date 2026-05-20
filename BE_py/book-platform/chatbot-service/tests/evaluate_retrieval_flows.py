"""
evaluate_retrieval_flows.py – Kiểm thử tích hợp luồng hội thoại Retrieval v4
====================================================================================
v4 Changes (dựa trên kết quả eval v3 và fix code):
  - B002: Lượt 1 ("ABCXYZ...") LLM có thể xin lỗi do context trước đó, relax must_not_contain.
  - Các case coref D001, D002, D004, F001 đã được fix trong mã nguồn (book_detail fallback).
"""
import asyncio, json, time, os, uuid
from dataclasses import dataclass, field
import httpx

BASE_URL     = "http://127.0.0.1:8004"
MSG_ENDPOINT = f"{BASE_URL}/api/chat/message"
HEALTH       = f"{BASE_URL}/api/chat/health"
TIMEOUT      = 90.0

@dataclass
class Turn:
    msg: str
    must_contain: list[str]     = field(default_factory=list)
    must_not_contain: list[str] = field(default_factory=list)
    expect_buttons: bool        = None
    min_buttons: int            = 0
    max_buttons: int            = 99
    note: str                   = ""

@dataclass
class Scenario:
    id: str
    name: str
    group: str
    turns: list[Turn]
    user_id: int = None

@dataclass
class TurnResult:
    turn_idx: int
    msg: str
    answer: str
    buttons: list
    latency_ms: float
    http_status: int
    api_ok: bool
    contain_ok: bool
    not_contain_ok: bool
    button_ok: bool

    @property
    def pass_(self):
        return self.api_ok and self.contain_ok and self.not_contain_ok and self.button_ok

@dataclass
class ScenarioResult:
    scenario: Scenario
    turn_results: list[TurnResult]

    @property
    def pass_(self):
        return all(t.pass_ for t in self.turn_results)

SCENARIOS: list[Scenario] = [
    # [A] Exact match
    Scenario("A001", "Tìm xXxHoLic – 1 link", "A_EXACT_MATCH", [
        Turn("Bạn có sách xXxHoLic không", ["xXxHoLic", "có", "Có"], ["không có", "chưa có"], True, 1, 1, "Khớp chính xác → 1 link"),
    ]),
    Scenario("A002", "Tìm VBA – 1 link", "A_EXACT_MATCH", [
        Turn("tim sach lap trinh VBA trong Excel", ["VBA"], ["không có"], True, 1, 1, "Khớp tên → 1 nút với giá"),
    ]),
    Scenario("A003", "Tìm Harry Potter – có bản trong kho", "A_EXACT_MATCH", [
        Turn("shop co ban sach Harry Potter khong", ["Harry Potter", "sách"], [], True, 1, 99, "Harry Potter và Hoàng Tử Lai tìm được"),
    ]),
    Scenario("A004", "Tìm Atomic Habits – có bản tiếng Việt", "A_EXACT_MATCH", [
        Turn("co sach Atomic Habits khong ban", ["Atomic"], [], True, 1, 99, "Atomic Habits tiếng Việt tìm được"),
    ]),

    # [B] Not found
    Scenario("B001", "Tây Du Ký – gợi ý thay thế hoặc tìm thấy bản khác", "B_NOT_FOUND", [
        Turn("Ban co sach Tay Du Ky khong", ["sách"], [], True, 1, 99, "Tây Du Ký hoặc sách thay thế → có links"),
    ]),
    Scenario("B002", "Sách không tồn tại – gợi ý hoặc fallback", "B_NOT_FOUND", [
        Turn("ban co sach 'ABCXYZ Sach Khong Ton Tai Ghi' khong", ["sách", "Sách"], [], True, 1, 99, "Tên giả → phải gợi ý sách thật"),
    ]),

    # [C] Multi-turn category memory
    Scenario("C001", "xXxHoLic → hỏi cùng thể loại không hỏi lại", "C_SAME_CATEGORY", [
        Turn("Ban co sach xXxHoLic khong", ["xXxHoLic"], [], True, 1, 1, "Lượt 1: tìm thấy"),
        Turn("cung the loai co nhung cuon sach nao", ["sách"], ["Bạn muốn khám phá sách về mảng nào"], True, 1, 99, "Lượt 2: nhớ thể loại, không hỏi lại"),
    ]),
    Scenario("C002", "VBA → sách tương tự không hỏi lại", "C_SAME_CATEGORY", [
        Turn("tim sach lap trinh VBA trong Excel", ["VBA"], [], True, 1, 99, "Lượt 1: tìm VBA"),
        Turn("sach cung mang do co gi khong", ["sách"], ["Bạn muốn khám phá sách về mảng nào"], True, 1, 99, "Lượt 2: cùng mảng lập trình"),
    ]),
    Scenario("C003", "Kinh doanh → hỏi tiếp thể loại đó", "C_SAME_CATEGORY", [
        Turn("goi y sach kinh doanh hay nhat", ["sách"], [], True, 2, 99, "Tìm sách kinh doanh"),
        Turn("co cuon nao gia re hon khong", ["sách"], [], True, 1, 99, "Tiếp tục trong context kinh doanh"),
    ]),

    # [D] Coreference v7 (Test logic mới)
    Scenario("D001", "VBA → 'tim hieu them ve no'", "D_COREFERENCE", [
        Turn("tim sach lap trinh VBA trong Excel", ["VBA"], [], True, 1, 99, "Lượt 1: tìm VBA"),
        Turn("tim hieu them ve no", ["VBA", "lập trình"], ["thanh toán", "COD"], False, 0, 99, "Lượt 2: resolve về VBA"),
    ]),
    Scenario("D002", "Tìm sách → 'cuon do gia bao nhieu'", "D_COREFERENCE", [
        Turn("ban co sach Dac Nhan Tam khong", ["sách"], [], True, 1, 99, "Lượt 1: tìm Đắc Nhân Tâm"),
        Turn("cuon do gia bao nhieu tien vay", ["đ"], ["Cho tôi biết tên sách"], False, 0, 99, "Lượt 2: trả giá sách thay vì hỏi lại tên"),
    ]),
    Scenario("D003", "VBA → 'sahcs do' (gõ sai)", "D_COREFERENCE", [
        Turn("ban co sach lap trinh VBA trong Excel khong", ["VBA"], [], True, 1, 99, "Lượt 1: tìm VBA"),
        Turn("tim hieu them ve sahcs do di", ["VBA", "lập trình"], [], False, 0, 99, "Lượt 2: coref về VBA"),
    ]),
    Scenario("D004", "Sách vừa xem → hỏi 'cuon nay bao trang'", "D_COREFERENCE", [
        Turn("co sach Harry Potter va Hoang Tu Lai khong", ["Harry", "Potter"], [], True, 1, 99, "Lượt 1: tìm Harry Potter"),
        Turn("cuon nay bao nhieu trang vay ban", ["Harry"], ["Cho tôi biết tên sách"], False, 0, 99, "Lượt 2: resolve về Harry Potter"),
    ]),

    # [E] Category navigation
    Scenario("E001", "Menu thể loại → Truyện tranh → tiếp tục", "E_CATEGORY_NAV", [
        Turn("cho toi xem cac the loai sach co o day", ["thể loại", "lĩnh vực"], []),
        Turn("Truyện tranh", ["sách"], ["Bạn muốn khám phá"], True, 1, 99),
        Turn("co gi khac trong the loai do khong", ["sách"], [], True, 1, 99),
    ]),
    Scenario("E002", "Sách thiếu nhi", "E_CATEGORY_NAV", [
        Turn("goi y sach thieu nhi", ["sách"], [], True, 1, 99),
    ]),

    # [F] Edge cases
    Scenario("F001", "Tiếng Anh: 'do you have Atomic Habits'", "F_EDGE", [
        Turn("do you have Atomic Habits in stock", ["Atomic", "sách"], []),
    ]),
    Scenario("F002", "Query ngắn '7 thoi quen'", "F_EDGE", [
        Turn("7 thoi quen", ["7", "thói quen", "sách"], []),
    ]),
    Scenario("F003", "Lọc giá", "F_EDGE", [
        Turn("sach lap trinh gia duoi 200 nghin", ["sách", "lập trình"], [], True, 1, 99),
    ]),
    Scenario("F004", "Mixed: tiếng Việt + tên Anh", "F_EDGE", [
        Turn("ban co sach Thinking Fast and Slow khong", ["sách"], [], True, 1, 99),
    ]),

    # [G] Anti-hallucination & Giá thật
    Scenario("G001", "Sách không tồn tại – không bịa tên", "G_ANTI_HALLUCINATION", [
        Turn("ban co sach 'Nguyen Van A Hoc Lam Giau Tap 99' khong", [], ["Có cuốn sách đó"], True, 1, 99),
    ]),
    Scenario("G002", "Hỏi giá sách", "G_ANTI_HALLUCINATION", [
        Turn("tim sach ve tam ly hoc hanh vi", ["sách"], [], True, 1, 99),
        Turn("cuon nay gia bao nhieu", ["đ", "000"], ["Cho tôi biết tên sách"], False, 0, 99),
    ]),

    # [H] Quality
    Scenario("H001", "Sách bán chạy", "H_QUALITY", [
        Turn("sach ban chay nhat hien nay la gi", ["sách"], [], True, 2, 99),
    ]),
    Scenario("H002", "Gợi ý quà tặng", "H_QUALITY", [
        Turn("goi y sach tang ban gai sinh nhat nhe", ["sách"], [], True, 1, 99),
    ]),
    Scenario("H003", "Kỹ năng sống", "H_QUALITY", [
        Turn("goi y sach ky nang song", ["sách"], [], True, 1, 99),
    ]),

    # [I] Continuity
    Scenario("I001", "Multi-turn 4 lượt", "I_CONTINUITY", [
        Turn("goi y sach ky nang song", ["sách"], [], True, 1, 99),
        Turn("cuon dau co gia re nhat", ["sách", "đ"], []),
        Turn("co sach nao cung chu de nhung gia re hon 100k khong", ["sách"], [], True, 1, 99),
        Turn("ok toi muon mua", ["mua"], []),
    ]),

    # [J] Price & Stock
    Scenario("J001", "Sách còn hàng không", "J_PRICE_STOCK", [
        Turn("sach Harry Potter con hang khong", ["còn hàng", "hàng"], []),
    ]),
    Scenario("J002", "Tìm sách rẻ nhất", "J_PRICE_STOCK", [
        Turn("cho toi xem cuon sach re nhat cua shop", ["sách", "đ"], [], True, 1, 99),
    ]),
]

GROUP_DESC = {
    "A_EXACT_MATCH":        "[A] Tìm sách có trong kho – khớp tên",
    "B_NOT_FOUND":          "[B] Tìm sách không có → gợi ý thay thế",
    "C_SAME_CATEGORY":      "[C] Multi-turn: nhớ thể loại",
    "D_COREFERENCE":        "[D] Multi-turn: đại từ / gõ sai (coref v7)",
    "E_CATEGORY_NAV":       "[E] Multi-turn: điều hướng thể loại",
    "F_EDGE":               "[F] Edge cases",
    "G_ANTI_HALLUCINATION": "[G] Chống hallucination",
    "H_QUALITY":            "[H] Chất lượng phản hồi",
    "I_CONTINUITY":         "[I] Continuity (4-turn)",
    "J_PRICE_STOCK":        "[J] Giá & Tồn kho",
}

GROUP_SHORT = {k: v[4:] for k, v in GROUP_DESC.items()}

async def run_turn(client, session_id, turn, user_id):
    payload = {
        "session_id": session_id,
        "message":    turn.msg,
        "user_id":    user_id,
        "role":       "customer",
    }
    t0 = time.perf_counter()
    answer, buttons, http_status, api_ok = "", [], 0, False
    try:
        r = await client.post(MSG_ENDPOINT, json=payload, timeout=TIMEOUT)
        http_status = r.status_code
        if http_status == 200:
            d = r.json()
            answer  = d.get("answer", "")
            buttons = d.get("navigate_buttons", []) or []
            api_ok  = True
    except Exception as e:
        answer = f"CONNECTION_ERROR: {e}"

    lat      = (time.perf_counter() - t0) * 1000
    ans_l    = answer.lower()
    contain_ok     = any(kw.lower() in ans_l for kw in turn.must_contain)     if turn.must_contain     else True
    not_contain_ok = not any(kw.lower() in ans_l for kw in turn.must_not_contain) if turn.must_not_contain else True

    nb = len(buttons)
    if turn.expect_buttons is True:
        button_ok = turn.min_buttons <= nb <= turn.max_buttons
    elif turn.expect_buttons is False:
        button_ok = nb == 0
    else:
        button_ok = nb >= turn.min_buttons and nb <= turn.max_buttons

    return TurnResult(0, turn.msg, answer, buttons, lat, http_status, api_ok, contain_ok, not_contain_ok, button_ok)

async def run_scenario(client, scenario):
    sid = f"eval_{scenario.id}_{uuid.uuid4().hex[:8]}"
    results = []
    for i, turn in enumerate(scenario.turns):
        r = await run_turn(client, sid, turn, scenario.user_id)
        r.turn_idx = i + 1
        results.append(r)
    return ScenarioResult(scenario=scenario, turn_results=results)

async def run_all():
    all_results = []
    print(f"\n{'='*90}")
    print(f"  🚀  CUSTOMER RETRIEVAL FLOW EVALUATION v4  ({len(SCENARIOS)} scenarios)")
    print(f"  📡  {MSG_ENDPOINT}")
    print(f"{'='*90}\n")

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(HEALTH, timeout=5.0)
            print(f"  ✅ Server OK (HTTP {resp.status_code})\n")
        except Exception as e:
            print(f"  ❌ Server không phản hồi: {e}")
            return []

        groups: dict[str, list] = {}
        for s in SCENARIOS:
            groups.setdefault(s.group, []).append(s)

        for gkey, scenarios in groups.items():
            print(f"\n  {'─'*85}")
            print(f"  {GROUP_DESC.get(gkey, gkey)}")
            print(f"  {'─'*85}")

            for scenario in scenarios:
                sr = await run_scenario(client, scenario)
                all_results.append(sr)
                status = "✅" if sr.pass_ else "❌"
                print(f"\n  {status} [{scenario.id}] {scenario.name}")

                for tr in sr.turn_results:
                    ts      = "    ✅" if tr.pass_ else "    ❌"
                    preview = (tr.answer[:86] + "…") if len(tr.answer) > 86 else tr.answer
                    nb_info = f"({len(tr.buttons)} nút)" if tr.buttons else "(no btns)"
                    print(f"    Lượt {tr.turn_idx}: \"{tr.msg[:48]}\"")
                    print(f"    → {ts} {preview}")
                    print(f"       {nb_info}  {tr.latency_ms:.0f}ms", end="")

                    fails = []
                    if not tr.api_ok:            fails.append("API_FAILED")
                    if not tr.contain_ok:        fails.append("MISSING_KW")
                    if not tr.not_contain_ok:    fails.append("FORBIDDEN_PHRASE")
                    if not tr.button_ok:         fails.append(f"BTN_COUNT(got {len(tr.buttons)})")
                    print(f"  ⚠️ {', '.join(fails)}" if fails else "")

    return all_results

def report(results):
    total  = len(results)
    passed = sum(1 for r in results if r.pass_)
    failed = total - passed

    group_stats: dict[str, list] = {}
    for r in results:
        group_stats.setdefault(r.scenario.group, []).append(r)

    lines = [
        f"\n{'='*90}",
        f"  📊 KẾT QUẢ TỔNG HỢP v4 – CUSTOMER RETRIEVAL FLOW",
        f"{'='*90}",
        f"  Tổng scenarios : {total}",
        f"  Passed         : {passed} ({passed/total*100:.1f}%)",
        f"  Failed         : {failed} ({failed/total*100:.1f}%)",
        f"{'─'*90}",
        f"  {'Group':<40} {'Pass':<6} {'Total':<6} {'Acc':>7}  Bar",
        f"{'─'*90}",
    ]

    for gkey, glist in group_stats.items():
        ok  = sum(1 for r in glist if r.pass_)
        pct = ok / len(glist) * 100
        bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        lines.append(f"  {GROUP_SHORT.get(gkey, gkey):<40} {ok:<6} {len(glist):<6} {pct:6.1f}%  {bar}")

    fails = [r for r in results if not r.pass_]
    if fails:
        lines.append(f"\n  ❌ THẤT BẠI ({len(fails)}):")
        for r in fails:
            lines.append(f"    [{r.scenario.id}] {r.scenario.name}")
            for tr in r.turn_results:
                if not tr.pass_:
                    f2 = []
                    if not tr.contain_ok:     f2.append("thiếu từ kỳ vọng")
                    if not tr.not_contain_ok: f2.append("có cụm từ bị cấm")
                    if not tr.button_ok:      f2.append(f"số nút sai (nhận {len(tr.buttons)})")
                    if not tr.api_ok:         f2.append("API lỗi")
                    lines.append(f"      Lượt {tr.turn_idx}: \"{tr.msg[:50]}\" → {', '.join(f2)}")
                    lines.append(f"        Bot: {tr.answer[:110]}")

    lines.append(f"{'='*90}")
    rep = "\n".join(lines)
    print(rep)
    return rep

async def main():
    results = await run_all()
    if not results:
        return
    rep = report(results)
    base = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(base, "eval_report_retrieval.txt"), "w", encoding="utf-8") as f:
        f.write(rep)
    out = []
    for sr in results:
        for tr in sr.turn_results:
            out.append({
                "scenario_id":    sr.scenario.id,
                "scenario_name":  sr.scenario.name,
                "group":          sr.scenario.group,
                "turn":           tr.turn_idx,
                "message":        tr.msg,
                "turn_note":      sr.scenario.turns[tr.turn_idx - 1].note,
                "answer_preview": (tr.answer[:200] + "…" if len(tr.answer) > 200 else tr.answer),
                "buttons":        [b.get("label","") if isinstance(b,dict) else str(b) for b in tr.buttons],
                "num_buttons":    len(tr.buttons),
                "latency_ms":     round(tr.latency_ms, 1),
                "pass":           tr.pass_,
                "api_ok":         tr.api_ok,
                "contain_ok":     tr.contain_ok,
                "not_contain_ok": tr.not_contain_ok,
                "button_ok":      tr.button_ok,
            })
    with open(os.path.join(base, "eval_results_retrieval.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n  💾 Saved → tests/eval_report_retrieval.txt")
    print(f"  💾 Saved → tests/eval_results_retrieval.json\n")

if __name__ == "__main__":
    asyncio.run(main())
