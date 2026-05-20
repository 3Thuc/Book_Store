"""
evaluate_chatbot_multiturn.py – 200 sessions × 5 turns = 1000 turns
====================================================================
Mỗi session = 3 lượt liên tiếp trên cùng session_id → test contextual memory.
200 sessions = 70 customer + 70 staff + 60 admin.
  - Customer: 70 sessions cycle qua 20 CUSTOMER_FLOWS (3–4 sessions/flow)
  - Staff:    70 sessions cycle qua 14 STAFF_FLOWS    (5 sessions/flow)
  - Admin:    60 sessions cycle qua 20 ADMIN_FLOWS    (3 sessions/flow)

⚠️ Các turn TRONG 1 session chạy tuần tự (giữ context).
   Các sessions chạy song song (semaphore = 5 → ~15 concurrent DB calls).
   DB connection pool cần >= 20.

Cách chạy:
  Terminal 1: uvicorn chatbot_app.main:app --port 8004 --reload
  Terminal 2: python tests/evaluate_chatbot_multiturn.py

LOGIC NLU theo role:
  Customer: out_of_scope = OOS blacklist regex | general_query = SBERT fallback
  Staff:    staff_out_of_scope = SBERT fallback (score < 0.52)
  Admin:    admin_out_of_scope = SBERT fallback (score < 0.50)
  Tất cả turns phải gửi user_id và role để tránh DB crash

Bộ v5 (sửa lỗi từ report v4):
  - 20 Customer flows: sửa C-F09/12/18/19/20 (confirm/return_request/combo/trending)
  - 14 Staff flows: sửa S-F02/05/06/08/09/10/14 (pending/stats/inventory/return)
  - 20 Admin flows: sửa A-F02 (admin_top_books vs revenue_report conflict)
  - admin_order_stats vẫn gặp SERVER_ERROR (lỗi backend endpoint)
  - Tất cả turns regex-guaranteed (trừ out_of_scope)
"""
import asyncio, json, time, os, uuid
import httpx
from dataclasses import dataclass, field

BASE_URL  = "http://localhost:8004"
HEALTH_C  = f"{BASE_URL}/api/chat/health"
HEALTH_S  = f"{BASE_URL}/api/staff/chat/health"
HEALTH_A  = f"{BASE_URL}/api/admin/chat/health"
EP_CUST   = f"{BASE_URL}/api/chat/message"
EP_STAFF  = f"{BASE_URL}/api/staff/chat/message"
EP_ADMIN  = f"{BASE_URL}/api/admin/chat/message"
TIMEOUT   = 60.0
SEMAPHORE = 5

# ── Data structures ────────────────────────────────────────────────────────────
@dataclass
class Turn:
    msg: str
    expected: str

@dataclass
class Flow:
    name: str
    role: str      # "customer" | "staff" | "admin"
    turns: list[Turn]

@dataclass
class TurnResult:
    session_id: str; flow: str; role: str; turn_n: int
    msg: str; expected: str; actual: str
    conf: float; lat: float; ok: bool; api_ok: bool
    answer: str

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _contextual_flows import ALL_FLOWS, CUSTOMER_FLOWS, STAFF_FLOWS, ADMIN_FLOWS
# ──────────────────────────────────────────────────────────────────────────────
# Session builder
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class Session:
    id: str; flow: Flow; user_id: int = 1

def build_sessions() -> list[Session]:
    sessions: list[Session] = []
    roles_config = [
        (CUSTOMER_FLOWS, 70),
        (STAFF_FLOWS,    70),
        (ADMIN_FLOWS,    60),
    ]
    for flows, n in roles_config:
        for i in range(n):
            flow = flows[i % len(flows)]
            sid  = f"{flow.role}_{uuid.uuid4().hex[:8]}"
            sessions.append(Session(id=sid, flow=flow))
    return sessions

# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ──────────────────────────────────────────────────────────────────────────────
def endpoint_for(role: str) -> str:
    return {
        "customer": EP_CUST,
        "staff":    EP_STAFF,
        "admin":    EP_ADMIN,
    }[role]

async def run_turn(client: httpx.AsyncClient, sess: Session,
                   turn: Turn, turn_n: int) -> TurnResult:
    ep = endpoint_for(sess.flow.role)
    payload = {
        "session_id": sess.id,
        "message":    turn.msg,
        "user_id":    sess.user_id,
        "role":       sess.flow.role,
    }
    t0 = time.perf_counter()
    actual = "CONNECTION_ERROR"; conf = 0.0; api_ok = False; answer = ""
    try:
        r = await client.post(ep, json=payload, timeout=TIMEOUT)
        if r.status_code == 200:
            d = r.json()
            actual = d.get("intent", "MISSING_INTENT")
            conf   = d.get("confidence", 0.0)
            answer = d.get("answer", "")
            api_ok = True
            if actual == "error":
                actual = "SERVER_ERROR"
        else:
            actual = f"HTTP_{r.status_code}"
    except Exception as e:
        actual = "CONNECTION_ERROR"
    lat = (time.perf_counter() - t0) * 1000
    return TurnResult(
        session_id=sess.id, flow=sess.flow.name, role=sess.flow.role,
        turn_n=turn_n, msg=turn.msg, expected=turn.expected,
        actual=actual, conf=conf, lat=lat,
        ok=(actual == turn.expected), api_ok=api_ok, answer=answer
    )

async def run_session(sem: asyncio.Semaphore, client: httpx.AsyncClient,
                      sess: Session) -> list[TurnResult]:
    async with sem:
        results = []
        for i, turn in enumerate(sess.flow.turns, 1):
            r = await run_turn(client, sess, turn, i)
            results.append(r)
            si = "✅" if r.ok else "❌"
            m = turn.msg[:36] + "…" if len(turn.msg) > 36 else turn.msg
            print(f"  {sess.id[-7:]}:{i} {m:<38} {r.expected:<30} {r.actual:<30} {si}  {r.lat:>5.0f}")
        return results

# ──────────────────────────────────────────────────────────────────────────────
# Main runner
# ──────────────────────────────────────────────────────────────────────────────
async def run_all() -> list[TurnResult]:
    sessions = build_sessions()
    all_results: list[TurnResult] = []

    print(f"\n{'='*86}")
    print(f"  🚀 MULTI-TURN Chatbot Evaluation v5")
    print(f"  📋 {len(sessions)} sessions × 5 turns = {len(sessions)*3} turns")
    print(f"  🔀 Customer={sum(1 for s in sessions if s.flow.role=='customer')} | "
          f"Staff={sum(1 for s in sessions if s.flow.role=='staff')} | "
          f"Admin={sum(1 for s in sessions if s.flow.role=='admin')}")
    print(f"{'='*86}\n")

    # Health checks
    async with httpx.AsyncClient() as client:
        for health_url, label in [(HEALTH_C,"Customer"),(HEALTH_S,"Staff"),(HEALTH_A,"Admin")]:
            try:
                r = await client.get(health_url, timeout=5.0)
                print(f"  ✅ {label} server OK (HTTP {r.status_code})")
            except Exception as e:
                print(f"  ❌ {label} server ERROR: {e}")

    print()
    print(f"  {'ID:Turn':<9} {'Message':<38} {'Expected':<30} {'Got':<30} {'OK':3} {'ms':>6}")
    print(f"  {'-'*9} {'-'*37} {'-'*29} {'-'*29} {'---':3} {'---':>6}")
    sem = asyncio.Semaphore(SEMAPHORE)
    async with httpx.AsyncClient() as client:
        tasks = [run_session(sem, client, s) for s in sessions]
        batches = await asyncio.gather(*tasks)
        for batch in batches:
            all_results.extend(batch)

    return all_results

# ──────────────────────────────────────────────────────────────────────────────
# Report
# ──────────────────────────────────────────────────────────────────────────────
def report(results: list[TurnResult]) -> str:
    total    = len(results)
    api_ok   = sum(1 for r in results if r.api_ok)
    ok_all   = sum(1 for r in results if r.ok)
    srv_err  = sum(1 for r in results if r.actual == "SERVER_ERROR")
    conn_err = sum(1 for r in results if r.actual == "CONNECTION_ERROR")
    lats     = [r.lat for r in results if r.api_ok]
    avg_lat  = sum(lats) / len(lats) if lats else 0

    lines = [
        f"\n{'='*78}",
        f"  📊 KẾT QUẢ MULTI-TURN EVALUATION v5 – {total} turns",
        f"{'='*78}",
        f"  API OK       : {api_ok}/{total} = {api_ok/total*100:.1f}%",
        f"  Intent Acc   : {ok_all}/{total} = {ok_all/total*100:.1f}%",
        f"  SERVER_ERROR : {srv_err}",
        f"  CONN_ERROR   : {conn_err}",
        f"  Latency avg  : {avg_lat:.0f}ms",
        f"{'─'*78}",
    ]

    # Per-role breakdown
    for role in ["customer", "staff", "admin"]:
        grp = [r for r in results if r.role == role]
        ok  = sum(1 for r in grp if r.ok)
        pct = ok / len(grp) * 100 if grp else 0
        lines.append(f"  {role.upper():<12}: {ok:>3}/{len(grp):<4} = {pct:5.1f}%")

    # Per-turn breakdown
    lines.append(f"{'─'*78}  TURN BREAKDOWN")
    for turn_n in [1, 2, 3]:
        grp = [r for r in results if r.turn_n == turn_n]
        ok  = sum(1 for r in grp if r.ok)
        pct = ok / len(grp) * 100 if grp else 0
        lines.append(f"  Turn {turn_n}        : {ok:>3}/{len(grp):<4} = {pct:5.1f}%")

    # Failed cases (max 60)
    fail = [r for r in results if not r.ok]
    if fail:
        lines.append(f"\n  ❌ {len(fail)} FAILED TURNS (showing up to 60):")
        for r in fail[:60]:
            m = r.msg[:45]
            lines.append(
                f"    [{r.role:8}] {r.flow:30} T{r.turn_n} "
                f"[{r.expected}] → [{r.actual}]  \"{m}\""
            )

    lines.append(f"{'='*78}")
    rep = "\n".join(lines)
    print(rep)
    return rep


async def main():
    results = await run_all()
    if not results:
        return
    rep = report(results)
    d = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(d, "eval_report_multiturn.txt"), "w", encoding="utf-8") as f:
        f.write(rep)
    with open(os.path.join(d, "eval_results_multiturn.json"), "w", encoding="utf-8") as f:
        import json
        json.dump([{
            "session_id": r.session_id, "flow": r.flow, "role": r.role,
            "turn_n": r.turn_n, "message": r.msg,
            "expected_intent": r.expected, "actual_intent": r.actual,
            "confidence": round(r.conf, 3), "intent_correct": r.ok,
            "api_ok": r.api_ok, "latency_ms": round(r.lat, 1),
            "answer_preview": r.answer.strip(),
            "answer_full": r.answer.strip(),
        } for r in results], f, ensure_ascii=False, indent=2)
    print(f"\n  💾 Saved: eval_report_multiturn.txt + eval_results_multiturn.json")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
