import json, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

JSON_PATH = os.path.join(os.path.dirname(__file__), "eval_results_multiturn.json")
with open(JSON_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

def show(records, title, limit=8):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")
    for r in records[:limit]:
        ok = "✅" if r["intent_correct"] else "❌"
        ans = r.get("answer_full", "")[:150].replace("\n", " ")
        print(f"{ok} [{r['flow']} T{r['turn_n']}] {r['actual_intent']}")
        print(f"   MSG: {r['message'][:60]}")
        print(f"   ANS: {ans}")
        print()

# Staff
staff_ok   = [r for r in data if r["role"]=="staff" and r["intent_correct"]]
staff_fail = [r for r in data if r["role"]=="staff" and not r["intent_correct"]]
show(staff_ok[:8],   "STAFF – CORRECT SAMPLES")
show(staff_fail[:8], "STAFF – FAILED SAMPLES")

# Admin
admin_ok   = [r for r in data if r["role"]=="admin" and r["intent_correct"]]
admin_fail = [r for r in data if r["role"]=="admin" and not r["intent_correct"]]
show(admin_ok[:8],   "ADMIN – CORRECT SAMPLES")
show(admin_fail[:8], "ADMIN – FAILED SAMPLES")

# Answer quality issues
print(f"\n{'='*60}")
print("  ANSWER QUALITY ISSUES")
print(f"{'='*60}")
issues = {
    "book_detail asks name again": 0,
    "cart_help wrong book": 0,
    "payment missing bank info": 0,
    "OOS hallucinate (fixed)": 0,
}
for r in data:
    ans = r.get("answer_full","").lower()
    if r["actual_intent"]=="book_detail" and "ten sach" in ans and "vu tru" in r["message"]:
        issues["book_detail asks name again"] += 1
    if r["actual_intent"]=="cart_help" and ("dai hoc" in ans or "vo thuat" in ans) and "cuon do" in r["message"].lower():
        issues["cart_help wrong book"] += 1
    if r["actual_intent"]=="payment_method" and ("mst" in ans or "ten cong ty" in ans):
        issues["payment missing bank info"] += 1
    if r["actual_intent"]=="out_of_scope" and ("gio the thao" in ans or "28°c" in ans):
        issues["OOS hallucinate (fixed)"] += 1

for k, v in issues.items():
    print(f"  {k}: {v} occurrences")

print(f"\n--- SUMMARY ---")
total = len(data)
correct = sum(1 for r in data if r["intent_correct"])
for role in ["customer","staff","admin"]:
    grp = [r for r in data if r["role"]==role]
    ok  = sum(1 for r in grp if r["intent_correct"])
    print(f"  {role.upper():12}: {ok}/{len(grp)} = {ok/len(grp)*100:.1f}%")
print(f"  OVERALL     : {correct}/{total} = {correct/total*100:.1f}%")
