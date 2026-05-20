import re

# Load the flows we generated
from tests.flows import CUSTOMER_FLOWS, STAFF_FLOWS, ADMIN_FLOWS, Flow, Turn
from tests.flows_part2 import CUSTOMER_FLOWS_2, STAFF_FLOWS_2, ADMIN_FLOWS_2

# Define the remaining 5 Staff flows and 5 Admin flows
STAFF_FLOWS_3 = [
    Flow("S-F11-Escalate-Revenue-CheckO-Low-OOS", "staff", [
        Turn("danh sach cac ticket dang bi delay", "staff_escalated_issues"),
        Turn("thu nhap ngay nay cua shop la", "staff_revenue_today"),
        Turn("check in don hang #10023", "staff_order_lookup"),
        Turn("ton kho sap het canh bao gap", "staff_inventory_low"),
        Turn("toi can 1 ky nghi ngot ngao", "staff_out_of_scope")
    ]),
    Flow("S-F12-Cust-ListPending-OOS-UpdateInv-Top", "staff", [
        Turn("gui ho infor khach id 44", "staff_customer_lookup"),
        Turn("gui ds don can chua xu ly hom nay", "staff_order_list_pending"),
        Turn("nay t thich uong cfe qua", "staff_out_of_scope"),
        Turn("update vao kho them 12 sach A", "staff_inventory_update"),
        Turn("dau la sp dang ban chay nhat hien tai", "staff_top_selling")
    ]),
    Flow("S-F13-Resolve-UpdateO-OOS-Chit-CheckInv", "staff", [
        Turn("close cai ticket #999 gap", "staff_complaint_resolve"),
        Turn("update trang thai giao hang don 11", "staff_order_status_update"),
        Turn("may bay sap bay roi", "staff_out_of_scope"),
        Turn("help toi tinh nang nay", "staff_chitchat"),
        Turn("stock hien ton co ma x0", "staff_inventory_check")
    ]),
    Flow("S-F14-CheckO-Return-Stats-Cust-OOS", "staff", [
        Turn("kiem tra xem don khach 329 sao roi", "staff_order_lookup"),
        Turn("xu ly doi tra hang loi cho kh", "staff_return_handle"),
        Turn("bao bao tk tong don cua thang n", "staff_order_statistics"),
        Turn("xem info khach id=11", "staff_customer_lookup"),
        Turn("dang lanh ghe mn oi", "staff_out_of_scope")
    ]),
    Flow("S-F15-UpdateO-Top-Rev-Chit-Pending", "staff", [
        Turn("chuyen don sang hoan thanh cua dh 70", "staff_order_status_update"),
        Turn("top sell nhat cua ht la", "staff_top_selling"),
        Turn("nay thu dc bn xien tu kh", "staff_revenue_today"),
        Turn("cam on em tro ly", "staff_chitchat"),
        Turn("xem log pending la", "staff_order_list_pending")
    ])
]

ADMIN_FLOWS_3 = [
    Flow("A-F11-PromoL-StatsU-OOS-ChkP-Health", "admin", [
        Turn("list giup may dang run voucher cai", "admin_promotion_list"),
        Turn("nhieu user moi add hnay k bo", "admin_user_stats"),
        Turn("toi o vinh ha long roi day", "admin_out_of_scope"),
        Turn("co check ho hop le code MUA10 khong", "admin_promotion_check"),
        Turn("xem hthong server con khoe", "admin_system_health")
    ]),
    Flow("A-F12-UpdateU-Top-Rev-OOS-StatsO", "admin", [
        Turn("thu hoi role staff thang nv kem id 2", "admin_user_update_role"),
        Turn("sach no thich dc nhat ht hien", "admin_top_books"),
        Turn("tong danh thu cnam the nao roi admin", "admin_revenue_report"),
        Turn("khoe ti tien xep nhe", "admin_out_of_scope"),
        Turn("thk đon bi reject r", "admin_order_stats")
    ]),
    Flow("A-F13-Dashboard-CreateP-Exp-Chit-OOS", "admin", [
        Turn("tong the quan tri dh ", "admin_dashboard_summary"),
        Turn("cho m them NEW1 code 15 nhe nhay", "admin_promotion_create"),
        Turn("ma dead roi ", "admin_promotion_expiring"),
        Turn("admin ", "admin_chitchat"),
        Turn("an dkem", "admin_out_of_scope")
    ]),
    Flow("A-F14-Health-PromoD-OOS-ListP-UpdateU", "admin", [
        Turn("check sys h g k o h h a x", "admin_system_health"),
        Turn("xoa ma giam x u n code v a k q i u m a q s d t", "admin_promotion_delete"),
        Turn("ngu ", "admin_out_of_scope"),
        Turn("v n y d b k m i l c t r b a m y a ", "admin_promotion_list"),
        Turn("chuyen tk 1 sang admin deee", "admin_user_update_role")
    ]),
    Flow("A-F15-OOS-StatsO-Top-Rev-Dash", "admin", [
        Turn("ngo ngan", "admin_out_of_scope"),
        Turn("thong ke tong don thang nay", "admin_order_stats"),
        Turn("sach b c cao", "admin_top_books"),
        Turn("bao cao dt", "admin_revenue_report"),
        Turn("xem db", "admin_dashboard_summary")
    ])
]

ALL_C = CUSTOMER_FLOWS + CUSTOMER_FLOWS_2
ALL_S = STAFF_FLOWS + STAFF_FLOWS_2 + STAFF_FLOWS_3
ALL_A = ADMIN_FLOWS + ADMIN_FLOWS_2 + ADMIN_FLOWS_3

def flow_to_code(f, var_name):
    # Generates code representation of a Flow list
    res = f"{var_name}: list[Flow] = [\n"
    for fl in f:
        res += f'    Flow("{fl.name}", "{fl.role}", [\n'
        for t in fl.turns:
            res += f'        Turn("{t.msg}", "{t.expected}"),\n'
        res += f'    ]),\n'
    res += "]\n"
    return res

code_c = flow_to_code(ALL_C, "CUSTOMER_FLOWS")
code_s = flow_to_code(ALL_S, "STAFF_FLOWS")
code_a = flow_to_code(ALL_A, "ADMIN_FLOWS")

new_code_block = code_c + "\n" + code_s + "\n" + code_a

# read the original file
target_file = r"tests\evaluate_chatbot_multiturn.py"
with open(target_file, "r", encoding="utf-8") as f:
    text = f.read()

# Replace everything from CUSTOMER_FLOWS: list[Flow] = [ ... to ADMIN_FLOWS: list[Flow] = [ ... ]
first_part = text.split("CUSTOMER_FLOWS: list[Flow] = [")[0]
last_part = text.split("@dataclass\nclass Session:\n")[1]

updated_text = first_part + new_code_block + "\n# ──────────────────────────────────────────────────────────────────────────────\n# Session builder\n# ──────────────────────────────────────────────────────────────────────────────\n@dataclass\nclass Session:\n" + last_part

# update comments "200 sessions × 3 turns = 600 turns"
updated_text = updated_text.replace("× 3 turns", "× 5 turns")
updated_text = updated_text.replace("= 600 turns", "= 1000 turns")

with open(target_file, "w", encoding="utf-8") as f:
    f.write(updated_text)

print("Patch complete! Generated file length:", len(updated_text))
