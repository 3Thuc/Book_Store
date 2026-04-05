"""
seed_interactions.py – Sinh dữ liệu tương tác giả để làm giàu hệ thống gợi ý
==============================================================================
Script này tạo dữ liệu REALISTIC (không phải ngẫu nhiên hoàn toàn) cho:
  1. user_actions   – view / add_to_cart / purchase  (dùng để train CF, CB)
  2. orders         – đơn hàng giả với sách mới
  3. order_details  – chi tiết đơn (book_id, quantity, price)
  4. cart_items     – giỏ hàng hiện tại của user
  5. ratings        – đánh giá sách (1-5 sao, avg_rating update theo)

Chiến lược phân bổ tương tác:
  - Sách mới (chưa có interaction nào) → nhận nhiều interaction hơn
  - Phân bổ theo danh mục (sách cùng nhóm category hay được mua chung)
  - Tỉ lệ thực tế: view >> add_to_cart > purchase
  - User hành vi: xem nhiều sách cùng category

Cách dùng:
  python seed_interactions.py                    # Mặc định: 500 orders
  python seed_interactions.py --orders 1000      # Sinh 1000 orders giả
  python seed_interactions.py --dry-run          # Xem thống kê, không ghi
  python seed_interactions.py --only-actions     # Chỉ sinh user_actions
  python seed_interactions.py --only-ratings     # Chỉ sinh ratings
  python seed_interactions.py --new-books-only   # Chỉ tập trung sách chưa có data
"""

import argparse
import json
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

import mysql.connector

# ─── CẤU HÌNH ────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "1900561275Nghia",
    "database": "bookstore",
    "charset":  "utf8mb4",
}

DEFAULT_ORDERS         = 500    # số đơn hàng sinh ra
VIEWS_PER_ORDER        = 12     # trung bình 12 lượt xem → 1 đơn
CART_RATIO             = 0.4    # 40% xem → thêm vào giỏ
RATING_RATIO           = 0.35   # 35% người mua → để lại đánh giá
BOOKS_PER_ORDER_MAX    = 4      # tối đa 4 sách/đơn
DATE_RANGE_DAYS        = 180    # sinh data trải đều trong 6 tháng qua

# Template review theo số sao
REVIEW_TEMPLATES = {
    5: ["Sách rất hay, đọc xong còn muốn đọc lại!", "Nội dung xuất sắc, rất đáng tiền.",
        "Tuyệt vời! Giao hàng nhanh, sách đẹp.", "Một trong những cuốn hay nhất tôi từng đọc.",
        "Hoàn toàn hài lòng, sẽ giới thiệu cho bạn bè."],
    4: ["Sách hay, nội dung bổ ích.", "Đọc được, có một số chỗ hơi dài dòng.",
        "Nhìn chung tốt, giao hàng khá nhanh.", "Sách chất lượng tốt, nội dung phong phú."],
    3: ["Sách bình thường, không có gì đặc biệt.", "Nội dung ổn nhưng hơi nhàm.",
        "Đọc được, phù hợp cho người mới bắt đầu.", "Tạm được, mình mong đợi nhiều hơn."],
    2: ["Nội dung không như kỳ vọng.", "Khá nhàm, khó đọc.", "Không hài lòng lắm."],
    1: ["Sách không hay, không đáng tiền.", "Thất vọng với nội dung."],
}


def connect():
    return mysql.connector.connect(**DB_CONFIG)


def rand_date(days_back=DATE_RANGE_DAYS):
    """Random timestamp trong range_days ngày qua."""
    now = datetime.now()
    delta = timedelta(seconds=random.randint(0, days_back * 86400))
    return now - delta


# ═══════════════════════════════════════════════════════════════════════════════
#  LẤY DỮ LIỆU TỪ DB
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_users(cur):
    cur.execute("""
        SELECT user_id FROM users
        WHERE role='customer' AND status='active' AND is_deleted=0
        ORDER BY user_id
    """)
    return [r[0] for r in cur.fetchall()]


def fetch_books(cur, new_books_only=False):
    """Trả về list of (book_id, price, category_id) – sách active."""
    if new_books_only:
        # Sách chưa có bất kỳ interaction nào
        cur.execute("""
            SELECT b.book_id, b.price,
                   (SELECT bc.category_id FROM book_categories bc
                    WHERE bc.book_id=b.book_id LIMIT 1) AS cat_id
            FROM books b
            WHERE b.status='active'
              AND b.book_id NOT IN (SELECT DISTINCT book_id FROM user_actions WHERE book_id IS NOT NULL)
            ORDER BY b.book_id
        """)
    else:
        cur.execute("""
            SELECT b.book_id, b.price,
                   (SELECT bc.category_id FROM book_categories bc
                    WHERE bc.book_id=b.book_id LIMIT 1) AS cat_id
            FROM books b
            WHERE b.status='active'
            ORDER BY b.book_id
        """)
    return cur.fetchall()


def fetch_addresses(cur, user_id):
    cur.execute("SELECT address_id FROM addresses WHERE user_id=%s LIMIT 1", (user_id,))
    r = cur.fetchone()
    return r[0] if r else None


def fetch_cart_id(cur, user_id):
    """Lấy hoặc tạo cart cho user."""
    cur.execute("SELECT cart_id FROM carts WHERE user_id=%s", (user_id,))
    r = cur.fetchone()
    if r:
        return r[0]
    cur.execute("INSERT INTO carts (user_id) VALUES (%s)", (user_id,))
    return cur.lastrowid


def books_by_category(books):
    """Nhóm book_ids theo category_id."""
    cat_map: dict[int, list] = {}
    for (book_id, price, cat_id) in books:
        key = cat_id if cat_id else 0
        cat_map.setdefault(key, []).append((book_id, price))
    return cat_map


# ═══════════════════════════════════════════════════════════════════════════════
#  1. SINH USER_ACTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def seed_user_actions(cur, users, books, n_orders, dry_run=False):
    """
    Sinh user_actions (view, add_to_cart, purchase).
    Chiến lược: mỗi 'purchase session' = xem nhiều sách → thêm vài cái → mua.
    """
    print("\n" + "="*55)
    print("📊 SINH USER_ACTIONS")
    print("="*55)

    cat_map = books_by_category(books)
    cat_keys = list(cat_map.keys())
    book_list = [(b[0], b[1]) for b in books]  # (book_id, price)

    actions_view = actions_cart = actions_purchase = 0
    batch = []

    for _ in range(n_orders):
        user_id = random.choice(users)
        session_date = rand_date()

        # Chọn category session (user hay xem cùng nhóm)
        cat_id = random.choice(cat_keys)
        cat_books = cat_map.get(cat_id, book_list)
        if not cat_books:
            cat_books = book_list

        # Số sách xem trong session
        n_view = random.randint(5, VIEWS_PER_ORDER)
        viewed = random.sample(cat_books, min(n_view, len(cat_books)))

        for i, (book_id, price) in enumerate(viewed):
            view_time = session_date + timedelta(minutes=i * random.randint(2, 8))
            batch.append((user_id, book_id, "view",
                          json.dumps({"source": "category_page", "duration_sec": random.randint(30, 300)}),
                          view_time))
            actions_view += 1

        # Add to cart (subset)
        n_cart = max(1, int(len(viewed) * CART_RATIO))
        carted = random.sample(viewed, min(n_cart, len(viewed)))
        for book_id, price in carted:
            cart_time = session_date + timedelta(minutes=random.randint(15, 40))
            batch.append((user_id, book_id, "add_to_cart",
                          json.dumps({"quantity": random.randint(1, 2)}),
                          cart_time))
            actions_cart += 1

        # Purchase (subset của carted)
        n_buy = max(1, int(len(carted) * 0.8))
        bought = random.sample(carted, min(n_buy, len(carted)))
        purchase_time = session_date + timedelta(minutes=random.randint(45, 90))
        for book_id, price in bought:
            batch.append((user_id, book_id, "purchase",
                          json.dumps({"quantity": 1, "price": float(price)}),
                          purchase_time))
            actions_purchase += 1

        # Insert theo batch 500
        if len(batch) >= 500 and not dry_run:
            cur.executemany("""
                INSERT INTO user_actions (user_id, book_id, action_type, metadata, action_date)
                VALUES (%s, %s, %s, %s, %s)
            """, batch)
            batch.clear()

    if batch and not dry_run:
        cur.executemany("""
            INSERT INTO user_actions (user_id, book_id, action_type, metadata, action_date)
            VALUES (%s, %s, %s, %s, %s)
        """, batch)

    total = actions_view + actions_cart + actions_purchase
    print(f"  view       : {actions_view:>6,}")
    print(f"  add_to_cart: {actions_cart:>6,}")
    print(f"  purchase   : {actions_purchase:>6,}")
    print(f"  Tổng       : {total:>6,} actions")
    return total


# ═══════════════════════════════════════════════════════════════════════════════
#  2. SINH ORDERS + ORDER_DETAILS
# ═══════════════════════════════════════════════════════════════════════════════

def seed_orders(cur, conn, users, books, n_orders, dry_run=False):
    print("\n" + "="*55)
    print("🛒 SINH ORDERS + ORDER_DETAILS")
    print("="*55)

    cat_map    = books_by_category(books)
    cat_keys   = list(cat_map.keys())
    book_list  = [(b[0], b[1]) for b in books]
    statuses   = ["delivered"] * 6 + ["shipped"] * 2 + ["processing"] * 1 + ["pending"] * 1
    pay_methods = ["COD"] * 5 + ["E-Wallet"] * 3 + ["CreditCard"] * 2

    order_count = detail_count = 0

    for i in range(n_orders):
        user_id  = random.choice(users)
        addr_id  = fetch_addresses(cur, user_id) if not dry_run else 1
        order_dt = rand_date()

        # Chọn sách cho đơn hàng (ưu tiên cùng category)
        cat_id = random.choice(cat_keys)
        pool = cat_map.get(cat_id, book_list)
        if not pool:
            pool = book_list
        n_books     = random.randint(1, min(BOOKS_PER_ORDER_MAX, len(pool)))
        order_books = random.sample(pool, n_books)

        subtotal = sum(float(p) * random.randint(1, 2) for _, p in order_books)
        discount = round(subtotal * random.choice([0, 0, 0, 0.05, 0.1]), 2)
        total    = round(subtotal - discount, 2)
        status   = random.choice(statuses)
        pay_m    = random.choice(pay_methods)
        pay_s    = "paid" if status in ("delivered", "shipped") else "unpaid"

        if not dry_run:
            cur.execute("""
                INSERT INTO orders
                  (user_id, address_id, order_date, status, payment_method,
                   payment_status, subtotal, discount_amount, total_amount, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, addr_id, order_dt, status, pay_m,
                  pay_s, round(subtotal, 2), discount, total, order_dt))
            order_id = cur.lastrowid

            for (book_id, price) in order_books:
                qty        = random.randint(1, 2)
                unit_price = float(price)
                total_p    = round(unit_price * qty, 2)

                # Lấy thêm thông tin sách cho order_details
                cur.execute("""
                    SELECT b.title, b.description, b.publication_year, b.isbn,
                           a.author_name, p.publisher_name
                    FROM books b
                    LEFT JOIN authors a ON b.author_id = a.author_id
                    LEFT JOIN publishers p ON b.publisher_id = p.publisher_id
                    WHERE b.book_id = %s
                """, (book_id,))
                binfo = cur.fetchone()
                book_name   = binfo[0][:255] if binfo else ""
                book_desc   = binfo[1] if binfo else ""
                pub_year    = binfo[2] if binfo else None
                isbn        = binfo[3] if binfo else None
                author_name = binfo[4][:256] if binfo and binfo[4] else None
                pub_name    = binfo[5][:100] if binfo and binfo[5] else None

                cur.execute("""
                    INSERT INTO order_details
                      (order_id, book_id, book_name, book_description, author_name,
                       publication_year, publisher_name, isbn, quantity, unit_price, total_price)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (order_id, book_id, book_name, book_desc, author_name,
                      pub_year, pub_name, isbn, qty, unit_price, total_p))
                detail_count += 1

            # Cập nhật stock_quantity
            for (book_id, _) in order_books:
                cur.execute("""
                    UPDATE books SET stock_quantity = GREATEST(0, stock_quantity - 1)
                    WHERE book_id = %s
                """, (book_id,))

        order_count += 1
        if order_count % 100 == 0:
            if not dry_run:
                conn.commit()
            print(f"  ... {order_count}/{n_orders} orders")

    print(f"  ✅ {order_count:,} orders | {detail_count:,} order_details")
    return order_count


# ═══════════════════════════════════════════════════════════════════════════════
#  3. SINH CART_ITEMS
# ═══════════════════════════════════════════════════════════════════════════════

def seed_cart_items(cur, users, books, dry_run=False):
    print("\n" + "="*55)
    print("🛍  SINH CART_ITEMS (giỏ hàng hiện tại)")
    print("="*55)

    book_list = [(b[0], b[1]) for b in books]
    count = 0

    # Lấy 40% user ngẫu nhiên để có hàng trong giỏ
    sample_users = random.sample(users, max(1, len(users) // 2))

    for user_id in sample_users:
        if dry_run:
            count += random.randint(1, 3)
            continue
        cart_id = fetch_cart_id(cur, user_id)
        n_items = random.randint(1, 3)
        cart_books = random.sample(book_list, min(n_items, len(book_list)))
        for (book_id, _) in cart_books:
            try:
                cur.execute("""
                    INSERT IGNORE INTO cart_items (cart_id, book_id, quantity)
                    VALUES (%s, %s, %s)
                """, (cart_id, book_id, random.randint(1, 2)))
                count += 1
            except Exception:
                pass

    print(f"  ✅ {count:,} cart_items")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  4. SINH RATINGS VÀ CẬP NHẬT avg_rating
# ═══════════════════════════════════════════════════════════════════════════════

def seed_ratings(cur, users, books, dry_run=False):
    print("\n" + "="*55)
    print("⭐ SINH RATINGS")
    print("="*55)

    book_ids = [b[0] for b in books]
    count = 0
    pairs_done = set()

    # Lấy các cặp (user_id, book_id) đã tồn tại để tránh trùng UNIQUE KEY
    cur.execute("SELECT user_id, book_id FROM ratings")
    for row in cur.fetchall():
        pairs_done.add((row[0], row[1]))

    batch = []
    for book_id in book_ids:
        n_raters = random.randint(3, 20)
        raters = random.sample(users, min(n_raters, len(users)))
        for user_id in raters:
            if (user_id, book_id) in pairs_done:
                continue
            pairs_done.add((user_id, book_id))

            # Phân bổ sao: nghiêng về 4-5
            star = random.choices([1, 2, 3, 4, 5],
                                   weights=[2, 3, 10, 35, 50])[0]
            review = random.choice(REVIEW_TEMPLATES[star])
            rate_date = rand_date()
            batch.append((user_id, book_id, star, review, "approved", rate_date))
            count += 1

    if not dry_run and batch:
        cur.executemany("""
            INSERT IGNORE INTO ratings (user_id, book_id, rating, review, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, batch)

        # Cập nhật avg_rating và rating_count cho từng sách
        print("  🔄 Cập nhật avg_rating trong bảng books...")
        cur.execute("""
            UPDATE books b
            JOIN (
                SELECT book_id,
                       AVG(rating) AS avg_r,
                       COUNT(*)    AS cnt_r
                FROM ratings
                WHERE status = 'approved'
                GROUP BY book_id
            ) r ON b.book_id = r.book_id
            SET b.avg_rating    = ROUND(r.avg_r, 2),
                b.rating_count  = r.cnt_r
        """)

    print(f"  ✅ {count:,} ratings mới")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Sinh dữ liệu tương tác giả để làm giàu hệ thống gợi ý")
    parser.add_argument("--orders",         type=int, default=DEFAULT_ORDERS,
                        help=f"Số đơn hàng sinh ra (mặc định: {DEFAULT_ORDERS})")
    parser.add_argument("--dry-run",        action="store_true",
                        help="Xem thống kê, không ghi DB")
    parser.add_argument("--only-actions",   action="store_true",
                        help="Chỉ sinh user_actions")
    parser.add_argument("--only-ratings",   action="store_true",
                        help="Chỉ sinh ratings")
    parser.add_argument("--only-orders",    action="store_true",
                        help="Chỉ sinh orders + order_details")
    parser.add_argument("--new-books-only", action="store_true",
                        help="Chỉ tập trung vào sách chưa có interaction")
    args = parser.parse_args()

    print(f"🔗 Kết nối MySQL...")
    conn = connect()
    cur  = conn.cursor()
    print("✅ Kết nối thành công")

    # Lấy dữ liệu gốc
    users = fetch_users(cur)
    books = fetch_books(cur, new_books_only=args.new_books_only)

    if not users:
        print("❌ Không có user nào (role=customer, status=active). Vui lòng seed users trước.")
        sys.exit(1)
    if not books:
        print("❌ Không có sách nào (status=active).")
        sys.exit(1)

    print(f"\n📋 Nguồn dữ liệu:")
    print(f"   Users  : {len(users):,}")
    print(f"   Sách   : {len(books):,} {'(mới, chưa có interaction)' if args.new_books_only else ''}")
    print(f"   Orders : {args.orders:,} sẽ được sinh")
    if args.dry_run:
        print("\n🔍 DRY-RUN – không ghi DB\n")

    total = 0

    only_something = args.only_actions or args.only_ratings or args.only_orders

    if not only_something or args.only_actions:
        total += seed_user_actions(cur, users, books, args.orders, args.dry_run)
        if not args.dry_run: conn.commit()

    if not only_something or args.only_orders:
        total += seed_orders(cur, conn, users, books, args.orders, args.dry_run)
        if not args.dry_run: conn.commit()

    if not only_something:
        total += seed_cart_items(cur, users, books, args.dry_run)
        if not args.dry_run: conn.commit()

    if not only_something or args.only_ratings:
        total += seed_ratings(cur, users, books, args.dry_run)
        if not args.dry_run: conn.commit()

    cur.close()
    conn.close()

    print(f"\n{'='*55}")
    if args.dry_run:
        print(f"🔍 DRY-RUN xong. Ước tính tổng ~{total:,} bản ghi.")
        print(f"   Chạy không có --dry-run để ghi thật.")
    else:
        print(f"🎉 HOÀN TẤT! Đã sinh ~{total:,} bản ghi tương tác.")
        print(f"   Hệ thống gợi ý có thể được re-train ngay bây giờ.")


if __name__ == "__main__":
    main()
