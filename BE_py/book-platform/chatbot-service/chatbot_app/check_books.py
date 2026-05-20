import sys
sys.path.append(r"d:\12_22110190_22110243_XayDungWebsiteBanSachTichHopHeThongGoiY\source-code\BE_py\book-platform\chatbot-service")
from chatbot_app.db import get_connection

conn = get_connection()
cur = conn.cursor(dictionary=True)
cur.execute("SELECT b.title FROM books b JOIN book_categories bc ON bc.book_id = b.book_id JOIN categories c ON c.category_id = bc.category_id WHERE c.category_name LIKE '%Truyện tranh%';")
books = cur.fetchall()
for b in books:
    print(b['title'])
print(f"Total: {len(books)}")
cur.close()
conn.close()
