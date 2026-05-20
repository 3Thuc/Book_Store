# 📋 Chatbot API – Bộ Test Case Đầy Đủ (50 Testcase)

**Base URL:** `http://localhost:8004`

---

## 🔧 Endpoint Health Check

### TC00 – Health Check
```
GET http://localhost:8004/api/chat/health
```
**Expected:** `{"status":"ok","ollama":"ready"}`

---

## 📌 NHÓM A – CHITCHAT (Regex Tầng 1 – Instant 0ms)

### TC01 – Lời chào có dấu
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t01","message":"xin chào","user_id":null,"history":[]}
```
**Expected:** `intent=chitchat`, `confidence=0.95`, Trả lời bằng greeting template

### TC02 – Lời chào không dấu
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t02","message":"xin chao","user_id":null,"history":[]}
```
**Expected:** `intent=chitchat`, `confidence=0.95`

### TC03 – Hỏi bot là ai
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t03","message":"ban la ai","user_id":null,"history":[]}
```
**Expected:** `intent=chitchat`

### TC04 – Cảm ơn
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t04","message":"cảm ơn bạn nhé","user_id":null,"history":[]}
```
**Expected:** `intent=chitchat`, Trả lời "Không có gì..."

### TC05 – Tạm biệt
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t05","message":"tam biet nhe","user_id":null,"history":[]}
```
**Expected:** `intent=chitchat`, Farewell template

---

## 📌 NHÓM B – TÌM SÁCH

### TC06 – Tìm sách có dấu
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t06","message":"tìm sách kỹ năng sống","user_id":null,"history":[]}
```
**Expected:** `intent=book_search`, Trả về list ≥1 sách, có giá và trạng thái

### TC07 – Tìm sách không dấu
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t07","message":"tim sach ky nang song","user_id":null,"history":[]}
```
**Expected:** `intent=book_search`, `confidence=0.95` (Regex match)

### TC08 – Tìm theo tác giả
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t08","message":"tìm sách của Nguyễn Nhật Ánh","user_id":null,"history":[]}
```
**Expected:** `intent=book_search`, Sách của Nguyễn Nhật Ánh

### TC09 – Tìm sách thể loại trinh thám
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t09","message":"tim sach the loai trinh tham","user_id":null,"history":[]}
```
**Expected:** `intent=book_search`, sách có nội dung phù hợp

### TC10 – Cụm từ phức hợp (SBERT Tầng 2)
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t10","message":"cho tôi xem những cuốn sách về tâm lý học","user_id":null,"history":[]}
```
**Expected:** `intent=book_search` hoặc `recommend_category`, có sách trả về

---

## 📌 NHÓM C – GỢI Ý SÁCH

### TC11 – Gợi ý cho người mới học lập trình (Guest)
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t11","message":"goi y sach cho nguoi moi bat dau hoc lap trinh","user_id":null,"history":[]}
```
**Expected:** `intent=recommend_personal`, Sách lập trình, hint đăng nhập ở cuối

### TC12 – Gợi ý cho người đã đăng nhập
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t12","message":"gợi ý sách cho tôi","user_id":123,"history":[]}
```
**Expected:** `intent=recommend_personal`, Sách tùy profile user 123

### TC13 – Sách đang bán chạy
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t13","message":"sach ban chay nhat hien nay","user_id":null,"history":[]}
```
**Expected:** `intent=recommend_trending`, Top 5 bestseller

### TC14 – Sách làm quà tặng
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t14","message":"muon mua sach tang ban gai","user_id":null,"history":[]}
```
**Expected:** `intent=recommend_gift`, Sách phù hợp làm quà

### TC15 – Sách theo thể loại
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t15","message":"gợi ý sách văn học hay nhất","user_id":null,"history":[]}
```
**Expected:** `intent=recommend_category`, Sách văn học

---

## 📌 NHÓM D – ĐƠN HÀNG

### TC16 – Kiểm tra đơn hàng (không dấu)
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t16","message":"kiem tra don hang cua toi","user_id":null,"history":[]}
```
**Expected:** `intent=order_status`, Hỏi mã đơn hàng

### TC17 – Theo dõi đơn hàng
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t17","message":"đơn hàng của tôi đến đâu rồi","user_id":null,"history":[]}
```
**Expected:** `intent=order_status`, `confidence=0.95`

### TC18 – Xem lịch sử mua hàng
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t18","message":"xem lich su mua hang cua toi","user_id":null,"history":[]}
```
**Expected:** `intent=order_history`

### TC19 – Hủy đơn hàng
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t19","message":"toi muon huy don hang","user_id":null,"history":[]}
```
**Expected:** `intent=order_cancel`, Hướng dẫn hủy đơn

---

## 📌 NHÓM E – ĐỔI TRẢ & KHIẾU NẠI

### TC20 – Chính sách đổi trả
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t20","message":"chinh sach doi tra nhu the nao","user_id":null,"history":[]}
```
**Expected:** `intent=return_policy`, Nội dung từ KB (knowledge base)

### TC21 – Muốn đổi sách bị hỏng
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t21","message":"sach toi mua bi rach bia muon doi","user_id":null,"history":[]}
```
**Expected:** `intent=complaint_damaged` hoặc `return_request`

### TC22 – Giao sai sách
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t22","message":"họ giao nhầm sách rồi không đúng cuốn tôi đặt","user_id":null,"history":[]}
```
**Expected:** `intent=complaint_wrong`

### TC23 – Yêu cầu hoàn tiền
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t23","message":"toi muon hoan tien","user_id":null,"history":[]}
```
**Expected:** `intent=return_request` hoặc `return_policy`

---

## 📌 NHÓM F – THANH TOÁN

### TC24 – Phương thức thanh toán
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t24","message":"shop co nhan thanh toan qua momo khong","user_id":null,"history":[]}
```
**Expected:** `intent=payment_method`, Thông tin từ KB

### TC25 – Thanh toán COD
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t25","message":"có nhận COD không","user_id":null,"history":[]}
```
**Expected:** `intent=payment_method`, Xác nhận COD

### TC26 – Lỗi thanh toán
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t26","message":"thanh toán thất bại rồi phải làm gì","user_id":null,"history":[]}
```
**Expected:** `intent=payment_issue`, Hướng dẫn xử lý lỗi

---

## 📌 NHÓM G – KHUYẾN MÃI

### TC27 – Có khuyến mãi gì không
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t27","message":"dang co chuong trinh khuyen mai gi khong","user_id":null,"history":[]}
```
**Expected:** `intent=promotion_current`

### TC28 – Mã giảm giá
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t28","message":"ma giam gia BOOK50 con dung duoc khong","user_id":null,"history":[]}
```
**Expected:** `intent=voucher_apply` hoặc `promotion_current`

### TC29 – Điểm thưởng
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t29","message":"diem tich luy cua toi con bao nhieu","user_id":null,"history":[]}
```
**Expected:** `intent=loyalty_points`

---

## 📌 NHÓM H – THÔNG TIN CỬA HÀNG

### TC30 – Hotline hỗ trợ
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t30","message":"so hotline ho tro la bao nhieu","user_id":null,"history":[]}
```
**Expected:** `intent=store_info`, Số điện thoại từ KB

### TC31 – Thời gian giao hàng
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t31","message":"giao hang mat may ngay","user_id":null,"history":[]}
```
**Expected:** `intent=store_info`, Thông tin vận chuyển

---

## 📌 NHÓM I – CONTEXT / MULTI-TURN (Hội Thoại Nhiều Lượt)

### TC32 – Hỏi follow-up với history
```json
POST http://localhost:8004/api/chat/message
{
  "session_id":"t32",
  "message":"cuốn thứ 2 giá bao nhiêu",
  "user_id":null,
  "history":[
    {"role":"user","content":"tìm sách kỹ năng sống"},
    {"role":"assistant","content":"1. Đắc Nhân Tâm - 89,000đ\n2. 7 Thói Quen - 115,000đ\n3. Tuổi Trẻ Đáng Giá Bao Nhiêu - 79,000đ"}
  ]
}
```
**Expected:** `intent=book_detail`, Trả lời "7 Thói Quen - 115,000đ"

### TC33 – Xác nhận sau gợi ý
```json
POST http://localhost:8004/api/chat/message
{
  "session_id":"t33",
  "message":"ok tôi muốn mua cuốn đó",
  "user_id":null,
  "history":[
    {"role":"user","content":"gợi ý sách lập trình Python"},
    {"role":"assistant","content":"Cuốn Lập Trình Python Cơ Bản - 120,000đ đang còn hàng"}
  ]
}
```
**Expected:** `intent=confirmation_yes`, Hướng dẫn thêm vào giỏ

---

## 📌 NHÓM J – EDGE CASES

### TC34 – Câu rất ngắn
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t34","message":"sách","user_id":null,"history":[]}
```
**Expected:** Không crash, trả lời hợp lý

### TC35 – Câu quá dài (stress test)
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t35","message":"tôi muốn tìm một cuốn sách rất hay về chủ đề kỹ năng mềm dành cho sinh viên mới ra trường đang tìm việc làm với giá cả phải chăng dưới 100 ngàn đồng có thể giao hàng nhanh trong ngày","user_id":null,"history":[]}
```
**Expected:** `intent=book_search`, Trả về sách phù hợp, không timeout

### TC36 – Ngoài phạm vi (out of scope)
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t36","message":"thoi tiet hom nay the nao","user_id":null,"history":[]}
```
**Expected:** `intent=out_of_scope` hoặc `general_query`, Từ chối khéo léo

### TC37 – Tiếng Anh
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t37","message":"can you recommend me some good books","user_id":null,"history":[]}
```
**Expected:** Trả lời được (có thể bằng tiếng Anh hoặc Việt)

### TC38 – Lẫn lộn tiếng Việt - tiếng Anh
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t38","message":"tôi muốn order một cuốn book về marketing","user_id":null,"history":[]}
```
**Expected:** `intent=book_search`

### TC39 – Nhiều intent trong 1 câu
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t39","message":"tìm sách Python vừa rẻ vừa hay và cho tôi biết chính sách đổi trả","user_id":null,"history":[]}
```
**Expected:** Xử lý intent chính (book_search), không crash

### TC40 – Câu chứa ký tự đặc biệt
```json
POST http://localhost:8004/api/chat/message
{"session_id":"t40","message":"sách <script>alert(1)</script> hay nhất","user_id":null,"history":[]}
```
**Expected:** Không bị XSS, trả lời an toàn

---

## 📌 NHÓM K – SESSION & PERSISTENCE

### TC41 – Lưu session DB (cần kiểm tra MySQL sau)
```json
POST http://localhost:8004/api/chat/message
{"session_id":"persistent_01","message":"xin chào","user_id":null,"history":[]}
```
Sau đó kiểm tra DB:
```sql
SELECT * FROM chat_messages WHERE session_id = 'persistent_01';
```
**Expected:** Có record trong bảng `chat_messages`

### TC42 – Cùng session_id nhiều lần (thread-safe)
Gửi 3 request liên tiếp với cùng `session_id: "concurrent_test"` 
**Expected:** Cả 3 đều trả về response hợp lệ

---

## 🔎 Lệnh Curl nhanh (Terminal Windows CMD)

```cmd
:: TC01 - Greeting
curl -X POST http://localhost:8004/api/chat/message -H "Content-Type: application/json" -d "{\"session_id\":\"t01\",\"message\":\"xin chao\",\"user_id\":null,\"history\":[]}"

:: TC06 - Book Search
curl -X POST http://localhost:8004/api/chat/message -H "Content-Type: application/json" -d "{\"session_id\":\"t06\",\"message\":\"tim sach ky nang song\",\"user_id\":null,\"history\":[]}"

:: TC11 - Recommend (Guest)
curl -X POST http://localhost:8004/api/chat/message -H "Content-Type: application/json" -d "{\"session_id\":\"t11\",\"message\":\"goi y sach lap trinh Python\",\"user_id\":null,\"history\":[]}"

:: TC13 - Trending
curl -X POST http://localhost:8004/api/chat/message -H "Content-Type: application/json" -d "{\"session_id\":\"t13\",\"message\":\"sach ban chay nhat hien nay\",\"user_id\":null,\"history\":[]}"

:: TC16 - Order Status
curl -X POST http://localhost:8004/api/chat/message -H "Content-Type: application/json" -d "{\"session_id\":\"t16\",\"message\":\"kiem tra don hang cua toi\",\"user_id\":null,\"history\":[]}"

:: TC20 - Return Policy
curl -X POST http://localhost:8004/api/chat/message -H "Content-Type: application/json" -d "{\"session_id\":\"t20\",\"message\":\"chinh sach doi tra nhu the nao\",\"user_id\":null,\"history\":[]}"

:: TC24 - Payment
curl -X POST http://localhost:8004/api/chat/message -H "Content-Type: application/json" -d "{\"session_id\":\"t24\",\"message\":\"co nhan thanh toan qua momo khong\",\"user_id\":null,\"history\":[]}"

:: TC30 - Store Info
curl -X POST http://localhost:8004/api/chat/message -H "Content-Type: application/json" -d "{\"session_id\":\"t30\",\"message\":\"so hotline ho tro la bao nhieu\",\"user_id\":null,\"history\":[]}"

:: Health Check
curl http://localhost:8004/api/chat/health
```

---

## 📊 Expected Results Summary

| Nhóm | TC | Engine | Expected Intent | Nguồn dữ liệu |
|------|-----|--------|----------------|--------------|
| Chitchat | TC01-05 | Regex (Tầng 1) | chitchat | Template |
| Tìm sách | TC06-10 | Regex + SBERT | book_search | OpenSearch |
| Gợi ý | TC11-15 | Regex + SBERT | recommend_* | OpenSearch/MySQL |
| Đơn hàng | TC16-19 | Regex (Tầng 1) | order_* | Template/KB |
| Đổi trả | TC20-23 | Regex + SBERT | return_* | KB (FAQ) |
| Thanh toán | TC24-26 | Regex (Tầng 1) | payment_* | KB (FAQ) |
| Khuyến mãi | TC27-29 | SBERT (Tầng 2) | promotion_* | KB (FAQ) |
| Store Info | TC30-31 | Regex (Tầng 1) | store_info | KB (FAQ) |
| Multi-turn | TC32-33 | SBERT w/ history | varies | context |
| Edge cases | TC34-40 | varies | varies | fallback safe |
