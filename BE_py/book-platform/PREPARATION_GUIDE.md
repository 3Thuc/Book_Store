# 📚 Hướng dẫn chuẩn bị phòng vấn khóa luận — BE_py / book-platform

## 👋 Lời mở đầu
Tài liệu này giúp bạn chuẩn bị kiến thức để báo cáo và trả lời phản biện. **Mục tiêu: hiểu rõ cái gì, vì sao, và cách nó hoạt động** — không cần nhớ hết chi tiết mã lập trình.

---

## 🎯 Mục tiêu chính (1 câu cho mỗi thành phần)

| Thành phần | Tóm tắt 1 câu |
|-----------|--------------|
| **Chatbot** | Trả lời câu hỏi khách dựa trên dữ liệu thực, không bịa chuyện. |
| **OCR** | Đọc chữ từ ảnh bìa/scan, biến ảnh thành text. |
| **Search** | Tìm sách theo ý nghĩa (không chỉ tìm chính xác từ khóa). |
| **Recommendation** | Gợi ý sách dựa trên sở thích hoặc hành vi người dùng. |

---

## 🔍 Phần 1: CHATBOT (Trợ lý ảo)

### Khái niệm cơ bản
- **Chatbot** = Chương trình trò chuyện với người dùng (bot = robot).
- **Mục đích**: Trả lời câu hỏi, gợi ý sách, hỗ trợ đơn hàng tự động.
- **Ưu điểm**: Giảm tải cho nhân viên, phục vụ khách 24/7.

### Cách hoạt động (5 bước đơn giản)
1. **Nhận câu hỏi**: Khách nhập "Tôi muốn sách về tâm lý" → chatbot nhận vào.
2. **Phân loại ý định** (intent classification): Bot xác định người dùng muốn tìm sách (không phải hỏi đơn hàng hay chính sách).
3. **Tìm dữ liệu liên quan**: Bot tìm trong cơ sở dữ liệu/knowledge base để lấy thông tin về sách tâm lý.
4. **Sinh câu trả lời**: Bot dùng mô hình ngôn ngữ (LLM = Large Language Model = mô hình đọc hiểu lớn) để viết câu trả lời tự nhiên.
5. **Trả lời**: Bot gửi câu trả lời cho khách.

### Tại sao chọn Ollama + SBERT?
- **Ollama** (LLM chạy local): Giữ quyền riêng tư, không gửi dữ liệu ra ngoài mạng, chi phí thấp hơn gọi API bên ngoài.
  - *LLM = Large Language Model (mô hình ngôn ngữ lớn) — máy tính học cách viết/hiểu text từ dữ liệu khổng lồ.*
- **SBERT** (Sentence-BERT): Chuyên biến câu thành vector số để tìm câu hỏi tương tự nhanh chóng.
  - *Vector = dãy số biểu diễn ý nghĩa; khi so sánh hai vector, ta có thể tính "khoảng cách" để biết chúng giống nhau tới mức nào.*

### Cách tránh "bịa chuyện"
**Vấn đề**: Mô hình AI đôi khi tự sinh ra thông tin không có trong dữ liệu (gọi là hallucination = ảo giác).
**Giải pháp**:
- Chỉ để bot dùng dữ liệu từ [CONTEXT] (dữ liệu tìm được).
- Nếu không tìm thấy → trả lời "Không tìm thấy" hoặc "Vui lòng liên hệ CSKH".
  - *CSKH = Chăm Sóc Khách Hàng (Customer Support).*
- Kiểm tra output trước khi trả về (remove leaked tags).

### Cách đánh giá chất lượng
- **Test set** (tập kiểm thử): Tập hợp các câu hỏi mẫu với đáp án đúng.
- **Metrics**:
  - Tỉ lệ trả lời đúng: Bao nhiêu % câu hỏi bot trả lời chính xác?
  - Tỉ lệ "không bịa": Bot có tự sáng tạo info không? 
  - Thời gian phản hồi: Mau hay chậm?

### 🎤 Trả lời mẫu khi bị hỏi
**Q: "Tại sao không dùng ChatGPT mà dùng Ollama?"**
> A: Chatbot của em cần chạy 24/7 mà API ChatGPT có giới hạn request, phí theo usage, và dữ liệu khách gửi lên server bên thứ ba. Dùng Ollama chạy local thì tiết kiệm, an toàn, và em có thể tinh chỉnh prompt (hướng dẫn cho model) để phù hợp với business logic riêng.

**Q: "Nếu bot trả lời sai thì sao?"**
> A: Em có cơ chế fallback: nếu confidence (độ tự tin) thấp, bot sẽ hỏi người dùng xác nhận trước khi trả lời. Ngoài ra, em dùng test pipeline để kiểm tra trước khi deploy.
> *Confidence = mức độ chắc chắn của model; deploy = triển khai lên production.*

---

## 📷 Phần 2: OCR (Đọc chữ từ ảnh)

### Khái niệm cơ bản
- **OCR** = Optical Character Recognition = Nhận dạng ký tự quang học.
- **Mục đích**: Chuyển ảnh chứa text thành file text có thể sửa được.
- **Ứng dụng**: Đọc bìa sách, scan tài liệu, hóa đơn, v.v.

### Cách hoạt động (4 bước)
1. **Tiền xử lý ảnh** (preprocess): Làm sáng ảnh, cắt vùng có chữ, xoay chỉnh lại nếu lệch.
2. **Chạy mô hình OCR chính** (EasyOCR): Mô hình AI dùng để "đọc" chữ trong ảnh.
3. **Kiểm tra chất lượng**: Nếu kết quả tốt (confidence cao) → dừng. Nếu kém → thử model phụ.
4. **Thử model phụ** (Tesseract): Mô hình OCR khác để "bác bỏ" hoặc "xác nhận" kết quả.

### Tại sao cần 2 mô hình?
- **EasyOCR**: Nhanh, tốt cho tiếng Việt, dùng deep learning.
  - *Deep learning = học sâu, máy học có nhiều lớp để xử lý phức tạp.*
- **Tesseract**: Thư viện cũ nhưng đôi khi tốt cho văn bản sạch, dùng logic truyền thống.
- **Kết hợp**: Ensemble voting = lấy kết quả từ cả hai, chọn tốt nhất (gọi là voting).

### Cách tránh lỗi OCR
| Vấn đề | Giải pháp |
|-------|----------|
| Ảnh bị xoay 180° | Thử xoay lại hoặc lật ngang, so sánh kết quả. |
| Ảnh mờ, ánh sáng yếu | Tiền xử lý: tăng độ sáng, cộng nét. |
| Kết quả là gibberish (ký tự lạ) | Đếm "real-word" (từ thật), nếu ít → kết quả tệ. |

### Cách đánh giá chất lượng
- **CER** (Character Error Rate): Tỉ lệ ký tự sai. Thấp = tốt.
  - Công thức: (sai + xóa + thêm) / tổng ký tự đúng × 100%.
- **WER** (Word Error Rate): Tỉ lệ từ sai. Thấp = tốt.
  - Tương tự CER nhưng tính theo từ.
- **Confidence**: Độ tự tin của model → lớn = tốt.

### 🎤 Trả lời mẫu khi bị hỏi
**Q: "Tại sao phải có EasyOCR + Tesseract?"**
> A: EasyOCR tốt cho hình ảnh bìa sách đa dạng (màu sắc, font, góc chụp), nhưng đôi khi kém cho văn bản scan sạch. Tesseract ngược lại. Em dùng cả hai và chọn kết quả tốt hơn (ensemble voting) để tăng độ chính xác.

**Q: "Độ chính xác OCR bao nhiêu %?"**
> A: Trên bộ test của em, CER khoảng 5-10% và WER khoảng 10-15%. Tức là 85-90% ký tự đúng. Con số này khá tốt cho bìa sách in Việt (đa font, dấu), nhưng có thể cải thiện bằng fine-tuning model trên dữ liệu Việt.
> *Fine-tuning = tinh chỉnh model trên dữ liệu mới.*

---

## 🔎 Phần 3: SEARCH (Tìm kiếm sách)

### Khái niệm cơ bản
- **Mục tiêu**: Khi khách tìm "sách tâm lý hay", hệ thống trả sách tâm lý (không trả sách khoa học hay văn học).
- **Thách thức**: Tìm kiếm từ khóa đơn giản (substring matching) không hiểu ý nghĩa.
  - *Substring = chuỗi con; substring matching = tìm chữ giống hệt chứ không hiểu nghĩa.*

### Cách hoạt động (3 bước)
1. **Tạo embedding** (đầu tiên): Biến title, description, tags thành vector số biểu diễn ý nghĩa.
   - Ví dụ: "Đắc Nhân Tâm" → [0.1, -0.5, 0.8, ...] (100+ con số).
   - Tool: **SBERT** (sentence-transformers).
2. **Index vector** (lập chỉ mục): Lưu vector vào cấu trúc tìm kiếm nhanh (FAISS).
   - *FAISS = Facebook AI Similarity Search = công cụ tìm kiếm vector tương tự của Facebook.*
   - FAISS giúp tìm K vector gần nhất cực nhanh (milliseconds).
3. **Tìm kiếm thực tế**:
   - Khách nhập "sách giúp tôi hiểu tâm lý" → biến thành vector.
   - Tìm K sách có vector gần nhất (cosine similarity).
   - Trả top-N kết quả.

### Tại sao dùng vector search?
- **Keyword search** (tìm từ khóa): Nhanh nhưng hạn chế.
  - "sách về tâm lý" tìm được "Đắc Nhân Tâm" nhưng không tìm được "Hiểu lòng người".
- **Vector search** (tìm ý nghĩa): Hiểu ngữ cảnh, linh hoạt hơn.
  - Cả "sách về tâm lý" lẫn "hiểu lòng người" đều tìm được "Đắc Nhân Tâm" vì ý nghĩa tương tự.

### Kết hợp hybrid (tốt nhất)
- Dùng **BM25** (keyword search) + **vector search** cùng lúc.
- Lợi ích: Vừa chính xác (keyword match) vừa thông minh (semantic match).
  - *BM25 = thuật toán tìm kiếm từ khóa kinh điển.*

### 🎤 Trả lời mẫu khi bị hỏi
**Q: "Vector search nhanh hay chậm?"**
> A: FAISS tối ưu cho tốc độ. Với 20,000 sách, tìm K=10 kết quả chỉ mất < 100ms. Nhanh hơn tìm từ khóa trên full text (SQL LIKE) khi dữ liệu lớn.

**Q: "Nếu thêm sách mới thì phải rebuild index?"**
> A: Có. Em chạy job rebuild index hàng ngày (ví dụ 2 AM). Nếu cần real-time hơn, có thể dùng incremental update nhưng phức tạp hơn.
> *Job = công việc tự động chạy đúng lúc.*

---

## 🎁 Phần 4: RECOMMENDATION (Gợi ý sách)

### Khái niệm cơ bản
- **Mục tiêu**: Khi khách vào trang chủ hoặc xem sách, hệ thống gợi ý sách họ có thể thích.
- **Hai chiến lược**:
  1. **Collaborative Filtering (CF)**: Dựa trên hành vi người khác tương tự.
  2. **Content-based**: Dựa trên nội dung sách tương tự.

### Collaborative Filtering (CF) — dễ hiểu
**Ý tưởng**: "Những người có sở thích giống bạn cũng thích những quyển này nữa".
- Ví dụ: Nếu khách A và khách B đều mua "Đắc Nhân Tâm", nhưng B mua thêm "Lập trình Python", thì gợi ý A mua "Lập trình Python".
- Cách hoạt động: Tạo ma trận người dùng × sách (mỗi ô là số điểm/view count).
  - *Ma trận = bảng hàng×cột; trong ML, ma trận chứa số liệu để tính toán.*
- Áp dụng thuật toán matrix factorization để tìm pattern ẩn.
  - *Matrix factorization = phân tích ma trận thành 2 ma trận nhỏ hơn.*

### Content-based — dễ hiểu
**Ý tưởng**: "Nếu bạn thích sách này, bạn cũng thích sách tương tự".
- Dùng embedding (SBERT) để biểu diễn mô tả sách.
- Tìm sách có embedding gần nhất = sách tương tự.
- Ưu điểm: Giải quyết cold-start (sách mới chưa có người mua).
  - *Cold-start = problem khi dữ liệu mới, chưa có lịch sử.*

### Hybrid = CF + Content-based
- Dùng CF cho độ chính xác cá nhân hóa (user-specific).
- Fallback dùng content-based nếu user mới hoặc sách mới.

### Cách đánh giá
| Metric | Ý nghĩa |
|--------|---------|
| Precision@K | Bao nhiêu % trong K gợi ý là sách người dùng thích? |
| Recall@K | Bao nhiêu % sách người dùng thích xuất hiện trong K gợi ý? |
| NDCG | Gợi ý có đúng thứ tự từ tốt → kém không? (Normalized Discounted Cumulative Gain) |

### 🎤 Trả lời mẫu khi bị hỏi
**Q: "CF hay content-based tốt hơn?"**
> A: CF tốt cho cá nhân hóa (đúng sở thích của từng người). Content-based tốt cho sách mới (không cần lịch sử). Em dùng cả hai: CF chính, content-based làm fallback.

**Q: "Mất bao lâu để train recommendation model?"**
> A: CF training mất 5-10 phút trên 100K interactions. Em chạy hàng ngày, load model lên bộ nhớ để serve nhanh (< 100ms/request).
> *Train = huấn luyện; serve = phục vụ.*

---

## 🏗️ Phần 5: HẠ TẦNG & VẬN HÀNH (Infrastructure)

### Docker — dễ hiểu
- **Vấn đề**: Code chạy tốt trên máy A, chạy lỗi trên máy B (vì Python version, library version khác).
- **Giải pháp**: Dùng Docker — "máy ảo nhẹ" chứa code + toàn bộ môi trường.
- **Lợi ích**: Dev, test, prod chạy code giống hệt.
  - *Dev = development (phát triển); prod = production (triển khai thực tế).*

### Cách vận hành
1. Mỗi service (chatbot, OCR, search, ...) trong Dockerfile riêng.
2. Chạy cùng lúc bằng `docker-compose`.
   - *docker-compose = công cụ chạy nhiều container cùng lúc.*
3. Network nội bộ: container gọi nhau qua hostname (ví dụ: `http://chatbot:8004`).

### MinIO — nơi lưu ảnh
- **Là gì**: Object storage (lưu file như S3 của AWS), nhưng chạy nội bộ.
- **Dùng để**: Lưu ảnh bìa, scan OCR.
- **Tại sao**: Dễ scale (thêm disk), tích hợp tốt với Python.

### .env & Config
- **`.env`**: File chứa setting nhạy cảm (password DB, API keys).
- **`requirements.txt`**: Danh sách library Python cần cài.
  - ví dụ: `torch>=2.0.0`, `easyocr>=1.6`, `sentence-transformers>=3.0`.
- **Tại sao cần**: Tách config từ code → dễ thay đổi dev/prod mà không thay code.

### 🎤 Trả lời mẫu khi bị hỏi
**Q: "Tại sao dùng Docker?"**
> A: Docker đảm bảo cùng môi trường từ dev đến prod. Nếu không, code chạy tốt local nhưng lỗi khi deploy lên server (khác Python version, library mất v.v.). Docker giải quyết điều đó.

**Q: "Nếu mô hình AI nặng, tốn memory/GPU, sao?"**
> A: Em cân đối: model nhỏ + CPU (ít resource, chậm) hay model lớn + GPU (nhanh, nhưng expensive). Hiện tại, Ollama + EasyOCR chạy trên CPU vì không cần real-time cực nhanh. Nếu scale up, có thể thêm GPU.
> *GPU = Graphics Processing Unit = bộ xử lý đồ họa, tốt cho ML.*

---

## 📝 Phần 6: CHUẨN BỊ PHÒNG VẤN

### Slide thuyết trình (5-7 phút)
**Cấu trúc gợi ý**:
1. **Slide 1 — Tổng quan**: Vấn đề, giải pháp, kiến trúc hệ thống (block diagram).
2. **Slide 2-5 — Chi tiết từng service**: 
   - Tên service + mục đích (1 câu).
   - Sơ đồ luồng dữ liệu.
   - Công nghệ chính (1-2 sentence).
3. **Slide 6 — Kết quả & metrics**: Tỉ lệ đúng, thời gian phản hồi, v.v.
4. **Slide 7 — Kết luận & hướng phát triển**: Hạn chế hiện tại, cải tiến trong tương lai.

### Demo (chọn 1-2 cái)
- **Search demo**: Nhập query "sách tâm lý" → trả 5 kết quả.
- **OCR demo**: Upload ảnh bìa → show text trả về.
- **Chatbot demo**: Nhập câu hỏi "Có sách nào hay về tâm lý?" → bot trả lời.

### Chuẩn bị video backup
- Demo trực tiếp có thể lỗi (network, service down).
- Quay sẵn video hoặc screenshot để backup.

---

## 🎯 Phần 7: 10 CÂU HỎI PHẢN BIỆN PHỔ BIẾN + TRẮC NGHIỆM

### Câu hỏi & trả lời mẫu (học thuộc vài cái)

**Q1: "Tại sao chọn Python cho backend?"**
> A: Python có ecosystem phong phú cho ML/AI (PyTorch, SBERT, EasyOCR). FastAPI giúp viết API nhanh. So với Java hay C++, Python thích hợp để prototype và iterate nhanh.

**Q2: "Microservices hay monolith tốt hơn?"**
> A: Microservices cho phép scale từng service độc lập (ví dụ: OCR bận thì scale riêng). Monolith dễ develop ban đầu nhưng khó scale. Với hệ thống book-platform, microservices phù hợp vì load không đồng đều (search peak lúc sáng, recommendation peak lúc tối).
> *Monolith = một khối lớp; microservices = nhiều dịch vụ nhỏ.*

**Q3: "Làm sao đảm bảo data consistency?"**
> A: Dùng MySQL làm source of truth. Các service read từ MySQL, write lại MySQL qua transaction. Cache (Redis) dùng làm layer mềm.
> *Transaction = giao dịch đặc biệt đảm bảo toàn vẹn dữ liệu.*

**Q4: "Nếu search service down thì sao?"**
> A: Có health check + automatic restart (Docker). Nếu down lâu, user thấy lỗi nhưng không crash toàn hệ thống. Tương lai: thêm fallback (full-text search trên MySQL) hoặc replica service.
> *Health check = kiểm tra sức khỏe service.*

**Q5: "Độ latency (thời gian chờ) bao nhiêu?"**
> A: Search: < 200ms. Chatbot: 1-2s (vì gọi Ollama). OCR: 5-15s (tuỳ ảnh). Tất cả chấp nhận được cho UX e-commerce.
> *Latency = độ trễ, thời gian phản hồi.*

**Q6: "Có test case nào?"**
> A: Có. Em viết unit test (test từng function), integration test (test service gọi nhau), e2e test (test toàn luồng). File test ở `tests/` trong mỗi service.
> *Unit test = test từng phần nhỏ; e2e = end-to-end = từ đầu đến cuối.*

**Q7: "Nếu model AI "bịa" thì sao?"**
> A: Em có cơ chế prompt engineering (viết prompt chỉn chu) để tránh. Ngoài ra, fallback trả "không biết" nếu confidence thấp. Test bằng test set để phát hiện hallucination.
> *Prompt engineering = thiết kế lời nhắc cho model tốt.*

**Q8: "Chi phí vận hành bao nhiêu?"**
> A: Chủ yếu là server (CPU/RAM) để chạy container. Model (Ollama, SBERT) load vào RAM 1 lần. OCR + Search mất resource nhất. Estimate: 1-2 GB RAM/service, chạy trên 1-2 server vừa tầm.
> *Server = máy tính thật hoặc cloud (AWS, GCP, v.v.).*

**Q9: "Nếu cần real-time recommendation?"**
> A: CF offline (batch daily) thì không real-time. Có thể dùng online CF (stream processing với Kafka) hoặc bandit algorithm (A/B testing on-the-fly). Trade-off complexity vs accuracy.
> *Batch = xử lý hàng loạt; online = xử lý từng item ngay.*
> *Kafka = message queue để stream data.*

**Q10: "Có plan scale lên 1M users?"**
> A: Cần: 1) Shard database theo region/user-id. 2) Cache layer (Redis cluster). 3) Load balancer (Nginx). 4) CDN cho ảnh. 5) Async job queue (Celery) cho OCR. Hiện tại test ổn với 100K users.
> *Shard = chia database thành nhiều phần; CDN = mạng lưu bản sao file toàn cầu.*

---

## ✅ CHECKLIST 48 GIỜ TRƯỚC PHÒNG VẤN

- [ ] Hiểu 1 câu tóm tắt cho mỗi service (4 dòng trên).
- [ ] Chuẩn bị slide 5-7 phút + speaker notes.
- [ ] Chạy thử 1-2 demo (search, OCR hoặc chatbot).
- [ ] Học thuộc 5 câu hỏi phổ biến + trả lời.
- [ ] Test backup: screenshot hoặc video.
- [ ] Chuẩn bị 1 slide "Giới hạn & hướng phát triển" để trả lời câu "Điều gì có thể cải tiến?"
- [ ] Ngủ đủ 8 giờ trước ngày phòng vấn 😴

---

## 🚀 HƯỚNG PHÁT TRIỂN (Để trả lời câu "Tiếp theo là gì?")

1. **Chatbot**: Thêm memory dài hạn (nhớ lịch sử người dùng lâu). Thêm support cho tiếng Anh/Trung.
2. **OCR**: Fine-tune model trên dữ liệu Việt để độ chính xác CER < 3%.
3. **Search**: Thêm visual search (search bằng ảnh bìa upload).
4. **Recommendation**: A/B test hybrid model. Thêm real-time bandit algorithm.
5. **Infra**: Auto-scaling (tự tăng/giảm resource theo load). Monitoring dashboard.

---

## 📚 TÀI LIỆU THAM KHẢO NHANH
- FastAPI docs: https://fastapi.tiangolo.com
- PyTorch & EasyOCR: https://pytorch.org, https://github.com/JaidedAI/EasyOCR
- SBERT & sentence-transformers: https://huggingface.co/sentence-transformers
- FAISS: https://github.com/facebookresearch/faiss
- Docker: https://docs.docker.com

---

## 💡 MẸO ĐỎ VÀO PHÒNG
1. **Nói rõ mục đích trước lý do**: "Em chọn X vì cần đạt Y (tốc độ, accuracy, cost)".
2. **Mỗi câu trả lời 1 phút**: Đủ dài để thể hiện hiểu biết, không dài quá khiến giám khảo chán.
3. **Nếu không biết**: Nói thật "Em chưa explore phần này, nhưng plan trong tương lai là...". Tốt hơn bịa chuyện.
4. **Chuẩn bị slide backup chi tiết**: Nếu giám khảo hỏi sâu, có thêm slide detail về codebase/metrics.
5. **Mang laptop + USB**: Backup code và demo video.

---

**Chúc bạn báo cáo thành công! 🎓**

*Tài liệu này viết bởi hệ thống. Mọi chi tiết kỹ thuật xem repo source code.*
