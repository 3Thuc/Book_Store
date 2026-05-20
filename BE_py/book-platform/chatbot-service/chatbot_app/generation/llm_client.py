"""
llm_client.py – Gọi Ollama REST API (LLM chạy local, hoàn toàn miễn phí).

v2 – Nâng cấp:
  - SYSTEM_PROMPT chi tiết hơn: quy tắc format, giới hạn sách, xử lý edge case
  - Tăng history window từ 6 → 8 turn
  - Retry logic khi Ollama timeout lần đầu
  - Hàm generate_with_intent() để customize tone theo intent cụ thể
"""
import httpx
import asyncio
import json as _json
from chatbot_app.config import OLLAMA_HOST, OLLAMA_MODEL, OLLAMA_NUM_PREDICT

SYSTEM_PROMPT = """Bạn là trợ lý ảo thông minh của website bán sách BookStore – nền tảng mua sách trực tuyến uy tín hàng đầu Việt Nam.

## Nhiệm vụ của bạn:
- Tư vấn và gợi ý sách phù hợp với nhu cầu khách hàng
- Hỗ trợ tra cứu đơn hàng, giải thích chính sách đổi trả, giao hàng
- Giải đáp mọi thắc mắc liên quan đến sản phẩm và dịch vụ BookStore

## Nguyên tắc TRẢ LỜI (bắt buộc tuân thủ):

### 1. Sử dụng dữ liệu thực
- BẮT BUỘC CHỈ SỬ DỤNG thông tin từ [CONTEXT] được cung cấp.
- TUYỆT ĐỐI KHÔNG bịa đặt tên sách, giá tiền, mã đơn hàng hay thông tin chính sách.
- TUYỆT ĐỐI KHÔNG dùng cụm "Dựa vào thông tin từ [CONTEXT]", "Theo hệ thống". Hãy trả lời tự nhiên!

### 2. Format câu trả lời theo loại nội dung:
- **Tìm sách / gợi ý sách / danh sách sách**: Viết **đúng 1 câu** dẫn dắt thân thiện. **TUYỆT ĐỐI KHÔNG nhắc tên sách cụ thể trong text** – giao diện đã tự render thẻ sách đẹp bên dưới rồi. **TUYỆT ĐỐI KHÔNG liệt kê sách dưới dạng bullet list (- Cuốn sách..., 1. Tên sách...) khi đã có buttons hiển thị** – làm vậy sẽ gây trùng lặp và nhầm lẫn.
- **TUYỆT ĐỐI KHÔNG dùng các từ mô tả chung chung thay cho tên thật**: "Cuốn sách đầu tiên", "Cuốn thứ hai", "Cuốn tiếp theo" – những cụm này vô nghĩa với khách, phải dùng tên thật từ [CONTEXT] hoặc KHÔNG đề cập.
- **Chính sách / thông tin liên hệ / hướng dẫn**: Trình bày dưới dạng danh sách (sử dụng bullet points, tối đa 5 mục), ngắn gọn rõ ràng.
- **Đơn hàng**: Trả lời trực tiếp trạng thái + gợi ý action tiếp theo (1-2 câu).
- **Câu hỏi chi tiết về 1 cuốn sách cụ thể** (khách hỏi đúng tên): Được nhắc tên cuốn đó + thông tin chi tiết (giá, tác giả, mô tả).
- **Khuyến mãi / giảm giá**: KHÔNG được tự tính hay bịa % giảm giá. Chỉ được nêu giá thực tế từ [CONTEXT].
- **Chào hỏi / chitchat**: Đúng 1 câu thân thiện, ngắn gọn.

### 3. Xử lý thông tin thiếu (CHỐNG ẢO GIÁC - HALLUCINATION PREVENTION)
- NẾU [CONTEXT] chứa chữ "Không tìm thấy", "Không có", hoặc context rỗng:
  + BẠN BẮT BUỘC chỉ được thông báo "Không có" hoặc "Không tìm thấy" theo ngữ cảnh.
  + TUYỆT ĐỐI KHÔNG tự động gợi ý hay bịa đặt ra tên sách, số lượng, hoặc số liệu nào khác để làm hài lòng người dùng.
- Câu hỏi ngoài phạm vi sách/BookStore → nhẹ nhàng chuyển về chủ đề sách.

### 4. Ngôn ngữ – BẮT BUỘC
- TUYỆT ĐỐI CHỈ TRẢ LỜI BẰNG TIẾNG VIỆT. Không được dùng Tiếng Anh, Tiếng Trung, hay bất kỳ ngôn ngữ nào khác.
- Nếu context chứa text nước ngoài → tự động dịch sang Tiếng Việt trước khi dùng.
- Thân thiện nhưng chuyên nghiệp. Câu trả lời súc tích, không dài dòng.
- Đồng cảm khi khách phàn nàn: xin lỗi TRƯỚC, giải thích SAU.
- TUYỆT ĐỐI KHÔNG bắt đầu câu bằng "Dựa vào thông tin từ", "Theo dữ liệu từ", "Từ context". Trả lời tự nhiên như người thật!

### 5. Các tình huống đặc biệt
- Khách hỏi "bạn là ai": "Tôi là trợ lý ảo BookStore, được thiết kế để hỗ trợ bạn tìm sách và giải đáp thắc mắc. Tôi có thể giúp gì cho bạn?"
- Khách chào/tạm biệt: phản hồi ngắn gọn, thân thiện.

### 6. QUY TẮC VÀNG – BẮT BUỘC TUÂN THỦ
> **CHỈ GỢI Ý SÁCH CÓ TÊN THẬT TỪ [CONTEXT].** Nếu muốn gợi ý thêm nhưng không có dữ liệu, hãy nói "Bạn có thể khám phá thêm tại mục Tìm kiếm" – KHÔNG ĐƯỢC TỰ NGHĨ RA TÊN SÁCH KHÁC.
> **KHI KHÔNG ĐỦ DỮ LIỆU:** Viết đúng 1 câu dẫn dắt ngắn gọn và DỪNG LẠI. KHÔNG được liệt kê thêm bất kỳ tên sách, chương trình hay nội dung bịa nào khác.
> **KHUYẾN MÃI:** CHỈ nêu thông tin có trong [CONTEXT]. KHÔNG được tự nghĩ ra phiếu giảm giá, %, hay chương trình tích điểm.
> **HOTLINE/LIÊN HỆ:** Số hotline CHÍNH XÁC của BookStore là **0353260721**. Email: cskh@bookstore.vn. TUYỆT ĐỐI KHÔNG tự bịa số hotline khác (VD: 1900123456).
> **ANTI-HALLUCINATION – TÊN SÁCH:** Nếu [CONTEXT] có danh sách sách cụ thể, CHỈ ĐƯỢC nhắc đúng những tên trong danh sách đó. KHÔNG ĐƯỢC thêm tên sách khác nghe có vẻ hợp lý (VD: "Tấm Cám truyền thống", "Khi Em Là Hoa" – đây là HÃO GIÁC). NẾU CỤ THỂ hơn: bất kỳ tên sách bạn nhắc đến phải xuất hiện trong [CONTEXT]."""


# ── Response Cleaner – strip leaked internal tags ──────────────────────────────
# qwen2.5:3b (model nhỏ) đôi khi echo lại [CONTEXT], [HỆ THỐNG] vào response người dùng.
# Ví dụ lỗi thực tế: "Tìm sách thể loại Kinh dị ngay nhé! [CONTEXT]"
import re as _re

_LEAK_PATTERNS = [
    # [CONTEXT] ... [/CONTEXT] block (cả block lẫn chỉ tag đơn)
    _re.compile(r'\[CONTEXT\][\s\S]*?\[/CONTEXT\]', _re.IGNORECASE),
    _re.compile(r'\[/?CONTEXT\]', _re.IGNORECASE),
    # [HỆ THỐNG]: ... – internal system prefix bị echo
    _re.compile(r'\[HỆ THỐNG\]\s*:?\s*', _re.IGNORECASE),
    _re.compile(r'\[HE THONG\]\s*:?\s*', _re.IGNORECASE),
    # [KHÁCH]: prefix
    _re.compile(r'\[KHÁCH\]\s*:?\s*', _re.IGNORECASE),
    # Directive text bị echo (qwen3b hay làm vậy)
    _re.compile(r'NHIỆM VỤ\s*:\s*', _re.IGNORECASE),
    _re.compile(r'LUẬT\s*:\s*', _re.IGNORECASE),
    _re.compile(r'KHÔNG liệt kê tên sách\.?', _re.IGNORECASE),
    _re.compile(r'KHÔNG thêm bullet\.?', _re.IGNORECASE),
    _re.compile(r'KHÔNG tự chế tên sách\.?', _re.IGNORECASE),
    _re.compile(r'KHÔNG bịa tên sách\.?', _re.IGNORECASE),
    # Fix: Cụm "Dựa vào thông tin từ ," hoặc "Dựa vào thông tin từ [tên]"
    _re.compile(r'Dựa vào thông tin từ\s*[,.]?\s*', _re.IGNORECASE),
    _re.compile(r'Theo (thông tin|dữ liệu) từ\s*[,.]?\s*', _re.IGNORECASE),
    # Lọc Chinese characters – loại bỏ toàn bộ ký tự CJK nếu lọt vào response
    _re.compile(r'[\u4e00-\u9fff\u3400-\u4dbf\uff00-\uffef]+'),
    # Fix: @[CONTEX...] tag và orphan @ bị template lọt ra ngoài (TC27-T05)
    _re.compile(r'@\s*\[CONTEX[^\]]*\]', _re.IGNORECASE),
    _re.compile(r'@\[CONTEXT\]', _re.IGNORECASE),
    # Orphan @ đứng đầu dòng (dấu hiệu template broken)
    _re.compile(r'^\s*@\s*\|?\s*$', _re.MULTILINE),
    _re.compile(r'\|\s*@\s*\|', _re.IGNORECASE),
    # Fix TC30-T05: "_ sách này..." – dấu underscore italic prefix rò từ system prompt
    _re.compile(r'^_\s+', _re.MULTILINE),
    _re.compile(r'^\s*_\s*\|', _re.MULTILINE),          # orphan "_ |" đầu dòng
    # Fix hallucination customer list patterns – block LLM sinh tên khách bịa
    _re.compile(r'Khách hàng (ID\s*\d+|#\d+)\s*:', _re.IGNORECASE),
    _re.compile(r'(Ms\.|Mr\.|Ông|Bà|Anh|Chị)\s+[A-ZÀÁẢÃẠĂẮẶ][a-zàáảãạ]+\s+đã đặt sách', _re.IGNORECASE),
    # Fix TC25-T01: Indonesian/Malay words hallucinated by qwen2.5:3b (corpus contamination)
    # "untuk" = "to/for" in Indonesian – block nếu đứng độc lập giữa câu
    _re.compile(r'\buntuk\b', _re.IGNORECASE),
    _re.compile(r'\byang\b(?=\s+[a-z])', _re.IGNORECASE),   # "yang" + lowercase Malay
    _re.compile(r'\bdengan\b', _re.IGNORECASE),              # "dengan" = "with" in Malay
    _re.compile(r'\badalah\b', _re.IGNORECASE),              # "adalah" = "is" in Indonesian
    _re.compile(r'\bsangat\b', _re.IGNORECASE),              # "sangat" = "very" in Malay
    # Fix TC16-T01: "hệ thống," / "Hệ thống," rò từ system prompt đầu response
    _re.compile(r'^hệ thống,?\s*', _re.IGNORECASE | _re.MULTILINE),
    _re.compile(r'^he thong,?\s*', _re.IGNORECASE | _re.MULTILINE),
    # Block hallucinated order IDs in customer list
    _re.compile(r'm\u00e3 \u0111\u01a1n h\u00e0ng #\d{5,}\s+(\u0127a|\u0111\u00e3|cho)', _re.IGNORECASE),
    # FIX TC25: Strip _<img> and similar markdown/HTML artifact tags that LLM injects
    _re.compile(r'_<img[^>]*>_?\s*', _re.IGNORECASE),
    _re.compile(r'_\s*\u0110ang ki\u1ec3m tra[^_]*_\s*\|?\s*', _re.IGNORECASE),
    _re.compile(r'_\s*\u0110ang t\u1ea3i[^_]*_\s*\|?\s*', _re.IGNORECASE),
    # Block fabricated cart total amounts (e.g. "200.000\u0111" when no data)
    _re.compile(r'gi\u1ecf h\u00e0ng.*?l\u00e0\s+\*{0,2}\d[\d.,]+\u0111\*{0,2}', _re.IGNORECASE),
    # FIX P3: block placeholder text LLM b\u1ecba
    _re.compile(r'\[T\u00ean t\u00e1c gi\u1ea3\]', _re.IGNORECASE),
    _re.compile(r'\[T\u00ean s\u00e1ch\]', _re.IGNORECASE),
    _re.compile(r'\[C\u1ea7n x\u00e1c nh\u1eadn\]', _re.IGNORECASE),
    _re.compile(r'\[T\u00f4i ch\u01b0a bi\u1ebft\]', _re.IGNORECASE),
    # Block hallucinated generic book names: "Cuốn sách đầu tiên", "Cuốn thứ hai"...
    _re.compile(r'[-\*]\s+\*{0,2}Cu\u1ed1n s\u00e1ch (\u0111\u1ea7u ti\u00ean|th\u1ee9 hai|th\u1ee9 ba|th\u1ee9 t\u01b0|cu\u1ed1i c\u00f9ng)\*{0,2}.*', _re.IGNORECASE),
    _re.compile(r'\*\*Cu\u1ed1n s\u00e1ch (\u0111\u1ea7u ti\u00ean|th\u1ee9 hai|th\u1ee9 ba|th\u1ee9 t\u01b0|cu\u1ed1i c\u00f9ng)\*\*', _re.IGNORECASE),
    # Block Japanese/CJK punctuation (。」「 etc.) leaked into Vietnamese responses
    _re.compile(r'[\u3000-\u303f\uff00-\uffef]'),
    # Block fabricated discount percentage: "giảm 50%", "giảm giá 30%" khi context không có
    _re.compile(r'gi\u1ea3m (gi\u00e1 )?\d+%(?!.*[CONTEXT])', _re.IGNORECASE),
    # Block incomplete trailing sentences: "– Tác giả:" rỗng cuối dòng
    _re.compile(r'\u2013\s*T\u00e1c gi\u1ea3:\s*$', _re.MULTILINE),
    _re.compile(r'–\s*T\u00e1c gi\u1ea3:\s*$', _re.MULTILINE),
    # Block hallucinated voucher/loyalty program text
    _re.compile(r'phiếu giảm giá \d+%', _re.IGNORECASE),
    _re.compile(r'voucher \d+%', _re.IGNORECASE),
    _re.compile(r'ưu đãi \d+% cho lần mua', _re.IGNORECASE),
    _re.compile(r'lần mua tiếp theo.*?\d+%', _re.IGNORECASE),
    _re.compile(r'tich diem|tích điểm', _re.IGNORECASE),
    # Block fabricated book-name bullet patterns after context is exhausted
    _re.compile(r'^\s*-\s+"[^"]{5,}"\s*$', _re.MULTILINE),   # - "tên bịa"
    _re.compile(r'^\s*-\s+\*\*"[^"]{5,}"\*\*', _re.MULTILINE), # - **"tên bịa"
    # Block "Kỹ" or "Nghệ" prefix bị cắt nửa chừng đầu dòng
    _re.compile(r'^- "K\u1ef9\s*$', _re.MULTILINE),
    _re.compile(r'^- "Nghệ\s*$', _re.MULTILINE),
    # Block hallucinated ordinal phrases: “Cuốn tiếp theo:”, “Cuốn sách tiếp theo:”, etc.
    _re.compile(r'\*{0,2}Cuốn tiếp theo\*{0,2}\s*:?\s*', _re.IGNORECASE),
    _re.compile(r'\*{0,2}Cuốn sách tiếp theo\*{0,2}\s*:?\s*', _re.IGNORECASE),
    _re.compile(r'\*{0,2}Cuốn đại diện\*{0,2}\s*:?\s*', _re.IGNORECASE),
    _re.compile(r'\*{0,2}Cuốn gợi ý\*{0,2}\s*:?\s*(?=")', _re.IGNORECASE),
    # Block artifact “• :” or “•  :” from broken bullets
    _re.compile(r'^\s*•\s*:\s*', _re.MULTILINE),
    _re.compile(r'^\s*•\s*\.\s*', _re.MULTILINE),
    # Block bare book-name bullet without quotes: “- Tên sách” (all caps first word indicator)
    _re.compile(r'^-\s+[A-Z\u00c0-\u1ef9][a-z\u00e0-\u1ef9]+ [A-Z\u00c0-\u1ef9].*\n', _re.MULTILINE),
    # Block cut-off sentence endings – Phase 1 patterns
    _re.compile(r'(trong danh mục|sách khác trong|các cuốn sách khác trong)\s*$', _re.MULTILINE | _re.IGNORECASE),
    # Block cut-off sentence endings – Phase 2 extended
    _re.compile(r'(tìm kiếm thêm các cuốn sách hay|các chương trình khuyến mãi|thêm gì vào giỏ hàng hay cần|tìm kiếm thêm các sách khác)\s*$', _re.MULTILINE | _re.IGNORECASE),
    # Block hallucinated invented 2nd-book title suggestions
    _re.compile(r'[Cc]uộc sống là cuộc chơi', _re.IGNORECASE),
    # Phase 3: Block pseudo-markdown table tags [table]...[/table]
    _re.compile(r'\[/?table\]', _re.IGNORECASE),
    _re.compile(r'\[/?\w{2,10}\]', _re.IGNORECASE),
    # Phase 3: Cut-off sentence endings – extended set
    _re.compile(r'sách cần còn\s*$', _re.MULTILINE | _re.IGNORECASE),
    _re.compile(r'câu chuyện của\s*$', _re.MULTILINE | _re.IGNORECASE),
    _re.compile(r'\bhãy\s*$', _re.MULTILINE | _re.IGNORECASE),
    _re.compile(r'cùng thể loại\s*$', _re.MULTILINE | _re.IGNORECASE),
    _re.compile(r'nếu bạn đang tìm kiếm thêm các cuốn sách hay\s*$', _re.MULTILINE | _re.IGNORECASE),
    _re.compile(r'nhiều thông điệp\s*$', _re.MULTILINE | _re.IGNORECASE),
    # Phase 3: Block hallucinated book titles when context is empty
    _re.compile(r'"404\s*[-\u2013]\s*Cuốn Sách Không Tìm Thấy"', _re.IGNORECASE),
    _re.compile(r'"Cuốn Sách Về Sự Thật"', _re.IGNORECASE),
    # Phase 4: Block cut-off fragments – micro patterns
    # FIX TC29-T07 extended: micro patterns
    _re.compile(r'\btôi có\s*$', _re.MULTILINE | _re.IGNORECASE),
    _re.compile(r'\btôi có thể\s*$', _re.MULTILINE | _re.IGNORECASE),
    _re.compile(r'\bNếu bạn cho phép\s*,?\s*tôi có thể\s*$', _re.MULTILINE | _re.IGNORECASE),
    _re.compile(r'\bBạn muốn tìm thêm các\s*$', _re.MULTILINE | _re.IGNORECASE),
    _re.compile(r'\bcuốn sách này\s*$', _re.MULTILINE | _re.IGNORECASE),
    # FIX P1: Block "Cuốn sách đầu tiên", "Cuốn đầu tiên", "cuốn 1" referencing ordinals in text body
    # (các cụm này vô nghĩa với khách, vi phạm SYSTEM_PROMPT rule)
    _re.compile(r'Cu[oô]n s[aá]ch (đầu tiên|thứ (nhất|hai|ba|tư|năm|sáu|bảy|tám|chín|mười)|cuối cùng)\s*:?\s*', _re.IGNORECASE),
    _re.compile(r'Cu[oô]n (đầu tiên|thứ (nhất|hai|ba|tư|năm|sáu|bảy|tám|chín|mười)|tiếp theo)\s*:?\s*', _re.IGNORECASE),
    _re.compile(r'\*{1,2}Cu[oô]n (đầu tiên|thứ (nhất|hai|ba|tư|năm|sáu|bảy|tám|chín|mười)|cuối cùng)\*{0,2}\s*:?\s*', _re.IGNORECASE),
    # FIX ACTION-1: Block dòng "N. – Tác giả" khi tên sách bị mất (LLM regression)
    _re.compile(r'^\d+\.\s+[–—-]\s+[A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝĂĐƠƯA-Z][^\n]{0,60}đ\b', _re.MULTILINE),
    # FIX P3: Block broken "[Card sách: ...]" artifact from LLM
    _re.compile(r'\[Card s[aá]ch\s*:?[^\]]*\]', _re.IGNORECASE),
    _re.compile(r'\[card\s*:?[^\]]*\]', _re.IGNORECASE),
    # FIX V6-S02: Block LLM follow-up 'sach nao khac khong?' -> block-keyword violation
    _re.compile(r's[a\u00e1]ch\s+n[a\u00e0]o\s+kh[a\u00e1]c\s+kh[o\u00f4]ng', _re.IGNORECASE),
    _re.compile(r'g[o\u1ee3]i\s+[y\u00fd]\s+th[e\u00ea]m\s+v[e\u1ec1]\s+s[a\u00e1]ch\s+n[a\u00e0]o', _re.IGNORECASE),
]



def _clean_llm_response(text: str) -> str:
    """
    Xóa các internal format tag bị qwen2.5:3b leak vào response.

    Ví dụ đầu vào:  "Tìm sách thể loại Kinh dị ngay nhé! [CONTEXT]"
    Sau clean:       "Tìm sách thể loại Kinh dị ngay nhé!"
    """
    if not text:
        return text
    cleaned = text
    for pattern in _LEAK_PATTERNS:
        cleaned = pattern.sub('', cleaned)
    # Normalize whitespace thừa
    cleaned = _re.sub(r'[ \t]{2,}', ' ', cleaned)          # nhiều space → 1 space
    cleaned = _re.sub(r'\n{3,}', '\n\n', cleaned)           # 3+ newlines → 2
    # FIX TC29-T07: LLM hay nhầm "cấp 3 = THPT" → "lớp 3" khi nói về sách giáo dục
    # "lớp 3" có thể đúng khi nói tiểu học, nhưng trong context "sách cho học sinh cấp 3" thì sai
    # Chú ý: chỉ normalize khi "lớp 3" đứng trong phrase gợi ý cấp trung học
    cleaned = _re.sub(
        r'(học sinh|sách cho|phù hợp cho|dành cho)\s+lớp\s*3',
        lambda m: m.group(1) + ' cấp 3', cleaned, flags=_re.IGNORECASE
    )
    # Trường hợp "lớp 3" đứng đầu hoặc sau từ "học sinh" riêng lẻ
    cleaned = _re.sub(
        r'\blớp\s*3\b(?=\s*(trở|học|năm|tuổi))',
        'lớp 3', cleaned, flags=_re.IGNORECASE  # keep original - these are OK
    )
    # Phase 4: Clean artifact "trong ." left after context-tag removal
    cleaned = _re.sub(r'\btrong\s*\.', 'trong', cleaned)
    # Phase 4: Clean dangling "trong " at end of sentence (before newline/button)
    cleaned = _re.sub(r'\btrong\s*\n', '\n', cleaned)
    return cleaned.strip()


def _capitalize_first(text: str) -> str:
    """Đảm bảo ký tự đầu tiên của câu trả lời luôn viết hoa (an toàn với Unicode tiếng Việt)."""
    if not text:
        return text
    # Bỏ qua leading whitespace/newline, capitalize ký tự đầu tiên visible
    for i, ch in enumerate(text):
        if ch.strip():  # ký tự đầu tiên không phải whitespace
            return text[:i] + ch.upper() + text[i+1:]
    return text



def _trim_incomplete_sentence(text: str) -> str:
    """
    ACTION-6: Nếu response bị cắt giữa câu (không kết thúc bằng dấu câu),
    cắt tại vị trí dấu câu cuối cùng để tránh câu cụt.
    Chỉ áp dụng khi response đủ dài (>80 chars) để tránh cắt nhầm câu ngắn.
    """
    if not text or len(text) < 80:
        return text
    if text and text[-1] in '.!?…。':
        return text
    # Tìm dấu câu cuối cùng
    last_punct = max(text.rfind('.'), text.rfind('!'), text.rfind('?'),
                     text.rfind('\n'), text.rfind('…'))
    # Chỉ cắt nếu vị trí đủ xa (giữ >=65% nội dung) để không mất quá nhiều
    if last_punct > 0 and last_punct > len(text) * 0.65:
        return text[:last_punct + 1].strip()
    return text


# ── Tone templates theo loại intent ─────────────────────────────────────────
INTENT_TONES: dict[str, str] = {
    "book_search":         "ngắn gọn, gợi ý cụ thể, kèm giá và tác giả",
    "book_detail":         "chi tiết, đầy đủ thông tin sách",
    "book_compare":        "khách quan, so sánh điểm mạnh/yếu của từng cuốn",
    "recommend_personal":  "cá nhân hóa, giải thích lý do gợi ý",
    "recommend_trending":  "nhiệt tình, nhấn mạnh sách đang hot",
    "recommend_gift":      "ấm áp, nhấn mạnh ý nghĩa cuốn sách làm quà",
    "order_status":        "chuyên nghiệp, ngắn gọn, cung cấp thông tin chính xác",
    "return_policy":       "rõ ràng, từng bước, tránh gây hiểu nhầm",
    "complaint_damaged":   "đồng cảm sâu sắc, xin lỗi trước, hướng giải quyết",
    "payment_issue":       "bình tĩnh, từng bước hướng dẫn, cuối cùng giới thiệu hotline",
    "promotion_current":   "hứng khởi, nhấn mạnh ưu đãi; KHÔNG tự điền ngày tháng hay phần trăm cụ thể nếu không có trong dữ liệu",
    "promotion_info":      "thông tin rõ ràng; Nếu không có số liệu cụ thể trong context, HƯỚNG DẪN khách xem trang chủ - KHÔNG đoán mò ngày tháng",
    "store_info":          "thông tin rõ ràng, đầy đủ",
    "chitchat":            "thân thiện, ngắn gọn, vui vẻ",
}

# ── Intent-aware token limit ──────────────────────────────────────────────────
# Ít token ⇒ generation nhanh hơn. Càng ngắn càng tốt với câu trả lời cố định.
INTENT_MAX_TOKENS: dict[str, int] = {
    "chitchat":             80,   # "Không có gì! Còn gì tôi giúp không?"
    "book_search":         120,   # 1 câu dẫn dắt ngắn gọn — FIX truncation (80→120)
    "book_detail":         250,   # tên + giá + tác giả + tồn kho + mô tả
    "book_compare":        280,   # so sánh 2 cuốn cần đủ chi tiết
    "book_review":         180,   # review chi tiết
    "order_status":        180,   # trạng thái + gợi ý action tiếp theo
    "order_history":       280,   # lịch sử nhiều đơn – cần token để list đủ + câu kết
    "return_policy":       350,   # policy cần đủ thông tin
    "complaint_damaged":   150,   # xin lỗi + hướng giải quyết
    "store_info":          200,   # thông tin liên hệ, giao hàng
    "promotion_current":   130,   # chỉ nêu giá thực — FIX truncation (100→130)
    "promotion_info":      130,   # tăng để không cắt câu
    "recommend_personal":   90,   # 1 câu ngắn + button
    "recommend_trending":  100,   # 1 câu ngắn
    "recommend_category":  100,   # 1 câu ngắn
    "recommend_gift":      120,   # gợi ý quà
    "cart_help":           100,   # hướng dẫn cart — FIX truncation (80→100)
    "voucher_apply":       150,   # kết quả voucher
    "confirmation_yes":    180,   # xác nhận đặt hàng
    "out_of_scope":         80,   # template cứng – ít token
    # Staff/Admin intents
    "staff_inventory_check":  200,
    "staff_order_statistics": 280,
    "staff_revenue_today":    220,
    "staff_top_selling":      220,
    "staff_return_workflow":  350,
    "admin_dashboard":        300,
    "admin_user_stats":       200,
}
_DEFAULT_MAX_TOKENS = OLLAMA_NUM_PREDICT  # dùng config (450) thay vì hardcode 120


async def generate_stream(
    user_message: str,
    context: str,
    history: list[dict],
    tone: str = "thân thiện, ngắn gọn",
    intent: str = "",
):
    """
    Streaming version của generate() – dùng cho SSE endpoint.
    Yield từng token ngay khi Ollama trả về, thay vì chờ cả đoạn.

    Cách hoạt động:
      - Gọi Ollama với stream=True
      - Dùng aiter_lines() để đọc từng dòng JSON ngay khi có
      - Yield mỗi token → FastAPI StreamingResponse sẽ đẩy ngay cho browser
      - Khi done=True → hết, yield "" để báo hiệu kết thúc

    Returns:
        AsyncGenerator[str, None] – mỗi lần yield là 1 token text
    """
    # Auto-lookup tone
    if intent and intent in INTENT_TONES and tone == "thân thiện, ngắn gọn":
        tone = INTENT_TONES[intent]

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages.extend(history[-8:])  # 8 turns ~ 2400 tokens context

    user_content = f"""[CONTEXT]
{context if context else "Không có dữ liệu liên quan."}
[/CONTEXT]

Phong cách trả lời: {tone}

Câu hỏi: {user_message}"""

    messages.append({"role": "user", "content": user_content})

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            async with client.stream(
                "POST",
                f"{OLLAMA_HOST}/api/chat",
                json={
                    "model":    OLLAMA_MODEL,
                    "messages": messages,
                    "stream":   True,
                    "options": {
                        "temperature":    0.35,  # ↑ 0.2→0.35: tự nhiên hơn, bớt cứng nhắc
                        "top_p":          0.85,  # ↑ 0.8→0.85: đa dạng từ vựng hơn
                        "num_predict":    INTENT_MAX_TOKENS.get(intent, _DEFAULT_MAX_TOKENS),
                        "repeat_penalty": 1.1,   # ↓ 1.15→1.1: bớt phạt lặp từ tiếng Việt
                        "stop":           ["[KHÁCH]", "[HỆ THỐNG]", "User:", "Khách:"],
                    },
                },
            ) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line.strip():
                        continue
                    try:
                        chunk = _json.loads(line)
                    except Exception:
                        continue
                    token = chunk.get("message", {}).get("content", "")
                    if token:
                        yield token
                    if chunk.get("done"):
                        break
    except Exception as e:
        print(f"[generate_stream] error: {e}")
        result = _fallback_from_context(context, intent)
        # Capitalize first character for stream fallback too
        result = _capitalize_first(result)
        yield result




async def generate(
    user_message: str,
    context: str,
    history: list[dict],
    tone: str = "thân thiện, ngắn gọn",
    intent: str = "",
    max_retries: int = 1,
) -> str:
    """
    Gọi Ollama /api/chat để generate câu trả lời.

    Args:
        user_message: Câu hỏi của người dùng
        context:      Dữ liệu từ DB/OpenSearch (đã format)
        history:      Lịch sử hội thoại (giữ 8 turn cuối)
        tone:         Hướng dẫn phong cách trả lời
        intent:       Intent đã phân loại (để auto-lookup tone nếu cần)
        max_retries:  Số lần retry khi timeout

    Returns:
        Câu trả lời text
    """
    # Auto-lookup tone từ intent nếu chưa có custom tone
    if intent and intent in INTENT_TONES and tone == "thân thiện, ngắn gọn":
        tone = INTENT_TONES[intent]

    # Build message list: system + history (8 turn) + user
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages.extend(history[-8:])  # 8 turns để bot nhớ đủ ngữ cảnh hội thoại dài

    user_content = f"""[CONTEXT]
{context if context else "Không có dữ liệu liên quan."}
[/CONTEXT]

Phong cách trả lời: {tone}

Câu hỏi: {user_message}"""

    messages.append({"role": "user", "content": user_content})

    # Thử gọi Ollama với retry
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                resp = await client.post(
                    f"{OLLAMA_HOST}/api/chat",
                    json={
                        "model":    OLLAMA_MODEL,
                        "messages": messages,
                        "stream":   False,
                        "options": {
                            "temperature":    0.35,  # ↑ tự nhiên hơn
                            "top_p":          0.85,  # ↑ đa dạng từ vựng
                            "num_predict":    INTENT_MAX_TOKENS.get(intent, _DEFAULT_MAX_TOKENS),
                            "repeat_penalty": 1.1,   # ↓ bớt phạt lặp tiếng Việt
                            "stop":           ["[KHÁCH]", "[HỆ THỐNG]", "User:", "Khách:"],
                        },
                    },
                )
                resp.raise_for_status()
                result = resp.json()["message"]["content"]
                result = _clean_llm_response(result)          # ✅ Strip leaked tags
                result = _trim_incomplete_sentence(result)      # ✅ ACTION-6: cắt câu bị truncate
                result = _capitalize_first(result.strip())      # ✅ Đảm bảo viết hoa chữ đầu
                return result

        except (httpx.TimeoutException, httpx.ConnectError) as e:
            last_error = e
            if attempt < max_retries:
                print(f"[llm_client] Timeout attempt {attempt+1}, retrying...")
                await asyncio.sleep(1.0)
            continue
        except Exception as e:
            last_error = e
            break

    # Fallback khi Ollama không sẵn sàng
    print(f"[llm_client] Ollama unavailable: {last_error}")
    return _fallback_from_context(context, intent)


async def check_ollama_health() -> bool:
    """Kiểm tra Ollama đang chạy và model đã sẵn sàng."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{OLLAMA_HOST}/api/tags")
            models = [m["name"] for m in r.json().get("models", [])]
            return any(OLLAMA_MODEL.split(":")[0] in m for m in models)
    except Exception:
        return False


def _normalize_list_text(text: str) -> str:
    """
    Chuẩn hóa text từ knowledge base: tách các mục danh sách bị gộp vào 1 dòng.

    Ví dụ KB trả về:
      "1. Liên hệ hotline. 2. Cung cấp mã đơn. 3. Nhân viên xác nhận."
    Sau normalize:
      "1. Liên hệ hotline.\n2. Cung cấp mã đơn.\n3. Nhân viên xác nhận."

    Nguyên nhân: OpenSearch/KB lưu nội dung 1 dòng → fallback dump thẳng ra
    → Markdown renderer thấy chỉ 1 item có số "1." với nội dung gộp.
    """
    import re
    # Pattern: dấu chấm/chấm than/hỏi chấm + khoảng trắng + số thứ tự tiếp theo
    # VD: ". 2." hoặc "! 3." hoặc "? 4."
    text = re.sub(
        r"([.!?])\s+(\d{1,2})\.\s",
        lambda m: f"{m.group(1)}\n{m.group(2)}. ",
        text,
    )
    # Đảm bảo bullet points (•) cũng xuống dòng
    text = re.sub(r"\s+(•|–|-)\s+", r"\n\1 ", text)
    return text


def _fallback_from_context(context: str, intent: str = "") -> str:
    """
    Fallback khi Ollama không sẵn sàng.
    - Có context hữu ích → hiển thị trực tiếp với tiêu đề phù hợp
    - Context rỗng → hướng dẫn liên hệ CSKH
    """
    _empty_contexts = {
        "", "Không có thông tin liên quan.",
        "Không tìm thấy sách phù hợp.",
        "Vui lòng nêu tên sách cụ thể.",
    }
    if context and context.strip() not in _empty_contexts:
        # Thêm prefix phù hợp theo intent
        prefix_map = {
            "book_search":        "📚 Sách có thể liên quan trong kho BookStore:\n\n",
            "recommend_personal": "📖 Gợi ý sách phù hợp cho bạn:\n\n",
            "recommend_trending": "🔥 Sách đang bán chạy nhất:\n\n",
            "recommend_gift":     "🎁 Gợi ý sách làm quà:\n\n",
            "order_status":       "📦 Thông tin đơn hàng:\n\n",
            "return_policy":      "🔄 Chính sách đổi trả:\n\n",
            "promotion_current":  "🏷️ Khuyến mãi hiện tại:\n\n",
        }
        prefix = prefix_map.get(intent, "")
        # FIX: chuẩn hóa numbered list bị gộp 1 dòng → xuống dòng đúng
        normalized = _normalize_list_text(context.strip())
        return prefix + normalized

    return (
        "Xin lỗi, tôi không tìm thấy thông tin phù hợp cho câu hỏi này.\n"
        "Vui lòng liên hệ CSKH: **Hotline 0353260721** hoặc **Email cskh@bookstore.vn**"
    )

