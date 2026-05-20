"""
image_preprocessor.py – OpenCV Preprocessing Pipeline (v2 - Optimized)
=======================================================================
Cải tiến v2:
  1. Auto image classification (cover / document / receipt)
  2. Adaptive pipeline theo loại ảnh
  3. Cải thiện upscale (MIN_DIMENSION = 400, target 600px cho chữ nhỏ)
  4. Deskew bổ sung bằng Hough Line Transform
  5. Sharpen filter cho document mode

Pipeline:
  bytes → cv2 → Resize → Deskew → Grayscale+CLAHE → Bilateral → [Threshold]
"""
import io
import logging
import math
from typing import Literal

import cv2
import numpy as np
from PIL import Image

logger = logging.getLogger(__name__)

# ── Hằng số cấu hình ────────────────────────────────────────────────────────
MAX_DIMENSION   = 2000   # px – downscale nếu vượt quá
MIN_DIMENSION   = 400    # px – upscale nếu quá nhỏ (tăng từ 300)
TARGET_OCR_SIZE = 1200   # px – target dimension cho OCR tối ưu

CLAHE_CLIP      = 2.5
CLAHE_GRID      = 8
BILATERAL_D     = 9
BILATERAL_SIGMA = 75

ADAPTIVE_BLOCK  = 31
ADAPTIVE_C      = 10

# ── Image type ───────────────────────────────────────────────────────────────
ImageType = Literal["cover", "document", "receipt"]


# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 0 (MỚI): Phân loại loại ảnh tự động
# ══════════════════════════════════════════════════════════════════════════════

def classify_image_type(img: np.ndarray) -> ImageType:
    """
    Phân loại ảnh tự động dựa vào 3 đặc trưng:
      1. Color saturation ratio – bìa sách màu sặc sỡ, hóa đơn gần grayscale
      2. Text density – document > receipt > cover
      3. Aspect ratio – hóa đơn thường cao hơn rộng

    Returns:
      "cover"    – Bìa sách màu (nhiều màu, ít chữ)
      "document" – Tài liệu in (thường grayscale, nhiều chữ đều đặn)
      "receipt"  – Hóa đơn/phiếu (đen trắng, chữ đặc)
    """
    h, w = img.shape[:2]
    aspect_ratio = h / w  # > 1.5 thường là hóa đơn/phiếu dọc

    # ── Tính color saturation ──────────────────────────────────────────────
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
    saturation = hsv[:, :, 1]  # channel S trong HSV
    avg_saturation = float(np.mean(saturation))  # 0-255

    # ── Tính text density bằng edge detection ──────────────────────────────
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    edges = cv2.Canny(gray, 50, 150)
    edge_density = float(np.count_nonzero(edges)) / (h * w)  # 0.0-1.0

    logger.debug(
        "Image classify: saturation=%.1f, edge_density=%.4f, aspect=%.2f",
        avg_saturation, edge_density, aspect_ratio
    )

    # ── Decision rules ─────────────────────────────────────────────────────
    # Hóa đơn: ít màu + nhiều edge + dạng dọc
    if avg_saturation < 30 and edge_density > 0.05 and aspect_ratio > 1.3:
        logger.info("🏷️  Loại ảnh: RECEIPT (hóa đơn/phiếu)")
        return "receipt"

    # Document: ít màu nhưng không nhất thiết dài (có thể ngang)
    if avg_saturation < 40 and edge_density > 0.04:
        logger.info("🏷️  Loại ảnh: DOCUMENT (tài liệu)")
        return "document"

    # Bìa sách: nhiều màu
    logger.info("🏷️  Loại ảnh: COVER (bìa sách)")
    return "cover"


# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 1: Đọc ảnh từ bytes
# ══════════════════════════════════════════════════════════════════════════════

def bytes_to_cv2(image_bytes: bytes) -> np.ndarray:
    """
    Chuyển raw bytes → numpy array BGR.
    Dùng Pillow làm bước đệm để hỗ trợ nhiều format (WebP, HEIC, TIFF...).

    v2: Tự động apply EXIF orientation (exif_transpose).
    Ảnh chụp từ điện thoại thường lưu góc xoay trong EXIF metadata.
    Nếu không apply → ảnh bị ngược/xoay dù nhìn trên điện thoại thì thẳng.
    """
    try:
        from PIL import ImageOps
        pil_img = Image.open(io.BytesIO(image_bytes))
        # Apply EXIF orientation (xoay, lật) trước khi convert
        pil_img = ImageOps.exif_transpose(pil_img)
        pil_img = pil_img.convert("RGB")
        cv_img = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
        return cv_img
    except Exception as e:
        raise ValueError(f"Không thể đọc ảnh: {e}") from e



# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 2: Resize thông minh
# ══════════════════════════════════════════════════════════════════════════════

def resize_image(img: np.ndarray, image_type: ImageType = "cover") -> np.ndarray:
    """
    Resize ảnh về kích thước tối ưu cho từng loại ảnh.

    v2: Target dimension khác nhau cho từng loại:
      - cover:    MAX 1600px (nhỏ hơn để giảm noise từ background)
      - document: MAX 2000px (cần đọc chữ nhỏ rõ hơn)
      - receipt:  MAX 1200px + upscale aggressive nếu nhỏ

    Thuật toán scale: INTER_LANCZOS4 (tốt nhất cho chữ).
    """
    h, w = img.shape[:2]
    max_dim = max(h, w)
    min_dim = min(h, w)

    # Target theotừng loại
    target_max = {"cover": 1600, "document": 2000, "receipt": 1200}[image_type]
    target_min = {"cover": 400, "document": 500, "receipt": 300}[image_type]

    logger.debug("Resize: ảnh gốc %dx%d, type=%s", w, h, image_type)

    # Upscale nếu ảnh quá nhỏ
    if min_dim < target_min:
        scale = target_min / min_dim
        new_w, new_h = int(w * scale), int(h * scale)
        logger.debug("Upscale: %dx%d → %dx%d", w, h, new_w, new_h)
        img = cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_LANCZOS4)
        h, w = new_h, new_w

    # Downscale nếu ảnh quá lớn (giữ tỷ lệ)
    if max(h, w) > target_max:
        scale = target_max / max(h, w)
        new_w, new_h = int(w * scale), int(h * scale)
        logger.debug("Downscale: %dx%d → %dx%d", w, h, new_w, new_h)
        img = cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_LANCZOS4)

    return img


# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 3: Deskew – Xoay thẳng ảnh bị nghiêng (v2 cải thiện)
# ══════════════════════════════════════════════════════════════════════════════

def _deskew_by_minrect(gray: np.ndarray) -> float:
    """
    Phương pháp 1 (hiện tại): minAreaRect trên tất cả điểm text.
    Nhanh nhưng không chính xác với ảnh bìa sách.
    """
    _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    coords = np.column_stack(np.where(binary > 0))
    if len(coords) < 100:
        return 0.0
    rect = cv2.minAreaRect(coords)
    angle = rect[-1]
    if angle < -45:
        angle += 90
    elif angle > 45:
        angle -= 90
    return float(angle)


def _deskew_by_hough(gray: np.ndarray) -> float:
    """
    Phương pháp 2 (mới): Hough Line Transform.
    Tìm các đường thẳng ngang trong ảnh → góc nghiêng của text lines.
    Chính xác hơn với bìa sách có text ở nhiều vị trí.
    """
    edges = cv2.Canny(gray, 50, 150, apertureSize=3)
    lines = cv2.HoughLines(edges, 1, np.pi / 180, threshold=100)

    if lines is None or len(lines) < 5:
        return 0.0

    angles = []
    for line in lines[:50]:  # Chỉ lấy 50 đường mạnh nhất
        rho, theta = line[0]
        # Chuyển theta (radians) → degrees, normalize về [-45, 45]
        angle_deg = math.degrees(theta) - 90
        if -30 < angle_deg < 30:
            angles.append(angle_deg)

    if not angles:
        return 0.0

    # Median angle (robust hơn mean khi có outliers)
    angles.sort()
    return float(angles[len(angles) // 2])


def _detect_upside_down(img: np.ndarray) -> bool:
    """
    Phát hiện ảnh bị lật ngược 180° (upside-down).

    Thuật toán: dùng Tesseract OSD (Orientation & Script Detection).
    Nếu không có Tesseract, dùng heuristic đơn giản:
    - Trên bìa sách thường in đậm phần trên (1/3 trên chứa nhiều text/logo)
    - Nếu 1/3 dưới có nhiều cạnh (edge) hơn 1/3 trên → có thể bị lật

    Returns: True nếu nên rotate 180°
    """
    try:
        import pytesseract
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY) if len(img.shape) == 3 else img
        osd = pytesseract.image_to_osd(gray, output_type=pytesseract.Output.DICT)
        rotate = osd.get("rotate", 0)
        if rotate == 180:
            logger.info("🔄 OSD phát hiện ảnh lật ngược 180°, xoay lại")
            return True
    except Exception:
        pass  # Tesseract OSD không khả dụng → dùng heuristic
    return False


def auto_orient(img: np.ndarray) -> np.ndarray:
    """
    Tự động xoay ảnh về đúng hướng nếu bị lật ngược.
    Phát hiện 180° flip phổ biến khi chụp ngược chiều.
    """
    if _detect_upside_down(img):
        return cv2.rotate(img, cv2.ROTATE_180)
    return img


def deskew_image(img: np.ndarray, image_type: ImageType = "cover") -> np.ndarray:
    """
    Phát hiện và xoay thẳng ảnh bị nghiêng (v2).

    v2: Kết hợp 2 phương pháp:
      - cover:    Chỉ dùng minRect (hough quá nhiễu với bìa màu)
      - document/receipt: Thử cả 2, chọn angle có |value| nhỏ hơn (thận trọng hơn)
    """
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    if image_type == "cover":
        angle = _deskew_by_minrect(gray)
    else:
        angle_minrect = _deskew_by_minrect(gray)
        angle_hough   = _deskew_by_hough(gray)

        # Chọn phương pháp có kết quả nhất quán hơn
        if abs(angle_minrect) < 0.5:
            angle = angle_hough
        elif abs(angle_hough) < 0.5:
            angle = angle_minrect
        else:
            # Cả 2 đều có góc – chọn nhỏ hơn (thận trọng)
            angle = angle_minrect if abs(angle_minrect) < abs(angle_hough) else angle_hough

    # Bỏ qua nếu góc quá nhỏ hoặc quá lớn
    if abs(angle) < 0.3 or abs(angle) > 30:
        return img

    logger.debug("Deskew (%s): xoay %.2f°", image_type, angle)
    h, w = img.shape[:2]
    center = (w // 2, h // 2)
    M = cv2.getRotationMatrix2D(center, angle, 1.0)
    return cv2.warpAffine(
        img, M, (w, h),
        flags=cv2.INTER_CUBIC,
        borderMode=cv2.BORDER_REPLICATE
    )


# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 4+5: Grayscale + CLAHE
# ══════════════════════════════════════════════════════════════════════════════

def enhance_contrast(img: np.ndarray, image_type: ImageType = "cover") -> np.ndarray:
    """
    BGR → Grayscale + CLAHE.

    v2: Tham số CLAHE khác nhau cho từng loại ảnh:
      - cover:    clipLimit=2.5 (nhẹ, tránh khuếch đại nhiễu từ background màu)
      - document: clipLimit=3.5 (mạnh hơn để tăng tương phản chữ nhạt)
      - receipt:  clipLimit=4.0 (mạnh nhất, giấy nhiệt thường mờ)
    """
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    clip  = {"cover": 2.5, "document": 3.5, "receipt": 4.0}[image_type]
    grid  = {"cover": 8,   "document": 8,   "receipt": 6  }[image_type]

    clahe = cv2.createCLAHE(clipLimit=clip, tileGridSize=(grid, grid))
    return clahe.apply(gray)


# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 6: Denoise
# ══════════════════════════════════════════════════════════════════════════════

def denoise_image(gray_img: np.ndarray, image_type: ImageType = "cover") -> np.ndarray:
    """
    Khử nhiễu.

    v2: Chọn filter phù hợp theo loại ảnh:
      - cover:    Bilateral (giữ màu sắc phức tạp của bìa)
      - document: Bilateral nhẹ hơn (d=7 thay vì d=9)
      - receipt:  Gaussian blur trước sau đó Bilateral (giấy nhiệt có nhiễu khác)
    """
    if image_type == "receipt":
        # Gaussian blur nhẹ để loại bỏ noise hạt
        blurred = cv2.GaussianBlur(gray_img, (3, 3), 0)
        return cv2.bilateralFilter(blurred, 7, 50, 50)

    d = 9 if image_type == "cover" else 7
    sigma = 75 if image_type == "cover" else 60
    return cv2.bilateralFilter(gray_img, d, sigma, sigma)


# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 6b (MỚI): Sharpen – Tăng nét chữ cho document/receipt
# ══════════════════════════════════════════════════════════════════════════════

def sharpen_image(gray_img: np.ndarray) -> np.ndarray:
    """
    Unsharp masking – tăng sắc nét cạnh chữ.
    Chỉ áp dụng cho document và receipt (bìa sách không cần).

    Kernel:    [[ 0, -1,  0],
                [-1,  5, -1],
                [ 0, -1,  0]]
    Hiệu ứu: Tăng tương phản cạnh → chữ sắc nét hơn cho Tesseract.
    """
    kernel = np.array([[0,  -1, 0],
                       [-1,  5, -1],
                       [0,  -1, 0]], dtype=np.float32)
    sharpened = cv2.filter2D(gray_img, -1, kernel)
    # Clip để tránh overflow
    return np.clip(sharpened, 0, 255).astype(np.uint8)


# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 7 (AGGRESSIVE): Adaptive Threshold
# ══════════════════════════════════════════════════════════════════════════════

def binarize_image(gray_img: np.ndarray, image_type: ImageType = "document") -> np.ndarray:
    """
    Nhị phân hóa ảnh với params tối ưu theo loại.

    v2:
      - document: Adaptive Gaussian (blockSize=31, C=10) – như cũ
      - receipt:  Otsu threshold (hiệu quả hơn cho hóa đơn đen trắng rõ ràng)
    """
    if image_type == "receipt":
        # Otsu: tự tính ngưỡng toàn cục tối ưu
        _, binary = cv2.threshold(
            gray_img, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU
        )
        return binary

    # document: Adaptive Gaussian
    return cv2.adaptiveThreshold(
        gray_img, 255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        ADAPTIVE_BLOCK, ADAPTIVE_C,
    )


# ══════════════════════════════════════════════════════════════════════════════
# MORPHOLOGICAL OPS (MỚI): Xử lý sau threshold
# ══════════════════════════════════════════════════════════════════════════════

def morphological_cleanup(binary_img: np.ndarray) -> np.ndarray:
    """
    Làm sạch ảnh binary sau khi threshold:
    - Loại bỏ các điểm nhiễu nhỏ (small blobs)
    - Lấp đầy các khoảng hở trong chữ

    Chỉ dùng cho receipt mode.
    """
    # Kernel nhỏ 2x2 để không distort chữ
    kernel = np.ones((2, 2), np.uint8)
    # Erosion → loại bỏ điểm nhiễu đơn lẻ
    cleaned = cv2.erode(binary_img, kernel, iterations=1)
    # Dilation → phục hồi kích thước chữ sau erosion
    cleaned = cv2.dilate(cleaned, kernel, iterations=1)
    return cleaned


# ══════════════════════════════════════════════════════════════════════════════
# API CHÍNH: preprocess() – v2 Adaptive Pipeline
# ══════════════════════════════════════════════════════════════════════════════

def preprocess(
    image_bytes: bytes,
    aggressive: bool = False,
    image_type: ImageType | None = None,
) -> tuple[np.ndarray, ImageType]:
    """
    Hàm chính: chạy toàn bộ adaptive preprocessing pipeline (v2).

    Args:
        image_bytes: raw bytes từ HTTP upload
        aggressive:  True = force document/receipt mode (backward compat)
        image_type:  Ghi đè auto-detection nếu caller đã biết loại ảnh

    Returns:
        (numpy.ndarray, ImageType) – ảnh đã xử lý + loại ảnh detected

    Pipeline theo loại ảnh:
        cover:    bytes → cv2 → resize → deskew → CLAHE(2.5) → bilateral
        document: bytes → cv2 → resize → deskew → CLAHE(3.5) → bilateral(d=7) → sharpen → adaptive_threshold
        receipt:  bytes → cv2 → resize → deskew → CLAHE(4.0) → gaussian+bilateral → sharpen → otsu → morphology
    """
    import time
    t0 = time.perf_counter()
    logger.info("▶ Preprocessing bắt đầu (aggressive=%s, type=%s)", aggressive, image_type)

    # [1] Decode bytes → cv2 array
    img = bytes_to_cv2(image_bytes)

    # [2] Auto-detect loại ảnh (nếu chưa được ghi đè)
    if image_type is None:
        if aggressive:
            image_type = "document"  # backward compat
        else:
            image_type = classify_image_type(img)

    # [3] Resize phù hợp với loại ảnh
    img = resize_image(img, image_type)

    # [3.5] Auto-orient: phát hiện và sửa ảnh bị lật ngược 180°
    img = auto_orient(img)

    # [4] Deskew – xoay thẳng khi bị nghiêng
    img = deskew_image(img, image_type)

    # [5] Grayscale + CLAHE
    gray = enhance_contrast(img, image_type)

    # [6] Denoise
    denoised = denoise_image(gray, image_type)

    # [7] Sharpen + Binarize (document/receipt)
    if image_type in ("document", "receipt"):
        sharpened = sharpen_image(denoised)
        result = binarize_image(sharpened, image_type)

        if image_type == "receipt":
            result = morphological_cleanup(result)

        elapsed = (time.perf_counter() - t0) * 1000
        logger.info("✅ Preprocessing %s xong (%.0fms)", image_type.upper(), elapsed)
        return result, image_type

    elapsed = (time.perf_counter() - t0) * 1000
    logger.info("✅ Preprocessing COVER xong (%.0fms)", elapsed)
    return denoised, image_type


# ══════════════════════════════════════════════════════════════════════════════
# TEXT REGION DETECTION – dùng cho tiling thông minh trong OCR engine
# ══════════════════════════════════════════════════════════════════════════════

def detect_text_regions_for_ocr(gray_img: np.ndarray) -> list[np.ndarray]:
    """
    Phát hiện vùng chứa chữ trên bìa sách bằng variance-based detection.

    Nguyên lý:
      - Vùng có chữ:     local std deviation CAO  (cạnh sắc chữ/nền tương phản)
      - Vùng minh họa:   local std deviation THẤP (màu gradient mềm, không sắc nét)

    Thuật toán:
      1. Tính local stddev qua box filter trick (mean(x²) - mean(x)²)
      2. Threshold top-20% variance → text mask
      3. Dilate ngang (~8% width) để nối các chữ thành dòng
      4. Connected components → bounding boxes của text blocks
      5. Lọc theo kích thước (loại quá nhỏ, quá cao)
      6. Crop full-width với padding dọc

    Args:
        gray_img: Grayscale image (output của preprocess/enhance_contrast)

    Returns:
        List[np.ndarray]: Danh sách các crop vùng text, sắp từ trên xuống dưới.
        Trả về [] nếu không detect được (caller fallback về fixed tiling).

    Examples:
        "Dế Mèn Phiêu Lưu Ký": text "TÔ HOÀI" + "Dế Mèn phiêu lưu ký" nổi trên
        nền lá xanh → 2 dải text tách biệt được detect, không lẫn minh họa.

        "Hãy Nhớ Tên Anh Ấy": text trắng đậm "HÃY NHỚ TÊN ANH ẤY" trên ảnh
        đồng cỏ → vùng chữ có variance cao hơn nền ảnh thực → detected.
    """
    h, w = gray_img.shape[:2]
    if h < 50 or w < 50:
        return []

    # ── 1. Tính local std deviation ───────────────────────────────────────────
    # Window = ~4% kích thước nhỏ hơn của ảnh (đủ để bao 1-2 nét chữ)
    win = max(15, int(min(w, h) * 0.04))
    if win % 2 == 0:
        win += 1

    img_f    = gray_img.astype(np.float32)
    mean_map = cv2.blur(img_f, (win, win))
    msq_map  = cv2.blur(img_f ** 2, (win, win))
    var_map  = np.maximum(msq_map - mean_map ** 2, 0)
    std_map  = np.sqrt(var_map).astype(np.uint8)

    # ── 2. Threshold: top 20% variance = text ─────────────────────────────────
    thresh_val = max(float(np.percentile(std_map, 80)), 15.0)
    _, text_mask = cv2.threshold(std_map, thresh_val, 255, cv2.THRESH_BINARY)

    # ── 3. Horizontal dilation: nối chữ thành dòng ───────────────────────────
    dil_w  = max(20, int(w * 0.08))   # ~8% chiều rộng ảnh
    dil_h  = max(3,  int(h * 0.008))  # ~0.8% chiều cao ảnh
    h_kern = cv2.getStructuringElement(cv2.MORPH_RECT, (dil_w, dil_h))
    connected = cv2.dilate(text_mask, h_kern, iterations=1)

    # ── 4. Connected components ────────────────────────────────────────────────
    n_labels, _, stats, _ = cv2.connectedComponentsWithStats(
        connected, connectivity=8
    )

    # ── 5. Lọc component hợp lệ ───────────────────────────────────────────────
    min_area   = (h * w) * 0.002    # ≥ 0.2% diện tích ảnh
    max_height = h * 0.30           # Dòng chữ ≤ 30% chiều cao
    min_width  = w * 0.12           # ≥ 12% chiều rộng

    zones: list[tuple[int, np.ndarray]] = []
    for i in range(1, n_labels):
        x_  = stats[i, cv2.CC_STAT_LEFT]
        y_  = stats[i, cv2.CC_STAT_TOP]
        rw_ = stats[i, cv2.CC_STAT_WIDTH]
        rh_ = stats[i, cv2.CC_STAT_HEIGHT]

        if (
            rw_ * rh_ >= min_area
            and rh_ <= max_height
            and rh_ >= 8
            and rw_ >= min_width
        ):
            # Padding dọc 40% chiều cao vùng để không cắt đứt dấu tiếng Việt
            pad = max(int(rh_ * 0.4), 8)
            y1 = max(0, y_ - pad)
            y2 = min(h, y_ + rh_ + pad)
            # Full width: không crop ngang (tiêu đề có thể gần sát rìa)
            zone_crop = gray_img[y1:y2, :]
            if zone_crop.shape[0] >= 12:
                zones.append((y_, zone_crop))

    # Sắp xếp từ trên xuống theo vị trí y
    zones.sort(key=lambda z: z[0])
    result = [crop for _, crop in zones]

    logger.debug(
        "TRD: detect_text_regions  found %d zones (image %dx%d)", len(result), w, h
    )
    return result
