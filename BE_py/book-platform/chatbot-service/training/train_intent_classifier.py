"""
train_intent_classifier.py – Train TF-IDF + Logistic Regression cho Intent Classification.

Thuật toán:
  1. TfidfVectorizer: chuyển câu text thành vector đặc trưng (bag of n-grams)
  2. LogisticRegression: phân loại multi-class với softmax (predict_proba để lấy confidence)

Chạy:
  cd chatbot-service
  python training/train_intent_classifier.py

Sau khi chạy, model được lưu tại:
  training/intent_model.pkl       ← LogisticRegression model
  training/intent_vectorizer.pkl  ← TfidfVectorizer
"""
import json
import pickle
from pathlib import Path
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import Pipeline

BASE_DIR = Path(__file__).resolve().parent
DATA_PATH = BASE_DIR / "intent_training_data.json"
MODEL_PATH = BASE_DIR / "intent_model.pkl"
VEC_PATH   = BASE_DIR / "intent_vectorizer.pkl"


def train():
    # ── 1. Load dữ liệu ──────────────────────────────────────
    with open(DATA_PATH, encoding="utf-8") as f:
        data = json.load(f)

    texts  = [d["text"]   for d in data]
    labels = [d["intent"] for d in data]

    print(f"📖 {len(texts)} samples, {len(set(labels))} intents")

    # ── 2. TF-IDF Vectorizer ──────────────────────────────────
    # ngram_range=(1,2): unigram + bigram → bắt được cụm từ 2 chữ
    # min_df=1: giữ tất cả từ (dataset nhỏ)
    vectorizer = TfidfVectorizer(
        ngram_range=(1, 2),
        analyzer="word",
        token_pattern=r"(?u)\b\w+\b",
        min_df=1,
        max_features=5000,
    )
    X = vectorizer.fit_transform(texts)

    # ── 3. Logistic Regression ────────────────────────────────
    # C=5: regularization (điều chỉnh nếu overfit/underfit)
    # max_iter=500: tăng nếu chưa converge
    clf = LogisticRegression(C=5, max_iter=500, random_state=42)
    clf.fit(X, labels)

    # ── 4. Cross-validation (đánh giá nội bộ) ─────────────────
    scores = cross_val_score(clf, X, labels, cv=3, scoring="accuracy")
    print(f"✅ Cross-val accuracy: {scores.mean():.3f} ± {scores.std():.3f}")

    # ── 5. Lưu model ─────────────────────────────────────────
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(clf, f)
    with open(VEC_PATH, "wb") as f:
        pickle.dump(vectorizer, f)

    print(f"💾 Saved: {MODEL_PATH}")
    print(f"💾 Saved: {VEC_PATH}")

    # ── 6. Test nhanh ─────────────────────────────────────────
    test_phrases = [
        "gợi ý sách kỹ năng",
        "đơn 12345 ở đâu",
        "phí ship bao nhiêu",
        "muốn đổi trả sách",
    ]
    print("\n🔍 Quick test:")
    for phrase in test_phrases:
        vec  = vectorizer.transform([phrase])
        pred = clf.predict(vec)[0]
        conf = clf.predict_proba(vec).max()
        print(f"   '{phrase}' → {pred} ({conf:.2f})")


if __name__ == "__main__":
    train()
