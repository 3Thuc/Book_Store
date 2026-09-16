# 📚 BookStorage

A distributed microservices platform for an online bookstore, combining a full-featured e-commerce experience with AI-powered features such as personalized recommendations, semantic search, OCR, and a RAG-based chatbot.

Built by a team of 2 as a full-stack learning project, focused on real-world microservices architecture, distributed deployment, and integrating AI services into a production-style system.

---

## ✨ Features

### Core E-commerce
- Browse, search, and view book catalog
- Advanced search & filtering by title, author, category, price range, and keyword
- Shopping cart management (add / update / remove items)
- Order placement, checkout, and order status tracking
- Real-time order alerts for admins via WebSocket
- Automated order confirmation emails

### AI-Powered Services
- **Recommendation Engine** — personalized book suggestions based on user behavior (search history, viewed products, purchase history)
- **Semantic Search** — vector-based search powered by OpenSearch
- **OCR Service** — text extraction from book cover / page images
- **RAG Chatbot** — retrieval-augmented chatbot for user queries

---

## 🏗️ Architecture

BookStorage is built as a set of independently deployable microservices rather than a single monolith:

```
┌─────────────────────┐
│  Frontend (React)    │  ReactJS + Vite + Tailwind — deployed on Vercel
└──────────┬───────────┘
           │
┌──────────▼───────────┐      ┌──────────────────────────┐
│  Core Backend         │      │  AI Services (Python)     │
│  Java Spring Boot     │◄────►│  Recommendation           │
│  - Users               │      │  Semantic Search           │
│  - Book Catalog        │      │  OCR                       │
│  - Cart & Orders       │      │  RAG Chatbot               │
└──────────┬───────────┘      └────────────┬──────────────┘
           │                                │
           ▼                                ▼
┌───────────────────────────────────────────────────────┐
│                     Data Tier                          │
│  MySQL (relational data) · Redis (shared cache)        │
│  OpenSearch (vector / semantic search)                  │
│  MinIO (image storage for OCR)                          │
└───────────────────────────────────────────────────────┘
```

**Deployment:** self-hosted VPS running Docker Compose, with all services containerized for independent scaling and deployment.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Frontend | ReactJS, Vite, TailwindCSS, TypeScript |
| Core Backend | Java, Spring Boot (REST APIs) |
| AI Services | Python, FastAPI |
| Database | MySQL |
| Cache | Redis |
| Search / Vector DB | OpenSearch |
| Object Storage | MinIO |
| Realtime | WebSocket |
| Deployment | Docker Compose, self-hosted VPS |

---

## 📦 Modules

- `FE/` — React + TypeScript client application
- `BE/` — Java Spring Boot backend (users, catalog, cart, orders)
- `BE_py/` — Python FastAPI services (recommendation, OCR, semantic search, RAG chatbot)

---

## 🚀 Getting Started

```bash
# Clone the repository
git clone https://github.com/3Thuc/Book_Store.git
cd bookstorage

# Start all services with Docker Compose
docker compose up -d
```

Then visit `http://localhost:<port>` to access the frontend.

---

## 🔑 Environment Variables

Create a `.env` file (or set these as environment variables) before running the project. **Never commit real secrets to the repository** — use this as a `.env.example` template:

```properties
# Server
SERVER_PORT=8443

# Database
SPRING_DATASOURCE_URL=jdbc:mysql://localhost:3306/bookstore
SPRING_DATASOURCE_USERNAME=root
SPRING_DATASOURCE_PASSWORD=

# Mail (SMTP)
MAIL_USERNAME=
MAIL_PASSWORD=

# App URLs
APP_FRONTEND_URL=http://localhost:3000
APP_BASE_URL=https://localhost:8443

# JWT
JWT_SIGNERKEY=

# Google OAuth2
GOOGLE_CLIENT_ID=
GOOGLE_CLIENT_SECRET=
GOOGLE_REDIRECT_URI=

# VNPay
VNP_TMN_CODE=
VNP_HASH_SECRET=

# PayOS
PAYOS_CLIENT_ID=
PAYOS_API_KEY=
PAYOS_CHECKSUM_KEY=

# MinIO
MINIO_URL=http://localhost:9000
MINIO_ACCESS_KEY=
MINIO_SECRET_KEY=
```
---

## 👥 Contributors

- **ThucTran** — Core backend (Spring Boot REST APIs), frontend (TypeScript), advanced search, cart/order management, WebSocket notifications, email notifications
- **TrongNghia** — Personalized recommendation system (Python)

---
