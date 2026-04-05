# build stage
FROM node:20 AS builder

WORKDIR /app

COPY package*.json ./
RUN npm ci                    # ← dùng npm ci thay vì npm install

COPY . .
RUN npm run build

# run stage
FROM nginx:stable-alpine

COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]