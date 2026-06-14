# Build stage
FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn package -DskipTests

# Runtime stage
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /app/target/*.jar app.jar
EXPOSE 8443 8080

# How to run with environment variables:
# 
# docker build -t bookstore-be .
# 
# docker run -d --name bookstore-be -p 8443:8443 \
#   -e SERVER_PORT=8443 \
#   -e SPRING_DATASOURCE_URL="jdbc:mysql://host.docker.internal:3306/bookstore?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC" \
#   -e SPRING_DATASOURCE_USERNAME="root" \
#   -e SPRING_DATASOURCE_PASSWORD="YOUR_PASSWORD" \
#   -e JWT_SIGNERKEY="YOUR_SECRET_KEY" \
#   -e MAIL_USERNAME="your-email@gmail.com" \
#   -e MAIL_PASSWORD="YOUR_APP_PASSWORD" \
#   -e GOOGLE_CLIENT_ID="YOUR_GOOGLE_CLIENT_ID" \
#   -e GOOGLE_CLIENT_SECRET="YOUR_GOOGLE_CLIENT_SECRET" \
#   -e GOOGLE_REDIRECT_URI="https://YOUR_PUBLIC_IP:8443/bookdb/auth/login/google" \
#   -e APP_FRONTEND_URL="https://your-fe-vercel.app" \
#   -e APP_BASE_URL="https://YOUR_PUBLIC_IP:8443" \
#   bookstore-be
#
# Note: host.docker.internal maps localhost from Docker to host machine

ENTRYPOINT ["java", "-jar", "app.jar"]
