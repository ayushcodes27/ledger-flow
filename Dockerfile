# Use official Java 21 image
FROM eclipse-temurin:21-jdk-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy the compiled jar into the container
COPY target/*.jar app.jar

# Expose our application port
EXPOSE 8088

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]