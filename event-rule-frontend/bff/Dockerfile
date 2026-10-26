# Runtime image for BFF (Backend for Frontend)
# Note: Install dependencies locally first with: npm install --production

ARG BFF_BASE_IMAGE=node:18-alpine
FROM ${BFF_BASE_IMAGE}

RUN apk add --no-cache openjdk21-jre-headless

WORKDIR /app

# Copy package files
COPY package*.json ./

# Copy pre-installed node_modules
COPY node_modules ./node_modules

# Copy application code
COPY server.js ./
COPY routes/ ./routes/

# Expose port
EXPOSE 8080

# Start the server
CMD ["node", "server.js"]

