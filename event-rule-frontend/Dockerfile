# Serve pre-built React frontend with nginx
# Note: Build the React app locally first with: npm run build

ARG FRONTEND_BASE_IMAGE=nginx:alpine
FROM ${FRONTEND_BASE_IMAGE}

# Copy pre-built assets from local build directory
COPY build /usr/share/nginx/html

# Copy nginx configuration template; envsubst fills in BFF_URL_WS / BFF_URL_HTTP
# at container start so the CSP header works for any deployment host.
COPY nginx.conf.template /etc/nginx/templates/default.conf.template

# Expose port 80
EXPOSE 80

# nginx:alpine's official entrypoint already runs envsubst on /etc/nginx/templates/*.template
# and writes the result to /etc/nginx/conf.d/ before starting nginx.
# Default values let the container work out-of-the-box with docker-compose on localhost.
ENV BFF_URL_WS=ws://localhost:8080
ENV BFF_URL_HTTP=http://localhost:8080

# Start nginx (uses the official nginx:alpine entrypoint for template substitution)
CMD ["/docker-entrypoint.sh", "nginx", "-g", "daemon off;"]
