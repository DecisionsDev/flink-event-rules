# Serve pre-built React frontend with nginx
# Note: Build the React app locally first with: npm run build

ARG FRONTEND_BASE_IMAGE=nginx:alpine
FROM ${FRONTEND_BASE_IMAGE}

# Copy pre-built assets from local build directory
COPY build /usr/share/nginx/html

# Copy custom nginx configuration (optional)
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Expose port 80
EXPOSE 80

# Start nginx
CMD ["nginx", "-g", "daemon off;"]

