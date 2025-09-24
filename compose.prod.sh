#!/bin/bash

# Source the environment file and export all variables
set -a  # automatically export all variables
source .env.prod
set +a  # stop automatically exporting

echo "🐳 Starting Polygon MCP Server in PRODUCTION mode..."

# Check if mcp-shared network exists, create if not
if ! docker network ls | grep -q "mcp-shared"; then
    echo "📡 Creating mcp-shared network..."
    docker network create mcp-shared
fi

# Ensure production environment file exists
if [ ! -f ".env.prod" ]; then
    echo "❌ .env.prod file not found!"
    echo "Please copy .env.prod and configure it for production"
    exit 1
fi

# Stop and remove existing containers to ensure fresh deployment
echo "🛑 Stopping and removing existing containers..."
docker compose -f docker-compose.prod.yml down

# Remove existing images to force rebuild
echo "🗑️  Removing existing images to force rebuild..."
docker rmi -f mcp-polygon-mcp-polygon 2>/dev/null || true

# Build and start services in production mode
echo "🏗️  Building and starting services in production mode..."
docker compose -f docker-compose.production.yml up --build -d

echo "✅ Polygon MCP Server is running in production mode!"
echo "🌐 Service is accessible via reverse proxy"
echo "🔧 MCP Endpoint: ${MCP_PUBLIC_BASE_URL}/polygon/"
echo ""
echo "To check logs:"
echo "  docker compose -f docker-compose.production.yml logs -f"
echo ""
echo "To stop:"
echo "  docker compose -f docker-compose.production.yml down"