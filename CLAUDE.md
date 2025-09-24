# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Polygon MCP (Model Context Protocol) server that exposes Polygon API endpoints as MCP tools. The project is built with Python and FastMCP, designed to run in a containerized environment.

## Architecture

### Core Components

- **FastMCP Server** (`backend/main.py`): Main server implementation using the FastMCP framework
  - Implements MCP tools for Polygon.io financial data API
  - Uses Starlette for ASGI application
  - Includes token-based authentication middleware
  - Supports both header-based and URL-based token authentication

- **Image Utilities** (`backend/mcp_image_utils.py`): Helper functions for image processing
  - PIL image conversion to MCP format
  - Base64 encoding/decoding
  - Image loading from various sources (URL, file path, bytes)
  - Reserved for future use with financial charts/visualizations

### Key Features

- **Service Naming**: Uses `MCP_NAME` environment variable (defaults to "polygon") to configure tool names and paths
- **Authentication**: Token-based auth via `TokenAuthMiddleware`
  - Header-based: `Authorization: Bearer <token>`
  - Query-based: `?token=<token>` (if `MCP_ALLOW_URL_TOKENS=true`)
  - Path-based: `/<service>/<token>/...` (if `MCP_ALLOW_URL_TOKENS=true`)
- **Sentry Integration**: Optional error tracking via `SENTRY_DSN` environment variable

### MCP Tools

- **polygon_news**: Fetches market news from Polygon.io
  - Parameters: `start_date` (optional), `end_date` (optional)
  - Returns: Markdown-formatted table with datetime and topic columns

## Development Commands

### Running the Server Locally

**Using Docker (Recommended):**
```bash
./compose.local.sh
```

**Direct Python (for development):**
```bash
cd backend
python main.py
```

Server starts on port specified by `PORT` environment variable (default: 8009)

### Production Deployment

```bash
./compose.prod.sh
```

This script:
- Sources `.env.prod` for environment variables
- Creates `mcp-shared` Docker network if needed
- Rebuilds and starts containers via `docker-compose.production.yml`

### Docker Commands

```bash
# View logs
docker compose -f docker-compose.production.yml logs -f

# Stop services
docker compose -f docker-compose.production.yml down

# Rebuild specific service
docker compose -f docker-compose.production.yml up --build -d
```

### Viewing Logs

```bash
./logs.sh
```

## Environment Configuration

Key environment variables (see `.env.production` for full list):

- `MCP_NAME`: Service name used for routing and tool naming (default: "polygon")
- `MCP_TOKENS`: Comma-separated list of valid auth tokens
- `MCP_REQUIRE_AUTH`: Enable/disable authentication (`true`/`false`)
- `MCP_ALLOW_URL_TOKENS`: Allow tokens in URL/query params
- `MCP_PUBLIC_BASE_URL`: Base URL for service access
- `POLYGON_API_KEY`: API key for Polygon.io (required for production)
- `PORT`: Server port (default: 8009)
- `SENTRY_DSN`: Optional Sentry DSN for error tracking

## Claude Desktop Configuration

**For local development:**
```json
{
  "mcpServers": {
    "polygon_local": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "http://localhost:8009/polygon/"
      ]
    }
  }
}
```

**Health check endpoint:** `http://localhost:8009/health`

## Important Patterns

### Adding New MCP Tools

Tools are defined using the `@mcp.tool()` decorator in `main.py`. Each tool:
- Must have a docstring (becomes tool description)
- Can return strings, lists, or content blocks
- Should handle API errors gracefully with proper logging

### Adding New Resources

Resources are defined using `@mcp.resource()` decorator:
- URI pattern with path parameters in curly braces
- Must specify MIME type
- Return bytes for binary data, str for text

### API Integration

When adding new Polygon API endpoints:
1. Use `POLYGON_API_KEY` environment variable for authentication
2. Log API calls with relevant parameters
3. Return data in structured format (markdown tables recommended)
4. Handle rate limits and API errors appropriately

## Dependencies

- `mcp==1.13.1`: Model Context Protocol SDK
- `starlette>=0.28.0`: ASGI framework
- `uvicorn[standard]>=0.31.1`: ASGI server
- `pillow==11.3.0`: Image processing (for future chart visualizations)
- `requests>=2.32.5`: HTTP client for Polygon API calls
- `sentry-sdk==2.38.0`: Error tracking