#!/bin/bash
# FinGuard AI - Docker Cleanup Script
# Frees up disk space by removing unused Docker data

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║              FinGuard AI - Docker Cleanup                    ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Show current disk usage
echo -e "${YELLOW}Current Docker disk usage:${NC}"
docker system df
echo ""

# Stop and remove containers
echo -e "${YELLOW}Stopping and removing containers...${NC}"
docker-compose down 2>/dev/null || true
docker container prune -f
echo ""

# Remove images
echo -e "${YELLOW}Removing images...${NC}"
docker image prune -af --filter "label=finguard"
docker rmi finguard-ai-api finguard-ai-celery-worker finguard-ai-celery-beat 2>/dev/null || true
docker image prune -af
echo ""

# Remove build cache
echo -e "${YELLOW}Removing build cache...${NC}"
docker builder prune -af
echo ""

# Remove volumes (optional - keeps data)
read -p "Remove Docker volumes too? (y/N): " remove_volumes
if [[ $remove_volumes == [yY] ]]; then
    docker volume prune -f
    echo -e "${GREEN}Volumes removed${NC}"
else
    echo -e "${YELLOW}Volumes kept (data preserved)${NC}"
fi
echo ""

# Final cleanup
echo -e "${YELLOW}Final system prune...${NC}"
docker system prune -af --volumes
echo ""

# Show new disk usage
echo -e "${GREEN}Cleanup complete!${NC}"
echo ""
echo -e "${YELLOW}New Docker disk usage:${NC}"
docker system df
echo ""
echo -e "${GREEN}Space freed up!${NC}"
