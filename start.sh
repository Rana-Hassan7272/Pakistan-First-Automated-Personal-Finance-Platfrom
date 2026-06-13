#!/bin/bash
# FinGuard AI - Docker Start Script for Git Bash
# Usage: ./start.sh [command]
# Commands: up, down, logs, build, status, clean

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_banner() {
    echo -e "${GREEN}"
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║                  FinGuard AI - Docker Stack                   ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_services() {
    echo -e "${YELLOW}Services (Core Only):${NC}"
    echo "  • API (FastAPI)         → http://localhost:8000"
    echo "  • Celery Worker         → background tasks"
    echo "  • Celery Beat           → scheduler"
    echo "  • Redis                 → localhost:6379"
    echo ""
    echo -e "${YELLOW}Note:${NC} Monitoring (Grafana, Prometheus, MLflow, Elasticsearch)"
    echo "      will be added at deployment time."
    echo ""
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}Error: Docker not found${NC}"
        echo "Install Docker Desktop: https://docs.docker.com/desktop/install/windows-install/"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${RED}Error: docker-compose not found${NC}"
        exit 1
    fi
}

check_env() {
    if [ ! -f .env ]; then
        echo -e "${RED}Error: .env file not found${NC}"
        echo "Create .env file from template:"
        echo "  cp .env.example .env"
        exit 1
    fi
}

start_all() {
    print_banner
    check_docker
    check_env
    
    echo -e "${GREEN}Starting FinGuard AI stack...${NC}"
    echo ""
    
    # Create necessary directories
    mkdir -p logs models monitoring/grafana/dashboards
    
    # Pull and start services
    docker-compose pull
    docker-compose up -d --build
    
    echo ""
    echo -e "${GREEN}✓ Stack started successfully!${NC}"
    echo ""
    print_services
    echo -e "${YELLOW}Logs:${NC} docker-compose logs -f [service]"
    echo -e "${YELLOW}Stop:${NC} docker-compose down"
}

stop_all() {
    echo -e "${YELLOW}Stopping FinGuard AI stack...${NC}"
    docker-compose down
    echo -e "${GREEN}✓ Stopped${NC}"
}

show_logs() {
    if [ -z "$1" ]; then
        docker-compose logs -f
    else
        docker-compose logs -f "$1"
    fi
}

show_status() {
    echo -e "${YELLOW}Service Status:${NC}"
    docker-compose ps
}

rebuild() {
    print_banner
    echo -e "${YELLOW}Rebuilding services...${NC}"
    docker-compose down
    docker-compose build --no-cache
    docker-compose up -d
    echo -e "${GREEN}✓ Rebuilt and started${NC}"
}

clean_all() {
    echo -e "${RED}WARNING: This will delete all data!${NC}"
    read -p "Are you sure? (y/N): " confirm
    if [[ $confirm == [yY] ]]; then
        docker-compose down -v
        docker system prune -f
        echo -e "${GREEN}✓ Cleaned up${NC}"
    else
        echo "Cancelled"
    fi
}

# Main command handler
case "${1:-up}" in
    up|start)
        start_all
        ;;
    down|stop)
        stop_all
        ;;
    logs)
        show_logs "$2"
        ;;
    status|ps)
        show_status
        ;;
    build|rebuild)
        rebuild
        ;;
    clean)
        clean_all
        ;;
    help|--help|-h)
        echo "FinGuard AI Docker Manager"
        echo ""
        echo "Usage: ./start.sh [command]"
        echo ""
        echo "Commands:"
        echo "  up, start     Start all services (default)"
        echo "  down, stop    Stop all services"
        echo "  logs [svc]    Show logs (optionally for specific service)"
        echo "  status, ps    Show service status"
        echo "  build, rebuild Rebuild and restart"
        echo "  clean         Remove all data (DANGEROUS)"
        echo "  help          Show this help"
        echo ""
        echo "Examples:"
        echo "  ./start.sh              # Start everything"
        echo "  ./start.sh logs api     # View API logs"
        echo "  ./start.sh status       # Check if running"
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        echo "Run './start.sh help' for usage"
        exit 1
        ;;
esac
