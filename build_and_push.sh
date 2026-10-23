#!/bin/bash

# 多架构Docker镜像构建和推送脚本
# 支持AMD64和ARM64架构

set -e  # 遇到错误时退出

# 配置变量
DOCKER_USERNAME="charmingcheung000"  # 请填写你的Docker Hub用户名
IMAGE_NAME="charming-epg"
TAG="latest"
DOCKERFILE="Dockerfile"  # 统一的Dockerfile

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 输出函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查必要的工具
check_requirements() {
    log_info "检查必要的工具..."

    if ! command -v docker &> /dev/null; then
        log_error "Docker 未安装或不在PATH中"
        exit 1
    fi

    if ! docker buildx version &> /dev/null; then
        log_error "Docker buildx 未安装"
        exit 1
    fi

    log_info "检查Docker引擎连接..."
    if ! docker system info >/dev/null; then
        log_error "无法连接Docker引擎，请启动 Docker Desktop/OrbStack，并用 docker context ls 检查当前 context"
        exit 1
    fi

    log_success "工具检查完成"
}

# 获取用户输入
get_user_input() {
    if [ -z "$DOCKER_USERNAME" ]; then
        read -p "请输入你的Docker Hub用户名: " DOCKER_USERNAME
        if [ -z "$DOCKER_USERNAME" ]; then
            log_error "用户名不能为空"
            exit 1
        fi
    fi

    FULL_IMAGE_NAME="$DOCKER_USERNAME/$IMAGE_NAME:$TAG"
    log_info "将构建镜像: $FULL_IMAGE_NAME"
}

# 检查Dockerfile是否存在
check_dockerfile() {
    log_info "检查Dockerfile文件..."

    if [ ! -f "$DOCKERFILE" ]; then
        log_error "Dockerfile ($DOCKERFILE) 不存在"
        exit 1
    fi

    log_success "Dockerfile文件检查完成"
}

# Docker Hub 认证以 docker login 的退出状态为准。
# 保留现有配置中的 context、credential store 和 Buildx 设置。
docker_login() {
    log_info "准备登录Docker Hub..."
    log_info "目标用户名: $DOCKER_USERNAME"

    local max_attempts=3
    local attempt=1
    while [ "$attempt" -le "$max_attempts" ]; do
        echo "登录尝试 $attempt/$max_attempts"
        if docker login -u "$DOCKER_USERNAME"; then
            log_success "Docker Hub 登录成功 - 用户: $DOCKER_USERNAME"
            return 0
        fi

        if [ "$attempt" -eq "$max_attempts" ]; then
            log_error "达到最大尝试次数，登录失败"
            return 1
        fi
        log_warning "Docker Hub 登录失败，请重试..."
        attempt=$((attempt + 1))
        sleep 2
    done
}

# 设置buildx构建器
setup_buildx() {
    log_info "设置Docker buildx构建器..."

    local docker_context
    docker_context=$(docker context show)
    BUILDER_NAME="charming-epg-${docker_context}"
    log_info "Docker context: ${docker_context}，构建器: $BUILDER_NAME"

    # 检查构建器是否已存在
    if docker buildx inspect "$BUILDER_NAME" &> /dev/null; then
        log_info "构建器 $BUILDER_NAME 已存在，正在使用..."
        docker buildx use "$BUILDER_NAME"
    else
        log_info "创建新的构建器: $BUILDER_NAME"
        docker buildx create --name "$BUILDER_NAME" --driver docker-container --use "$docker_context"
    fi

    log_info "启动构建器..."
    local builder_info node_status
    builder_info=$(docker buildx inspect "$BUILDER_NAME" --bootstrap)
    printf '%s\n' "$builder_info"
    node_status=$(printf '%s\n' "$builder_info" | awk '/^[[:space:]]*Status:/ {print $2}')
    if [ "$node_status" != "running" ]; then
        log_error "构建器未就绪 (状态: $node_status)，请检查上述 endpoint 和连接错误"
        return 1
    fi

    log_success "构建器设置完成"
}

# 统一构建多架构镜像
build_multiarch() {
    log_info "开始构建多架构镜像..."
    log_info "支持架构: linux/amd64, linux/arm64"
    echo ""

    log_info "构建参数:"
    log_info "  Docker用户: $DOCKER_USERNAME"
    log_info "  Dockerfile: $DOCKERFILE"
    log_info "  镜像名称: $FULL_IMAGE_NAME"
    log_info "  平台架构: linux/amd64,linux/arm64"
    echo ""

    # 再次确认推送的镜像名称
    log_warning "即将推送到: $FULL_IMAGE_NAME"
    read -p "确认继续？(y/N): " confirm
    if [[ ! $confirm =~ ^[Yy]$ ]]; then
        log_info "用户取消操作"
        exit 0
    fi

    docker buildx build \
        --builder "$BUILDER_NAME" \
        --pull \
        --no-cache \
        --platform linux/amd64,linux/arm64 \
        --file "$DOCKERFILE" \
        --tag "$FULL_IMAGE_NAME" \
        --push \
        .

    log_success "多架构镜像构建和推送完成!"
}

# 验证镜像
verify_image() {
    log_info "验证多架构镜像..."
    echo ""

    echo "=== 镜像架构信息 ==="
    docker buildx imagetools inspect "$FULL_IMAGE_NAME"
    echo ""

    log_success "镜像验证完成"
}

# 主函数
main() {
    echo "=================================="
    echo "  多架构Docker镜像构建脚本"
    echo "  支持架构: AMD64, ARM64"
    echo "  统一Dockerfile构建"
    echo "  Docker Hub 手动登录"
    echo "=================================="
    echo ""

    check_requirements
    get_user_input
    check_dockerfile
    docker_login
    setup_buildx
    build_multiarch
    verify_image

    echo ""
    log_success "所有操作完成!"
    echo "Docker用户: $DOCKER_USERNAME"
    echo "镜像名称: $FULL_IMAGE_NAME"
    echo "支持架构: linux/amd64, linux/arm64"
    echo ""
    echo "使用方法:"
    echo "  docker pull $FULL_IMAGE_NAME"
    echo "  docker run $FULL_IMAGE_NAME"
    echo ""
    echo "查看镜像架构:"
    echo "  docker buildx imagetools inspect $FULL_IMAGE_NAME"
}

# 运行主函数
main "$@"
