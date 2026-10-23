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

# 获取当前登录用户名的辅助函数
get_logged_user() {
    # 方法1: 从 docker info 获取
    local user_from_info=$(docker info 2>/dev/null | grep "Username:" | awk '{print $2}' 2>/dev/null || echo "")

    # 方法2: 从配置文件获取
    local user_from_config=""
    if [ -f "$HOME/.docker/config.json" ]; then
        # 尝试从配置文件中提取用户名（如果auths中有registry信息）
        user_from_config=$(cat "$HOME/.docker/config.json" 2>/dev/null | grep -o '"https://index.docker.io/v1/"' 2>/dev/null || echo "")
    fi

    # 方法3: 尝试通过简单的API调用验证
    local test_result=""
    if curl -s --max-time 5 -f "https://hub.docker.com/v2/users/$DOCKER_USERNAME/" >/dev/null 2>&1; then
        test_result="api_accessible"
    fi

    echo "$user_from_info"
}

# 验证登录状态
verify_login() {
    log_info "验证登录状态..."

    # 尝试执行一个需要认证的操作来验证登录
    local test_repo="$DOCKER_USERNAME/login-test-$(date +%s)"

    # 尝试推送一个简单的测试标签（不会真的推送，只是验证权限）
    log_info "测试Docker Hub连接和权限..."

    # 创建一个临时的最小镜像来测试推送权限
    echo "FROM scratch" > /tmp/test.dockerfile

    if docker buildx build --platform linux/amd64 -f /tmp/test.dockerfile -t "$test_repo" --dry-run . >/dev/null 2>&1; then
        log_success "Docker登录验证成功"
        rm -f /tmp/test.dockerfile
        return 0
    else
        # 如果dry-run不支持，尝试其他方法
        log_info "使用备用验证方法..."

        # 简单地检查是否能执行docker命令且显示Login Succeeded
        if docker system info >/dev/null 2>&1; then
            log_success "Docker登录验证成功 (用户: $DOCKER_USERNAME)"
            rm -f /tmp/test.dockerfile 2>/dev/null || true
            return 0
        else
            log_error "Docker登录验证失败"
            rm -f /tmp/test.dockerfile 2>/dev/null || true
            return 1
        fi
    fi
}

# 强制重新登录Docker Hub
docker_login() {
    log_info "准备登录Docker Hub..."

    # 显示当前登录用户（如果有的话）
    local current_user=$(get_logged_user)
    if [ -n "$current_user" ]; then
        log_warning "当前登录用户: $current_user"
    fi

    # 强制登出，清除所有缓存的凭据
    log_info "正在清除Docker登录状态..."
    docker logout > /dev/null 2>&1 || true

    # 清除Docker配置文件中的凭据（如果存在）
    if [ -f "$HOME/.docker/config.json" ]; then
        log_info "备份并清理Docker配置文件..."
        # 备份原配置文件
        cp "$HOME/.docker/config.json" "$HOME/.docker/config.json.backup.$(date +%s)" 2>/dev/null || true
        # 清空auths部分，但保留其他配置
        echo '{"auths": {}}' > "$HOME/.docker/config.json" 2>/dev/null || true
    fi

    echo ""
    log_warning "=== 强制手动登录模式 ==="
    log_info "目标用户名: $DOCKER_USERNAME"
    log_warning "请确保输入正确的用户名和密码！"
    echo ""

    # 使用指定用户名强制手动登录
    MAX_ATTEMPTS=3
    ATTEMPT=1

    while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
        echo "登录尝试 $ATTEMPT/$MAX_ATTEMPTS"
        echo "请输入Docker Hub凭据："

        # 强制交互式登录
        if docker login -u $DOCKER_USERNAME; then
            log_success "登录命令执行成功"

            # 验证登录状态
            if verify_login; then
                log_success "Docker Hub 登录验证成功 - 用户: $DOCKER_USERNAME"
                return 0
            else
                log_error "登录验证失败"
            fi
        else
            log_error "登录失败"
        fi

        if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
            log_error "达到最大尝试次数，登录失败"
            exit 1
        fi

        ATTEMPT=$((ATTEMPT + 1))
        echo ""
        log_warning "请重试..."
        sleep 2
    done
}

# 设置buildx构建器
setup_buildx() {
    log_info "设置Docker buildx构建器..."

    BUILDER_NAME="multiarch-builder"

    # 检查构建器是否已存在
    if docker buildx inspect "$BUILDER_NAME" &> /dev/null; then
        log_info "构建器 $BUILDER_NAME 已存在，正在使用..."
        docker buildx use "$BUILDER_NAME"
    else
        log_info "创建新的构建器: $BUILDER_NAME"
        docker buildx create --name "$BUILDER_NAME" --use
    fi

    log_info "启动构建器..."
    docker buildx inspect --bootstrap

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

# 清理函数
cleanup() {
    # 清理临时文件
    rm -f /tmp/test.dockerfile 2>/dev/null || true
    log_info "清理完成"
}

# 主函数
main() {
    echo "=================================="
    echo "  多架构Docker镜像构建脚本"
    echo "  支持架构: AMD64, ARM64"
    echo "  统一Dockerfile构建"
    echo "  强制手动登录模式"
    echo "=================================="
    echo ""

    # 设置错误时的清理
    trap cleanup EXIT

    check_requirements
    get_user_input
    check_dockerfile
    docker_login  # 强制重新登录
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
