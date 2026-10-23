# 安全检查记录

本文维护 Docker 部署环境的组件版本、安全检查结果、修复措施和待跟进项。每次检查按日期新增记录，注明验证平台和范围。

运行环境以 Dockerfile 为准：Python 3.12.15 / Debian trixie（`python:3.12.15-slim-trixie`）。构建时安装系统安全更新，检查依赖后移除运行时不需要的 pip，避免携带安装器内置的旧依赖。依赖更新后需重新构建镜像，已有容器不会自动更新内部 Python 包。

安全更新部署：`docker compose build --pull --no-cache && docker compose up -d`。`build_and_push.sh` 同样强制拉取基础镜像并禁用构建缓存；Dependabot 每周检查 Docker 基础镜像版本。

## 2026-10-23 检查记录

实际构建验证平台为 Linux arm64：Python 3.12.15、OpenSSL 3.5.7、Expat 2.8.5、FastAPI 0.156.8、Starlette 1.9.0、Uvicorn 0.54.0、aiohttp 3.14.5、requests 2.34.2、urllib3 2.8.0、curl_cffi 0.16.3。最终镜像的 Trivy 扫描未发现 Python 包漏洞；系统包有 44 条高危包级告警，去重为 8 个 CVE，无严重级别告警，无 Debian 已提供补丁的高危项。另行核对的内置 libcurl 风险和缓解见下文。扫描结果仅代表检查当日新构建镜像，不代表线上已有容器；发布脚本的 amd64 构建仍应执行同样扫描。

- Python 官方 [3.12.15 安全公告](https://blog.python.org/2026/10/python-31022-31117/)包含 TLS、XML 库、压缩包处理等修复；浮动标签无法证明已部署容器的补丁版本，本项目明确最低构建版本并在 CI 输出真实运行时清单。
- OpenSSL 实际 Debian 包已升级到 `3.5.7-1~deb13u3`，包含 [CVE-2026-84782 的回移补丁](https://security-tracker.debian.org/tracker/CVE-2026-84782)。不能只根据 `ssl.OPENSSL_VERSION` 显示的上游 3.5.7 判断是否有漏洞。
- 应用依赖在 Python 3.12 Docker 镜像内用 pip-audit 审计。FastAPI / Starlette / aiohttp / requests 等当前约束解析的版本已包含已知安全修复，无需为安全目的跨主版本升级。
- `curl_cffi 0.16.3` 内置 libcurl 8.21.0，普通 pip-audit 不会覆盖其原生代码。针对 [CVE-2026-92392](https://curl.se/docs/CVE-2026-92392.html)（HTTP/2 上传 use-after-free），4GTV / Telus 暂时强制 HTTP/1.1，采用官方缓解措施；当前 GET 调用不满足公告中的上传条件。这是缓解，后续仍需升级到内置已修复 libcurl 的稳定版。4GTV 同时恢复 HTTPS 证书验证。
- Debian 镜像仍有尚无稳定版补丁的高危扫描告警，涉及 util-linux、acl、ncurses、systemd、Perl。Debian 将这些项列为 `no-dsa` 或延后处理，例如 [util-linux](https://security-tracker.debian.org/tracker/CVE-2026-76642)、[acl](https://security-tracker.debian.org/tracker/CVE-2026-54369)、[ncurses](https://security-tracker.debian.org/tracker/CVE-2025-69720)、[systemd](https://security-tracker.debian.org/tracker/CVE-2026-16742)、[Perl](https://security-tracker.debian.org/tracker/CVE-2026-9538)。它们多要求本地工具、挂载权限或本服务未使用的组件，不能把包级告警直接视为 HTTP 接口可利用漏洞，也不能视为已修复。
- 每周 CI 直接构建 Dockerfile，在容器中审计实际安装的包并运行测试，再用 Trivy 扫描整个镜像。展示全部高危/严重告警，有修复版本的高危/严重项使检查失败；无补丁项继续展示和跟踪。pip-audit 和 Trivy 的结果应结合原生库公告判断，不能等同于“无任何漏洞”。

## 本地辅助验证

上线环境的结论以 Docker 镜像内的检查为准。

```bash
python -m pip install -r requirements.txt 'pip-audit==2.10.1' 'httpx~=0.28.1'
python -m pip check
python -m pip_audit -r requirements.txt
python -m unittest discover -s tests -v
```
