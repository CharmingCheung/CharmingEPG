# CharmingEPG

## Support

- myTV SUPER
- NowTV
- RTHK
- HOY
- Hami
- ELTA（因转播权差异，ELTA 与 HAMI 的愛爾達频道在同一时段播出的节目可能不一致）
- Astro Go（中文台都是中文描述）
- StarHub（中文台都是中文描述）
- Mewatch
- Singtel (有IP限制，如需代理请设置SINGTEL_PROXY)
- UnifiTV
- FengShows（凤凰秀：资讯台/中文台/香港台）感谢`THX1166`贡献的代码
- 4GTV（频道较多且站点有Cloudflare防护，每次更新较慢）感谢`THX1166`贡献的代码
- CatchPlay（有台湾IP限制，如需代理请设置CATCHPLAY_PROXY）
- SkyGo NZ
- Telus TV+ （考虑数据量太大，只获取4天）
- beIN SPORTS（多个区域抓取；频道名带区域前缀，例如 `[US] beIN SPORTS 3`）
- Sling（非体育 3 天 EPG）
- Sling Sports（体育 7 天 EPG）
- CN

## Feature

- 获取多个平台的7天EPG，每天更新一次。
- 每天生成的epg以xml存在本地。
- 如需持久化epg文件，请挂载/code/epg_files目录。
- `all`合并缓存随各平台抓取完成增量刷新（不必等待最慢的平台如4gtv），且更新期间接口会回退返回上一次可用的数据，避免凌晨更新窗口出现404。
- 提供`/status`接口查看各平台数据更新到哪一天、频道/节目数量与是否成功。

## How to use

运行环境以 Dockerfile 为准：Python 3.12.15 / Debian trixie（`python:3.12.15-slim-trixie`）。构建时安装系统安全更新，检查依赖后移除运行时不需要的 pip，避免携带安装器内置的旧依赖。依赖更新后需重新构建镜像，已有容器不会自动更新内部 Python 包。

安全更新部署：`docker compose build --pull --no-cache && docker compose up -d`。`build_and_push.sh` 同样强制拉取基础镜像并禁用构建缓存；Dependabot 每周检查 Docker 基础镜像版本。

2026-10-23 安全检查：

实际构建验证平台为 Linux arm64：Python 3.12.15、OpenSSL 3.5.7、Expat 2.8.5、FastAPI 0.156.8、Starlette 1.9.0、Uvicorn 0.54.0、aiohttp 3.14.5、requests 2.34.2、urllib3 2.8.0、curl_cffi 0.16.3。最终镜像的 Trivy 扫描未发现 Python 包漏洞；系统包有 44 条高危包级告警，去重为 8 个 CVE，无严重级别告警，无 Debian 已提供补丁的高危项。另行核对的内置 libcurl 风险和缓解见下文。扫描结果仅代表检查当日新构建镜像，不代表线上已有容器；发布脚本的 amd64 构建仍应执行同样扫描。

- Python 官方 [3.12.15 安全公告](https://blog.python.org/2026/10/python-31022-31117/)包含 TLS、XML 库、压缩包处理等修复；浮动标签无法证明已部署容器的补丁版本，本项目明确最低构建版本并在 CI 输出真实运行时清单。
- OpenSSL 实际 Debian 包已升级到 `3.5.7-1~deb13u3`，包含 [CVE-2026-84782 的回移补丁](https://security-tracker.debian.org/tracker/CVE-2026-84782)。不能只根据 `ssl.OPENSSL_VERSION` 显示的上游 3.5.7 判断是否有漏洞。
- 应用依赖在 Python 3.12 Docker 镜像内用 pip-audit 审计。FastAPI / Starlette / aiohttp / requests 等当前约束解析的版本已包含已知安全修复，无需为安全目的跨主版本升级。
- `curl_cffi 0.16.3` 内置 libcurl 8.21.0，普通 pip-audit 不会覆盖其原生代码。针对 [CVE-2026-92392](https://curl.se/docs/CVE-2026-92392.html)（HTTP/2 上传 use-after-free），4GTV / Telus 暂时强制 HTTP/1.1，采用官方缓解措施；当前 GET 调用不满足公告中的上传条件。这是缓解，后续仍需升级到内置已修复 libcurl 的稳定版。4GTV 同时恢复 HTTPS 证书验证。
- Debian 镜像仍有尚无稳定版补丁的高危扫描告警，涉及 util-linux、acl、ncurses、systemd、Perl。Debian 将这些项列为 `no-dsa` 或延后处理，例如 [util-linux](https://security-tracker.debian.org/tracker/CVE-2026-76642)、[acl](https://security-tracker.debian.org/tracker/CVE-2026-54369)、[ncurses](https://security-tracker.debian.org/tracker/CVE-2025-69720)、[systemd](https://security-tracker.debian.org/tracker/CVE-2026-16742)、[Perl](https://security-tracker.debian.org/tracker/CVE-2026-9538)。它们多要求本地工具、挂载权限或本服务未使用的组件，不能把包级告警直接视为 HTTP 接口可利用漏洞，也不能视为已修复。
- 每周 CI 直接构建 Dockerfile，在容器中审计实际安装的包并运行测试，再用 Trivy 扫描整个镜像。展示全部高危/严重告警，有修复版本的高危/严重项使检查失败；无补丁项继续展示和跟踪。pip-audit 和 Trivy 的结果应结合原生库公告判断，不能等同于“无任何漏洞”。

本地验证：

```bash
python -m pip install -r requirements.txt 'pip-audit==2.10.1' 'httpx~=0.28.1'
python -m pip check
python -m pip_audit -r requirements.txt
python -m unittest discover -s tests -v
```

### 环境变量

```dotenv
#配置需要启用的平台
EPG_ENABLE_CN=true
EPG_ENABLE_TVB=true
EPG_ENABLE_NOWTV=false
EPG_ENABLE_HAMI=true
EPG_ENABLE_ELTA=true
EPG_ENABLE_ASTRO=false
EPG_ENABLE_RTHK=false
EPG_ENABLE_HOY=false
EPG_ENABLE_STARHUB=false
EPG_ENABLE_MEWATCH=false
EPG_ENABLE_SINGTEL=false
EPG_ENABLE_UNIFITV=false
EPG_ENABLE_FENGSHOWS=false
EPG_ENABLE_4GTV=false
EPG_ENABLE_CATCHPLAY=false
EPG_ENABLE_SKYGONZ=false
EPG_ENABLE_TELUS=false
EPG_ENABLE_BEIN=false
EPG_ENABLE_SLING=false
EPG_ENABLE_SLING_SPORTS=false
#支持`1`/`0` `yes`/`no` `true`/`false` `on`/`off`
#这些配置已经在`docker-compose.example.yml`中列好，自行配置即可。

###以下为可选项###
#日志
LOG_LEVEL=INFO
LOG_ROTATION=10 MB
LOG_RETENTION=7 days

#EPG
EPG_CACHE_TTL=3600 #EPG返回header的缓存ttl，方便配合CF做缓存
EPG_UPDATE_INTERVAL=10 #每10分钟检查一次是否要更新（如果当天已更新会忽略）

#HTTP
HTTP_TIMEOUT=30 #默认30秒超时
HTTP_MAX_RETRIES=3 #默认3次重试

#Proxy
PROXY_HTTP=http://proxy.example.com:8080
PROXY_HTTPS=http://proxy.example.com:8080

#Singtel专属代理（Singtel接口有IP地区限制，需要新加坡IP）
#支持 http/https/socks5/socks5h，例如：
#SINGTEL_PROXY=socks5://user:pass@host:1080
#SINGTEL_PROXY=http://host:8080
SINGTEL_PROXY=

#CatchPlay专属代理（CatchPlay接口有IP地区限制，需要台湾IP）
#支持 http/https/socks5/socks5h，例如：
#CATCHPLAY_PROXY=socks5://user:pass@host:1080
#CATCHPLAY_PROXY=http://host:8080
CATCHPLAY_PROXY=
```



### Docker Compose
docker-compose.yml示例
```yaml
version: '3.3'
services:
  charming_epg:
    image: charmingcheung000/charming-epg:latest
    container_name: charming_epg
    environment:
      - EPG_ENABLE_CN=true
      - EPG_ENABLE_TVB=true
      - EPG_ENABLE_NOWTV=true
      - EPG_ENABLE_HAMI=true
      - EPG_ENABLE_ASTRO=true
      - EPG_ENABLE_RTHK=true
      - EPG_ENABLE_HOY=true
      - EPG_ENABLE_STARHUB=true
      - EPG_ENABLE_MEWATCH=true
      - EPG_ENABLE_SINGTEL=true
      - EPG_ENABLE_UNIFITV=true
      - EPG_ENABLE_FENGSHOWS=true
      - EPG_ENABLE_4GTV=true
      - EPG_ENABLE_CATCHPLAY=true
      - EPG_ENABLE_SKYGONZ=true
      - EPG_ENABLE_TELUS=true
      - EPG_ENABLE_BEIN=true
      - EPG_ENABLE_SLING=false
      - EPG_ENABLE_SLING_SPORTS=false
      - SINGTEL_PROXY=socks5://user:pass@host:1080
      - CATCHPLAY_PROXY=socks5://user:pass@host:1080
      - TZ=Asia/Shanghai
      - EPG_CACHE_TTL=3600
    volumes:
      - /root/docker/epg_data/epg_files:/code/epg_files
    ports:
      - "30008:80"
    restart: always
```


### Docker Cli

```bash
# 自行配置平台开关
docker run -d \
  -p 30008:80 \
  --name charming_epg \
  -e EPG_ENABLE_CN=true \
  -e EPG_ENABLE_TVB=true \
  -e EPG_ENABLE_NOWTV=false \
  -e EPG_ENABLE_HAMI=true \
  -e EPG_ENABLE_ASTRO=false \
  -e EPG_ENABLE_RTHK=false \
  -e EPG_ENABLE_HOY=false \
  -e EPG_ENABLE_MEWATCH=false \
  -e EPG_ENABLE_STARHUB=false \
  -e EPG_ENABLE_SINGTEL=false \
  -e EPG_ENABLE_UNIFITV=false \
  -e EPG_ENABLE_FENGSHOWS=false \
  -e EPG_ENABLE_4GTV=false \
  -e EPG_ENABLE_CATCHPLAY=false \
  -e EPG_ENABLE_SKYGONZ=false \
  -e EPG_ENABLE_TELUS=false \
  -e EPG_ENABLE_BEIN=false \
  -e EPG_ENABLE_SLING=false \
  -e EPG_ENABLE_SLING_SPORTS=false \
  -e SINGTEL_PROXY=socks5://user:pass@host:1080 \
  -e CATCHPLAY_PROXY=socks5://user:pass@host:1080 \
  charmingcheung000/charming-epg:latest
```

### Request

#### 请求所有平台

```
http://[ip]:[port]/all  #xml
http://[ip]:[port]/all.xml.gz #gzip压缩包
```

> 更新期间若当天文件尚未生成，接口会回退返回最近一次可用数据；响应头 `X-EPG-Date` 标识数据日期，`X-EPG-Stale: true` 表示当前返回的是上一天的数据。

#### 查看各平台更新状态

```
http://[ip]:[port]/status
```

返回每个平台数据更新到哪一天（`updated_to`）、频道数（`channels`）、节目数（`programs`）以及状态（`ok`=当天最新 / `stale`=仍是旧数据 / `missing`=暂无数据 / `invalid`=文件损坏），方便判断各平台是否抓取成功。

#### 请求单个或多个平台

```
http://[ip]:[port]/epg/tvb
http://[ip]:[port]/epg/nowtv
http://[ip]:[port]/epg/rthk
http://[ip]:[port]/epg/hoy
http://[ip]:[port]/epg/hami
http://[ip]:[port]/epg/elta
http://[ip]:[port]/epg/astro
http://[ip]:[port]/epg/starhub
http://[ip]:[port]/epg/mewatch
http://[ip]:[port]/epg/singtel
http://[ip]:[port]/epg/unifitv
http://[ip]:[port]/epg/fengshows
http://[ip]:[port]/epg/4gtv
http://[ip]:[port]/epg/catchplay
http://[ip]:[port]/epg/skygonz
http://[ip]:[port]/epg/telus
http://[ip]:[port]/epg/bein
http://[ip]:[port]/epg/sling
http://[ip]:[port]/epg/sling_sports
http://[ip]:[port]/epg/cn
http://[ip]:[port]/epg?platforms=tvb,nowtv,rthk,hoy,hami,elta,astro,starhub,mewatch,singtel,unifitv,fengshows,4gtv,catchplay,skygonz,telus,bein,sling,sling_sports,cn
```
