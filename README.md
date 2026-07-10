# CharmingEPG

## Support

- MyTV Super
- NowTV
- RTHK
- HOY
- Hami
- Astro Go（中文台都是中文描述）
- StarHub（中文台都是中文描述）
- Mewatch
- Singtel (有IP限制，如需代理请设置SINGTEL_PROXY)
- UnifiTV
- FengShows（凤凰秀：资讯台/中文台/香港台）感谢`THX1166`贡献的代码
- 4GTV（频道较多且站点有Cloudflare防护，每次更新较慢）感谢`THX1166`贡献的代码
- CatchPlay（有台湾IP限制，如需代理请设置CATCHPLAY_PROXY）
- CN

## Feature

- 获取多个平台的7天EPG，每天更新一次。
- 每天生成的epg以xml存在本地。
- 如需持久化epg文件，请挂载/code/epg_files目录。
- `all`合并缓存随各平台抓取完成增量刷新（不必等待最慢的平台如4gtv），且更新期间接口会回退返回上一次可用的数据，避免凌晨更新窗口出现404。
- 提供`/status`接口查看各平台数据更新到哪一天、频道/节目数量与是否成功。

## How to use

### 环境变量

```dotenv
#配置需要启用的平台
EPG_ENABLE_CN=true
EPG_ENABLE_TVB=true
EPG_ENABLE_NOWTV=false
EPG_ENABLE_HAMI=true
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
http://[ip]:[port]/epg/astro
http://[ip]:[port]/epg/starhub
http://[ip]:[port]/epg/mewatch
http://[ip]:[port]/epg/singtel
http://[ip]:[port]/epg/unifitv
http://[ip]:[port]/epg/fengshows
http://[ip]:[port]/epg/4gtv
http://[ip]:[port]/epg/catchplay
http://[ip]:[port]/epg/cn
http://[ip]:[port]/epg?platforms=tvb,nowtv,rthk,hoy,hami,astro,starhub,mewatch,singtel,unifitv,fengshows,4gtv,catchplay,cn
```
