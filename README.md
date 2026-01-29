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
- CN

## Feature

- 获取多个平台的7天EPG，每天更新一次。
- 每天生成的epg以xml存在本地。
- 如需持久化epg文件，请挂载/code/epg_files目录。

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
  charmingcheung000/charming-epg:latest
```

### Request

#### 请求所有平台

```
http://[ip]:[port]/all  #xml
http://[ip]:[port]/all.xml.gz #gzip压缩包
```

#### 请求单个或多个平台

```
http://[ip]:[port]/epg?platforms=tvb,nowtv,rthk,hoy,hami,astro,starhub,mewatch,cn
```
