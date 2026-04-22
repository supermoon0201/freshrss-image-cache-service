# FreshRSS 图片缓存服务

这是一个单文件 Go 服务，用于为 FreshRSS 拉取并缓存上游图片或视频资源，减少重复请求并降低上游访问压力。

## 功能说明

- 提供 `GET /healthz` 健康检查接口
- 提供 `GET /piccache?url=...` 读取缓存或从上游抓取资源
- 提供 `HEAD /piccache?url=...` 只返回响应头，可用于探测缓存状态与资源元信息
- 提供 `POST /piccache` 主动预热缓存，需传入 `url` 和 `access_token`
- 支持按域名配置上游请求头，例如 `Referer`、`Origin`、`User-Agent`、`Accept-Language`
- 支持 `POST /piccache` 受控透传白名单请求头到上游，适配需要登录态或动态 `Referer` 的图片源
- 支持 `GET /piccache` 从当前请求继承非敏感白名单头到上游；若未提供 `Referer` / `Origin`，会从目标 URL 自动推导并在 403 时有限回退
- 内置 SSRF 防护，拒绝访问私网、回环、链路本地、多播和未指定地址
- 采用磁盘分片缓存，并支持 TTL 过期清理和最大容量淘汰

## 环境变量

| 变量名 | 默认值 | 说明 |
| --- | --- | --- |
| `LISTEN_ADDR` | `127.0.0.1:9090` | HTTP 监听地址 |
| `CACHE_DIR` | `./data/cache` | 缓存根目录 |
| `ACCESS_TOKEN` | `change-me` | 主动预热接口使用的访问令牌 |
| `FETCH_TIMEOUT` | `15s` | 上游抓取超时时间 |
| `MAX_BODY_BYTES` | `20971520` | 上游响应体最大字节数 |
| `CACHE_TTL` | `720h` | 缓存过期时间 |
| `JANITOR_INTERVAL` | `1h` | 清理任务执行间隔 |
| `MAX_CACHE_BYTES` | `10737418240` | 缓存允许的最大总大小 |
| `UPSTREAM_CONCURRENCY` | `64` | 同时抓取上游资源的并发上限 |
| `UPSTREAM_HEADER_RULES_JSON` | 空 | 按域名匹配的上游请求头规则，JSON 数组 |
| `CREDENTIAL_FORWARD_HOSTS` | 空 | 允许通过 `POST /piccache` 透传 `Cookie`/`Authorization` 的域名白名单，逗号分隔，支持 `*.example.com` |

## 上游请求头规则

`UPSTREAM_HEADER_RULES_JSON` 使用 JSON 数组配置，示例：

```json
[
  {
    "name": "example-cdn",
    "hosts": ["img.example.com", "*.img.example.com"],
    "referer": "https://www.example.com/post/123",
    "origin": "https://www.example.com",
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "accept_language": "zh-CN,zh;q=0.9,en;q=0.8",
    "headers": {
      "Cache-Control": "no-cache"
    }
  }
]
```

说明：

- 域名规则仅适合无敏感凭证的静态请求头，例如 `Referer`、`Origin`、`User-Agent`
- 规则会参与公共缓存分片；不同请求头组合会生成不同缓存键，避免互相污染
- 不建议在规则中放入 `Cookie` 或 `Authorization`

## POST 透传上游请求头

`POST /piccache` 现在支持可选的 `upstream_headers` 字段：

```json
{
  "url": "https://target.example.com/image.jpg",
  "access_token": "change-me",
  "upstream_headers": {
    "Referer": "https://target.example.com/post/123",
    "Cookie": "session=abc"
  }
}
```

限制如下：

- 只允许透传 `Referer`、`Origin`、`Cookie`、`Authorization`、`User-Agent`、`Accept-Language`
- `Cookie` 和 `Authorization` 仅允许发往 `CREDENTIAL_FORWARD_HOSTS` 白名单域名
- 带 `Cookie` 或 `Authorization` 的抓取结果不会进入公共缓存，避免用户态资源泄漏

## GET 继承请求头

`GET /piccache?url=...` 在缓存未命中时，会从当前请求自动继承以下非敏感请求头到上游：

- `Referer`
- `Origin`
- `User-Agent`
- `Accept-Language`

如果当前请求没有显式提供 `Referer` / `Origin`，服务会基于目标 URL 自动推导：

- 优先尝试注册域根站点，例如 `https://hellogithub.com/`
- 若上游返回 `403 Forbidden`，再回退尝试目标 host 根路径，例如 `https://img.hellogithub.com/`

这意味着像 `https://img.hellogithub.com/...` 这类基于主站 `Referer` 防盗链的图片源，即使调用方不手工补头，首次 `GET` 也有机会直接抓图并缓存。

## HEAD 探测

`HEAD /piccache?url=...` 与 `GET` 使用同一套缓存和回源逻辑：

- 命中缓存时直接返回响应头
- 未命中缓存时会回源抓取、写入缓存，再返回响应头
- 响应不包含 body

适合用于查看：

- `X-Piccache-Status`
- `Content-Type`
- `Content-Length`
- `ETag`
- `Last-Modified`

## 本地运行

```bash
go run .
```

## 运行测试

```bash
go test ./...
```

## Docker 构建与运行

```bash
docker build -t freshrss-image-cache-service .
docker run --rm -p 9090:9090 freshrss-image-cache-service
```

当前 Docker 构建阶段使用 Go 1.25，与仓库中的 `go.mod` 版本要求保持一致。

容器启动时会先尝试修复 `CACHE_DIR` 的目录权限，再以 `app` 用户启动主程序。这可以改善宿主机绑定挂载目录时的权限兼容性；如果宿主机启用了更严格的文件系统或安全策略，仍可能需要手动调整目录属主或改用 Docker 命名卷。

## GitHub 自动构建镜像

仓库已添加 GitHub Actions 工作流：

- [`docker.yml`](/c:/Dev/Code/go-workspace/freshrss-image-cache-service/.github/workflows/docker.yml)

工作流触发规则如下：

- 推送到 `main` 分支时自动构建并推送 `latest`
- 推送形如 `v1.0.0` 的标签时自动推送对应版本标签
- 同时会生成一个基于提交哈希的镜像标签，便于回滚

镜像会被推送到 GitHub Container Registry，地址格式如下：

```text
ghcr.io/<你的 GitHub 用户名或组织名>/freshrss-image-cache-service:<标签>
```

例如：

```text
ghcr.io/example-user/freshrss-image-cache-service:latest
ghcr.io/example-user/freshrss-image-cache-service:v1.0.0
```

### GitHub 侧准备

1. 将代码仓库推送到 GitHub。
2. 确认默认分支为 `main`，或者自行修改工作流中的分支名。
3. 确认仓库允许 GitHub Actions 使用 `GITHUB_TOKEN` 推送包到 `ghcr.io`。
4. 首次推送后，到 GitHub 的 Packages 页面确认镜像已经生成。

### VPS 拉取镜像

如果镜像是公开的，直接拉取：

```bash
docker pull ghcr.io/<你的 GitHub 用户名或组织名>/freshrss-image-cache-service:latest
```

如果镜像是私有的，需要先登录 GHCR：

```bash
echo <你的 GitHub Token> | docker login ghcr.io -u <你的 GitHub 用户名> --password-stdin
docker pull ghcr.io/<你的 GitHub 用户名或组织名>/freshrss-image-cache-service:latest
```

### VPS 运行示例

```bash
docker run -d \
  --name freshrss-image-cache-service \
  --restart unless-stopped \
  -p 9090:9090 \
  -e LISTEN_ADDR=0.0.0.0:9090 \
  -e CACHE_DIR=/data/cache \
  -e ACCESS_TOKEN=please-change-this \
  -v /opt/freshrss-image-cache-service/cache:/data/cache \
  ghcr.io/<你的 GitHub 用户名或组织名>/freshrss-image-cache-service:latest
```
