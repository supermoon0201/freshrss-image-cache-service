# FreshRSS 图片缓存服务

这是一个单文件 Go 服务，用于为 FreshRSS 拉取并缓存上游图片或视频资源，减少重复请求并降低上游访问压力。

## 功能说明

- 提供 `GET /healthz` 健康检查接口
- 提供 `GET /piccache?url=...` 读取缓存或从上游抓取资源
- 提供 `POST /piccache` 主动预热缓存，需传入 `url` 和 `access_token`
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
| `UPSTREAM_CONCURRENCY` | `16` | 同时抓取上游资源的并发上限 |

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
