# K8s Pod WebTerminal

基于 K8s 原生 `kubectl exec` 协议的生产级 Pod 远程终端（WebSSH）方案。前端 xterm.js + 后端 Go WebSocket 网关 + K8s apiserver SPDY exec，不依赖容器内 SSHd。

## 架构

```
浏览器 (xterm.js)  ←→  Go 网关 (WebSocket)  ←→  K8s API Server (SPDY exec)  ←→  kubelet  ←→  Pod (TTY)
```

## 快速开始

### 本地运行

```bash
# 前置：本地有 kubeconfig
git clone <repo> && cd kube-webterminal
go mod tidy
go build -o webterminal-gateway .
./webterminal-gateway --port=8080 --kubeconfig=$HOME/.kube/config
```

打开 `http://localhost:8080`，填入 Namespace 和 Pod 名称，点击连接终端。

### 集群内部署

```bash
# 1. 创建 ServiceAccount 和 RBAC
kubectl apply -f rbac.yaml

# 2. 构建镜像
docker build -t your-registry/webterminal-gateway:v1.0 .

# 3. 部署
kubectl apply -f deploy.yaml

# 4. 访问 http://<node-ip>:30080
```

## 启动参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--port` | `8080` | HTTP 监听端口 |
| `--kubeconfig` | `""` | kubeconfig 路径（集群内留空） |
| `--tls-cert` | `""` | TLS 证书（启用 WSS） |
| `--tls-key` | `""` | TLS 私钥 |
| `--static-dir` | `./static` | 静态文件目录 |
| `--default-user` | `""` | 默认 Impersonate 用户 |
| `--debug` | `false` | 开启调试端点 |

## 功能

### 终端操作
- 指定 Namespace / Pod / Container 进入终端
- 默认 `/bin/bash`，自动 fallback 到 `/bin/sh`
- 多标签页，每标签独立 WebSocket 连接
- 单 Pod 最多 3 个并发终端，不同 Pod 互不影响
- TTY resize 实时同步
- 15 分钟空闲超时自动断开
- 断线自动重连（3 次，指数退避）

### 标签右键菜单
- **重命名**：双击标签名或右键选重命名
- **复制终端**：同 Pod 配置再开一个终端
- **锁定/解锁**：锁定后不可关闭
- **全屏**：终端全屏，Esc 退出

### 快捷键
- `Ctrl + 滚轮`：调整终端字体大小（10~32px）
- 拖拽文件到终端窗口：上传

### 文件管理（弹窗）
- 点击 📂 文件管理 按钮打开
- 默认显示容器根目录 `/`
- 路径栏输入路径回车跳转
- ⬆ 上级目录 / 🔄 刷新 / 📤 上传（文件/文件夹）
- 点击文件夹：进入子目录
- 点击文件：下载到浏览器默认目录
- 表头点击：按名称/大小/时间排序
- 上传大小限制 5GB

### 安全
- 后端统一鉴权，前端不暴露 kubeconfig
- 支持 ServiceAccount + Impersonate 用户模拟
- 终端连接零 stdin 注入，无闪烁
- Pod 不存在时立即提示，不卡等待

### 容器依赖说明
- **终端连接**：仅需 `/bin/bash` 或 `/bin/sh`，无其他依赖
- **文件上传**：需容器内有 `tar`（GNU/busybox 均可；与 `kubectl cp` 要求一致）
- **文件下载**：需容器内有 `cat`
- **文件列表**：需容器内有 `ls`
- 以上是 K8s exec 协议本身限制，`kubectl cp` 也同样需要 `tar`。distroless 镜像需添加调试容器或改用挂载卷方式传输文件

## 项目结构

```
├── main.go              # 入口，路由注册
├── handlers/
│   ├── terminal.go      # WebSocket 终端处理器
│   ├── auth.go          # 认证 + 限流
│   └── upload.go        # 文件上传/下载/列表
├── k8s/
│   └── exec.go          # K8s exec (SPDY) + tar 管道
├── session/
│   └── manager.go       # 会话管理（Pod 级别 3 上限，超时回收）
├── static/
│   ├── index.html       # 前端主页面
│   └── xterm/           # xterm.js 本地文件
├── rbac.yaml            # K8s RBAC
├── deploy.yaml          # K8s Deployment + Service
└── Dockerfile           # 容器镜像构建
```

## RBAC 最小权限

```yaml
rules:
  - apiGroups: [""]
    resources: ["pods/exec"]
    verbs: ["create", "get"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
```

## 常见问题

**Q: 终端连接后无法输入？**  
A: 确认 Pod 名称正确（先填 busybox 测试）。后端地址应自动填充为当前页面地址。

**Q: 文件管理显示解析错误？**  
A: 容器需有 `ls` 命令。distroless 镜像无法列出文件。

**Q: 上传大文件失败？**  
A: 检查容器磁盘空间。上传依赖 `tar` 命令（GNU/busybox 均可）。

**Q: 如何启用 HTTPS？**  
A: 启动时加 `--tls-cert server.crt --tls-key server.key`，前端地址自动切换为 `wss://`。

## License

MIT
