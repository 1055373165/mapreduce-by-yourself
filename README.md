# MapReduce

[![Go Version](https://img.shields.io/badge/Go-1.23.12-00ADD8?logo=go)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

一个简洁、高效的 MapReduce 分布式计算框架的 Go 语言实现，适用于学习和理解 MapReduce 核心原理。

## ✨ 特性

- **🚀 简洁架构** - 清晰的 Coordinator-Worker 模型，易于理解和扩展
- **⚡ 高性能** - 支持并发任务处理，充分利用多核 CPU
- **🔄 容错机制** - 自动检测并重试超时任务（10 秒超时）
- **💾 原子操作** - 使用临时文件 + 原子重命名确保数据一致性
- **📝 详细日志** - 完整的任务执行追踪和状态监控
- **🎯 即插即用** - 通过简单的 Map/Reduce 函数接口快速实现自定义应用

## 📋 目录

- [快速开始](#快速开始)
- [架构设计](#架构设计)
- [使用指南](#使用指南)
- [开发自定义应用](#开发自定义应用)
- [API 文档](#api-文档)
- [贡献指南](#贡献指南)

## 🚀 快速开始

### 前置要求

- Go 1.23.12 或更高版本
- Unix/Linux 或 macOS 操作系统（使用 Unix Socket 进行 RPC 通信）

### 安装

```bash
# 克隆仓库
git clone https://github.com/yourusername/mapreduce.git
cd mapreduce

# 构建可执行文件
go build -o mrcoordinator main/mrcoordinator.go
go build -o mrworker main/mrworker.go
```

### 运行 Word Count 示例

```bash
# 1. 清理旧的输出文件
rm -f mr-out-* mr-*-*

# 2. 启动 Coordinator（协调器）
go run main/mrcoordinator.go pg-*.txt &

# 3. 启动多个 Worker（工作节点）
go run main/mrworker.go mrapps/wc.go &
go run main/mrworker.go mrapps/wc.go &
go run main/mrworker.go mrapps/wc.go &

# 4. 等待任务完成，查看输出结果
cat mr-out-* | sort
```

或者使用提供的测试脚本：

```bash
./test.sh
```

## 🏗️ 架构设计

### 系统组件

```
┌─────────────────────────────────────────────────────────┐
│                      Coordinator                        │
│  - 任务调度与分配                                        │
│  - 状态管理（Idle → InProgress → Completed）           │
│  - 超时监控与重试                                        │
│  - 阶段转换（Map → Reduce → Done）                      │
└────────────┬────────────────────────────┬───────────────┘
             │ RPC (Unix Socket)          │
    ┌────────┴────────┐         ┌────────┴────────┐
    │    Worker 1     │         │    Worker N     │
    │  - 执行 Map     │   ...   │  - 执行 Map     │
    │  - 执行 Reduce  │         │  - 执行 Reduce  │
    └─────────────────┘         └─────────────────┘
```

### 执行流程

```
1. Map 阶段
   ┌──────────┐
   │ 输入文件  │
   └────┬─────┘
        │ Map Function
        ▼
   ┌──────────────┐
   │ 中间文件      │ (mr-X-Y: X=MapID, Y=ReduceID)
   │ 按 key hash  │
   │ 分区到 R 个   │
   └────┬─────────┘
        │
        ▼
2. Reduce 阶段
   ┌──────────────┐
   │ 读取分区数据  │
   │ 排序 & 分组   │
   └────┬─────────┘
        │ Reduce Function
        ▼
   ┌──────────────┐
   │ 最终输出      │ (mr-out-0, mr-out-1, ...)
   └──────────────┘
```

### 关键特性

#### 1️⃣ 容错机制
- **超时检测**：Worker 执行超过 10 秒自动标记为超时
- **任务重试**：超时任务自动重置为 Idle 状态，重新分配
- **状态验证**：Worker 完成报告时验证任务归属，忽略过期报告

#### 2️⃣ 原子操作
- **临时文件**：所有输出先写入临时文件
- **原子重命名**：使用 `os.Rename()` 确保文件写入的原子性
- **防止损坏**：避免 Worker 崩溃导致的部分写入问题

#### 3️⃣ 高效实现
- **缓冲 I/O**：使用 `bufio.Writer` 减少系统调用
- **预分配**：根据预估容量预分配切片，减少内存重分配
- **并行处理**：多 Worker 并发执行，充分利用多核 CPU

## 📖 使用指南

### 项目结构

```
mapreduce/
├── main/
│   ├── mrcoordinator.go    # Coordinator 启动程序
│   └── mrworker.go          # Worker 启动程序（支持多应用）
├── mr/
│   ├── coordinator.go       # Coordinator 核心逻辑
│   ├── worker.go            # Worker 核心逻辑
│   └── rpc.go               # RPC 数据结构定义
├── mrapps/
│   ├── wc.go                # Word Count 应用示例
│   └── urlcount/
│       └── urlcount.go      # URL 访问统计应用示例
├── pg-*.txt                 # 示例输入文件（文本数据）
├── access-log-*.txt         # 示例 Web 日志文件
├── test.sh                  # Word Count 测试脚本
└── test-urlcount.sh         # URL Count 测试脚本
```

### 运行参数

#### Coordinator

```bash
go run main/mrcoordinator.go <input-files>...

# 示例：处理所有 pg-*.txt 文件
go run main/mrcoordinator.go pg-11.txt pg-1342.txt pg-1661.txt
```

#### Worker

```bash
./mrworker <app-name>

# 可用的应用：
# - wc / wc.so        : Word Count（词频统计）
# - urlcount / urlcount.so : URL 访问统计

# 示例：运行 Word Count 应用
./mrworker wc

# 示例：运行 URL 访问统计应用
./mrworker urlcount
```

### 输出文件

- **中间文件**：`mr-X-Y`（X = Map 任务 ID，Y = Reduce 分区 ID）
- **最终输出**：`mr-out-0`, `mr-out-1`, ..., `mr-out-R`

## 💻 开发自定义应用

### 实现 Map 和 Reduce 函数

在 `mrapps/` 目录下创建新的应用文件：

```go
package mrapps

import "mapreduce/mr"

// Map 函数：处理输入数据，输出 key-value 对
func Map(filename string, contents string) []mr.KeyValue {
    // 你的 Map 逻辑
    var kva []mr.KeyValue
    // ...
    return kva
}

// Reduce 函数：合并相同 key 的 values
func Reduce(key string, values []string) string {
    // 你的 Reduce 逻辑
    result := ""
    // ...
    return result
}
```

### Word Count 示例解析

```go
// Map: 将文本分割成单词，每个单词输出 (word, "1")
func Map(filename string, contents string) []mr.KeyValue {
    words := strings.FieldsFunc(contents, func(r rune) bool {
        return !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'))
    })

    var kva []mr.KeyValue
    for _, w := range words {
        kva = append(kva, mr.KeyValue{Key: w, Value: "1"})
    }
    return kva
}

// Reduce: 统计每个单词的出现次数
func Reduce(key string, values []string) string {
    count := 0
    for _, v := range values {
        n, _ := strconv.Atoi(v)
        count += n
    }
    return strconv.Itoa(count)
}
```

### URL 访问统计示例

**应用场景**：分析 Web 服务器日志，统计每个 URL 的访问次数

#### 实现代码

```go
package urlcount

import (
    "mapreduce/mr"
    "strconv"
    "strings"
)

// Map: 从 Web 服务器日志中提取 URL
// 支持 Apache/Nginx Common/Combined Log Format
// 示例: 127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
func Map(filename string, contents string) []mr.KeyValue {
    lines := strings.Split(contents, "\n")
    var kva []mr.KeyValue

    for _, line := range lines {
        // 跳过空行
        if len(strings.TrimSpace(line)) == 0 {
            continue
        }

        // 提取 URL
        url := extractURL(line)
        if url != "" {
            kva = append(kva, mr.KeyValue{Key: url, Value: "1"})
        }
    }

    return kva
}

// extractURL 从日志行中提取 URL
func extractURL(line string) string {
    // 查找 HTTP 请求部分（在引号之间）
    start := strings.Index(line, "\"")
    if start == -1 {
        return ""
    }

    end := strings.Index(line[start+1:], "\"")
    if end == -1 {
        return ""
    }

    // 提取请求: "GET /path HTTP/1.1"
    request := line[start+1 : start+1+end]

    // 按空格分割: [GET, /path, HTTP/1.1]
    parts := strings.Fields(request)
    if len(parts) < 2 {
        return ""
    }

    // 返回 URL（第二部分）
    return parts[1]
}

// Reduce: 统计每个 URL 的访问次数
func Reduce(key string, values []string) string {
    count := 0
    for _, v := range values {
        n, _ := strconv.Atoi(v)
        count += n
    }
    return strconv.Itoa(count)
}
```

#### 运行示例

```bash
# 1. 准备示例日志文件（access-log-*.txt）
# 2. 启动 Coordinator
./mrcoordinator access-log-*.txt &

# 3. 启动多个 Worker（使用 urlcount 应用）
./mrworker urlcount &
./mrworker urlcount &
./mrworker urlcount &

# 4. 等待任务完成，查看结果
cat mr-out-* | sort -t$'\t' -k2 -nr
```

#### 示例输出

```
/index.html     14
/products.html  8
/about.html     7
/api/login      6
/services.html  5
/contact.html   3
/api/register   2
```

### 更多应用场景

- **倒排索引**：构建文档搜索引擎
- **分布式排序**：对大规模数据集排序
- **图计算**：PageRank、社交网络分析

## 📚 API 文档

### RPC 接口

#### GetTask - 获取任务

**请求**
```go
type GetTaskArgs struct {
    WorkerID string  // Worker 唯一标识
}
```

**响应**
```go
type GetTaskReply struct {
    Task Task
}

type Task struct {
    TaskType  TaskType  // MapTask | ReduceTask | WaitTask | ExitTask
    TaskID    int       // 任务 ID
    InputFile string    // Map 任务的输入文件
    NReduce   int       // Reduce 任务数量
    NMap      int       // Map 任务数量
    ReduceID  int       // Reduce 任务 ID
}
```

#### TaskComplete - 报告任务完成

**请求**
```go
type TaskCompleteArgs struct {
    TaskType TaskType
    TaskID   int
    WorkerID string
}
```

**响应**
```go
type TaskCompleteReply struct {
    Success bool  // 是否成功接受完成报告
}
```

### 配置参数

在 `mr/coordinator.go` 中可调整：

```go
const (
    TaskTimeout     = 10 * time.Second  // 任务超时时间
    MonitorInterval = 1 * time.Second   // 监控检查间隔
)
```

## 🧪 测试

```bash
# 运行完整测试
./test.sh

# 手动测试
# 1. 启动 Coordinator
go run main/mrcoordinator.go pg-*.txt &

# 2. 启动多个 Worker（建议 3-5 个）
for i in {1..3}; do
    go run main/mrworker.go mrapps/wc.go &
done

# 3. 等待完成并验证输出
wait
cat mr-out-* | sort | head -20
```

## 🤝 贡献指南

欢迎贡献！请遵循以下步骤：

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

### 开发规范

- 遵循 Go 代码规范（`gofmt`, `golint`）
- 添加必要的注释和文档
- 编写单元测试
- 保持向后兼容性

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

## 🙏 致谢

- 本项目受 [MIT 6.824: Distributed Systems](https://pdos.csail.mit.edu/6.824/) 课程启发
- 参考了 Google MapReduce 论文的设计思想
- 感谢所有贡献者的支持

## 📬 联系方式

- 提交 Issue：[GitHub Issues](https://github.com/yourusername/mapreduce/issues)
- 邮箱：your.email@example.com

---

**⭐ 如果这个项目对你有帮助，请给一个 Star！**
