# ShardKV1 系统架构可视化

本文档展示当前实验代码中的分层架构、核心数据流和一次 `Get/Put` 请求的执行路径。

## 0. 集中与分布式主图（推荐先看）

```mermaid
flowchart LR
    APP[应用/测试]
    CK[shardkv1 Clerk\n上层客户端]

    subgraph CP[集中控制面 Centralized Control Plane]
      C1[ShardCtrler\n统一配置入口]
      C2[ShardConfig\nshard->gid, gid->servers]
      C1 --> C2
    end

    subgraph DP[分布式数据面 Distributed Data Plane]
      subgraph G1[Shard Group gid=1]
        S11[Server 1]
        S12[Server 2]
        S13[Server 3]
        R1[(Raft/RSM)]
        S11 --> R1
        S12 --> R1
        S13 --> R1
      end

      subgraph G2[Shard Group gid=2]
        S21[Server 1]
        S22[Server 2]
        S23[Server 3]
        R2[(Raft/RSM)]
        S21 --> R2
        S22 --> R2
        S23 --> R2
      end

      GN[... 更多 gid 组 ...]
    end

    APP --> CK
    CK -- Query配置 --> C1
    CK -- 按配置路由请求 --> G1
    CK -- 按配置路由请求 --> G2
    C2 -. Join/Leave/Rebalance 更新路由 .-> DP
```

一句话理解：

- 集中的是“配置决策”（由 ShardCtrler 统一给出路由）。
- 分布式的是“数据存储与复制”（由多个 shard group 并行承载）。

## 1. 模块图（方框与箭头）

```mermaid
flowchart LR
  APP[应用/测试]
  SKV[模块: shardkv1/client.go\n职责: 路由与重试]
  SCT[模块: shardctrler\n职责: 配置管理]
  SCF[模块: shardcfg\n职责: 配置数据结构与重平衡]

  SGC[模块: shardgrp/client.go\n职责: 组内RPC客户端]
  SGS[模块: shardgrp/server.go\n职责: KV服务与分片迁移]
  RFT[模块: raft1 + rsm\n职责: 一致性复制]

  APP --> SKV
  SKV -- Query配置 --> SCT
  SCT --> SCF

  SKV -- 读取路由(shard->gid->servers) --> SCF
  SKV -- 按gid创建/复用 --> SGC
  SGC -- Get/Put/迁移RPC --> SGS
  SGS --> RFT
```

补充说明：

- `shardkv1` 是上层入口模块，负责“找对组再发请求”。
- `shardctrler + shardcfg` 是控制平面，维护配置与重平衡。
- `shardgrp + raft/rsm` 是数据平面，负责实际数据读写与复制。

## 2. 组件总览

```mermaid
flowchart LR
    U[业务调用方 / 测试代码]
    CK[shardkv1 Clerk\n上层路由客户端]
    SC[ShardCtrler\n配置控制器]
    CFG[ShardConfig\nNum + Shards + Groups]

    subgraph G1[Shard Group gid=1]
      C1[shardgrp Clerk]
      S1[Server 1]
      S2[Server 2]
      S3[Server 3]
      R1[(Raft 复制状态机)]
    end

    subgraph G2[Shard Group gid=2]
      C2[shardgrp Clerk]
      T1[Server 1]
      T2[Server 2]
      T3[Server 3]
      R2[(Raft 复制状态机)]
    end

    U --> CK
    CK -- Query() --> SC
    SC --> CFG

    CK -- 根据 key 计算 shard 并查 gid --> CFG
    CK -- 按 gid 选择/创建组客户端 --> C1
    CK -- 按 gid 选择/创建组客户端 --> C2

    C1 --> S1
    C1 --> S2
    C1 --> S3
    S1 --> R1
    S2 --> R1
    S3 --> R1

    C2 --> T1
    C2 --> T2
    C2 --> T3
    T1 --> R2
    T2 --> R2
    T3 --> R2
```

## 3. 路由与数据分层

```mermaid
graph TD
    K[key]
    SH[Key2Shard(key)]
    SHID[shard id]
    MAP1[ShardConfig.Shards\nshard -> gid]
    GID[gid]
    MAP2[ShardConfig.Groups\ngid -> servers[]]
    SRVS[servers[]]
    RCK[shardgrp Clerk]
    KV[目标 shardgrp Raft 组]

    K --> SH --> SHID --> MAP1 --> GID --> MAP2 --> SRVS --> RCK --> KV
```

说明：

- 上层 `shardkv1/client.go` 负责路由（找对组）。
- 下层 `shardgrp/client.go` + `shardgrp/server.go` 负责实际读写与复制。
- `shardctrler` 负责配置变更（Join/Leave/Rebalance）并对外提供 Query。

## 4. Get/Put 时序（含重配置）

```mermaid
sequenceDiagram
    participant App as 调用方
    participant CK as shardkv1 Clerk
    participant Ctl as ShardCtrler
    participant GC as shardgrp Clerk
    participant GS as shardgrp Server(当前组)

    App->>CK: Get(key) / Put(key, value, ver)
    CK->>Ctl: Query()
    Ctl-->>CK: 最新配置 cfg
    CK->>CK: shard=Key2Shard(key), gid=cfg.Shards[shard]
    CK->>GC: 选择或创建 gid 对应的 rck
    GC->>GS: 发送 Get/Put

    alt 组正确且已接管该 shard
      GS-->>GC: value/version/OK
      GC-->>CK: 成功
      CK-->>App: 返回结果
    else ErrWrongGroup
      GS-->>GC: ErrWrongGroup
      GC-->>CK: ErrWrongGroup
      CK->>CK: 删除该 gid 的缓存 rck
      CK->>CK: Sleep 后重试
    end
```

## 5. 关键设计点

- 控制平面与数据平面分离：
  - 控制平面：`shardctrler` + `ShardConfig`。
  - 数据平面：各个 `shardgrp` 的 Raft 组。
- 客户端分层：
  - `shardkv1 Clerk` 管“路由”。
  - `shardgrp Clerk` 管“对组内 server 发 RPC”。
- 对动态重配置的兼容：
  - 通过 `ErrWrongGroup` 触发“丢弃旧路由并重试”。
