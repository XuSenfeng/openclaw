# Python Platform Plugin for OpenClaw

该目录包含 OpenClaw `python-platform` 插件实现，用于将 OpenClaw 网关与 Python 虚拟消息平台进行 WebSocket 对接。

## 目录说明

1. `index.ts`: 插件主逻辑（连接、收发消息、流式转发）
2. `openclaw.plugin.json`: 插件元信息与配置 Schema
3. `setup-entry.ts`: 插件安装/引导入口

## 配置项

插件配置定义见 `openclaw.plugin.json`：

1. `channels.python-platform.wsUrl`: Python 服务端地址，默认 `ws://127.0.0.1:8765`

也可通过环境变量覆盖：

`PYTHON_PLATFORM_WS_URL`

## 关联仓库

1. OpenClaw Fork: https://github.com/XuSenfeng/openclaw
2. Python Server: https://github.com/XuSenfeng/open-claw-python-channel-server
3. Android Client: https://github.com/XuSenfeng/openclaw-channel-android-client
4. Integration Docs: https://github.com/XuSenfeng/openclaw-android-demo

## AI 生成声明

本项目文档与部分代码在开发过程中使用了 AI 辅助生成与修改。
所有 AI 产出内容均经过人工审查、调试与验证后再提交。
