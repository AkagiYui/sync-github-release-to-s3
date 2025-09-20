FROM ghcr.io/astral-sh/uv:python3.13-alpine

# 设置工作目录
WORKDIR /app

# 复制项目文件
COPY pyproject.toml uv.lock ./
COPY main.py sync.py util.py ./

# 安装依赖
RUN uv sync --frozen --no-cache-dir

# 创建非 root 用户
RUN adduser -D -s /bin/sh app && \
    chown -R app:app /app
USER app

# 暴露配置文件挂载点
VOLUME ["/app/config.toml"]

# 设置环境变量
ENV PYTHONUNBUFFERED=1

# 运行应用
CMD ["uv", "run", "main.py"]
