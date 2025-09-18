FROM python:3.13-slim

# 设置工作目录
WORKDIR /app

RUN pip install uv

# 复制项目文件
COPY pyproject.toml uv.lock ./
COPY main.py sync.py ./

# 安装依赖
RUN uv sync --frozen

# 创建非 root 用户
RUN useradd --create-home --shell /bin/bash app
USER app

# 暴露配置文件挂载点
VOLUME ["/app/config.toml"]

# 设置环境变量
ENV PYTHONUNBUFFERED=1

# 运行应用
CMD ["uv", "run", "python", "main.py"]
