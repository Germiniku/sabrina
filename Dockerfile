FROM python:3.12-slim


RUN pip install pdm


WORKDIR /app

# 复制文件
COPY . /app

# 安装依赖
RUN pdm install

# 默认命令（可被 docker-compose 覆盖）
CMD ["pdm", "run", "python", "main.py", "start"]
