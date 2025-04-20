FROM python:3.12-slim


RUN pip install pdm

# 设置虚拟环境位置
ENV PDM_VENV_IN_PROJECT=1
ENV PDM_IGNORE_SAVED_PYTHON=1

WORKDIR /app

# 复制文件
COPY . .

# 安装依赖
RUN pdm install

RUN pdm list

# 默认命令（可被 docker-compose 覆盖）
CMD ["pdm", "run", "python", "main.py", "start"]
