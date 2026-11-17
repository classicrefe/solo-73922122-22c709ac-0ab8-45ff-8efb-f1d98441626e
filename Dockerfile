FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY sample_survey.csv ./sample_survey.csv
COPY sample_survey_b.csv ./sample_survey_b.csv

ENV DB_PATH=/data/app.db
EXPOSE 8000

# 数据目录（由 compose 挂载卷持久化；两个实例共享同一 SQLite 文件）
VOLUME ["/data"]

# 单 worker：并发约束由 SQLite 写锁跨进程保证，可安全扩展副本数。
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
