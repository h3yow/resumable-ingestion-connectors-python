"""仓库根入口：运行本地摄入服务。"""
import uvicorn

from app.main import app  # noqa: F401

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
