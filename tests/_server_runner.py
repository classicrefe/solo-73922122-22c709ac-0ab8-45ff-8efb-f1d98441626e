"""测试用服务启动器：运行一个 uvicorn 实例；父进程关闭 stdin 即退出。

沙箱环境禁止向子进程发信号，因此测试通过关闭 stdin 通知服务进程退出。
这里用 os.read 直接读文件描述符，避免解释器退出时与缓冲流的锁竞争。
"""

import os
import sys
import threading

import uvicorn

from app.main import app


def main() -> None:
    port = int(sys.argv[1])
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)

    def wait_for_parent_eof() -> None:
        try:
            while os.read(0, 1024):
                pass
        except OSError:
            pass
        finally:
            server.should_exit = True

    threading.Thread(target=wait_for_parent_eof, daemon=True).start()
    server.run()


if __name__ == "__main__":
    main()
