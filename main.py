"""
GitHub Release to S3 同步工具主程序
"""

import asyncio
from datetime import datetime
from pathlib import Path
import tomllib
import logging
import sys
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sync import execute_sync

CONFIG_FILE = Path(__file__).parent / "config.toml"

# 配置日志 - 只输出到标准输出
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


async def main():
    """主函数：使用APScheduler管理定时任务"""
    try:
        # 加载配置
        if not CONFIG_FILE.exists():
            logger.error(f"配置文件不存在: {CONFIG_FILE}")
            sys.exit(1)

        global_config = tomllib.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        check_interval = global_config["global"]["check_interval"]
        task_configs = global_config["tasks"]

        if not task_configs:
            logger.error("没有配置任何同步任务")
            sys.exit(1)

        # 配置APScheduler
        scheduler = AsyncIOScheduler()

        logger.info(f"启动APScheduler任务调度系统：{len(task_configs)} 个配置，检查间隔 {check_interval} 秒")

        # 为每个配置添加定时任务
        for i, config in enumerate(task_configs):
            job_id = f"sync_task_{i}_{config.get('github_owner', 'unknown')}_{config.get('github_repo', 'unknown')}"

            scheduler.add_job(
                execute_sync,
                "interval",
                seconds=check_interval,
                args=[config],
                id=job_id,
                max_instances=1,  # 防止任务重叠,
                next_run_time=datetime.now(),  # 程序启动后立即执行一次
            )

            logger.info(f"已添加定时任务: {job_id}")

        # 启动调度器
        scheduler.start()
        logger.info("APScheduler 已启动")

        try:
            # 保持程序运行
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("收到中断信号，正在关闭调度器...")
            scheduler.shutdown()
            logger.info("调度器已关闭")

    except Exception as e:
        logger.error(f"程序运行异常: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
