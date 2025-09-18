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
from sync import execute_sync_task as execute_sync

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

        s3_endpoint = global_config["global"].get("s3_endpoint", None)
        s3_region = global_config["global"].get("s3_region", "")
        s3_access_key = global_config["global"].get("s3_access_key", None)
        s3_secret_key = global_config["global"].get("s3_secret_key", None)
        s3_bucket = global_config["global"].get("s3_bucket", None)
        github_token = global_config["global"].get("github_token", None)
        # 将全局配置应用到每个任务配置中（如果任务配置中没有对应的键）
        for config in task_configs:
            config.setdefault("s3_endpoint", s3_endpoint)
            config.setdefault("s3_region", s3_region)
            config.setdefault("s3_access_key", s3_access_key)
            config.setdefault("s3_secret_key", s3_secret_key)
            config.setdefault("s3_bucket", s3_bucket)
            config.setdefault("github_token", github_token)

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
