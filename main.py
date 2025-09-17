import asyncio
from pathlib import Path
import tomllib
from typing import Any
from aiobotocore.session import get_session
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler

CONFIG_FILE = Path(__file__).parent / "config.toml"

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def execute_sync(config: dict[str, Any]) -> None:
    """执行单个同步任务"""
    s3_endpoint = config.get("s3_endpoint")
    s3_region = config.get("s3_region")
    s3_bucket = config.get("s3_bucket")
    s3_access_key = config.get("s3_access_key")
    s3_secret_key = config.get("s3_secret_key")
    s3_base_path = config.get("s3_base_path", "")

    github_owner = config.get("github_owner")
    github_repo = config.get("github_repo")
    github_token = config.get("github_token")
    github_release_include_prerelease = config.get("github_release_include_prerelease", False)
    github_release_include_draft = config.get("github_release_include_draft", False)

    task_id = f"{github_owner}/{github_repo}"
    logger.info(f"开始执行同步任务: {task_id} -> {s3_bucket}")

    try:
        # connect to S3
        session = get_session()
        async with session.create_client(
            "s3",
            region_name=s3_region,
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
        ) as s3_client:
            # Here you would implement the logic to:
            # 1. Fetch the .metainfo.toml from S3
            # 2. Fetch the latest releases from GitHub
            # 3. Compare and find new releases
            # 4. Download assets from GitHub
            # 5. Upload assets to S3
            # 6. Update the .metainfo.toml in S3
            logger.info(f"Syncing releases for {github_owner}/{github_repo} to bucket {s3_bucket}")

            if s3_bucket:  # 类型检查
                response = await s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=f'{s3_base_path}/.metainfo.toml',
                    Body="dummy data".encode('utf-8')
                )
                logger.info(f"任务 {task_id} 完成，响应: {response.get('ResponseMetadata', {}).get('HTTPStatusCode')}")
            else:
                logger.error(f"任务 {task_id} 失败: s3_bucket 未配置")

    except Exception as e:
        logger.error(f"任务 {task_id} 执行失败: {e}")

async def main():
    """主函数：使用APScheduler管理定时任务"""
    global_config = tomllib.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    check_interval = global_config["global"]["check_interval"]
    task_configs = global_config["tasks"]

    # 配置APScheduler
    scheduler = AsyncIOScheduler()

    logger.info(f"启动APScheduler任务调度系统：{len(task_configs)} 个配置，检查间隔 {check_interval} 秒")

    # 为每个配置添加定时任务
    for i, config in enumerate(task_configs):
        job_id = f"sync_task_{i}_{config.get('github_owner', 'unknown')}_{config.get('github_repo', 'unknown')}"

        scheduler.add_job(
            execute_sync,
            'interval',
            seconds=check_interval,
            args=[config],
            id=job_id,
            max_instances=1  # 防止任务重叠
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

def cli_main():
    """CLI入口点"""
    asyncio.run(main())

if __name__ == "__main__":
    cli_main()
