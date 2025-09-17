"""同步业务逻辑模块"""

import logging
import tomllib
from typing import Any
from aiobotocore.session import get_session
import httpx

logger = logging.getLogger(__name__)

async def sync_release_to_s3(
    s3_client: Any, 
    http_client: httpx.AsyncClient, 
    s3_bucket: str, 
    s3_base_path: str, 
    github_owner: str, 
    github_repo: str, 
    release: dict[str, Any]
) -> None:
    ...

async def execute_sync(config: dict[str, Any]) -> None:
    """执行单个同步任务"""
    s3_endpoint: str = config.get("s3_endpoint")  # type: ignore
    s3_region: str = config.get("s3_region")  # type: ignore
    s3_bucket: str = config.get("s3_bucket")  # type: ignore
    s3_access_key: str = config.get("s3_access_key")  # type: ignore
    s3_secret_key: str = config.get("s3_secret_key")  # type: ignore
    s3_base_path: str = config.get("s3_base_path", "")  # type: ignore

    github_owner: str = config.get("github_owner")  # type: ignore
    github_repo: str = config.get("github_repo")  # type: ignore
    github_token: str = config.get("github_token")  # type: ignore
    github_release_include_prerelease = config.get("github_release_include_prerelease", False)
    github_release_include_draft = config.get("github_release_include_draft", False)

    task_id = f"{github_owner}/{github_repo}"
    logger.info(f"开始执行同步任务: {task_id} -> {s3_bucket}")

    try:
        # connect to S3
        session = get_session()
        async with (
            session.create_client(
                "s3",
                region_name=s3_region,
                endpoint_url=s3_endpoint,
                aws_access_key_id=s3_access_key,
                aws_secret_access_key=s3_secret_key,
            ) as s3_client,
            httpx.AsyncClient(base_url="https://api.github.com", timeout=None, headers={"Authorization": f"Bearer {github_token}"}) as http_client,
        ):
            # Here you would implement the logic to:
            # 1. Fetch the .metainfo.toml from S3
            # 2. Fetch the latest releases from GitHub
            # 3. Compare and find new releases
            # 4. Download assets from GitHub
            # 5. Upload assets to S3
            # 6. Update the .metainfo.toml in S3
            logger.info(f"Syncing releases for {github_owner}/{github_repo} to bucket {s3_bucket}")

            # update README.md
            response = await http_client.get(f"/repos/{github_owner}/{github_owner}/readme")
            download_url = response.json().get("download_url", "")
            if download_url:
                readme_response = await http_client.get(download_url)
                readme_content = readme_response.text
                logger.info(f"README content: {readme_content[:100]}...")
                # upload to S3
                s3_response = await s3_client.put_object(
                    Bucket=s3_bucket,
                    Key=f"{s3_base_path}/README.md",
                    Body=readme_content.encode("utf-8"),
                    ContentType="text/markdown",
                )
                logger.info(f"Uploaded README.md to S3: {s3_response}")

            # get .metainfo.toml
            try:
                response = await s3_client.get_object(Bucket=s3_bucket, Key=f"{s3_base_path}/.metainfo.toml")
                metainfo_content = await response["Body"].read()
            except s3_client.exceptions.NoSuchKey:
                logger.warning(f".metainfo.toml not found in {s3_bucket}/{s3_base_path}, starting fresh.")
                metainfo_content = b""
            logger.info(f".metainfo.toml content: {metainfo_content.decode('utf-8')}")
            metainfo = tomllib.loads(metainfo_content.decode("utf-8")) if metainfo_content else {"synced_releases": [{"id": -1, "name": "initial"}], "last_synced_id": 0}
            
            # get latest releases from GitHub
            releases_info = (await http_client.get(f"/repos/{github_owner}/{github_repo}/releases")).json()
            # filter releases
            releases = [r for r in releases_info if (github_release_include_prerelease or not r["prerelease"]) and (github_release_include_draft or not r["draft"])]
            new_releases = [r for r in releases if r["id"] > metainfo["last_synced_id"]]
            releases_can_sync = [new_r for new_r in new_releases if all(new_r["id"] != synced_r["id"] for synced_r in metainfo["synced_releases"])]
            releases_can_sync.sort(key=lambda r: r["id"], reverse=True)  # sort by id descending
            
            if len(releases_can_sync) == 0:
                logger.info(f"No new releases to sync for {task_id}.")
                return
            
            logger.info(f"Found {len(releases_can_sync)} new releases to sync for {task_id}.")
            # sync first 1 release
            for release in releases_can_sync[:1]:
                await sync_release_to_s3(
                    s3_client,
                    http_client,
                    s3_bucket,
                    s3_base_path,
                    github_owner,
                    github_repo,
                    release,
                )
                # update metainfo
                metainfo["synced_releases"].append({"id": release["id"], "name": release["name"] or release["tag_name"]})
                metainfo["last_synced_id"] = max(metainfo["last_synced_id"], release["id"])
            

    except Exception as e:
        logger.error(f"任务 {task_id} 执行失败: {e}")
