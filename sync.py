"""同步业务逻辑模块"""

import asyncio
import logging
from typing import Any
from datetime import datetime
from zoneinfo import ZoneInfo
from aiobotocore.session import get_session
import httpx
import tomlkit

logger = logging.getLogger(__name__)


async def download_and_upload_asset(s3_client: Any, http_client: httpx.AsyncClient, s3_bucket: str, s3_base_path: str, asset: dict[str, Any]) -> None: ...


async def sync_release_to_s3(s3_client: Any, http_client: httpx.AsyncClient, s3_bucket: str, s3_base_path: str, github_owner: str, github_repo: str, release: dict[str, Any]) -> None:
    """同步单个 Release 到 S3"""
    # upload body
    release_name = release["name"] or release["tag_name"]
    release_body = release["body"] or ""
    release_id = release["id"]
    release_html_url = release["html_url"]
    release_publish_at = release["published_at"]  # e.g. "2023-10-01T12:34:56Z"
    # 正确转换UTC时间到上海时区
    utc_time = datetime.fromisoformat(release_publish_at.replace("Z", "+00:00"))
    shanghai_time = utc_time.astimezone(ZoneInfo("Asia/Shanghai"))
    release_publish_time_shanghai = shanghai_time.isoformat()
    release_is_deaft, release_is_prerelease = release["draft"], release["prerelease"]
    logger.info(f"syncing release {release_name} (ID: {release_id}) to S3")

    # add release summary to README.md
    release_summary = f"## {release_name}\n\n"
    release_summary += f"- Published at: {release_publish_time_shanghai} (Shanghai Time)\n"
    release_summary += f"- Published at (UTC): {release_publish_at}\n"
    release_summary += f"- Draft: {release_is_deaft}\n"
    release_summary += f"- Prerelease: {release_is_prerelease}\n"
    release_summary += f"- [View on GitHub]({release_html_url})\n\n"

    release_summary += "---\n\n"
    if release_body:
        release_summary += release_body + "\n\n"
    else:
        release_summary += "_No description provided._\n\n"

    s3_release_path = f"{s3_base_path}/{release_name}"
    s3_response = await s3_client.put_object(
        Bucket=s3_bucket,
        Key=f"{s3_release_path}/README.md",
        Body=release_summary.encode("utf-8"),
        ContentType="text/markdown",
    )
    logger.info(f"Uploaded release notes to S3: {s3_response}")
    
    # upload source code zip/tarball
    source_code_formats = [("zipball_url", "zip", "application/zip"), ("tarball_url", "tar.gz", "application/gzip")]
    for url_key, ext, content_type in source_code_formats:
        source_code_url = release.get(url_key)
        if not source_code_url:
            continue
        response = await http_client.get(source_code_url)
        if response.status_code == 200:
            s3_response = await s3_client.put_object(
                Bucket=s3_bucket,
                Key=f"{s3_release_path}/source_code.{ext}",
                Body=response.content,
                ContentType=content_type,
            )
            logger.info(f"Uploaded source code ({ext}) to S3: {s3_response}")
        else:
            logger.error(f"Failed to download source code from {source_code_url}: HTTP {response.status_code}")

    # upload assets
    assets = release.get("assets", [])
    file_sync_tasks = []
    for asset in assets:
        file_sync_tasks.append(download_and_upload_asset(s3_client, http_client, s3_bucket, s3_release_path, asset))
    await asyncio.gather(*file_sync_tasks)

    logger.info(f"Synced release {release_name} (ID: {release_id}) to S3")
    return


async def execute_sync_task(config: dict[str, Any]) -> None:
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
            metainfo = tomlkit.loads(metainfo_content.decode("utf-8")) if metainfo_content else {"synced_releases": [], "last_synced_id": 0}  # {"synced_releases": [{"id": -1, "name": "initial"}], "last_synced_id": 0}

            # get latest releases from GitHub
            releases_info = (await http_client.get(f"/repos/{github_owner}/{github_repo}/releases")).json()
            # filter releases
            releases = [r for r in releases_info if (github_release_include_prerelease or not r["prerelease"]) and (github_release_include_draft or not r["draft"])]  # releases that can be synced
            new_releases = [r for r in releases if r["id"] > metainfo["last_synced_id"]]  # releases that are newer than last synced id
            # releases exclude already synced releases
            releases_can_sync = []
            synced_ids = {synced_r["id"] for synced_r in metainfo.get("synced_releases", [])}
            for new_r in new_releases:
                if new_r["id"] not in synced_ids:
                    releases_can_sync.append(new_r)
            releases_can_sync.sort(key=lambda r: r["id"], reverse=True)  # sort by id descending

            if len(releases_can_sync) == 0:
                logger.info(f"No new releases to sync for {task_id}.")
                return

            logger.info(f"Found {len(releases_can_sync)} new releases to sync for {task_id}.")
            # sync first 1 release
            for release in releases_can_sync[:1]:
                await sync_release_to_s3(s3_client, http_client, s3_bucket, s3_base_path, github_owner, github_repo, release)
                # update metainfo
                synced_release_entry = {"id": release["id"], "name": release["name"] or release["tag_name"]}
                synced_releases = metainfo.get("synced_releases", [])
                synced_releases.append(synced_release_entry)
                metainfo["synced_releases"] = synced_releases
                metainfo["last_synced_id"] = max(metainfo.get("last_synced_id", 0), release["id"])

            # upload updated .metainfo.toml
            new_metainfo_content = tomlkit.dumps(metainfo)
            s3_response = await s3_client.put_object(
                Bucket=s3_bucket,
                Key=f"{s3_base_path}/.metainfo.toml",
                Body=new_metainfo_content.encode("utf-8"),
                ContentType="application/toml",
            )
            logger.info(f"Uploaded .metainfo.toml to S3: {s3_response}")

    except Exception as e:
        logger.error(f"任务 {task_id} 执行失败: {e}")
