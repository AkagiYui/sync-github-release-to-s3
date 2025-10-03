"""同步业务逻辑模块"""

import asyncio
import logging
from typing import Any
from datetime import datetime
from zoneinfo import ZoneInfo
from aiobotocore.session import get_session
import httpx
import tomlkit
import tempfile
import os

from types_aiobotocore_s3 import S3Client
from types_aiobotocore_s3.type_defs import CompletedPartTypeDef, CompletedMultipartUploadTypeDef

from github import GitHubAsset, GitHubRelease, OfficialGitHubClient
from util import async_retry


logger = logging.getLogger(__name__)


@async_retry(max_retries=3, delay=2.0, backoff=2.0, exceptions=(httpx.RequestError, httpx.TimeoutException, ConnectionError))
async def download_and_upload_asset(s3_client: Any, http_client: httpx.AsyncClient, s3_bucket: str, s3_base_path: str, asset: GitHubAsset) -> None:
    """下载文件到本地，然后分片上传到 S3"""
    asset_name = asset["name"]
    asset_url = asset["browser_download_url"]
    asset_size = asset["size"]
    asset_content_type = asset["content_type"]
    logger.info(f"Downloading asset {asset_name} ({asset_size} bytes) from {asset_url}")

    DOWNLOAD_CHUNK_SIZE = 4 * 1024 * 1024  # download in 4MB chunks
    UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024  # S3分片上传最小5MB，使用8MB更安全

    s3_key = f"{s3_base_path}/{asset_name}"

    # 创建临时文件
    with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{asset_name}") as temp_file:
        temp_file_path = temp_file.name

    try:
        await _download_file_chunked(http_client, asset_url, temp_file_path, asset_name, asset_size, DOWNLOAD_CHUNK_SIZE)
        await _upload_file_chunked(s3_client, s3_bucket, s3_key, temp_file_path, asset_content_type, asset_name, UPLOAD_CHUNK_SIZE)
        logger.info(f"Successfully downloaded and uploaded asset {asset_name}")
    except Exception as e:
        logger.error(f"Failed to download and upload asset {asset_name}: {e}")
        raise  # 重新抛出异常以触发重试机制
    finally:
        # 清理临时文件
        try:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
                logger.debug(f"Cleaned up temporary file for {asset_name}")
        except Exception as cleanup_error:
            logger.warning(f"Failed to cleanup temporary file for {asset_name}: {cleanup_error}")


async def _download_file_chunked(http_client: httpx.AsyncClient, url: str, file_path: str, asset_name: str, expected_size: int | None = None, chunk_size: int | None = None) -> None:
    """
    流式分片下载函数，支持缓冲区分块下载

    Args:
        http_client: HTTP客户端
        url: 下载URL
        file_path: 本地文件路径
        asset_name: 资源名称（用于日志）
        expected_size: 期望的文件大小，None表示未知大小
        chunk_size: 分片大小，None使用默认值
    """
    # 设置默认chunk_size
    if chunk_size is None:
        chunk_size = 4 * 1024 * 1024  # 4MB chunks

    downloaded_size = 0
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"}

    try:
        async with http_client.stream("GET", url, headers=headers, timeout=None) as response:
            # 检查HTTP状态码
            if response.status_code != 200:
                raise httpx.HTTPStatusError(f"HTTP {response.status_code}", request=response.request, response=response)

            # 以写入模式打开文件
            with open(file_path, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size):
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded_size += len(chunk)

                    # 显示下载进度
                    if downloaded_size % (chunk_size * 5) == 0:  # 每5个chunk显示一次进度
                        if expected_size is not None and expected_size > 0:
                            # 已知文件大小，显示百分比进度
                            progress = (downloaded_size / expected_size) * 100
                            logger.info(f"Downloading {asset_name}: {progress:.1f}% ({downloaded_size}/{expected_size} bytes)")
                        else:
                            # 未知文件大小，只显示已下载字节数
                            logger.info(f"Downloading {asset_name}: {downloaded_size} bytes downloaded")

            # 验证下载完整性（仅在已知文件大小时）
            if expected_size is not None and downloaded_size != expected_size:
                raise ValueError(f"Download size mismatch for {asset_name}: expected {expected_size}, got {downloaded_size}")

            logger.info(f"Successfully downloaded {asset_name} ({downloaded_size} bytes)")

    except Exception as e:
        logger.error(f"Failed to download {asset_name}: {e}")
        raise


async def _upload_part_with_retry_from_file(s3_client: S3Client, s3_bucket: str, s3_key: str, upload_id: str, part_number: int, file_path: str, start_pos: int, chunk_size: int, asset_name: str, max_retries: int = 3) -> CompletedPartTypeDef:
    """从文件按需读取并上传单个分片，带重试机制"""
    for retry in range(max_retries):
        try:
            # 每次上传时才读取对应的文件分片
            with open(file_path, "rb") as f:
                f.seek(start_pos)
                chunk = f.read(chunk_size)

            if not chunk:
                raise ValueError(f"No data read for part {part_number} at position {start_pos}")

            part_response = await s3_client.upload_part(
                Bucket=s3_bucket,
                Key=s3_key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=chunk,
            )

            logger.info(f"Uploaded part {part_number} for {asset_name} ({len(chunk)} bytes)")
            return {
                "ETag": part_response["ETag"],
                "PartNumber": part_number,
            }

        except Exception as part_error:
            if retry == max_retries - 1:
                logger.error(f"Part {part_number} upload failed after {max_retries} retries: {part_error}")
                raise part_error
            logger.warning(f"Part {part_number} upload failed (retry {retry + 1}/{max_retries}): {part_error}")
            await asyncio.sleep(2**retry)  # 指数退避

    # 这行代码理论上不会执行到，但为了类型检查添加
    raise RuntimeError(f"Unexpected error: all retries exhausted for part {part_number}")


async def _upload_file_chunked(s3_client: S3Client, s3_bucket: str, s3_key: str, file_path: str, content_type: str, asset_name: str, chunk_size: int, max_concurrent_parts: int = 5) -> None:
    """分片上传本地文件到S3，支持并行上传"""
    file_size = os.path.getsize(file_path)
    logger.info(f"Uploading {asset_name} ({file_size} bytes) to S3 using multipart upload")

    # S3分片上传的最小分片大小是5MB，如果文件小于5MB则直接上传
    MIN_MULTIPART_SIZE = 5 * 1024 * 1024

    if file_size < MIN_MULTIPART_SIZE:
        # 小文件直接上传
        with open(file_path, "rb") as f:
            s3_response = await s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=f,
                ContentType=content_type,
            )
            logger.info(f"Uploaded small file {asset_name} to S3: {s3_response}")
        return

    # 大文件使用分片上传
    multipart_response = await s3_client.create_multipart_upload(
        Bucket=s3_bucket,
        Key=s3_key,
        ContentType=content_type,
    )
    upload_id = multipart_response["UploadId"]

    try:
        # 计算分片信息（不预读数据，只计算位置和大小）
        part_info = []
        current_pos = 0
        part_number = 1

        while current_pos < file_size:
            # 计算当前分片的大小（最后一个分片可能小于chunk_size）
            current_chunk_size = min(chunk_size, file_size - current_pos)
            part_info.append((part_number, current_pos, current_chunk_size))
            current_pos += current_chunk_size
            part_number += 1

        logger.info(f"File {asset_name} will be uploaded in {len(part_info)} parts with max {max_concurrent_parts} concurrent uploads")

        # 使用信号量控制并发数量
        semaphore = asyncio.Semaphore(max_concurrent_parts)

        async def upload_part_with_semaphore(part_number: int, start_pos: int, part_chunk_size: int) -> CompletedPartTypeDef:
            async with semaphore:
                return await _upload_part_with_retry_from_file(s3_client, s3_bucket, s3_key, upload_id, part_number, file_path, start_pos, part_chunk_size, asset_name)

        # 并行上传所有分片
        upload_tasks = [upload_part_with_semaphore(part_number, start_pos, part_chunk_size) for part_number, start_pos, part_chunk_size in part_info]

        parts = await asyncio.gather(*upload_tasks)

        # 按分片号排序（虽然通常已经是有序的）
        parts.sort(key=lambda x: x.get("PartNumber", 0))

        # 完成分片上传
        multipart_upload: CompletedMultipartUploadTypeDef = {"Parts": parts}
        await s3_client.complete_multipart_upload(
            Bucket=s3_bucket,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload=multipart_upload,
        )

        logger.info(f"Completed multipart upload for {asset_name} with {len(parts)} parts")

    except Exception as e:
        # 如果上传失败，取消分片上传
        logger.error(f"Multipart upload failed for {asset_name}: {e}")
        try:
            await s3_client.abort_multipart_upload(
                Bucket=s3_bucket,
                Key=s3_key,
                UploadId=upload_id,
            )
            logger.info(f"Aborted multipart upload for {asset_name}")
        except Exception as abort_error:
            logger.error(f"Failed to abort multipart upload for {asset_name}: {abort_error}")
        raise


async def sync_release_to_s3(s3_client: Any, http_client: httpx.AsyncClient, s3_bucket: str, s3_base_path: str, github_owner: str, github_repo: str, release: GitHubRelease) -> None:
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

    # Add assets table if assets exist
    assets = release.get("assets", [])
    if assets:
        release_summary += "### Assets\n\n"
        release_summary += "| File Name | Size | Content Type | Downloads | Digest | Created At |\n"
        release_summary += "|-----------|------|--------------|-----------|--------|------------|\n"

        for asset in assets:
            asset_name = asset.get("name", "N/A")
            asset_size = asset.get("size", 0)
            asset_content_type = asset.get("content_type", "N/A")
            asset_download_count = asset.get("download_count", 0)
            asset_created_at = asset.get("created_at", "N/A")
            asset_browser_download_url = asset.get("browser_download_url", "")
            asset_digest = asset.get("digest", "N/A")

            # Format file size in human readable format
            if asset_size >= 1024 * 1024 * 1024:  # GB
                size_str = f"{asset_size / (1024 * 1024 * 1024):.2f} GB"
            elif asset_size >= 1024 * 1024:  # MB
                size_str = f"{asset_size / (1024 * 1024):.2f} MB"
            elif asset_size >= 1024:  # KB
                size_str = f"{asset_size / 1024:.2f} KB"
            else:
                size_str = f"{asset_size} B"

            # Format created_at time (convert from UTC to Shanghai time if possible)
            try:
                if asset_created_at != "N/A":
                    utc_time = datetime.fromisoformat(asset_created_at.replace("Z", "+00:00"))
                    shanghai_time = utc_time.astimezone(ZoneInfo("Asia/Shanghai"))
                    created_at_str = shanghai_time.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    created_at_str = "N/A"
            except Exception:
                created_at_str = asset_created_at

            # Create clickable file name if download URL is available
            if asset_browser_download_url:
                file_name_display = f"[{asset_name}]({asset_browser_download_url})"
            else:
                file_name_display = asset_name

            # Format digest - show full digest in code format for better readability
            if asset_digest and asset_digest != "N/A":
                digest_display = f"`{asset_digest}`"
            else:
                digest_display = "N/A"

            release_summary += f"| {file_name_display} | {size_str} | {asset_content_type} | {asset_download_count} | {digest_display} | {created_at_str} |\n"

        release_summary += "\n"

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

    # upload source code zip/tarball using streaming
    source_code_formats = [("zipball_url", "zip", "application/zip"), ("tarball_url", "tar.gz", "application/gzip")]
    for url_key, ext, content_type in source_code_formats:
        source_code_url = release.get(url_key)
        if not source_code_url:
            continue

        try:
            # 创建临时文件用于下载源代码
            with tempfile.NamedTemporaryFile(delete=False, suffix=f"_source_code.{ext}") as temp_file:
                temp_file_path = temp_file.name

            try:
                # 使用分片下载源代码文件
                await _download_file_chunked(http_client, source_code_url, temp_file_path, f"source_code.{ext}")

                # 使用分片上传到S3
                await _upload_file_chunked(s3_client, s3_bucket, f"{s3_release_path}/source_code.{ext}", temp_file_path, content_type, f"source_code.{ext}", 8 * 1024 * 1024)

                logger.info(f"Successfully downloaded and uploaded source code ({ext})")

            finally:
                # 清理临时文件
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)

        except Exception as e:
            logger.error(f"Failed to download and upload source code ({ext}) from {source_code_url}: {e}")

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
    start_time = datetime.now()

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
            httpx.AsyncClient(
                base_url="https://api.github.com",
                timeout=httpx.Timeout(
                    connect=30.0,  # 连接超时30秒
                    read=600.0,  # 读取超时10分钟（用于大文件下载）
                    write=60.0,  # 写入超时1分钟
                    pool=60.0,  # 连接池超时1分钟
                ),
                headers={"Authorization": f"Bearer {github_token}"},
                follow_redirects=True,
                limits=httpx.Limits(
                    max_keepalive_connections=10,  # 保持连接数
                    max_connections=20,  # 最大连接数
                    keepalive_expiry=30.0,  # 连接保持时间
                ),
            ) as http_client,
        ):
            logger.info(f"Syncing releases for {github_owner}/{github_repo} to bucket {s3_bucket}")
            github_api = OfficialGitHubClient(http_client)

            # update README.md
            if readme_content := await github_api.get_readme_text(github_owner, github_repo):
                logger.info(f"README content: {readme_content[:100].replace('\n', ' ')}...")
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
            logger.info(f"{task_id} .metainfo.toml content: {metainfo_content.decode('utf-8')}")
            metainfo = tomlkit.loads(metainfo_content.decode("utf-8")) if metainfo_content else {"synced_releases": [], "last_synced_id": 0}  # {"synced_releases": [{"id": -1, "name": "initial"}], "last_synced_id": 0}

            # get latest releases from GitHub
            releases_info = await github_api.get_releases(github_owner, github_repo)
            # filter releases
            releases = [r for r in releases_info if (github_release_include_prerelease or not r["prerelease"]) and (github_release_include_draft or not r["draft"])]  # releases that exclude draft/prerelease if not included
            new_releases = [r for r in releases if r["id"] > metainfo["last_synced_id"]]  # releases that are newer than last synced id

            releases_can_sync: list[GitHubRelease] = []  # releases exclude already synced releases
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

        # 记录任务执行时间
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        logger.info(f"任务 {task_id} 执行完成，耗时: {execution_time:.2f} 秒")

    except Exception as e:
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        logger.error(f"任务 {task_id} 执行失败，耗时: {execution_time:.2f} 秒，错误: {e}")
