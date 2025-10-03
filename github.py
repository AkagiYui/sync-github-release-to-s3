from abc import ABC, abstractmethod
from typing import TypedDict, NotRequired

import httpx

class GitHubAsset(TypedDict):
    name: str
    browser_download_url: str
    size: int
    content_type: str
    download_count: int
    created_at: str
    updated_at: str
    digest: NotRequired[str]  # e.g. "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

class GitHubRelease(TypedDict):
    id: int  # unique id
    name: NotRequired[str]
    tag_name: str  # e.g. "v1.0.0"
    body: NotRequired[str]  # Markdown
    draft: bool
    prerelease: bool
    published_at: str  # e.g. "2023-10-01T12:34:56Z"
    assets: list[GitHubAsset]


class GitHubClient(ABC):
    """GitHub客户端抽象基类"""

    def __init__(self, http_client: httpx.AsyncClient = None):
        self.http_client = http_client or httpx.AsyncClient()

    @abstractmethod
    async def get_releases(self, owner: str, repo: str) -> list[GitHubRelease]:
        """获取仓库的发布列表"""
        pass

    @abstractmethod
    async def get_readme_text(self, owner: str, repo: str) -> str:
        """获取仓库的README文本"""
        pass


class OfficialGitHubClient(GitHubClient):
    """官方GitHub客户端"""

    async def get_releases(self, owner, repo) -> list[GitHubRelease]:
        response = await self.http_client.get(f"/repos/{owner}/{repo}/releases")
        return response.json()

    async def get_readme_text(self, owner, repo):
        response = await self.http_client.get(f"/repos/{owner}/{repo}/readme")
        if download_url := response.json().get("download_url", ""):
            readme_response = await self.http_client.get(download_url)
            return readme_response.text
        return ""
