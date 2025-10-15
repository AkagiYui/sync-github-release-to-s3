import json
from datetime import datetime

from abc import ABC, abstractmethod
from urllib.parse import urlparse
from typing import Literal, TypedDict, NotRequired

import httpx
from lxml import etree


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
    html_url: str  # e.g. "https://github.com/owner/repo/releases/tag/v1.0.0"
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

    BASE_URL = "https://api.github.com"

    async def get_releases(self, owner, repo) -> list[GitHubRelease]:
        response = await self.http_client.get(f"{self.BASE_URL}/repos/{owner}/{repo}/releases")
        return response.json()

    async def get_readme_text(self, owner, repo):
        response = await self.http_client.get(f"{self.BASE_URL}/repos/{owner}/{repo}/readme")
        if download_url := response.json().get("download_url", ""):
            readme_response = await self.http_client.get(download_url)
            return readme_response.text
        return ""


class YybMockGithubClient(GitHubClient):
    """YYB模拟GitHub客户端"""

    BASE_URL = "https://sj.qq.com"

    async def get_readme_text(self, owner, repo) -> str:
        response = await self.http_client.get(f"{self.BASE_URL}/appdetail/{repo}")
        dom = etree.HTML(response.text)
        data = dom.xpath('//script[@id="__NEXT_DATA__"]/text()')[0]
        data = json.loads(data)
        cards = data["props"]["pageProps"]["dynamicCardResponse"]["data"]["components"]
        for card in cards:
            card_id = card["cardId"]
            if card_id == "yybn_game_basic_info":
                for item in card["data"]["itemData"]:
                    if "pkg_name" in item:
                        name = item["name"]  # "碧蓝航线"
                        developer = item["developer"]  # "上海蛮啾网络科技有限公司"
                        description = item["description"]  # "为了那片碧蓝之海！——由bilibili代理"
                        return f"# {name} - {developer}\n\n{description}"
        return ""

    def transform_time(self, time_str: str) -> str:
        """将 YYB 的时间字符串转换为 GitHub 的时间字符串"""
        # "123456789" -> "2023-10-01T12:34:56Z"
        return datetime.fromtimestamp(int(time_str)).isoformat() + "Z"

    async def _get_download_filename(self, download_url: str) -> str:
        response = await self.http_client.head(download_url)
        filename_from_header = response.headers["Content-Disposition"].split("filename=")[1].strip('"')
        if not filename_from_header:
            parsed_url = urlparse(download_url)
            # get query string "fsname" from url
            fsname = parsed_url.query.split("fsname=")[1].split("&")[0]
            filename_from_header = fsname.split("/")[-1]
        return filename_from_header

    async def get_releases(self, owner, repo) -> list[GitHubRelease]:
        response = await self.http_client.get(f"{self.BASE_URL}/appdetail/{repo}")
        dom = etree.HTML(response.text)
        data = dom.xpath('//script[@id="__NEXT_DATA__"]/text()')[0]
        data = json.loads(data)
        cards = data["props"]["pageProps"]["dynamicCardResponse"]["data"]["components"]
        for card in cards:
            card_id = card["cardId"]
            if card_id == "yybn_game_basic_info":
                for item in card["data"]["itemData"]:
                    if "pkg_name" in item:
                        pkg_name: str = item["pkg_name"]  # "com.tencent.tmgp.bilibili.blhx"
                        app_id: str = item["app_id"]  # "52433541"
                        name = item["name"]  # "碧蓝航线"
                        icon: str = item["icon"]  # "http://pp.myapp.com/ma_icon/0/icon_52433541_1756353622/256"
                        md_5: str = item["md_5"]  # "9903C0B0EB455A130EC7B802EB38F346"
                        download_url: str = item["download_url"]  # "http://imtt2.dd.qq.com/sjy.00008/sjy.00002/16891/apk/9903C0B0EB455A130EC7B802EB38F346.apk?fsname=com.tencent.tmgp.bilibili.blhx_9611.apk"
                        apk_size: str = item["apk_size"]  # "123456789"
                        developer = item["developer"]  # "上海蛮啾网络科技有限公司"
                        update_time = item["update_time"]  # "1758992453"
                        version_name = item["version_name"]  # "9.6.11"
                        description = item["description"]  # "为了那片碧蓝之海！——由bilibili代理"
                        download_num: str = item["download_num"]  # "123456789"

                        return [
                            {
                                "id": int(update_time),
                                "name": version_name,
                                "tag_name": version_name,
                                "body": version_name,
                                "html_url": f"{self.BASE_URL}/appdetail/{repo}",
                                "draft": False,
                                "prerelease": False,
                                "published_at": self.transform_time(update_time),
                                "assets": [
                                    {
                                        "name": await self._get_download_filename(download_url),
                                        "browser_download_url": download_url,
                                        "size": int(apk_size),
                                        "content_type": "application/vnd.android.package-archive",
                                        "download_count": int(download_num),
                                        "created_at": self.transform_time(update_time),
                                        "updated_at": self.transform_time(update_time),
                                        "digest": f"md5:{md_5.lower()}",
                                    }
                                ],
                            }
                        ]
        return []


class BiliGameMockGithubClient(GitHubClient):
    """BiliGame模拟GitHub客户端"""

    BASE_URL = "https://line1-h5-pc-api.biligame.com/game/detail"

    def __init__(self, http_client: httpx.AsyncClient = None):
        super().__init__(http_client)
        self.http_client.headers.update({"Referer": "https://www.biligame.com/"})

    def _transform_time(self, time_str: str) -> str:
        """将 BiliGame 的时间字符串转换为 GitHub 的时间字符串"""
        # "2023-10-01 12:34:56" -> "2023-10-01T12:34:56Z"
        return time_str.replace(" ", "T") + "Z"

    def _extract_filename_from_url(self, url: str) -> str:
        """从 URL 中提取文件名"""
        parsed_url = urlparse(url)
        return parsed_url.path.split("/")[-1]

    async def get_readme_text(self, owner, repo) -> str:
        response = await self.http_client.get(f"{self.BASE_URL}/gameinfo?game_base_id={repo}")
        data = response.json()["data"]
        title: str = data["title"]  # "明日方舟"
        developer_name: str = data["developer_name"]  # "上海鹰角网络科技有限公司"
        desc: str = data["desc"]  # "起因不明并四处肆虐的天灾，席卷过的土地上出现"
        return f"# {title} - {developer_name}\n\n{desc}"

    async def get_releases(self, owner, repo) -> list[GitHubRelease]:
        response = await self.http_client.get(f"{self.BASE_URL}/gameinfo?game_base_id={repo}")
        data = response.json()["data"]
        android_pkg_ver: int = data["android_pkg_ver"]  # 96
        android_download_link: str = data["android_download_link"]  # "https://pkg.biligame.com/games/mrfz_2.6.41_20250822_113218_508c0.apk"
        android_pkg_size: int = data["android_pkg_size"]  # 2017303501
        download_count: int = data["download_count"]  # 32816665
        android_sign: str = data["android_sign"]  # "f3b603d47ce8bce5e2449852c2054283"

        response_content = await self.http_client.get(f"{self.BASE_URL}/content?game_base_id={repo}")
        data_content = response_content.json()["data"]
        android_version: str = data_content["android_version"]  # "9.6.0"
        android_latest_update: str = data_content["android_latest_update"]  # "- SideStory「无忧梦呓」限时活动开启，详情请参照官"
        android_update_time: str = data_content["android_update_time"]  # "2023-10-01 12:34:56"

        return [
            {
                "id": android_pkg_ver,
                "name": android_version,
                "tag_name": android_version,
                "body": android_latest_update,
                "html_url": f"https://www.biligame.com/detail/?id={repo}",
                "draft": False,
                "prerelease": False,
                "published_at": self._transform_time(android_update_time),
                "assets": [
                    {
                        "name": self._extract_filename_from_url(android_download_link),
                        "browser_download_url": android_download_link,
                        "size": android_pkg_size,
                        "content_type": "application/vnd.android.package-archive",
                        "download_count": download_count,
                        "created_at": self._transform_time(android_update_time),
                        "updated_at": self._transform_time(android_update_time),
                        "digest": f"md5:{android_sign.lower()}",
                    }
                ],
            }
        ]


ClientType = Literal["official", "yyb", "biligame"]


def get_github_client_class_by_type(client_type: ClientType) -> type[GitHubClient]:
    try:
        return {
            "official": OfficialGitHubClient,
            "yyb": YybMockGithubClient,
            "biligame": BiliGameMockGithubClient,
        }[client_type]
    except KeyError:
        raise ValueError(f"Unknown client type: {client_type}")
