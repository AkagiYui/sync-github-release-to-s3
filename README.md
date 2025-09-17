# 同步 GitHub Release 到 S3

- 读取 `config.toml` 配置文件，获取 GitHub 仓库信息和 S3 存储桶信息。
- 从存储桶读取`.metainfo.toml`文件，获取已同步的 Release 信息，如果没有就同步最新的 Release。
- 使用 GitHub API 获取最新的 Release 信息。
- 对比最新的 Release 和已同步的 Release，找出需要同步的 Release。
- 下载需要同步的 Release 资产文件。
- 将下载的资产文件上传到 S3 存储桶。
- 更新存储桶中的`.metainfo.toml`文件，记录已同步的 Release 信息。