# 同步 GitHub Release 到 S3

- 读取 `config.toml` 配置文件，获取 GitHub 仓库信息和 S3 存储桶信息。
- 从存储桶读取`.metainfo.toml`文件，获取已同步的 Release 信息，如果没有就同步最新的 Release。
- 使用 GitHub API 获取最新的 Release 信息。
- 对比最新的 Release 和已同步的 Release，找出需要同步的 Release。
- 下载需要同步的 Release 资产文件。
- 将下载的资产文件上传到 S3 存储桶。
- 更新存储桶中的`.metainfo.toml`文件，记录已同步的 Release 信息。

## 功能特性

### Release 摘要增强

在上传的 Release 摘要（README.md）中，如果 Release 包含 assets，会自动生成一个包含以下信息的 Markdown 表格：

- **文件名**：可点击的下载链接
- **文件大小**：人类可读的格式（B/KB/MB/GB）
- **内容类型**：文件的 MIME 类型
- **下载次数**：GitHub 统计的下载次数
- **摘要**：文件的完整哈希摘要（如 SHA256，以代码格式显示）
- **创建时间**：转换为上海时区的时间格式

### 示例表格

```markdown
### Assets

| File Name | Size | Content Type | Downloads | Digest | Created At |
|-----------|------|--------------|-----------|--------|------------|
| [app-windows-x64.exe](https://github.com/owner/repo/releases/download/v1.0.0/app-windows-x64.exe) | 50.00 MB | application/octet-stream | 1234 | `sha256:2151b604e3429bff440b9fbc03eb3617bc2603cda96c95b9bb05277f9ddba255` | 2023-10-01 20:30:00 |
| [app-linux-x64.tar.gz](https://github.com/owner/repo/releases/download/v1.0.0/app-linux-x64.tar.gz) | 40.00 MB | application/gzip | 567 | `sha256:abcd1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab` | 2023-10-01 20:31:00 |
| [checksums.txt](https://github.com/owner/repo/releases/download/v1.0.0/checksums.txt) | 512 B | text/plain | 45 | `sha256:short123456789abcdef` | 2023-10-01 20:33:00 |
```
