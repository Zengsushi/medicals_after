# 邮件模板目录

本目录包含系统邮件通知模板。

## 模板列表

### 1. 新设备登录提醒

文件名: `new_device_login.html`

发送条件:
- 用户在新设备登录时
- 检测到异地登录时

内容包含:
- 登录时间
- 设备信息 (浏览器、操作系统)
- IP 地址和地理位置
- 如果不是本人操作，提供"这不是我"链接

### 2. 密码重置确认

文件名: `password_reset.html`

发送条件:
- 用户请求密码重置时

内容包含:
- 重置链接 (有效期1小时)
- 忽略提示
- 如果不是本人操作，建议忽略

### 3. 密码修改通知

文件名: `password_changed.html`

发送条件:
- 用户密码被修改时

内容包含:
- 修改时间
- 设备信息
- 如果不是本人操作，立即联系支持

## 模板变量

| 变量 | 描述 |
|------|------|
| `{{username}}` | 用户名 |
| `{{login_time}}` | 登录时间 |
| `{{device_type}}` | 设备类型 |
| `{{browser}}` | 浏览器 |
| `{{os}}` | 操作系统 |
| `{{ip_address}}` | IP 地址 |
| `{{location}}` | 地理位置 |
| `{{reset_link}}` | 密码重置链接 |
| `{{expires_in}}` | 链接有效期 |
| {{#if is_suspicious}} | 可疑登录标记 |
| `{{support_email}}` | 支持邮箱 |

## 使用示例

```python
from users.notifications import EmailService

email_service = EmailService()

email_service.send_new_device_login(
    to_email="user@example.com",
    username="john",
    login_time=datetime.now(),
    device_type="Desktop",
    browser="Chrome",
    os="Windows 10",
    ip_address="1.2.3.4",
    location="北京"
)
```
