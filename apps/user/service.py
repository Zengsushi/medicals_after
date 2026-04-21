"""
    用户服务层
    封装用户相关业务逻辑
"""
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import uuid

from sqlalchemy.orm import Session

from apps.user.models import User
from apps.user import db as user_db_module
from apps.core import Security, Result

_AVATAR_MAX_BYTES = 2 * 1024 * 1024
_AVATAR_ALLOWED_CT = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _avatar_storage_dir() -> Path:
    d = _project_root() / "static" / "user_data" / "avatar"
    d.mkdir(parents=True, exist_ok=True)
    return d


class UserService:
    """用户服务类"""

    @staticmethod
    def build_user_response(user: User) -> Dict[str, Any]:
        """构建用户信息响应"""
        primary_role = user.get_primary_role()
        return {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "phone": user.phone,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "avatar": user.avatar,
            "is_active": user.is_active,
            "is_superuser": bool(getattr(user, "is_superuser", False)),
            "role": primary_role.code if primary_role else "guest",
            "role_name": primary_role.name if primary_role else "访客",
            "roles": [
                {
                    "id": role.id,
                    "code": role.code,
                    "name": role.name
                }
                for role in user.roles
            ] if user.roles else [],
            "introduce": user.introduce,
            "date_joined": user.date_joined.isoformat() if user.date_joined else None,
            "last_login": user.last_login.isoformat() if user.last_login else None
        }

    @staticmethod
    def build_permissions_response(user: User) -> list:
        """构建权限列表响应"""
        return user.get_permissions()

    @staticmethod
    def build_menus_response(user: User) -> list:
        """构建菜单列表响应"""
        from apps.menu.models import Menu

        menus = user.get_menus()

        def menu_to_dict(menu: Menu) -> Dict[str, Any]:
            return {
                "id": menu.id,
                "name": menu.name,
                "path": menu.path,
                "component": menu.component,
                "icon": menu.icon,
                "order": menu.order,
                "parent_id": menu.parent_id,
                "permission_code": menu.permission_code,
                "is_visible": menu.is_active if hasattr(menu, 'is_active') else True,
                "is_cached": menu.is_cached
            }

        menu_tree = []
        menu_map = {}

        for menu in menus:
            menu_dict = menu_to_dict(menu)
            menu_map[menu.id] = menu_dict
            menu_dict["children"] = []

        for menu in menus:
            if menu.parent_id is None:
                menu_tree.append(menu_map[menu.id])
            else:
                parent = menu_map.get(menu.parent_id)
                if parent:
                    parent["children"].append(menu_map[menu.id])

        return menu_tree

    @staticmethod
    async def upload_avatar(user: User, file, db: Session) -> Dict[str, Any]:
        """上传用户头像：校验类型与大小，写入 static/user_data/avatar，数据库保存 /static/... 相对路径。"""
        repo = user_db_module.UserService(db)
        try:
            user_obj = repo.user_search(user.id)
        except Exception as e:
            return Result.error(404, str(e))

        raw = await file.read()
        if len(raw) > _AVATAR_MAX_BYTES:
            return Result.error(400, f"头像不能超过 {_AVATAR_MAX_BYTES // 1024 // 1024}MB")

        ct = (file.content_type or "").split(";")[0].strip().lower()
        if ct not in _AVATAR_ALLOWED_CT:
            return Result.error(400, "仅支持 JPG、PNG、WebP、GIF 图片")

        ext = _AVATAR_ALLOWED_CT[ct]
        avatar_name = f"u{user_obj.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}{ext}"

        dest = _avatar_storage_dir() / avatar_name
        dest.write_bytes(raw)

        public_path = f"/static/user_data/avatar/{avatar_name}"
        user_obj.avatar = public_path
        db.add(user_obj)
        db.commit()
        db.refresh(user_obj)

        return Result.success(
            200,
            msg="用户头像更新成功",
            data={"avatar_url": public_path, "avatar": public_path},
        )

    @staticmethod
    def download_avatar(filename: str, user: User):
        """按文件名下载头像（仅 static/user_data/avatar 下文件）。"""
        from starlette.responses import FileResponse

        safe = Path(filename or "").name
        if not safe or ".." in filename:
            return Result.error(400, "文件名无效")

        path = _avatar_storage_dir() / safe
        if not path.is_file():
            return Result.error(404, "文件不存在")

        if not safe.startswith(f"u{user.id}_"):
            return Result.error(403, "无权下载该文件")

        return FileResponse(
            str(path),
            filename=safe,
            media_type="application/octet-stream",
        )
