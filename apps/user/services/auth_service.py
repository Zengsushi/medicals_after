"""
    认证服务
    完整的登录/刷新/登出流程
"""
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import desc

from apps.user.models import User
from apps.user.rbac_models import Role
from apps.user.security_models import DeviceSession, AuditLog, RefreshToken, PasswordResetToken, AUDIT_ACTIONS
from apps.user.auth.utils import (
    generate_jti,
    generate_device_fingerprint,
    extract_device_info,
    parse_location_from_ip,
    create_access_token,
    create_refresh_token,
    create_password_reset_token,
    verify_token,
    hash_token,
    verify_password_strength
)
from utils.security import Security
from utils.redisbase import RedisBase

logger = logging.getLogger(__name__)

MAX_CONCURRENT_SESSIONS = 3


class AuditService:
    """审计日志服务"""

    @staticmethod
    def log(
            db: Session,
            action: str,
            user_id: int = None,
            username: str = None,
            resource: str = None,
            resource_id: str = None,
            description: str = None,
            ip_address: str = None,
            user_agent: str = None,
            device_fingerprint: str = None,
            status: str = "success",
            error_message: str = None,
            request_data: dict = None
    ):
        """记录审计日志"""
        try:
            audit = AuditLog(
                user_id=user_id,
                username=username or (f"user_{user_id}" if user_id else "anonymous"),
                action=action,
                resource=resource,
                resource_id=resource_id,
                description=description or AUDIT_ACTIONS.get(action, action),
                ip_address=ip_address,
                user_agent=user_agent,
                device_fingerprint=device_fingerprint,
                status=status,
                error_message=error_message,
                request_data=request_data
            )
            db.add(audit)
            db.commit()
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")


class SessionService:
    """会话管理服务"""

    @staticmethod
    def create_session(
            db: Session,
            user: User,
            device_fingerprint: str,
            ip_address: str,
            user_agent: str = None,
            login_location: str = None
    ) -> DeviceSession:
        """创建设备会话"""
        jti = generate_jti()
        device_info = extract_device_info(user_agent)

        if not login_location:
            login_location = parse_location_from_ip(ip_address)

        session = DeviceSession(
            user_id=user.id,
            jti=jti,
            device_fingerprint=device_fingerprint,
            user_agent=user_agent,
            ip_address=ip_address,
            login_location=login_location,
            device_type=device_info.get("device_type"),
            browser=device_info.get("browser"),
            os=device_info.get("os"),
            is_active=True,
            last_active_at=datetime.utcnow()
        )
        db.add(session)
        db.commit()
        db.refresh(session)

        RedisBase.set_with_expiry(f"auth:session:{jti}", user.id, expire=60 * 60 * 24 * 7)

        return session

    @staticmethod
    def get_user_sessions(db: Session, user_id: int) -> List[DeviceSession]:
        """获取用户所有活跃会话"""
        return db.query(DeviceSession).filter(
            DeviceSession.user_id == user_id,
            DeviceSession.is_active == True
        ).order_by(desc(DeviceSession.last_active_at)).all()

    @staticmethod
    def get_session_by_fingerprint(db: Session, user_id: int, device_fingerprint: str) -> Optional[DeviceSession]:
        """根据设备指纹获取用户的活跃会话"""
        return db.query(DeviceSession).filter(
            DeviceSession.user_id == user_id,
            DeviceSession.device_fingerprint == device_fingerprint,
            DeviceSession.is_active == True
        ).first()

    @staticmethod
    def revoke_session(db: Session, session_id: int, user_id: int = None) -> bool:
        """撤销指定会话"""
        query = db.query(DeviceSession).filter(DeviceSession.id == session_id)
        if user_id:
            query = query.filter(DeviceSession.user_id == user_id)

        session = query.first()
        if session:
            session.is_active = False
            session.jti = f"revoked_{session.jti}"
            db.commit()

            RedisBase.delete_session(session.jti)
            return True
        return False

    @staticmethod
    def revoke_all_sessions(db: Session, user_id: int, except_session_id: int = None) -> int:
        """撤销用户所有会话"""
        query = db.query(DeviceSession).filter(
            DeviceSession.user_id == user_id,
            DeviceSession.is_active == True
        )

        if except_session_id:
            query = query.filter(DeviceSession.id != except_session_id)

        sessions = query.all()
        count = 0
        for session in sessions:
            session.is_active = False
            session.jti = f"revoked_{session.jti}"
            RedisBase.delete_session(session.jti)
            count += 1

        db.commit()
        return count

    @staticmethod
    def enforce_max_sessions(db: Session, user_id: int, max_sessions: int = MAX_CONCURRENT_SESSIONS) -> Optional[
        DeviceSession]:
        """强制最大会话数限制，踢出最早的会话"""
        sessions = SessionService.get_user_sessions(db, user_id)

        if len(sessions) >= max_sessions:
            oldest_session = sessions[-1]
            SessionService.revoke_session(db, oldest_session.id, user_id)
            return oldest_session

        return None

    @staticmethod
    def update_session_activity(db: Session, jti: str):
        """更新会话活跃时间"""
        session = db.query(DeviceSession).filter(DeviceSession.jti == jti).first()
        if session:
            session.last_active_at = datetime.utcnow()
            db.commit()


class AuthService:
    """认证服务"""

    @staticmethod
    def login(
            db: Session,
            username: str,
            password: str,
            ip_address: str,
            user_agent: str = None,
            device_fingerprint: str = None
    ) -> Dict[str, Any]:
        """
        用户登录
        返回: {
            success: bool,
            message: str,
            data: {
                user, access_token, refresh_token, permissions, menus, role
            } | None
        }
        """
        AuditService.log(
            db, "LOGIN", username=username,
            ip_address=ip_address, user_agent=user_agent
        )

        user = db.query(User).options(
            selectinload(User.roles).selectinload(Role.permissions),
            selectinload(User.roles).selectinload(Role.menus)
        ).filter(User.username == username).first()

        if not user or user.is_deleted:
            AuditService.log(
                db, "LOGIN_FAILED", username=username,
                ip_address=ip_address, user_agent=user_agent,
                status="failed", error_message="User not found"
            )
            return {"success": False, "message": "用户名或密码错误", "data": None}

        if not user.is_active:
            AuditService.log(
                db, "LOGIN_FAILED", username=username,
                ip_address=ip_address, user_agent=user_agent,
                status="failed", error_message="User disabled"
            )
            return {"success": False, "message": "账号已被停用", "data": None}

        if not Security.verify_password(password, user.password):
            AuditService.log(
                db, "LOGIN_FAILED", username=username,
                ip_address=ip_address, user_agent=user_agent,
                status="failed", error_message="Wrong password"
            )
            return {"success": False, "message": "用户名或密码错误", "data": None}

        if not device_fingerprint:
            device_fingerprint = generate_device_fingerprint(
                user_agent=user_agent,
                ip_address=ip_address
            )

        existing_session = SessionService.get_session_by_fingerprint(db, user.id, device_fingerprint)
        if existing_session and existing_session.is_active:
            existing_session.last_active_at = datetime.utcnow()
            db.commit()

            jti = generate_jti()
            access_token, _ = create_access_token(
                user_id=user.id,
                username=user.username,
                jti=jti,
                device_fingerprint=device_fingerprint,
                role=user.get_primary_role().code if user.get_primary_role() else "guest"
            )
            RedisBase.set_with_expiry(f"token:{jti}", user.id, expire=60 * 60 * 24 * 7)

            refresh_token, refresh_jti, refresh_expire = create_refresh_token(
                user_id=user.id,
                username=user.username,
                device_fingerprint=device_fingerprint
            )

            rt_record = RefreshToken(
                user_id=user.id,
                device_session_id=existing_session.id,
                jti=refresh_jti,
                token_hash=hash_token(refresh_token),
                device_fingerprint=device_fingerprint,
                ip_address=ip_address,
                expires_at=refresh_expire
            )
            db.add(rt_record)
            db.commit()

            user.last_login = datetime.utcnow()
            user.last_login_ip = ip_address
            db.commit()

            AuditService.log(
                db, "LOGIN", user_id=user.id, username=username,
                ip_address=ip_address, user_agent=user_agent,
                device_fingerprint=device_fingerprint,
                description=f"登录成功（设备已存在）"
            )

            return {
                "success": True,
                "message": "登录成功",
                "data": {
                    "user": AuthService._build_user_response(user),
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "permissions": user.get_permissions(),
                    "menus": AuthService._build_menus_response(user),
                    "role": user.get_primary_role().code if user.get_primary_role() else "guest"
                }
            }

        SessionService.revoke_all_sessions(db, user.id)

        session = SessionService.create_session(
            db, user, device_fingerprint, ip_address, user_agent
        )

        jti = generate_jti()
        access_token, _ = create_access_token(
            user_id=user.id,
            username=user.username,
            jti=jti,
            device_fingerprint=device_fingerprint,
            role=user.get_primary_role().code if user.get_primary_role() else "guest"
        )
        RedisBase.set_with_expiry(f"token:{jti}", user.id, expire=60 * 60 * 24 * 7)

        refresh_token, refresh_jti, refresh_expire = create_refresh_token(
            user_id=user.id,
            username=user.username,
            device_fingerprint=device_fingerprint
        )

        rt_record = RefreshToken(
            user_id=user.id,
            device_session_id=session.id,
            jti=refresh_jti,
            token_hash=hash_token(refresh_token),
            device_fingerprint=device_fingerprint,
            ip_address=ip_address,
            expires_at=refresh_expire
        )
        db.add(rt_record)
        db.commit()

        user.last_login = datetime.utcnow()
        user.last_login_ip = ip_address
        db.commit()

        AuditService.log(
            db, "LOGIN", user_id=user.id, username=username,
            ip_address=ip_address, user_agent=user_agent,
            device_fingerprint=device_fingerprint,
            description=f"登录成功，设备: {session.device_type or 'unknown'}"
        )

        result_data = {
            "user": AuthService._build_user_response(user),
            "access_token": access_token,
            "refresh_token": refresh_token,
            "permissions": user.get_permissions(),
            "menus": AuthService._build_menus_response(user),
            "role": user.get_primary_role().code if user.get_primary_role() else "guest"
        }

        return {"success": True, "message": "登录成功", "data": result_data}

    @staticmethod
    def refresh(
            db: Session,
            refresh_token: str,
            ip_address: str,
            user_agent: str = None
    ) -> Dict[str, Any]:
        """
        刷新令牌 (Refresh Token Rotation)
        使用刷新令牌获取新的 access_token
        """
        try:
            is_valid, payload, error = verify_token(refresh_token, expected_type="refresh")
            if not is_valid:
                return {"success": False, "message": error, "data": None}

            jti = payload.get("jti")
            sub = payload.get("sub") or payload.get("user_id")
            if sub is None:
                return {"success": False, "message": "刷新令牌缺少用户标识", "data": None}
            try:
                user_id = int(sub)
            except (TypeError, ValueError):
                return {"success": False, "message": "刷新令牌用户标识无效", "data": None}

            rt_record = db.query(RefreshToken).filter(
                RefreshToken.jti == jti,
                RefreshToken.is_revoked == False,
                RefreshToken.is_used == False
            ).first()

            if not rt_record or rt_record.is_expired:
                AuditService.log(
                    db, "LOGIN_FAILED",
                    user_id=user_id, ip_address=ip_address,
                    status="failed", error_message="Invalid refresh token"
                )
                return {"success": False, "message": "刷新令牌无效或已过期", "data": None}

            rt_record.is_used = True
            rt_record.used_at = datetime.utcnow()

            # 从数据库重新获取用户以获取最新的角色信息（包含菜单和权限）
            user = db.query(User).options(
                selectinload(User.roles).selectinload(Role.permissions),
                selectinload(User.roles).selectinload(Role.menus)
            ).filter(User.id == user_id).first()

            if not user:
                return {"success": False, "message": "用户不存在", "data": None}

            new_jti = generate_jti()
            primary_role = None
            try:
                primary_role = user.get_primary_role().code if user.get_primary_role() else None
            except Exception:
                primary_role = None

            new_access_token, _ = create_access_token(
                user_id=user_id,
                username=payload.get("username"),
                jti=new_jti,
                device_fingerprint=payload.get("device_fingerprint"),
                role=primary_role
            )
            RedisBase.set_with_expiry(f"token:{new_jti}", user_id, expire=60 * 60 * 24 * 7)

            new_refresh_token, new_rt_jti, new_expire = create_refresh_token(
                user_id=user_id,
                username=payload.get("username"),
                device_fingerprint=payload.get("device_fingerprint")
            )

            new_rt_record = RefreshToken(
                user_id=user_id,
                device_session_id=rt_record.device_session_id,
                jti=new_rt_jti,
                token_hash=hash_token(new_refresh_token),
                device_fingerprint=rt_record.device_fingerprint,
                ip_address=ip_address,
                expires_at=new_expire
            )
            db.add(new_rt_record)

            SessionService.update_session_activity(db, new_jti)

            db.commit()

            # 构建完整的响应数据，包含用户信息、菜单和权限（和登录接口一致）
            user_response = AuthService._build_user_response(user)
            menus_response = AuthService._build_menus_response(user)
            permissions_response = user.get_permissions()

            primary_role_obj = user.get_primary_role()
            role_response = primary_role_obj.code if primary_role_obj else "guest"

            return {
                "success": True,
                "message": "令牌刷新成功",
                "data": {
                    "access_token": new_access_token,
                    "refresh_token": new_refresh_token,
                    "user": user_response,
                    "menus": menus_response,
                    "permissions": permissions_response,
                    "role": role_response
                }
            }
        except Exception as e:
            logger.error(f"刷新令牌失败: {e}", exc_info=True)
            db.rollback()
            return {"success": False, "message": f"刷新令牌失败: {str(e)}", "data": None}

    @staticmethod
    def logout(
            db: Session,
            user_id: int,
            jti: str = None,
            revoke_all: bool = False,
            ip_address: str = None,
            user_agent: str = None
    ) -> Dict[str, Any]:
        """
        用户登出
        """
        if revoke_all:
            count = SessionService.revoke_all_sessions(db, user_id)
            AuditService.log(
                db, "LOGOUT", user_id=user_id,
                ip_address=ip_address, user_agent=user_agent,
                description=f"撤销所有会话，共{count}个"
            )
            return {"success": True, "message": f"已撤销所有会话", "data": {"count": count}}

        if jti:
            session = db.query(DeviceSession).filter(
                DeviceSession.jti == jti,
                DeviceSession.user_id == user_id
            ).first()

            if session:
                SessionService.revoke_session(db, session.id, user_id)

                RefreshToken.query.filter(
                    RefreshToken.device_session_id == session.id
                ).update({"is_revoked": True, "revoked_at": datetime.utcnow()})

        RedisBase.delete_token(jti)

        AuditService.log(
            db, "LOGOUT", user_id=user_id,
            ip_address=ip_address, user_agent=user_agent
        )

        return {"success": True, "message": "登出成功", "data": None}

    @staticmethod
    def get_user_devices(db: Session, user_id: int) -> List[Dict[str, Any]]:
        """获取用户登录设备列表"""
        sessions = SessionService.get_user_sessions(db, user_id)
        return [
            {
                "id": s.id,
                "device_type": s.device_type,
                "browser": s.browser,
                "os": s.os,
                "ip_address": s.ip_address,
                "login_location": s.login_location,
                "last_active_at": s.last_active_at.isoformat() if s.last_active_at else None,
                "created_at": s.created_at.isoformat() if s.created_at else None,
                "is_current": False
            }
            for s in sessions
        ]

    @staticmethod
    def logout_device(db: Session, user_id: int, session_id: int) -> Dict[str, Any]:
        """登出指定设备"""
        if SessionService.revoke_session(db, session_id, user_id):
            AuditService.log(
                db, "SESSION_REVOKED", user_id=user_id,
                description=f"撤销设备会话 {session_id}"
            )
            return {"success": True, "message": "设备已登出", "data": None}
        return {"success": False, "message": "会话不存在", "data": None}

    @staticmethod
    def _build_user_response(user: User) -> Dict[str, Any]:
        """构建用户响应数据"""
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
            "role": primary_role.code if primary_role else "guest",
            "role_name": primary_role.name if primary_role else "访客",
            "introduce": user.introduce
        }

    @staticmethod
    def _build_menus_response(user: User) -> List[Dict[str, Any]]:
        """构建菜单响应数据 - 完整的 Vue Router 风格菜单"""
        menus = user.get_menus()

        # 按 order 字段排序菜单
        menus = sorted(menus, key=lambda m: m.order or 0)

        def menu_to_route(menu) -> Dict[str, Any]:
            """转换为 Vue Router 风格的路由结构"""
            route_dict = {
                "id": menu.id,
                "name": menu.name,
                "path": menu.path,
                "component": menu.component,
                "redirect": None,
                "meta": {
                    "title": menu.name,
                    "icon": menu.icon,
                    "isCache": not menu.is_cached,
                    "isHide": not menu.is_active,
                    "permission": menu.permission_code,
                    "order": menu.order or 0,
                    "isFolder": menu.is_folder if hasattr(menu, 'is_folder') else False,
                    "position": menu.position if hasattr(menu, 'position') else 0
                },
                # 保留原始字段供其他用途
                "icon": menu.icon,
                "order": menu.order or 0,
                "parentId": menu.parent_id,
                "parent_id": menu.parent_id,
                "permissionCode": menu.permission_code,
                "permission_code": menu.permission_code,
                "is_visible": menu.is_active,
                "is_cached": menu.is_cached,
                "is_active": menu.is_active,
                "is_folder": menu.is_folder if hasattr(menu, 'is_folder') else False,
                "position": menu.position if hasattr(menu, 'position') else 0
            }
            return route_dict

        menu_tree = []
        menu_map = {}

        # 先构建所有菜单的字典
        for menu in menus:
            route_dict = menu_to_route(menu)
            menu_map[menu.id] = route_dict
            route_dict["children"] = []

        # 构建树形结构
        for menu in menus:
            if menu.parent_id is None:
                menu_tree.append(menu_map[menu.id])
            else:
                parent = menu_map.get(menu.parent_id)
                if parent:
                    parent["children"].append(menu_map[menu.id])
                    # 如果有子菜单，设置为目录类型
                    parent["redirect"] = parent["path"]
                    parent["meta"]["isFolder"] = True

        # 过滤掉空 children 数组
        def clean_empty_children(menu_list):
            for menu in menu_list:
                if menu["children"]:
                    clean_empty_children(menu["children"])
                else:
                    # 没有子菜单时删除 children 字段
                    del menu["children"]
            return menu_list

        menu_tree = clean_empty_children(menu_tree)
        return menu_tree


class PasswordService:
    """密码服务"""

    @staticmethod
    def reset_password(
            db: Session,
            token: str,
            new_password: str,
            ip_address: str
    ) -> Dict[str, Any]:
        """
        重置密码
        """
        is_valid, payload, error = verify_token(token, expected_type="password_reset")
        if not is_valid:
            return {"success": False, "message": error, "data": None}

        jti = payload.get("jti")

        prt = db.query(PasswordResetToken).filter(
            PasswordResetToken.jti == jti,
            PasswordResetToken.is_used == False
        ).first()

        if not prt or prt.is_expired:
            return {"success": False, "message": "重置令牌无效或已过期", "data": None}

        is_strong, error_msg = verify_password_strength(new_password)
        if not is_strong:
            return {"success": False, "message": error_msg, "data": None}

        user = db.query(User).filter(User.id == prt.user_id).first()
        if not user:
            return {"success": False, "message": "用户不存在", "data": None}

        user.password = Security.get_password_hash(new_password)

        prt.is_used = True
        prt.used_at = datetime.utcnow()

        SessionService.revoke_all_sessions(db, user.id)

        AuditService.log(
            db, "PASSWORD_RESET", user_id=user.id,
            ip_address=ip_address,
            description="密码已重置"
        )

        db.commit()

        return {"success": True, "message": "密码重置成功", "data": None}

    @staticmethod
    def create_password_reset_token(
            db: Session,
            username: str,
            email: str,
            ip_address: str
    ) -> Dict[str, Any]:
        """
        创建密码重置令牌
        """
        user = db.query(User).filter(User.username == username).first()

        if not user or user.email != email:
            return {"success": False, "message": "用户名或邮箱不匹配", "data": None}

        token, jti, expire = create_password_reset_token(user.id)

        prt = PasswordResetToken(
            user_id=user.id,
            jti=jti,
            token_hash=hash_token(token),
            ip_address=ip_address,
            expires_at=expire
        )
        db.add(prt)
        db.commit()

        return {
            "success": True,
            "message": "重置链接已生成",
            "data": {
                "reset_token": token,
                "expires_in": 3600
            }
        }
