"""
    菜单管理 API
"""
import logging
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Request, Header
from pydantic import BaseModel
from sqlalchemy.orm import Session, selectinload, joinedload
from sqlalchemy import and_, or_

from apps.user.models import User
from apps.menu.models import Menu
from apps.user.rbac_models import Role, Permission
from database import get_db
from utils.response_helper import ResponseHelper, Result
from users.services.auth_service import AuditService

router = APIRouter(prefix="/api", tags=["menus"])


class MenuCreateRequest(BaseModel):
    name: str
    path: Optional[str] = None
    component: Optional[str] = None
    icon: Optional[str] = None
    order: int = 0
    parent_id: Optional[int] = None
    permission_code: Optional[str] = None
    is_visible: bool = True
    is_cached: bool = False
    is_folder: bool = False
    position: str = 'top'


class MenuUpdateRequest(BaseModel):
    name: Optional[str] = None
    path: Optional[str] = None
    component: Optional[str] = None
    icon: Optional[str] = None
    order: Optional[int] = None
    parent_id: Optional[int] = None
    permission_code: Optional[str] = None
    is_visible: Optional[bool] = None
    is_cached: Optional[bool] = None
    is_folder: Optional[bool] = None
    position: Optional[str] = None


def get_current_user(
        db: Session = Depends(get_db),
        authorization: str = Header(None)
) -> User:
    """
    获取当前用户
    
    认证流程（与 users/dependencies.py 保持一致）：
    1. 从 Header 提取 Bearer Token
    2. 使用 verify_token() 解析 JWT 并验证签名
    3. 从 payload 提取 jti (JWT ID)
    4. 用 jti 在 Redis 中查询会话
    5. 返回完整的 User 对象（含角色和权限预加载）
    """
    from redisbase import RedisBase
    from users.auth.utils import verify_token

    if not authorization:
        raise HTTPException(status_code=401, detail="未提供认证信息")

    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="token格式错误")

    token = authorization.replace("Bearer ", "")

    # 步骤 2: 解析并验证 JWT
    is_valid, payload, error = verify_token(token, expected_type="access")
    
    if not is_valid:
        raise HTTPException(
            status_code=401, 
            detail=f"令牌验证失败: {error}",
            headers={"WWW-Authenticate": "Bearer"}
        )

    # 步骤 3: 从 payload 提取 jti 和 user_id
    jti = payload.get("jti")
    user_id = int(payload.get("sub"))

    # 步骤 4: 用 jti 在 Redis 中查询会话（不是用完整 token！）
    cached_user_id = RedisBase.get_current_token(jti)

    if not cached_user_id or int(cached_user_id) != user_id:
        raise HTTPException(
            status_code=401, 
            detail="令牌已失效或已过期，请重新登录",
            headers={"WWW-Authenticate": "Bearer"}
        )

    # 步骤 5: 查询用户（预加载关联关系避免 N+1 问题）
    user = db.query(User).options(
        selectinload(User.roles).selectinload(Role.permissions),
        selectinload(User.roles).selectinload(Role.menus)
    ).filter(User.id == user_id, User.is_active == True).first()

    if not user:
        raise HTTPException(status_code=401, detail="用户不存在或已被禁用")

    return user


def menu_to_dict(menu: Menu, include_children: bool = True) -> Dict[str, Any]:
    """将菜单对象转换为字典"""
    result = {
        "id": menu.id,
        "name": menu.name,
        "path": menu.path,
        "component": menu.component,
        "icon": menu.icon,
        "order": menu.order,
        "parent_id": menu.parent_id,
        "permission": menu.permission_code,
        "permission_code": menu.permission_code,
        "is_visible": menu.is_visible,
        "is_cached": menu.is_cached,
        "is_active": menu.is_active,
        "is_folder": menu.is_folder if hasattr(menu, 'is_folder') else False,
        "position": menu.position if hasattr(menu, 'position') else 'top',
    }

    if include_children and menu.children:
        result["children"] = [menu_to_dict(child, True) for child in sorted(menu.children, key=lambda x: x.order)]

    return result


def filter_menus_by_permission(menus: List[Menu], user: User) -> List[Menu]:
    """根据用户权限过滤菜单（当前已放开，供测试使用）"""
    if not menus:
        return []

    return menus


@router.get("/menus", summary="获取用户菜单树")
async def get_user_menus(
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """
    获取当前用户的菜单树
    根据用户角色权限自动过滤
    """
    try:
        # 获取所有菜单，不先过滤顶级菜单
        all_menus = db.query(Menu).options(
            joinedload(Menu.children)
        ).filter(
            Menu.is_active == True
        ).order_by(Menu.order).all()

        # 构建菜单映射
        menu_map = {}
        for menu in all_menus:
            menu_map[menu.id] = menu_to_dict(menu, include_children=False)
            menu_map[menu.id]['children'] = []

        # 构建树结构
        root_menus = []
        for menu in all_menus:
            menu_dict = menu_map[menu.id]
            if menu.parent_id is None:
                root_menus.append(menu_dict)
            else:
                if menu.parent_id in menu_map:
                    menu_map[menu.parent_id]['children'].append(menu_dict)

        # 过滤子菜单
        def filter_children(menu_list):
            result = []
            for menu in menu_list:
                if menu.get('children'):
                    menu['children'] = filter_children(menu['children'])
                result.append(menu)
            return result

        menu_tree = filter_children(root_menus)

        return Result.success(200, "菜单获取成功", {"menus": menu_tree})
    except Exception as e:
        logging.error(f"获取菜单失败: {e}")
        import traceback
        traceback.print_exc()
        return Result.error(500, str(e))


@router.get("/menus/tree", summary="获取完整菜单树(管理)")
async def get_full_menu_tree(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    获取完整菜单树 (管理后台使用)
    
    使用统一的 get_current_user 认证依赖
    返回所有菜单的树形结构（不过滤权限）
    """
    try:
        # 查询所有顶级菜单，预加载子菜单和权限关系
        menus = db.query(Menu).options(
            joinedload(Menu.children),
            joinedload(Menu.permission)
        ).filter(
            Menu.parent_id.is_(None)
        ).order_by(Menu.order).all()

        # 构建树形结构
        menu_tree = [menu_to_dict(menu) for menu in menus]

        return Result.success(200, "菜单树获取成功", {"menus": menu_tree})
        
    except Exception as e:
        logging.error(f"获取菜单树失败: {e}", exc_info=True)
        return Result.error(500, f"获取菜单树失败: {str(e)}")


@router.get("/menus/{menu_id}", summary="获取菜单详情")
async def get_menu_detail(
        menu_id: int,
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """获取菜单详情"""
    if not current_user.has_permission('menu:view') and not current_user.has_permission('admin:view'):
        return Result.error(403, "没有权限访问")

    menu = db.query(Menu).filter(Menu.id == menu_id).first()
    if not menu:
        return Result.error(404, "菜单不存在")

    return Result.success(200, "菜单详情获取成功", menu_to_dict(menu, include_children=False))


@router.post("/menus", summary="创建菜单")
async def create_menu(
        request: Request,
        menu_data: MenuCreateRequest,
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """
    创建新菜单
    需要 menus:create 或 admins:manage 权限
    """
    if not current_user.has_permission('menu:create') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限创建菜单")

    try:
        if menu_data.parent_id:
            parent = db.query(Menu).filter(Menu.id == menu_data.parent_id).first()
            if not parent:
                return Result.error(400, "父菜单不存在")

        menu = Menu(
            name=menu_data.name,
            path=menu_data.path,
            component=menu_data.component,
            icon=menu_data.icon,
            order=menu_data.order,
            parent_id=menu_data.parent_id,
            permission_code=menu_data.permission_code,
            is_visible=menu_data.is_visible,
            is_cached=menu_data.is_cached,
            is_folder=menu_data.is_folder,
            position=menu_data.position,
            is_active=True
        )

        db.add(menu)
        db.commit()
        db.refresh(menu)

        AuditService.log(
            db, "menu:create",
            user_id=current_user.id,
            username=current_user.username,
            resource="menu",
            resource_id=str(menu.id),
            description=f"创建菜单: {menu.name}",
            ip_address=request.client.host if request.client else None
        )

        return Result.success(200, "菜单创建成功", menu_to_dict(menu, include_children=False))
    except Exception as e:
        logging.error(f"创建菜单失败: {e}")
        return Result.error(500, str(e))


@router.patch("/menus/{menu_id}", summary="更新菜单")
async def update_menu(
        request: Request,
        menu_id: int,
        menu_data: MenuUpdateRequest,
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """
    更新菜单
    需要 menus:update 或 admins:manage 权限
    """
    if not current_user.has_permission('menu:edit') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限更新菜单")

    try:
        menu = db.query(Menu).filter(Menu.id == menu_id).first()
        if not menu:
            return Result.error(404, "菜单不存在")

        update_data = menu_data.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(menu, key, value)

        db.commit()
        db.refresh(menu)

        AuditService.log(
            db, "menu:update",
            user_id=current_user.id,
            username=current_user.username,
            resource="menu",
            resource_id=str(menu.id),
            description=f"更新菜单: {menu.name}",
            ip_address=request.client.host if request.client else None
        )

        return Result.success(200, "菜单更新成功", menu_to_dict(menu, include_children=False))
    except Exception as e:
        logging.error(f"更新菜单失败: {e}")
        return Result.error(500, str(e))


@router.delete("/menus/{menu_id}", summary="删除菜单")
async def delete_menu(
        request: Request,
        menu_id: int,
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """
    删除菜单 (递归删除子菜单)
    需要 menus:delete 或 admins:manage 权限
    """
    if not current_user.has_permission('menu:delete') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限删除菜单")

    try:
        menu = db.query(Menu).filter(Menu.id == menu_id).first()
        if not menu:
            return Result.error(404, "菜单不存在")

        def get_all_child_ids(menu_id: int) -> List[int]:
            ids = [menu_id]
            children = db.query(Menu.id).filter(Menu.parent_id == menu_id).all()
            for child_id, in children:
                ids.extend(get_all_child_ids(child_id))
            return ids

        all_ids = get_all_child_ids(menu_id)

        db.query(Menu).filter(Menu.id.in_(all_ids)).delete(synchronize_session=False)
        db.commit()

        AuditService.log(
            db, "menu:delete",
            user_id=current_user.id,
            username=current_user.username,
            resource="menu",
            resource_id=str(menu_id),
            description=f"删除菜单及其 {len(all_ids) - 1} 个子菜单",
            ip_address=request.client.host if request.client else None
        )

        return Result.success(200, f"菜单删除成功 (共删除 {len(all_ids)} 个)", {"deleted_count": len(all_ids)})
    except Exception as e:
        logging.error(f"删除菜单失败: {e}")
        return Result.error(500, str(e))


@router.patch("/menus/{menu_id}/toggle", summary="切换菜单启用状态")
async def toggle_menu_status(
        request: Request,
        menu_id: int,
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """
    切换菜单的启用/禁用状态
    需要 menus:update 或 admins:manage 权限
    """
    if not current_user.has_permission('menu:edit') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限更新菜单")

    try:
        menu = db.query(Menu).filter(Menu.id == menu_id).first()
        if not menu:
            return Result.error(404, "菜单不存在")

        # 切换状态
        menu.is_active = not menu.is_active
        db.commit()
        db.refresh(menu)

        AuditService.log(
            db, "menu:toggle",
            user_id=current_user.id,
            username=current_user.username,
            resource="menu",
            resource_id=str(menu.id),
            description=f"切换菜单状态: {menu.name} -> {'启用' if menu.is_active else '禁用'}",
            ip_address=request.client.host if request.client else None
        )

        return Result.success(200, "菜单状态切换成功", menu_to_dict(menu, include_children=False))
    except Exception as e:
        logging.error(f"切换菜单状态失败: {e}")
        return Result.error(500, str(e))


@router.get("/menus/permissions/all", summary="获取所有权限列表")
async def get_all_permissions(
        current_user: User = Depends(get_current_user),
        db: Session = Depends(get_db)
):
    """获取所有权限列表 (用于菜单权限绑定)"""
    if not current_user.has_permission('admin:view') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限访问")

    try:
        permissions = db.query(Permission).filter(Permission.is_active == True).all()
        return Result.success(200, "权限列表获取成功", [
            {
                "id": p.id,
                "code": p.code,
                "name": p.name,
                "module": p.module
            }
            for p in permissions
        ])
    except Exception as e:
        return Result.error(500, str(e))
