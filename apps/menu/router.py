"""
    菜单路由
    菜单的增删改查、树形结构等 API
"""
import logging
from typing import List
from fastapi import APIRouter, Depends, Request, HTTPException
from sqlalchemy.orm import Session

from apps.user.models import User
from apps.user.rbac_models import Permission
from apps.menu.models import Menu
from apps.menu.schemas import MenuCreateRequest, MenuUpdateRequest
from apps.menu.service import MenuService
from apps.user.services.auth_service import AuditService
from apps.core.database import get_db
from apps.core import Result
from utils.auth_helpers import require_auth, get_client_info


router = APIRouter(prefix="/api", tags=["menus"])


@router.get("/menus", summary="获取用户菜单树")
async def get_user_menus(
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    获取当前用户的菜单树
    根据用户角色权限自动过滤
    """
    try:
        menus = MenuService.get_user_menus(db, current_user)
        return Result.success(200, "菜单获取成功", {"menus": menus})
    except Exception as e:
        logging.error(f"获取菜单失败: {e}")
        return Result.error(500, str(e))


@router.get("/menus/tree", summary="获取完整菜单树(管理)")
async def get_full_menu_tree(
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    获取完整菜单树 (管理用，包括禁用的菜单)
    """
    try:
        menus = MenuService.get_menu_tree(db)
        return Result.success(200, "菜单树获取成功", {"menus": menus})
    except Exception as e:
        logging.error(f"获取菜单树失败: {e}")
        return Result.error(500, str(e))


@router.get("/menus/permissions/all", summary="获取所有权限列表")
async def get_all_permissions(
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    获取所有权限列表（用于菜单管理中的权限选择）
    """
    try:
        permissions = db.query(Permission).filter(
            Permission.is_active == True
        ).order_by(Permission.module, Permission.code).all()

        return Result.success(200, "权限列表获取成功", [
            {
                "id": p.id,
                "code": p.code,
                "name": p.name,
                "description": p.description,
                "module": p.module
            }
            for p in permissions
        ])
    except Exception as e:
        logging.error(f"获取权限列表失败: {e}")
        return Result.error(500, str(e))


@router.get("/menus/{menu_id}", summary="获取菜单详情")
async def get_menu_detail(
    menu_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """获取菜单详情"""
    if not current_user.has_permission('menu:view') and not current_user.has_permission('admin:view'):
        return Result.error(403, "没有权限访问")

    menu = db.query(Menu).filter(Menu.id == menu_id).first()
    if not menu:
        return Result.error(404, "菜单不存在")

    return Result.success(200, "菜单详情获取成功", 
                         MenuService.menu_to_dict(menu, include_children=False))


@router.post("/menus", summary="创建菜单")
async def create_menu(
    request: Request,
    menu_data: MenuCreateRequest,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    创建新菜单
    需要 menu:create 或 admin:manage 权限
    """
    if not current_user.has_permission('menu:create') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限创建菜单")

    client_info = get_client_info(request)

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
            is_cached=menu_data.is_cached,
            is_active=menu_data.is_active,
            is_folder=menu_data.is_folder,
            position=menu_data.position
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
            ip_address=client_info["ip_address"]
        )

        return Result.success(200, "菜单创建成功", 
                             MenuService.menu_to_dict(menu, include_children=False))
    except Exception as e:
        logging.error(f"创建菜单失败: {e}")
        return Result.error(500, str(e))


@router.patch("/menus/{menu_id}", summary="更新菜单")
async def update_menu(
    request: Request,
    menu_id: int,
    menu_data: MenuUpdateRequest,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    更新菜单
    需要 menu:edit 或 admin:manage 权限
    """
    if not current_user.has_permission('menu:edit') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限更新菜单")

    client_info = get_client_info(request)

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
            resource_id=str(menu_id),
            description=f"更新菜单: {menu.name}",
            ip_address=client_info["ip_address"]
        )

        return Result.success(200, "菜单更新成功", 
                             MenuService.menu_to_dict(menu, include_children=False))
    except Exception as e:
        logging.error(f"更新菜单失败: {e}")
        return Result.error(500, str(e))


@router.patch("/menus/{menu_id}/toggle", summary="启用/禁用菜单")
async def toggle_menu(
    request: Request,
    menu_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    启用/禁用菜单
    需要 menu:edit 或 admin:manage 权限
    """
    if not current_user.has_permission('menu:edit') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限修改菜单")

    client_info = get_client_info(request)

    try:
        menu = db.query(Menu).filter(Menu.id == menu_id).first()
        if not menu:
            return Result.error(404, "菜单不存在")

        # 切换菜单的启用状态
        menu.is_active = not menu.is_active
        db.commit()
        db.refresh(menu)

        action_text = "启用" if menu.is_active else "禁用"
        AuditService.log(
            db, "menu:toggle",
            user_id=current_user.id,
            username=current_user.username,
            resource="menu",
            resource_id=str(menu_id),
            description=f"{action_text}菜单: {menu.name}",
            ip_address=client_info["ip_address"]
        )

        return Result.success(200, f"菜单{action_text}成功", 
                             MenuService.menu_to_dict(menu, include_children=False))
    except Exception as e:
        logging.error(f"切换菜单状态失败: {e}")
        return Result.error(500, str(e))


@router.delete("/menus/{menu_id}", summary="删除菜单")
async def delete_menu(
    request: Request,
    menu_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    删除菜单 (递归删除子菜单)
    需要 menu:delete 或 admin:manage 权限
    """
    if not current_user.has_permission('menu:delete') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限删除菜单")

    client_info = get_client_info(request)

    try:
        menu = db.query(Menu).filter(Menu.id == menu_id).first()
        if not menu:
            return Result.error(404, "菜单不存在")

        all_ids = MenuService.get_child_ids(db, menu_id)

        db.query(Menu).filter(Menu.id.in_(all_ids)).delete(synchronize_session=False)
        db.commit()

        AuditService.log(
            db, "menu:delete",
            user_id=current_user.id,
            username=current_user.username,
            resource="menu",
            resource_id=str(menu_id),
            description=f"删除菜单及其 {len(all_ids) - 1} 个子菜单",
            ip_address=client_info["ip_address"]
        )

        return Result.success(200, f"菜单删除成功 (共删除 {len(all_ids)} 个)", 
                             {"deleted_count": len(all_ids)})
    except Exception as e:
        logging.error(f"删除菜单失败: {e}")
        return Result.error(500, str(e))


from pydantic import BaseModel, Field
from typing import Optional

class BatchUpdateRequest(BaseModel):
    menu_ids: List[int]
    is_active: Optional[bool] = None
    position: Optional[int] = None
    is_folder: Optional[bool] = None
    is_cached: Optional[bool] = None
    permission_code: Optional[str] = None
    parent_id: Optional[int] = None

@router.patch("/menus/batch", summary="批量更新菜单")
async def batch_update_menus(
    request: Request,
    batch_data: BatchUpdateRequest,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    批量更新菜单
    需要 menu:edit 或 admin:manage 权限
    """
    if not current_user.has_permission('menu:edit') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限更新菜单")

    client_info = get_client_info(request)

    try:
        menu_ids = batch_data.menu_ids
        if not menu_ids or len(menu_ids) == 0:
            return Result.error(400, "请提供要更新的菜单ID列表")

        # 构建更新数据
        update_data = {}
        if batch_data.is_active is not None:
            update_data['is_active'] = batch_data.is_active
        if batch_data.position is not None:
            update_data['position'] = batch_data.position
        if batch_data.is_folder is not None:
            update_data['is_folder'] = batch_data.is_folder
        if batch_data.is_cached is not None:
            update_data['is_cached'] = batch_data.is_cached
        if batch_data.permission_code is not None:
            update_data['permission_code'] = batch_data.permission_code
        # 特殊处理 parent_id，允许设置为 null（移动到根目录）
        # 使用 model_fields_set 检查字段是否被显式设置
        if 'parent_id' in batch_data.model_fields_set:
            update_data['parent_id'] = batch_data.parent_id
        
        if not update_data:
            return Result.error(400, "请提供要更新的数据")
        
        # 批量更新菜单
        try:
            updated_count = MenuService.batch_update_menus(db, menu_ids, update_data)
        except ValueError as e:
            return Result.error(400, str(e))

        AuditService.log(
            db, "menu:batch_update",
            user_id=current_user.id,
            username=current_user.username,
            resource="menu",
            resource_id=f"batch_{len(menu_ids)}",
            description=f"批量更新 {len(menu_ids)} 个菜单",
            ip_address=client_info["ip_address"]
        )

        return Result.success(200, f"批量更新成功，共更新 {updated_count} 个菜单", 
                             {"updated_count": updated_count})
    except Exception as e:
        logging.error(f"批量更新菜单失败: {e}")
        return Result.error(500, str(e))
