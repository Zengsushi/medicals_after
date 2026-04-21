"""
    菜单服务层
    封装菜单相关业务逻辑
"""
from typing import Dict, Any, List, Tuple, Optional
from sqlalchemy.orm import Session, joinedload
from apps.menu.models import Menu
from apps.user.models import User


class MenuService:
    """菜单服务类"""

    @staticmethod
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
            "is_cached": menu.is_cached,
            "is_active": menu.is_active,
            "is_visible": menu.is_active,
            "is_folder": menu.is_folder,
            "position": menu.position,
        }

        if include_children and menu.children:
            result["children"] = [
                MenuService.menu_to_dict(child, True)
                for child in sorted(menu.children, key=lambda x: x.order)
            ]

        return result

    @staticmethod
    def get_all_menus(db: Session) -> List[Menu]:
        """获取所有菜单（含子菜单）"""
        return db.query(Menu).options(
            joinedload(Menu.children)
        ).filter(
            Menu.is_active == True
        ).order_by(Menu.order).all()

    @staticmethod
    def get_menu_tree(db: Session) -> List[Dict[str, Any]]:
        """获取完整菜单树（包括禁用的菜单，用于管理）"""
        # 获取所有菜单，不筛选，包含禁用的
        all_menus = db.query(Menu).order_by(Menu.order).all()

        menu_map = {menu.id: MenuService.menu_to_dict(menu, include_children=False) for menu in all_menus}

        # 初始化 children 数组
        for menu_id in menu_map:
            menu_map[menu_id]['children'] = []

        # 构建树结构
        root_menus = []
        for menu in all_menus:
            menu_dict = menu_map[menu.id]
            if menu.parent_id is None:
                root_menus.append(menu_dict)
            else:
                if menu.parent_id in menu_map:
                    menu_map[menu.parent_id]['children'].append(menu_dict)

        return root_menus

    @staticmethod
    def _collect_visible_menus(db: Session, current_user: User) -> List[Menu]:
        """按角色菜单（及权限兜底）收集可见菜单，并批量补齐激活的父节点。"""
        if current_user.is_superuser:
            menus = (db.query(Menu)
                     .filter(Menu.is_active == True)
                     .order_by(Menu.order)
                     .all())
            return menus

        allowed: Dict[int, Menu] = {}
        for role in current_user.roles or []:
            if not getattr(role, "is_active", True):
                continue
            for menu in role.menus or []:
                if menu.is_active:
                    allowed[menu.id] = menu

        if not allowed:
            perm_codes = current_user.get_permissions()
            if perm_codes:
                for m in (
                        db.query(Menu)
                                .filter(
                            Menu.is_active == True,
                            Menu.permission_code.in_(perm_codes),
                        )
                                .order_by(Menu.order)
                                .all()
                ):
                    allowed[m.id] = m

        to_fetch = {
            m.parent_id
            for m in allowed.values()
            if m.parent_id and m.parent_id not in allowed
        }
        while to_fetch:
            parents = (
                db.query(Menu)
                .filter(
                    Menu.id.in_(to_fetch),
                    Menu.is_active == True,
                )
                .all()
            )
            if not parents:
                break
            for p in parents:
                allowed[p.id] = p
            to_fetch = {
                m.parent_id
                for m in parents
                if m.parent_id and m.parent_id not in allowed
            }

        return sorted(allowed.values(), key=lambda x: x.order)

    @staticmethod
    def _effective_menu_links(menus: List[Menu]) -> Dict[int, Tuple[str, Optional[int]]]:
        """计算展示用 path / parent_id，不修改 ORM 实例。"""
        database_root_id = next((m.id for m in menus if m.path == "/database"), None)
        links: Dict[int, Tuple[str, Optional[int]]] = {}
        for m in menus:
            path = m.path or ""
            if path == "/data/sync":
                path = "/database/sync"
            parent_id: Optional[int] = m.parent_id
            if path == "/database/sync" and database_root_id is not None and parent_id is None:
                parent_id = database_root_id
            links[m.id] = (path, parent_id)
        return links

    @staticmethod
    def _build_user_menu_tree_from_links(
            menus: List[Menu],
            links: Dict[int, Tuple[str, Optional[int]]],
    ) -> List[Dict[str, Any]]:
        menu_map: Dict[int, Dict[str, Any]] = {}
        for m in menus:
            path, parent_id = links[m.id]
            menu_map[m.id] = {
                "id": m.id,
                "name": m.name,
                "path": path,
                "component": m.component,
                "icon": m.icon,
                "order": m.order,
                "parent_id": parent_id,
                "parent_path": m.parent_path,
                "permission": m.permission_code,
                "permission_code": m.permission_code,
                "is_cached": m.is_cached,
                "is_active": m.is_active,
                "is_visible": m.is_active,
                "is_folder": m.is_folder,
                "position": m.position,
                "children": [],
            }

        root_menus: List[Dict[str, Any]] = []
        for m in menus:
            d = menu_map[m.id]
            pid = d["parent_id"]
            # 父节点不在可见集合时作为根展示，避免整棵树被丢弃成 []
            if pid is None or pid not in menu_map:
                root_menus.append(d)
            else:
                menu_map[pid]["children"].append(d)

        return root_menus

    @staticmethod
    def get_user_menus(db: Session, current_user: User) -> List[Dict[str, Any]]:
        """获取用户可见菜单树（按角色菜单关联过滤，并补齐父节点）"""
        menus = MenuService._collect_visible_menus(db, current_user)
        links = MenuService._effective_menu_links(menus)
        return MenuService._build_user_menu_tree_from_links(menus, links)

    @staticmethod
    def get_child_ids(db: Session, menu_id: int) -> List[int]:
        """递归获取所有子菜单 ID（用于删除）"""
        ids = [menu_id]
        children = db.query(Menu.id).filter(Menu.parent_id == menu_id).all()
        for child_id, in children:
            ids.extend(MenuService.get_child_ids(db, child_id))
        return ids

    @staticmethod
    def batch_update_menus(db: Session, menu_ids: List[int], update_data: Dict[str, Any]) -> int:
        """批量更新菜单

        Args:
            db: 数据库会话
            menu_ids: 要更新的菜单ID列表
            update_data: 要更新的数据

        Returns:
            int: 更新的菜单数量
        """
        if not menu_ids or not update_data:
            return 0

        # 如果更新包含父级菜单，需要进行额外检查
        if 'parent_id' in update_data:
            new_parent_id = update_data['parent_id']

            # 检查父级菜单是否存在
            if new_parent_id:
                parent_menu = db.query(Menu).filter(Menu.id == new_parent_id).first()
                if not parent_menu:
                    raise ValueError(f"父级菜单 {new_parent_id} 不存在")

                # 检查是否会形成循环引用
                # 循环引用：子菜单成为父菜单的父级
                if new_parent_id in menu_ids:
                    raise ValueError("不能将菜单批量设置为自身或同级菜单的子菜单")

                # 检查父级菜单是否是当前菜单的子菜单
                # 避免循环引用
                for menu_id in menu_ids:
                    # 检查新父级是否是当前菜单的子菜单
                    child_ids = MenuService.get_child_ids(db, menu_id)
                    if new_parent_id in child_ids:
                        raise ValueError(f"不能将菜单 {menu_id} 设置为其子菜单 {new_parent_id} 的子菜单，会形成循环引用")

        # 批量更新菜单
        updated = db.query(Menu).filter(Menu.id.in_(menu_ids)).update(
            update_data,
            synchronize_session=False
        )

        db.commit()
        return updated
