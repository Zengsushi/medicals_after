"""
    RBAC 初始化数据脚本
    用于迁移旧数据和初始化新权限系统
    权限代码统一使用复数形式
"""
import logging
from sqlalchemy.orm import Session
from sqlalchemy import insert, text
from apps.user.models import User, user_role
from apps.user.rbac_models import Role, Permission
from apps.menu.models import Menu
from utils.security import Security

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_permissions(db: Session):
    """初始化权限数据"""
    permissions_data = [
        {"name": "用户查看", "code": "users:view", "module": "用户管理", "description": "查看用户列表"},
        {"name": "用户创建", "code": "users:create", "module": "用户管理", "description": "创建新用户"},
        {"name": "用户编辑", "code": "users:edit", "module": "用户管理", "description": "编辑用户信息"},
        {"name": "用户删除", "code": "users:delete", "module": "用户管理", "description": "删除用户"},
        {"name": "用户授权", "code": "users:authorize", "module": "用户管理", "description": "授权用户"},
        {"name": "重置密码", "code": "users:resetpwd", "module": "用户管理", "description": "重置用户密码"},

        {"name": "角色查看", "code": "roles:view", "module": "角色管理", "description": "查看角色列表"},
        {"name": "角色创建", "code": "roles:create", "module": "角色管理", "description": "创建新角色"},
        {"name": "角色编辑", "code": "roles:edit", "module": "角色管理", "description": "编辑角色"},
        {"name": "角色删除", "code": "roles:delete", "module": "角色管理", "description": "删除角色"},

        {"name": "权限查看", "code": "permissions:view", "module": "权限管理", "description": "查看权限列表"},
        {"name": "权限创建", "code": "permissions:create", "module": "权限管理", "description": "创建新权限"},
        {"name": "权限编辑", "code": "permissions:edit", "module": "权限管理", "description": "编辑权限"},
        {"name": "权限删除", "code": "permissions:delete", "module": "权限管理", "description": "删除权限"},

        {"name": "菜单查看", "code": "menus:view", "module": "菜单管理", "description": "查看菜单列表"},
        {"name": "菜单创建", "code": "menus:create", "module": "菜单管理", "description": "创建新菜单"},
        {"name": "菜单编辑", "code": "menus:edit", "module": "菜单管理", "description": "编辑菜单"},
        {"name": "菜单删除", "code": "menus:delete", "module": "菜单管理", "description": "删除菜单"},

        {"name": "字典查看", "code": "dicts:view", "module": "字典管理", "description": "查看字典"},
        {"name": "字典创建", "code": "dicts:create", "module": "字典管理", "description": "创建字典"},
        {"name": "字典编辑", "code": "dicts:edit", "module": "字典管理", "description": "编辑字典"},
        {"name": "字典删除", "code": "dicts:delete", "module": "字典管理", "description": "删除字典"},

        {"name": "数据源查看", "code": "sources:view", "module": "数据源管理", "description": "查看数据源"},
        {"name": "数据源创建", "code": "sources:create", "module": "数据源管理", "description": "创建数据源"},
        {"name": "数据源编辑", "code": "sources:edit", "module": "数据源管理", "description": "编辑数据源"},
        {"name": "数据源删除", "code": "sources:delete", "module": "数据源管理", "description": "删除数据源"},
        {"name": "数据同步查看", "code": "sync:view", "module": "数据同步", "description": "查看数据同步"},
        {"name": "数据同步创建", "code": "sync:create", "module": "数据同步", "description": "创建数据同步任务"},
        {"name": "数据同步编辑", "code": "sync:edit", "module": "数据同步", "description": "编辑数据同步任务"},
        {"name": "数据同步删除", "code": "sync:delete", "module": "数据同步", "description": "删除数据同步任务"},
        {"name": "数据同步执行", "code": "sync:execute", "module": "数据同步", "description": "执行数据同步任务"},

        {"name": "集群查看", "code": "clusters:view", "module": "集群管理", "description": "查看集群"},
        {"name": "集群创建", "code": "clusters:create", "module": "集群管理", "description": "创建集群"},
        {"name": "集群编辑", "code": "clusters:edit", "module": "集群管理", "description": "编辑集群"},
        {"name": "集群删除", "code": "clusters:delete", "module": "集群管理", "description": "删除集群"},

        {"name": "数据分析查看", "code": "analyse:view", "module": "数据分析", "description": "查看数据分析"},
        {"name": "数据分析查询", "code": "analyse:query", "module": "数据分析", "description": "执行数据查询"},

        {"name": "可视化查看", "code": "visuals:view", "module": "可视化", "description": "查看可视化"},
        {"name": "可视化大屏", "code": "visuals:large", "module": "可视化", "description": "查看大屏"},

        {"name": "超级管理", "code": "admins:manage", "module": "系统管理", "description": "超级管理员权限"},
        {"name": "系统查看", "code": "admins:view", "module": "系统管理", "description": "查看系统"},
    ]

    # 增量更新：只添加缺失的权限，不删除现有权限
    existing_perms = db.query(Permission).all()
    existing_codes = {p.code: p for p in existing_perms}
    
    new_count = 0
    permissions = []
    for perm_data in permissions_data:
        if perm_data["code"] not in existing_codes:
            perm = Permission(**perm_data)
            db.add(perm)
            permissions.append(perm)
            new_count += 1
        else:
            permissions.append(existing_codes[perm_data["code"]])
    
    db.commit()
    if new_count > 0:
        logger.info(f"新增 {new_count} 个权限")
    else:
        logger.info(f"权限数据已完整 ({len(existing_codes)} 条)，无需更新")

    logger.info(f"初始化了 {len(permissions)} 个权限")
    return [p.code for p in permissions]


def init_roles(db: Session, permission_codes: list):
    """初始化角色数据"""
    roles_data = [
        {
            "name": "超级管理员",
            "code": "superadmin",
            "description": "系统超级管理员，拥有所有权限",
            "is_system": True,
            "permission_codes": permission_codes
        },
        {
            "name": "管理员",
            "code": "admin",
            "description": "系统管理员，拥有大部分管理权限",
            "is_system": True,
            "permission_codes": [
                "users:view", "users:create", "users:edit", "users:authorize",
                "roles:view", "menus:view", "menus:create", "menus:edit", "menus:delete",
                "dicts:view", "dicts:create", "dicts:edit",
                "sources:view", "sources:create", "sources:edit",
                "sync:view", "sync:create", "sync:edit", "sync:delete", "sync:execute",
                "clusters:view", "clusters:create", "clusters:edit",
                "analyse:view", "analyse:query",
                "visuals:view", "visuals:large", "admins:view"
            ]
        },
        {
            "name": "普通用户",
            "code": "user",
            "description": "普通用户，可查看和操作自己的数据",
            "is_system": True,
            "permission_codes": [
                "visuals:view", "visuals:large"
            ]
        },
        {
            "name": "访客",
            "code": "guest",
            "description": "访客用户，仅可查看公开信息",
            "is_system": True,
            "permission_codes": [
                "visuals:view"
            ]
        }
    ]

    existing_roles = db.query(Role).all()
    if existing_roles:
        logger.info("角色数据已存在，跳过初始化")
        return

    for role_data in roles_data:
        perm_codes = role_data.pop("permission_codes")
        role = Role(**role_data)

        perms = db.query(Permission).filter(Permission.code.in_(perm_codes)).all()
        role.permissions = perms

        db.add(role)

    db.commit()
    logger.info(f"初始化了 {len(roles_data)} 个角色")


def init_menus(db: Session):
    """初始化菜单数据"""
    menus_data = [
        {"name": "首页", "path": "/", "component": "views/user/UserHome.vue", "icon": "HomeOutlined", "order": 1, "permission_code": "visuals:view"},
        {"name": "管理首页", "path": "/admin/home", "component": "views/admin/adminHome.vue", "icon": "DashboardOutlined", "order": 10, "permission_code": "admins:view"},
        {"name": "用户管理", "path": "/user/manage", "component": "views/user/UserManage.vue", "icon": "UserOutlined", "order": 20, "permission_code": "users:view",
         "children": [
             {"name": "用户列表", "path": "list", "component": "views/user/UserList.vue", "icon": "UnorderedListOutlined", "order": 1, "permission_code": "users:view"},
             {"name": "新增用户", "path": "add", "component": "views/user/UserAdd.vue", "icon": "PlusOutlined", "order": 2, "permission_code": "users:create"},
             {"name": "用户授权", "path": "auth", "component": "views/user/UserAuth.vue", "icon": "SafetyCertificateOutlined", "order": 3, "permission_code": "users:authorize"},
             {"name": "用户分组", "path": "group", "component": "views/user/UserGroupManage.vue", "icon": "TeamOutlined", "order": 4, "permission_code": "users:view"},
         ]},
        {"name": "菜单管理", "path": "/admin/menumanage", "component": "views/admin/MenuTreeManage.vue", "icon": "MenuOutlined", "order": 25, "permission_code": "menus:view"},
        {"name": "字典管理", "path": "/admin/dictmanage", "component": "views/admin/adminDictManage.vue", "icon": "BookOutlined", "order": 30, "permission_code": "dicts:view"},
        {"name": "数据源管理", "path": "/database/manage", "component": "views/database/SourceManage.vue", "icon": "DatabaseOutlined", "order": 40, "permission_code": "sources:view",
         "children": [
             {"name": "数据源列表", "path": "list", "component": "views/database/SourceList.vue", "icon": "UnorderedListOutlined", "order": 1, "permission_code": "sources:view"},
             {"name": "数据同步", "path": "sync", "component": "views/sync/SyncManage.vue", "icon": "SyncOutlined", "order": 2, "permission_code": "sync:view"},
         ]},
        {"name": "集群管理", "path": "/cluster/manage", "component": "views/cluster/ClusterManage.vue", "icon": "CloudServerOutlined", "order": 45, "permission_code": "clusters:view",
         "children": [
             {"name": "集群列表", "path": "list", "component": "views/cluster/ClusterList.vue", "icon": "UnorderedListOutlined", "order": 1, "permission_code": "clusters:view"},
             {"name": "新增集群", "path": "add", "component": "views/cluster/ClusterAdd.vue", "icon": "PlusOutlined", "order": 2, "permission_code": "clusters:create"},
             {"name": "HDFS管理", "path": "hdfs", "component": "views/cluster/HDFSManage.vue", "icon": "FolderOutlined", "order": 3, "permission_code": "clusters:view"},
         ]},
        {"name": "数据分析", "path": "/analyse/manage", "component": "views/analyse/AnalyseManage.vue", "icon": "ExperimentOutlined", "order": 48, "permission_code": "analyse:view",
         "children": [
             {"name": "数据库列表", "path": "databases", "component": "views/analyse/DatabaseList.vue", "icon": "DatabaseOutlined", "order": 1, "permission_code": "analyse:view"},
             {"name": "SQL查询", "path": "query", "component": "views/analyse/SQLQuery.vue", "icon": "ConsoleSqlOutlined", "order": 2, "permission_code": "analyse:query"},
         ]},
        {"name": "可视化大屏", "path": "/visual/large", "component": "views/visual/Large.vue", "icon": "BarChartOutlined", "order": 50, "permission_code": "visuals:large"},
        {"name": "系统管理", "path": "/system/manage", "component": "views/system/SystemManage.vue", "icon": "SettingOutlined", "order": 51, "permission_code": "admins:view",
         "children": [
             {"name": "杜甫监控", "path": "monitor", "component": "views/system/ClusterMonitor.vue", "icon": "LineChartOutlined", "order": 1, "permission_code": "clusters:view"},
         ]},
    ]

    # 检查是否需要强制重新初始化（通过检查关键路径是否存在）
    critical_paths = ["/admin/menumanage", "/admin/settings/menus", "/visual/large", "/cluster/manage", "/analyse/manage", "/database/manage"]
    existing_critical = db.query(Menu).filter(Menu.path.in_(critical_paths)).first()
    
    if not existing_critical:
        # 关键路径不存在 → 删除旧数据并重新初始化
        db.query(Menu).delete()
        db.commit()
        logger.info("检测到旧版菜单结构，正在清理并重新初始化...")
    else:
        # 检查是否有任何菜单记录
        existing_count = db.query(Menu).count()
        if existing_count > 0:
            logger.info(f"菜单数据已存在 ({existing_count} 条)，跳过初始化")
            return

    def add_menus(parent_id, menus):
        for menu_data in menus:
            children = menu_data.pop("children", [])
            
            # 确保所有字段都有默认值
            defaults = {
                "parent_id": parent_id,
                "is_visible": True,
                "is_cached": False,
                "is_active": True,
                **menu_data
            }
            
            menu = Menu(**defaults)
            db.add(menu)
            db.flush()

            if children:
                add_menus(menu.id, children)

    add_menus(None, menus_data)
    db.commit()
    
    total = db.query(Menu).count()
    logger.info(f"✅ 菜单数据初始化完成！共 {total} 条菜单")


def migrate_old_users(db: Session):
    """迁移旧用户数据"""
    users = db.query(User).all()

    guest_role = db.query(Role).filter(Role.code == "guest").first()
    user_role_obj = db.query(Role).filter(Role.code == "user").first()
    admin_role = db.query(Role).filter(Role.code == "admin").first()
    superadmin_role = db.query(Role).filter(Role.code == "superadmin").first()

    for user in users:
        existing = db.execute(
            text("SELECT 1 FROM user_role WHERE user_id = :uid"),
            {"uid": user.id}
        ).fetchall()
        if len(existing) > 0:
            continue

        user_roles = []
        if getattr(user, 'is_superuser', False):
            user_roles.append(superadmin_role or admin_role)
        elif getattr(user, 'is_staff', False):
            user_roles.append(admin_role)
        else:
            user_roles.append(user_role_obj or guest_role)

        for role in user_roles:
            if role:
                db.execute(
                    text("INSERT INTO user_role (user_id, role_id) VALUES (:uid, :rid)"),
                    {"uid": user.id, "rid": role.id}
                )

    db.commit()
    logger.info("旧用户数据迁移完成")


def run_seed(db: Session):
    """执行数据初始化"""
    logger.info("开始 RBAC 数据初始化...")

    permission_codes = init_permissions(db)
    init_roles(db, permission_codes)
    init_menus(db)
    migrate_old_users(db)

    logger.info("RBAC 数据初始化完成!")


if __name__ == "__main__":
    from database import SessionLocal
    db = SessionLocal()
    try:
        run_seed(db)
    finally:
        db.close()
