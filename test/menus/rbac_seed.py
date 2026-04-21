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
    """初始化菜单数据 - 使用扁平列表，通过 position 决定位置"""
    menus_data = [
        # 顶部菜单 (position=0)
        {"name": "首页", "path": "/", "component": "views/visual/Large.vue", "icon": "HomeOutlined", "order": 1, "permission_code": "visuals:view", "is_folder": False, "position": 0},
        {"name": "可视化大屏", "path": "/visual/large", "component": "views/visual/Large.vue", "icon": "BarChartOutlined", "order": 2, "permission_code": "visuals:large", "is_folder": False, "position": 0},
        
        # 顶部菜单的子菜单 (position=1)
        {"name": "管理首页", "path": "/admin/home", "component": "views/admin/adminHome.vue", "icon": "DashboardOutlined", "order": 10, "permission_code": "admins:view", "is_folder": False, "position": 1, "parent_path": "/"},
        {"name": "用户管理", "path": "/user/manage", "component": "views/user/UserManage.vue", "icon": "UserOutlined", "order": 20, "permission_code": "users:view", "is_folder": True, "position": 1, "parent_path": "/"},
        {"name": "系统设置", "path": "/admin/settings", "component": "views/admin/MenuTreeManage.vue", "icon": "SettingOutlined", "order": 25, "permission_code": "menus:view", "is_folder": True, "position": 1, "parent_path": "/"},
        {"name": "字典管理", "path": "/admin/dict", "component": "views/admin/adminDictManage.vue", "icon": "BookOutlined", "order": 30, "permission_code": "dicts:view", "is_folder": True, "position": 1, "parent_path": "/"},
        {"name": "数据源管理", "path": "/database/manage", "component": "views/database/SourceManage.vue", "icon": "DatabaseOutlined", "order": 40, "permission_code": "sources:view", "is_folder": False, "position": 1, "parent_path": "/"},
        {"name": "数据同步", "path": "/data/sync", "component": "views/sync/DataSync.vue", "icon": "SyncOutlined", "order": 42, "permission_code": "sync:view", "is_folder": False, "position": 1, "parent_path": "/"},
        {"name": "集群管理", "path": "/cluster/manage", "component": "views/cluster/ClusterManage.vue", "icon": "CloudServerOutlined", "order": 45, "permission_code": "clusters:view", "is_folder": False, "position": 1, "parent_path": "/"},
        {"name": "数据分析", "path": "/analysis", "component": "views/analysis/Overview.vue", "icon": "BarChartOutlined", "order": 48, "permission_code": "analyse:view", "is_folder": True, "position": 1, "parent_path": "/"},
        {"name": "系统监控", "path": "/visual/monitor", "component": "views/monitor/SystemMonitor.vue", "icon": "EyeOutlined", "order": 51, "permission_code": "clusters:view", "is_folder": False, "position": 1, "parent_path": "/"},
        {"name": "模型菜单", "path": "/model/menu", "component": "views/model/Menu.vue", "icon": "AppstoreOutlined", "order": 55, "permission_code": "admins:view", "is_folder": False, "position": 1, "parent_path": "/"},
        
        # 顶部菜单的子菜单详情 (position=1) - 用户管理的子菜单
        {"name": "用户列表", "path": "/user/manage/list", "component": "views/user/UserList.vue", "icon": "UnorderedListOutlined", "order": 1, "permission_code": "users:view", "is_folder": False, "position": 1, "parent_path": "/user/manage"},
        {"name": "新增用户", "path": "/user/manage/add", "component": "views/user/UserAdd.vue", "icon": "PlusOutlined", "order": 2, "permission_code": "users:create", "is_folder": False, "position": 1, "parent_path": "/user/manage"},
        {"name": "用户授权", "path": "/user/manage/auth", "component": "views/user/UserAuth.vue", "icon": "SafetyCertificateOutlined", "order": 3, "permission_code": "users:authorize", "is_folder": False, "position": 1, "parent_path": "/user/manage"},
        {"name": "用户分组", "path": "/user/manage/group", "component": "views/user/UserGroupManage.vue", "icon": "TeamOutlined", "order": 4, "permission_code": "users:view", "is_folder": False, "position": 1, "parent_path": "/user/manage"},
        {"name": "用户详情", "path": "/user/manage/detail", "component": "views/user/UserProfile.vue", "icon": "FileTextOutlined", "order": 5, "permission_code": "users:view", "is_folder": False, "position": 1, "parent_path": "/user/manage"},
        
        # 顶部菜单的子菜单详情 - 系统设置的子菜单
        {"name": "菜单树管理", "path": "/admin/settings/menus", "component": "views/admin/MenuTreeManage.vue", "icon": "MenuOutlined", "order": 1, "permission_code": "menus:view", "is_folder": False, "position": 1, "parent_path": "/admin/settings"},
        {"name": "权限列表", "path": "/admin/settings/permission-list", "component": "views/admin/PermissionList.vue", "icon": "LockOutlined", "order": 2, "permission_code": "permissions:view", "is_folder": False, "position": 1, "parent_path": "/admin/settings"},
        {"name": "用户权限管理", "path": "/admin/settings/user-permission", "component": "views/admin/UserPermissionManage.vue", "icon": "UserSwitchOutlined", "order": 3, "permission_code": "users:authorize", "is_folder": False, "position": 1, "parent_path": "/admin/settings"},
        {"name": "菜单权限管理", "path": "/admin/settings/menu-permission", "component": "views/admin/MenuPermissionManage.vue", "icon": "AppstoreOutlined", "order": 4, "permission_code": "menus:view", "is_folder": False, "position": 1, "parent_path": "/admin/settings"},
        
        # 顶部菜单的子菜单详情 - 字典管理的子菜单
        {"name": "字典分类", "path": "/admin/dict/category", "component": "views/admin/adminDictCategoryList.vue", "icon": "FolderOpenOutlined", "order": 1, "permission_code": "dicts:view", "is_folder": False, "position": 1, "parent_path": "/admin/dict"},
        {"name": "字典列表", "path": "/admin/dict/list", "component": "views/admin/adminDictList.vue", "icon": "UnorderedListOutlined", "order": 2, "permission_code": "dicts:view", "is_folder": False, "position": 1, "parent_path": "/admin/dict"},
        {"name": "字典项列表", "path": "/admin/dict/diction", "component": "views/admin/adminDictionList.vue", "icon": "TagsOutlined", "order": 3, "permission_code": "dicts:view", "is_folder": False, "position": 1, "parent_path": "/admin/dict"},
        
        # 顶部菜单的子菜单详情 - 数据分析的子菜单
        {"name": "数据概览", "path": "/analysis/overview", "component": "views/analysis/Overview.vue", "icon": "DashboardOutlined", "order": 1, "permission_code": "analyse:view", "is_folder": False, "position": 1, "parent_path": "/analysis"},
        {"name": "疾病分析", "path": "/analysis/disease", "component": "views/analysis/DiseaseAnalysis.vue", "icon": "MedicineBoxOutlined", "order": 2, "permission_code": "analyse:view", "is_folder": False, "position": 1, "parent_path": "/analysis"},
        {"name": "问诊趋势", "path": "/analysis/consultation-trend", "component": "views/analysis/ConsultationTrend.vue", "icon": "LineChartOutlined", "order": 3, "permission_code": "analyse:view", "is_folder": False, "position": 1, "parent_path": "/analysis"},
        {"name": "满意度分析", "path": "/analysis/satisfaction", "component": "views/analysis/SatisfactionAnalysis.vue", "icon": "SmileOutlined", "order": 4, "permission_code": "analyse:view", "is_folder": False, "position": 1, "parent_path": "/analysis"},
        {"name": "医院排名", "path": "/analysis/hospital-ranking", "component": "views/analysis/HospitalRanking.vue", "icon": "TrophyOutlined", "order": 5, "permission_code": "analyse:view", "is_folder": False, "position": 1, "parent_path": "/analysis"},
        {"name": "医生排名", "path": "/analysis/doctor-ranking", "component": "views/analysis/DoctorRanking.vue", "icon": "UserOutlined", "order": 6, "permission_code": "analyse:view", "is_folder": False, "position": 1, "parent_path": "/analysis"},
        {"name": "医院等级分析", "path": "/analysis/hospital-level", "component": "views/analysis/HospitalLevelAnalysis.vue", "icon": "BuildOutlined", "order": 7, "permission_code": "analyse:view", "is_folder": False, "position": 1, "parent_path": "/analysis"},
        {"name": "医生职称分析", "path": "/analysis/doctor-title", "component": "views/analysis/DoctorTitleAnalysis.vue", "icon": "IdcardOutlined", "order": 8, "permission_code": "analyse:view", "is_folder": False, "position": 1, "parent_path": "/analysis"},
        {"name": "科室服务分析", "path": "/analysis/department-service", "component": "views/analysis/DepartmentServiceAnalysis.vue", "icon": "ApartmentOutlined", "order": 9, "permission_code": "analyse:view", "is_folder": False, "position": 1, "parent_path": "/analysis"},
        {"name": "价格区间分析", "path": "/analysis/price-range", "component": "views/analysis/PriceRangeAnalysis.vue", "icon": "DollarOutlined", "order": 10, "permission_code": "analyse:view", "is_folder": False, "position": 1, "parent_path": "/analysis"},
        {"name": "城市医疗对比", "path": "/analysis/city-medical", "component": "views/analysis/CityMedicalComparison.vue", "icon": "EnvironmentOutlined", "order": 11, "permission_code": "analyse:view", "is_folder": False, "position": 1, "parent_path": "/analysis"},
        {"name": "区域医疗资源", "path": "/analysis/region-resource", "component": "views/analysis/RegionMedicalResource.vue", "icon": "GlobalOutlined", "order": 12, "permission_code": "analyse:view", "is_folder": False, "position": 1, "parent_path": "/analysis"},
    ]

    # 检查是否已有菜单数据，如果有则不重复初始化
    existing_count = db.query(Menu).count()
    if existing_count > 0:
        logger.info(f"菜单数据已存在（{existing_count}条），跳过初始化")
        return

    logger.info("正在初始化菜单数据...")
    
    for menu_data in menus_data:
        # 确保所有字段都有默认值
        defaults = {
            "name": menu_data.get("name", ""),
            "path": menu_data.get("path", ""),
            "component": menu_data.get("component", ""),
            "icon": menu_data.get("icon", ""),
            "order": menu_data.get("order", 0),
            "permission_code": menu_data.get("permission_code", ""),
            "is_folder": menu_data.get("is_folder", False),
            "position": menu_data.get("position", 0),
            "parent_path": menu_data.get("parent_path"),
            "is_cached": False,
            "is_active": True,
        }
        
        menu = Menu(**defaults)
        db.add(menu)
    
    db.commit()
    logger.info(f"菜单数据初始化完成，共 {len(menus_data)} 个菜单")


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


def init_admin_user(db: Session):
    """初始化管理员用户"""
    logger.info("开始初始化管理员用户...")
    admin_user = db.query(User).filter(User.username == "admin").first()
    if not admin_user:
        logger.info("创建新的管理员用户...")
        admin_user = User(
            username="admin",
            password=Security.get_password_hash("admin123"),
            email="admin@example.com",
            first_name="Admin",
            last_name="User",
            is_active=True,
            is_staff=True,
            is_superuser=True
        )
        db.add(admin_user)
        db.commit()
        db.refresh(admin_user)
        
        # 为管理员用户分配超级管理员角色
        superadmin_role = db.query(Role).filter(Role.code == "superadmin").first()
        if superadmin_role:
            admin_user.roles.append(superadmin_role)
            db.commit()
            logger.info("为管理员用户分配超级管理员角色")
        else:
            logger.info("超级管理员角色不存在")
        
        logger.info("创建超级管理员用户: admin/admin123")
    else:
        logger.info("管理员用户已存在，跳过初始化")
    logger.info("管理员用户初始化完成")


def run_seed(db: Session):
    """执行数据初始化"""
    logger.info("开始 RBAC 数据初始化...")

    permission_codes = init_permissions(db)
    init_roles(db, permission_codes)
    init_menus(db)
    init_admin_user(db)
    migrate_old_users(db)

    logger.info("RBAC 数据初始化完成!")


if __name__ == "__main__":
    from database import SessionLocal
    db = SessionLocal()
    try:
        run_seed(db)
    finally:
        db.close()
