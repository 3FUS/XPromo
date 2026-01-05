# service/init_service.py
from sqlalchemy.orm import Session
from models.model import SysUser, SysRole, SysUserRole, SysMenu, SysMenuPermission, SysRoleMenuPermission
from utils.logger import app_logger
import bcrypt
from datetime import datetime


def init_system_data(session: Session):
    """
    初始化系统基础数据
    - 检查用户表和角色表是否为空
    - 如果为空，创建admin用户、角色和权限
    """
    try:
        # 检查用户表是否为空
        user_count = session.query(SysUser).count()
        role_count = session.query(SysRole).count()

        if user_count > 0 or role_count > 0:
            app_logger.info("System already initialized, skipping initialization")
            return

        app_logger.info("Initializing system with default admin user and roles")

        # 创建admin角色
        admin_role = SysRole(
            role_code="admin",
            role_description="Administrator Role",
            role_status="active",
            create_time=datetime.now(),
            create_user="system"
        )
        session.add(admin_role)

        # 创建admin用户
        hashed_password = bcrypt.hashpw("admin".encode('utf-8'), bcrypt.gensalt())
        admin_user = SysUser(
            user_code="admin",
            user_name="Administrator",
            user_password=hashed_password.decode('utf-8'),
            user_status="active",
            user_email="admin@example.com",
            create_time=datetime.now(),
            create_user="system"
        )
        session.add(admin_user)

        # 创建用户角色关联
        user_role = SysUserRole(
            user_code="admin",
            role_code="admin",
            create_time=datetime.now(),
            create_user="system"
        )
        session.add(user_role)

        # 初始化菜单数据
        init_menu_data(session)

        # 为admin角色分配所有菜单权限
        assign_admin_permissions(session)

        session.commit()
        app_logger.info("System initialization completed successfully")

    except Exception as e:
        session.rollback()
        app_logger.error(f"Error initializing system data: {str(e)}")
        raise


def init_menu_data(session: Session):
    """初始化菜单数据"""
    # 定义菜单结构 - 基于实际数据库数据
    menus = [
        # 顶级菜单
        {"parent_code": None, "menu_code": "Promotion", "menu_name": "Promotion", "menu_url": "/promotion", "menu_icon": "promotion"},
        {"parent_code": None, "menu_code": "Item", "menu_name": "Item", "menu_url": "/segment/item", "menu_icon": "item"},
        {"parent_code": None, "menu_code": "Location", "menu_name": "Location", "menu_url": "/segment/location", "menu_icon": "location"},
        {"parent_code": None, "menu_code": "Customer", "menu_name": "Customer", "menu_url": "/segment/customer", "menu_icon": "customer"},
        {"parent_code": None, "menu_code": "User", "menu_name": "User", "menu_url": "/user", "menu_icon": "user"},
        {"parent_code": None, "menu_code": "Role", "menu_name": "Role", "menu_url": "/role", "menu_icon": "role"},
        {"parent_code": None, "menu_code": "configuration", "menu_name": "Configuration", "menu_url": "/config", "menu_icon": "config"},
    ]

    # 添加菜单
    for menu_data in menus:
        existing_menu = session.query(SysMenu).filter(SysMenu.menu_code == menu_data["menu_code"]).first()
        if not existing_menu:
            menu = SysMenu(
                parent_code=menu_data["parent_code"],
                menu_code=menu_data["menu_code"],
                menu_name=menu_data["menu_name"],
                menu_url=menu_data["menu_url"],
                menu_icon=menu_data["menu_icon"]
            )
            session.add(menu)

    # 定义菜单权限类型 - 基于实际数据库数据
    permission_mapping = {
        "Promotion": ["Copy", "Delete", "Edit", "Export", "View"],
        "Item": ["Copy", "Delete", "Edit", "Export", "View"],
        "Location": ["Copy", "Delete", "Edit", "View"],
        "Customer": ["Copy", "Delete", "Edit", "View"],
        "User": ["Delete", "Edit"],
        "Role": ["Delete", "Edit"],
        "configuration": ["Edit", "View"]
    }

    # 为每个菜单创建对应的权限类型
    for menu_code, permissions in permission_mapping.items():
        for perm_type in permissions:
            existing_perm = session.query(SysMenuPermission).filter(
                SysMenuPermission.menu_code == menu_code,
                SysMenuPermission.permission_type == perm_type
            ).first()

            if not existing_perm:
                menu_perm = SysMenuPermission(
                    menu_code=menu_code,
                    permission_type=perm_type,
                    create_time=datetime.now(),
                    create_user="system"
                )
                session.add(menu_perm)



def assign_admin_permissions(session: Session):
    """为admin角色分配所有权限"""
    # 获取所有菜单权限
    all_permissions = session.query(SysMenuPermission).all()

    for perm in all_permissions:
        role_menu_perm = SysRoleMenuPermission(
            role_code="admin",
            menu_code=perm.menu_code,
            permission_type=perm.permission_type,
            create_time=datetime.now(),
            create_user="system"
        )
        session.add(role_menu_perm)
