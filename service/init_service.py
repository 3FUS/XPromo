# service/init_service.py
from sqlalchemy.orm import Session
from models.model import SysUser, SysRole, SysUserRole, SysMenu, SysMenuPermission, SysRoleMenuPermission, SegmentsItem
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

        is_initialized = False
        app_logger.info("Initializing system with default admin user and roles")

        admin_role_exists = session.query(SysRole).filter(SysRole.role_code == "admin").count() > 0

        if not admin_role_exists:
            admin_role = SysRole(
                role_code="admin",
                role_description="Administrator Role",
                role_status="active",
                create_time=datetime.now(),
                create_user="init_system"
            )
            session.add(admin_role)

            is_initialized = True

        admin_user_exists = session.query(SysUser).filter(SysUser.user_code == "admin").count() > 0

        if not admin_user_exists:
            hashed_password = bcrypt.hashpw("admin".encode('utf-8'), bcrypt.gensalt())
            admin_user = SysUser(
                user_code="admin",
                user_name="Administrator",
                user_password=hashed_password.decode('utf-8'),
                user_status="active",
                user_email="admin@example.com",
                create_time=datetime.now(),
                create_user="init_system"
            )
            session.add(admin_user)

            # 创建用户角色关联
            user_role = SysUserRole(
                user_code="admin",
                role_code="admin",
                create_time=datetime.now(),
                create_user="init_system"
            )
            session.add(user_role)

        segment_org_pairs = [
            {'segment_id': 50001, 'org_id': '5050'},
            {'segment_id': 50002, 'org_id': '5010'},
            {'segment_id': 50003, 'org_id': '5060'},
            {'segment_id': 50004, 'org_id': '5090'},
            {'segment_id': 50005, 'org_id': '5080'},
            {'segment_id': 50006, 'org_id': '5750'},
            {'segment_id': 50007, 'org_id': '5760'},
            {'segment_id': 50008, 'org_id': '5790'}
        ]

        for pair in segment_org_pairs:
            existing_record = session.query(SegmentsItem).filter(SegmentsItem.segment_id == pair['segment_id']).first()

            if not existing_record:
                segments_item = SegmentsItem(
                    segment_id=pair['segment_id'],
                    name='ALL ITEM',
                    description=f'ALL ITEM ({pair["org_id"]})',
                    segment_status='active',
                    condition_type='and',
                    create_type='condition',
                    public=0,
                    sub_count=0,
                    create_time=datetime.now(),
                    create_user='init_system',
                    org_id=pair['org_id']
                )
                session.add(segments_item)

        if is_initialized:
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
        {"parent_code": None, "menu_code": "Promotion", "menu_name": "Promotion", "menu_url": "/promotion",
         "menu_icon": "promotion"},
        {"parent_code": None, "menu_code": "Item", "menu_name": "Item", "menu_url": "/segment/item",
         "menu_icon": "item"},
        {"parent_code": None, "menu_code": "Location", "menu_name": "Location", "menu_url": "/segment/location",
         "menu_icon": "location"},
        {"parent_code": None, "menu_code": "Customer", "menu_name": "Customer", "menu_url": "/segment/customer",
         "menu_icon": "customer"},
        {"parent_code": None, "menu_code": "User", "menu_name": "User", "menu_url": "/user", "menu_icon": "user"},
        {"parent_code": None, "menu_code": "Role", "menu_name": "Role", "menu_url": "/role", "menu_icon": "role"},
        {"parent_code": None, "menu_code": "configuration", "menu_name": "Configuration", "menu_url": "/config",
         "menu_icon": "config"},
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
