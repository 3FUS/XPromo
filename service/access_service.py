from sqlalchemy import func
from sqlalchemy import func, literal, TypeDecorator
from sqlalchemy.types import CHAR, String
from models.model import SysMenu, SysRole, SysUser, SysUserRole, SysRoleMenuPermission, SysMenuPermission, \
    LOC_ORG_HIERARCHY, SysRoleOrgPermission

import bcrypt
from datetime import datetime
from sqlalchemy.orm import Session
from service.utils import resolve_permissions_with_inheritance

from utils.logger import app_logger


class FixedChar(TypeDecorator):
    impl = CHAR

    def __init__(self, length=1):
        self.length = length
        super().__init__(length=length)


async def verify_password(session: Session, user_code: str, user_password: str) -> bool:
    result = session.query(SysUser.user_password).filter(
        SysUser.user_code == user_code,
        SysUser.user_status == 'active'
    ).first()
    if not result:
        return False
    hashed_password = result[0]
    return bcrypt.checkpw(user_password.encode('utf-8'), hashed_password.encode('utf-8'))


async def get_sys_user_configuration(session: Session, user_code: str) -> dict:
    result = session.query(SysUser.configuration, SysUser.user_name, SysUser.user_code).filter(
        SysUser.user_code == user_code).first()
    if not result:
        return {"configuration": 0, "user_name": ""}
    return {"configuration": result[0], "user_name": result[1], "user_code": result[2]}


async def create_sys_user(session: Session, User):
    hashed_password = bcrypt.hashpw(User.user_password.encode('utf-8'), bcrypt.gensalt())
    new_user = SysUser(
        user_code=User.user_code,
        user_name=User.user_name,
        user_password=hashed_password.decode('utf-8'),
        user_email=User.user_email,
        user_status=User.user_status,
        create_time=datetime.now(),
        create_user=User.create_user
    )
    session.add(new_user)
    session.commit()
    session.refresh(new_user)
    return new_user


async def change_user_password(session: Session, user_code: str, old_password: str, new_password: str):
    """
    修改用户密码
    :param session: 数据库会话
    :param user_code: 用户代码
    :param old_password: 旧密码
    :param new_password: 新密码
    :return: 是否修改成功
    """
    # 验证旧密码是否正确
    if not await verify_password(session, user_code, old_password):
        raise ValueError("原密码错误")

    # 获取用户对象
    user = session.query(SysUser).filter(SysUser.user_code == user_code).first()
    if not user:
        raise ValueError("用户不存在")

    # 更新密码
    hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
    user.user_password = hashed_password.decode('utf-8')
    user.update_time = datetime.now()

    session.commit()
    session.refresh(user)

    return True


async def delete_user_by_code(session: Session, user_code: str):
    session.query(SysUserRole).filter(SysUserRole.user_code == user_code).delete(synchronize_session=False)
    user = session.query(SysUser).filter(SysUser.user_code == user_code).first()
    if user:
        session.delete(user)
        session.commit()
        return True
    return False


async def delete_role_by_code(session: Session, role_code: str):
    session.query(SysUserRole).filter(SysUserRole.role_code == role_code).delete(synchronize_session=False)
    role = session.query(SysRole).filter(SysRole.role_code == role_code).first()
    if role:
        session.delete(role)
        session.commit()
        return True
    return False


async def fetch_user_list(session: Session, key_word: str, pageNo: int = 1, pageSize: int = 20):
    try:
        app_logger.info(
            f"Starting to fetch user list with parameters: key_word={key_word}, pageNo={pageNo}, pageSize={pageSize}")

        query = session.query(
            SysUser.user_code,
            SysUser.user_name,
            func.string_agg(SysUserRole.role_code, literal(',', type_=FixedChar(1))).label('role_codes'),
            SysUser.user_email,
            SysUser.create_time,
            SysUser.user_status
        ).outerjoin(SysUserRole, SysUser.user_code == SysUserRole.user_code)

        # 添加关键词搜索条件
        if key_word:
            key_word = f"%{key_word}%"
            query = query.filter(
                (SysUser.user_code.like(key_word)) |
                (SysUser.user_name.like(key_word))
            )

        # 按用户字段分组
        query = query.group_by(
            SysUser.user_code,
            SysUser.user_name,
            SysUser.user_email,
            SysUser.create_time,
            SysUser.user_status
        )

        # 排序和分页
        query = query.order_by(SysUser.create_time.desc())
        total = query.count()
        app_logger.debug(f"Total users matching criteria: {total}")

        items = query.offset((pageNo - 1) * pageSize).limit(pageSize).all()
        app_logger.debug(f"Retrieved {len(items)} users for page {pageNo}")

        formatted_items = []
        for item in items:
            try:
                # 使用 _asdict() 方法而不是 __dict__，适用于 SQLAlchemy 查询结果
                item_dict = item._asdict() if hasattr(item, '_asdict') else vars(item)

                # 处理 create_time 字段
                create_time = item_dict.get('create_time')
                formatted_item = {
                    **item_dict,
                    'create_time': create_time.strftime('%Y-%m-%d %H:%M') if create_time else None
                }
                formatted_items.append(formatted_item)
            except Exception as item_error:
                app_logger.warning(f"Error processing item: {str(item_error)}, item: {repr(item)}")
                # 即使单个项目出错也要继续处理其他项目
                continue

        result = {
            "total": total,
            "page": pageNo,
            "page_size": pageSize,
            "data": formatted_items
        }

        app_logger.info(f"Successfully fetched user list, returning {len(formatted_items)} items")
        return result

    except Exception as e:
        app_logger.error(f"Error occurred while fetching user list: {str(e)}", exc_info=True)
        raise Exception(f"Failed to fetch user list: {str(e)}")


async def get_user_by_code(session: Session, user_code: str):
    user = session.query(SysUser).filter(SysUser.user_code == user_code).first()
    return user


async def update_sys_user(session: Session, user_code: str, User):
    updated_user = session.query(SysUser).filter(SysUser.user_code == user_code).first()
    if updated_user:
        if User.user_password:
            hashed_password = bcrypt.hashpw(User.user_password.encode('utf-8'), bcrypt.gensalt())
            updated_user.user_password = hashed_password.decode('utf-8')
        updated_user.user_name = User.user_name
        updated_user.user_email = User.user_email
        updated_user.user_status = User.user_status
        updated_user.update_time = User.update_time
        updated_user.update_user = User.update_user
        session.commit()
        session.refresh(updated_user)
    return updated_user


async def create_sys_role(session: Session, role):
    new_role = SysRole(
        role_code=role.role_code,
        role_status=role.role_status,
        role_description=role.role_description,
        create_time=datetime.now(),
        create_user=role.create_user
    )
    session.add(new_role)
    session.commit()
    session.refresh(new_role)
    return new_role


async def fetch_role_list(session: Session, key_word: str = '', pageNo: int = 1, pageSize: int = 20):
    try:
        app_logger.info(
            f"Starting to fetch role list with parameters: key_word={key_word}, pageNo={pageNo}, pageSize={pageSize}")

        # 使用提供的SQL逻辑进行查询
        query = session.query(
            SysRole.role_code,
            SysRole.role_description,
            SysRole.role_status,
            SysRole.create_time,
            func.count(SysUserRole.user_code).label('user_count')
        ).outerjoin(SysUserRole, SysRole.role_code == SysUserRole.role_code) \
            .group_by(
            SysRole.role_code,
            SysRole.role_description,
            SysRole.role_status,
            SysRole.create_time
        )

        # 添加关键词搜索条件
        if key_word:
            key_word = f"%{key_word}%"
            query = query.filter(
                (SysRole.role_code.like(key_word)) |
                (SysRole.role_description.like(key_word))
            )

        # 排序和分页
        query = query.order_by(SysRole.create_time.desc())
        total = query.count()
        app_logger.debug(f"Total roles matching criteria: {total}")

        items = query.offset((pageNo - 1) * pageSize).limit(pageSize).all()
        app_logger.debug(f"Retrieved {len(items)} roles for page {pageNo}")

        # 格式化结果
        formatted_items = []
        for item in items:
            try:
                item_dict = item._asdict() if hasattr(item, '_asdict') else vars(item)
                create_time = item_dict.get('create_time')
                formatted_item = {
                    **item_dict,
                    'create_time': create_time.strftime('%Y-%m-%d %H:%M') if create_time else None
                }
                formatted_items.append(formatted_item)
            except Exception as item_error:
                app_logger.warning(f"Error processing item: {str(item_error)}, item: {repr(item)}")
                continue

        result = {
            "total": total,
            "page": pageNo,
            "page_size": pageSize,
            "data": formatted_items
        }

        app_logger.info(f"Successfully fetched role list, returning {len(formatted_items)} items")
        return result

    except Exception as e:
        app_logger.error(f"Error occurred while fetching role list: {str(e)}", exc_info=True)
        raise Exception(f"Failed to fetch role list: {str(e)}")


async def get_role_by_code(session: Session, role_code: str):
    role = session.query(SysRole).filter(SysRole.role_code == role_code).first()
    return role


async def update_sys_role(session: Session, role_code: str, role):
    updated_role = session.query(SysRole).filter(SysRole.role_code == role_code).first()
    if updated_role:
        updated_role.role_status = role.role_status
        updated_role.role_description = role.role_description
        updated_role.update_time = role.update_time
        updated_role.update_user = role.update_user
        session.commit()
        session.refresh(updated_role)
    return updated_role


async def create_sys_user_role(session: Session, user_roles, user_code):
    query = session.query(SysUserRole)
    if user_code is not None:
        query = query.filter(SysUserRole.user_code == user_code)
        deleted_count = query.delete(synchronize_session=False)
        # session.commit()

    created_details = []
    for role in user_roles:
        new_user_role = SysUserRole(
            user_code=user_code,
            role_code=role.role_code,
            create_time=datetime.now()
        )
        session.add(new_user_role)
        created_details.append(new_user_role)
    session.commit()
    for detail in created_details:
        session.refresh(detail)
    return created_details


async def get_role_by_user_code(session: Session, user_code: str):
    role = session.query(SysUserRole).filter(SysUserRole.user_code == user_code).all()
    return role


async def update_user_status(session, user_code, user_status):
    updated_user = session.query(SysUser).filter(SysUser.user_code == user_code).first()
    if updated_user:
        updated_user.user_status = user_status
        session.commit()
        session.refresh(updated_user)
    return updated_user


async def update_role_status(session, role_code, role_status):
    updated_role = session.query(SysRole).filter(SysRole.role_code == role_code).first()
    if updated_role:
        updated_role.role_status = role_status
        session.commit()
        session.refresh(updated_role)
    return updated_role


async def get_permissions_with_user(session: Session, user_code: str):
    # 获取用户的所有角色
    user_roles = session.query(SysUserRole.role_code).join(
        SysRole, SysRole.role_code == SysUserRole.role_code
    ).filter(
        SysUserRole.user_code == user_code,
        SysRole.role_status == 'active'
    ).all()
    if not user_roles:
        raise ValueError("User has no roles assigned")

    role_codes = [ur.role_code for ur in user_roles]

    # 获取系统中所有菜单
    menus = session.query(SysMenu).all()

    role_permissions = (
        session.query(SysRoleMenuPermission.menu_code, SysRoleMenuPermission.permission_type)
            .filter(SysRoleMenuPermission.role_code.in_(role_codes))
            .all()
    )

    # 构建权限映射
    user_perm_map = {}
    for menu_code, permission_type in role_permissions:
        if menu_code not in user_perm_map:
            user_perm_map[menu_code] = set()
        user_perm_map[menu_code].add(permission_type)

    result_menus = []

    for menu in menus:
        menu_code = menu.menu_code
        # available_perms = list(permission_template.get(menu_code, []))
        user_perms = user_perm_map.get(menu_code, set())

        # 构建权限字典：所有权限字段都存在，只有用户拥有的为 True
        # perms_dict = {p: (p in user_perms) for p in available_perms}
        perms_dict = {p: True for p in user_perms}
        result_menu = {
            # "parent_code": menu.parent_code or "",
            "menu_code": menu.menu_code,
            # "menu_name": menu.menu_name,
            "permissions": perms_dict
        }

        result_menus.append(result_menu)

    return result_menus


async def get_permissions_with_role(session: Session, role_code: str):
    role = session.query(SysRole).filter(SysRole.role_code == role_code).first()
    if not role:
        role_code = None

    menus = session.query(SysMenu).all()

    # 获取菜单支持的所有权限（来自 sys_menu_permission）
    menu_perm_query = session.query(SysMenuPermission).all()
    permission_template = {}
    for perm in menu_perm_query:
        if perm.menu_code not in permission_template:
            permission_template[perm.menu_code] = set()
        permission_template[perm.menu_code].add(perm.permission_type)

    # 查询角色已配置的权限（来自 sys_role_menu_permission）
    role_permissions = session.query(SysRoleMenuPermission).filter(
        SysRoleMenuPermission.role_code == role_code
    ).all()

    # 构建权限映射
    role_perm_map = {}
    for rp in role_permissions:
        if rp.menu_code not in role_perm_map:
            role_perm_map[rp.menu_code] = set()
        role_perm_map[rp.menu_code].add(rp.permission_type)

    result_menus = []

    for menu in menus:
        menu_code = menu.menu_code
        available_perms = list(permission_template.get(menu_code, []))
        role_perms = role_perm_map.get(menu_code, set())

        # 构建权限字典
        perms_dict = {p: (p in role_perms) for p in available_perms}

        result_menu = {
            "parent_code": menu.parent_code or "",
            "menu_code": menu_code,
            "menu_name": menu.menu_name,
            "permissions": perms_dict
        }

        result_menus.append(result_menu)
    org_data = [] if role_code is None else await get_org_permissions_with_role(session, [role_code])
    return {
        "code": 200,
        "role": role,
        "data": result_menus,
        "org_data": org_data
    }


async def batch_update_role_permissions(session: Session, role_code: str, menu_permissions: dict):
    role = session.query(SysRole).filter(SysRole.role_code == role_code).first()
    if not role:
        raise ValueError("Role not found")

    try:
        with session.begin_nested():  # 或者使用 session.begin() 如果没有嵌套事务需求
            # 删除现有权限
            session.query(SysRoleMenuPermission).filter(SysRoleMenuPermission.role_code == role_code).delete()
            for item in menu_permissions:
                menu_code = item.get('menu_code')
                permissions = item.get('permissions', [])
                for permission_type, is_checked in permissions.items():
                    if is_checked:
                        new_permission = SysRoleMenuPermission(
                            role_code=role_code,
                            menu_code=menu_code,
                            permission_type=permission_type,
                            create_time=datetime.now()
                        )
                        session.add(new_permission)
            session.commit()
        return {"code": 200, "message": "Permissions updated successfully"}
    except Exception as e:
        session.rollback()
        raise ValueError(f"Failed to update permissions: {str(e)}")


async def batch_update_role_org_permissions(session: Session, role_code: str, org_permissions: list):
    role = session.query(SysRole).filter(SysRole.role_code == role_code).first()
    if not role:
        raise ValueError("Role not found")
    try:
        with session.begin_nested():
            session.query(SysRoleOrgPermission).filter(SysRoleOrgPermission.role_code == role_code).delete()
            for item in org_permissions:
                parts = item.split(':')
                new_permission = SysRoleOrgPermission(
                    role_code=role_code,
                    org_code=parts[0],
                    org_value=parts[1],
                    create_time=datetime.now()
                )
                session.add(new_permission)
            session.commit()
        return {"code": 200, "message": "Permissions updated successfully"}
    except Exception as e:
        session.rollback()
        raise ValueError(f"Failed to update org permissions: {str(e)}")


async def get_org_permissions_with_role(session: Session, role_code: list):
    try:
        org_records = session.query(
            SysRoleOrgPermission.org_code,
            SysRoleOrgPermission.org_value
        ).filter(
            SysRoleOrgPermission.role_code.in_(role_code)
        ).distinct().all()

        return [f"{record.org_code}:{record.org_value}" for record in org_records]

    except Exception as e:
        raise ValueError(f"Failed to get org permissions: {str(e)}")


def remove_empty_children(node):
    if isinstance(node, dict):
        if "children" in node:
            if not node["children"]:
                del node["children"]
            else:
                for child in node["children"]:
                    remove_empty_children(child)
    return node


async def get_org_hierarchy(session: Session):
    try:
        all_nodes = session.query(LOC_ORG_HIERARCHY).all()

        flat_data = [
            {
                "org_code": node.ORG_CODE,
                "org_value": node.ORG_VALUE,
                "parent_code": node.PARENT_CODE or "",
                "parent_value": node.PARENT_VALUE or ""
            }
            for node in all_nodes
        ]

        tree_data = build_generic_tree(flat_data)
        cleaned_tree = [remove_empty_children(n) for n in tree_data]
        return {
            "code": 200,
            "data": cleaned_tree
        }

    except Exception as e:
        raise ValueError(f"Failed to get org hierarchy: {str(e)}")


class OrgNode:
    def __init__(self, org_code, org_value):
        self.org_code = org_code
        self.org_value = org_value
        self.children = []
        self.has_permission = False

    def dict(self):
        return {
            "org_code": self.org_code,
            "org_value": self.org_value,
            "children": [child.dict() for child in self.children if self.children],
            "has_permission": self.has_permission
        }


def build_generic_tree(all_nodes, node_factory=None):
    if node_factory is None:
        def default_node_factory(org_code, org_value):
            return {
                "title": f"{org_code}:{org_value}",
                "value": f"{org_code}:{org_value}",
                "children": []
            }

        node_factory = default_node_factory

    # Step 1: 创建所有节点
    node_map = {}
    for node in all_nodes:
        key = (node["org_code"], node["org_value"])
        if key not in node_map:
            node_map[key] = node_factory(node["org_code"], node["org_value"])

    # Step 2: 建立父子关系
    for node in all_nodes:
        key = (node["org_code"], node["org_value"])
        parent_key = (node["parent_code"], node["parent_value"]) if node["parent_code"] else None

        current_node = node_map[key]

        if parent_key and parent_key in node_map:
            parent_node = node_map[parent_key]
            if isinstance(parent_node, dict) and "children" in parent_node:
                parent_node["children"].append(current_node)
            elif hasattr(parent_node, 'children'):
                parent_node.children.append(current_node)

    # Step 3: 收集根节点
    root_nodes = []
    for node in all_nodes:
        key = (node["org_code"], node["org_value"])
        parent_key = (node["parent_code"], node["parent_value"]) if node["parent_code"] else None
        if parent_key is None:
            root_nodes.append(node_map[key])

    return root_nodes


def mark_permissions_downward(node, permissions):
    key = (node.org_code, node.org_value)
    node.has_permission = key in permissions

    if node.children:
        for child in node.children:
            # 如果父节点有权限，则子节点自动拥有权限
            child_key = (child.org_code, child.org_value)
            child.has_permission = node.has_permission or (child_key in permissions)
            mark_permissions_downward(child, permissions)


def convert_to_tree_data_with_permission(nodes):
    def recursive_convert(node):
        node_title = f"{node.org_code}:{node.org_value}"
        node_value = f"{node.org_code}:{node.org_value}"

        children = []
        if node.children:
            for child in node.children:
                children.append(recursive_convert(child))

        return {
            "title": node_title,
            "value": node_value,
            "has_permission": getattr(node, 'has_permission', False),
            "children": children if children else None
        }

    tree_data = []
    for root in nodes:
        tree_data.append(recursive_convert(root))
    return tree_data


def filter_permission_nodes(node):
    if not hasattr(node, 'children') or not node.children:
        return node.has_permission

    valid_children = [child for child in node.children if filter_permission_nodes(child)]

    # 更新当前节点的子节点列表
    node.children = valid_children

    # 如果当前节点自身有权限或包含有效子节点，则保留该节点
    return node.has_permission or len(valid_children) > 0


async def get_max_permission_nodes(session: Session, user_code: str):
    app_logger.info(f"Starting to get max permission nodes for role_code: {user_code}")

    user_roles = session.query(SysUserRole.role_code).join(
        SysRole, SysRole.role_code == SysUserRole.role_code
    ).filter(
        SysUserRole.user_code == user_code,
        SysRole.role_status == 'active'
    ).all()
    if not user_roles:
        raise ValueError("User has no roles assigned")

    role_codes = [ur.role_code for ur in user_roles]
    try:
        permission_strings = await get_org_permissions_with_role(session, role_codes)
        app_logger.debug(f"Retrieved {len(permission_strings)} permission strings for role {role_codes}")

        raw_permissions = {
            tuple(ps.split(":", 1)) for ps in permission_strings if ":" in ps
        }
        app_logger.debug(f"Parsed {len(raw_permissions)} raw permissions from permission strings")

        resolved_permissions = resolve_permissions_with_inheritance(session, raw_permissions)
        app_logger.debug(f"Resolved {len(resolved_permissions)} permissions with inheritance")

        all_nodes = session.query(LOC_ORG_HIERARCHY).all()
        app_logger.debug(f"Retrieved {len(all_nodes)} organization hierarchy nodes")

        flat_data = [
            {
                "org_code": node.ORG_CODE,
                "org_value": node.ORG_VALUE,
                "parent_code": node.PARENT_CODE or "",
                "parent_value": node.PARENT_VALUE or ""
            }
            for node in all_nodes
        ]
        app_logger.debug(f"Converted {len(flat_data)} nodes to flat data structure")

        tree_roots = build_generic_tree(flat_data, lambda code, value: OrgNode(code, value))
        app_logger.debug(f"Built {len(tree_roots)} tree roots")

        for root in tree_roots:
            mark_permissions_downward(root, resolved_permissions)
        app_logger.debug("Marked permissions downward through the tree")

        filtered_roots = [root for root in tree_roots if filter_permission_nodes(root)]
        app_logger.debug(f"Filtered to {len(filtered_roots)} roots after permission filtering")

        result = convert_to_tree_data_with_permission(filtered_roots)
        app_logger.info(f"Successfully generated permission tree with {len(result)} root nodes")

        return result

    except Exception as e:
        app_logger.error(f"Error occurred while getting max permission nodes for role {role_code}: {str(e)}",
                         exc_info=True)
        raise
