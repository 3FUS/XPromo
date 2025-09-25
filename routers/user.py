from fastapi import APIRouter, HTTPException
from schemas import SysUserSubmit, SysRoleSubmit
from service import get_db

router = APIRouter()

from fastapi import Depends

from service.access_service import create_sys_user, create_sys_role, fetch_user_list, fetch_role_list, update_sys_user, \
    update_sys_role, get_user_by_code, get_role_by_code, create_sys_user_role, get_role_by_user_code, \
    update_role_status, update_user_status, get_permissions_with_role, batch_update_role_permissions, \
    get_permissions_with_user, delete_user_by_code, delete_role_by_code, get_org_hierarchy, \
    batch_update_role_org_permissions, get_max_permission_nodes


@router.get("/user/user_list", tags=["user"], description='获取用户列表')
async def get_user_list(key_word: str = None, pageNo: int = 1, pageSize: int = 20,
                        session=Depends(get_db)):
    try:
        return {"code": 200, "msg": "get user list successfully",
                "data": await fetch_user_list(session, key_word, pageNo, pageSize)}
    except Exception as e:
        return {"code": 301, "msg": f"获取角色列表失败{repr(e)}"}


@router.get("/user/role_list", tags=["user"], description='获取角色列表')
async def get_role_list(key_word: str, pageNo: int = 1, pageSize: int = 20,
                        session=Depends(get_db)):
    try:
        return {"code": 200, "msg": "获取角色列表成功", "data": await fetch_role_list(session, key_word, pageNo, pageSize)}
    except Exception as e:
        return {"code": 301, "msg": "获取角色列表失败"}


@router.post("/user/user_submit", tags=["user"], description='用户')
async def submit_user(user: SysUserSubmit, session=Depends(get_db)):
    try:
        if user.user_code:
            if await get_user_by_code(session, user.user_code):
                await update_sys_user(session, user.user_code, user)
                msg = "update successfully"
            else:
                await create_sys_user(session, user)
                msg = "create successfully"
            if user.role_code:
                await create_sys_user_role(session, user.role_code, user.user_code)
        return {"code": 200, "msg": msg}
    except Exception as e:
        return {"code": 301, "msg": f"submit error {repr(e)}"}


@router.post("/user/role_submit", tags=["user"], description='角色')
async def submit_role(role: SysRoleSubmit, session=Depends(get_db)):
    try:
        if role.role_code:
            if await get_role_by_code(session, role.role_code):
                await update_sys_role(session, role.role_code, role)

            else:
                await create_sys_role(session, role)
            if role.data:
                await batch_update_role_permissions(session, role.role_code, role.data)
            if role.org_data:
                await batch_update_role_org_permissions(session, role.role_code, role.org_data)

            msg = "create successfully"
        return {"code": 200, "msg": msg}
    except Exception as e:
        return {"code": 301, "msg": f"submit error {repr(e)}"}


@router.delete("/user/delete_user", tags=["user"], description='删除用户')
async def delete_user(user_code: str, session=Depends(get_db)):
    try:
        await delete_user_by_code(session, user_code)
        return {"code": 200, "msg": "delete successfully"}
    except Exception as e:
        return {"code": 301, "msg": f"delete error {repr(e)}"}


@router.delete("/user/delete_role", tags=["user"], description='删除角色')
async def delete_role(role_code: str, session=Depends(get_db)):
    try:
        await delete_role_by_code(session, role_code)
        return {"code": 200, "msg": "delete successfully"}
    except Exception as e:
        return {"code": 301, "msg": f"delete error {repr(e)}"}


@router.get("/user/role_by_user_code", tags=["user"], description='获取用户的角色')
async def role_by_user_code(user_code: str, session=Depends(get_db)):
    try:
        data = await get_role_by_user_code(session, user_code)

        return {"code": 200, "msg": "get role by user code successfully", "data": data}
    except Exception as e:
        return {"code": 301, "msg": f"get role by user code error {repr(e)}"}


@router.post("/user/update_user_status", tags=["user"], description='更新状态')
async def set_user_status(
        user_code: str,
        user_status: str,
        session=Depends(get_db)
):
    try:
        await update_user_status(session, user_code, user_status)
        return {'code': 200, "message": "user status updated successfully."}
    except Exception as e:
        return {'code': 301, "message": str(e)}


@router.post("/user/update_role_status", tags=["user"], description='更新状态')
async def set_role_status(
        role_code: str,
        role_status: str,
        session=Depends(get_db)
):
    try:
        await update_role_status(session, role_code, role_status)
        return {'code': 200, "message": "role status updated successfully."}
    except Exception as e:
        return {'code': 301, "message": str(e)}


@router.get("/user/role_permissions/", tags=["user"])
async def get_role_hierarchical_permissions(role_code: str = '', session=Depends(get_db)):
    try:
        return await get_permissions_with_role(session, role_code)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/user/user_permissions/", tags=["user"])
async def get_user_hierarchical_permissions(user_code: str, session=Depends(get_db)):
    try:
        # await get_max_permission_nodes(session, 'admin')
        result_menus = await get_permissions_with_user(session, user_code)
        result_org = await get_max_permission_nodes(session, user_code)
        return {
            "code": 200,
            "user_code": user_code,
            "data": result_menus,
            "org_data": result_org

        }
        # return await get_permissions_with_user(session, user_code)
        # return await get_max_permission_nodes(session, 'admin')
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/user/org_hierarchy/", tags=["user"])
async def get_org_hierarchy_tree(session=Depends(get_db)):
    try:
        return await get_org_hierarchy(session)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
