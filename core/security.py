# /Users/fu/Downloads/XY/TB/tbcommision/app/core/security.py

from datetime import datetime, timedelta
from typing import Optional, Union
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt

from service.access_service import verify_password, get_sys_user_configuration

SECRET_KEY = "65ec98c51c998e1f4bbdb90b2abf16e63d124087df4f93898da50ebd708f6f1a"

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 720

# OAuth2密码流
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="retail_hub_api/token")


def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    to_encode.update({"exp": datetime.utcnow() + expires_delta})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        # 示例中没有真正解码 JWT，根据实际业务实现
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        userid: str = payload.get("sub")

        if userid is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return userid


async def authenticate_user(username: str, password: str, session):
    if await verify_password(session, username, password):
        user_info = await get_sys_user_configuration(session, username)
        return {"user_code": user_info['user_code'], "username": user_info['user_name'],
                "configuration": user_info['configuration']}
    return None
