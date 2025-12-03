from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status, Response
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from app.db import get_session
from app.models import User, RoleEnum, UserCreate, UserUpdate, UserRead
from app.security import verify_password, create_access_token, get_current_user, hash_password, require_roles

router = APIRouter(prefix="/auth", tags=["auth"])

@router.post("/login")
async def login(form: OAuth2PasswordRequestForm = Depends(), session: AsyncSession = Depends(get_session)):
    # OAuth2PasswordRequestForm has fields: username, password
    stmt = select(User).where(User.email == form.username)
    user = (await session.execute(stmt)).scalar_one_or_none()
    if not user or not user.is_active or not verify_password(form.password, user.password_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    token = create_access_token(str(user.id), role=user.role.value)
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {"id": user.id, "email": user.email, "role": user.role.value},
    }

@router.get("/me")
async def me(current = Depends(get_current_user)):
    return {"id": current.id, "email": current.email, "role": current.role.value, "is_active": current.is_active}

# --------------------
# Admin-only: Users CRUD
# --------------------

AdminOnly = Depends(require_roles(RoleEnum.admin))

@router.get("/users", response_model=list[UserRead], dependencies=[AdminOnly])
async def list_users(session: AsyncSession = Depends(get_session)):
    users = (await session.execute(select(User).order_by(User.id))).scalars().all()
    # convert to read model list
    return [UserRead.model_validate(u) for u in users]

@router.get("/users/{user_id}", response_model=UserRead, dependencies=[AdminOnly])
async def get_user(user_id: int, session: AsyncSession = Depends(get_session)):
    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(404, "Not found")
    return UserRead.model_validate(user)

@router.post("/users", response_model=UserRead, status_code=201, dependencies=[AdminOnly])
async def create_user(payload: UserCreate, session: AsyncSession = Depends(get_session)):
    # unique email check
    exists = (await session.execute(select(User).where(User.email == payload.email))).scalar_one_or_none()
    if exists:
        raise HTTPException(400, "Email already registered")

    role = payload.role or RoleEnum.user
    user = User(
        email=str(payload.email),
        role=role,
        is_active=payload.is_active,
        password_hash=hash_password(payload.password),
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return UserRead.model_validate(user)

@router.patch("/users/{user_id}", response_model=UserRead, dependencies=[AdminOnly])
async def update_user(user_id: int, payload: UserUpdate, session: AsyncSession = Depends(get_session)):
    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(404, "Not found")

    if payload.role is not None:
        user.role = payload.role
    if payload.is_active is not None:
        user.is_active = payload.is_active
    if payload.password:
        user.password_hash = hash_password(payload.password)

    session.add(user)
    await session.commit()
    await session.refresh(user)
    return UserRead.model_validate(user)

@router.delete("/users/{user_id}", status_code=204, dependencies=[AdminOnly])
async def delete_user(user_id: int, session: AsyncSession = Depends(get_session)):
    user = await session.get(User, user_id)
    if not user:
        return Response(status_code=204)
    await session.delete(user)
    await session.commit()
    return Response(status_code=204)
