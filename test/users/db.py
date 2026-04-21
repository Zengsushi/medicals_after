import os

from sqlalchemy.dialects.postgresql import Any
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select, func, or_
from datetime import datetime

from apps.user.models import User
from utils.security import Security


#
class UserRegisterError(Exception):
    pass


class UserInfoModifyError(Exception):
    pass


class UserSearchError(Exception):
    pass


class UserService:

    def __init__(self, db: Session):
        self.db = db

    # function
    def user_search(self, user_id):
        """
        用户检索 [id]
        :param user_id: 用户id
        :return: 根据查询用户结果返回数据 [User | None] ->
            用户不存在则抛出异常 UserSearchError("用户状态异常,请重新登录")
            | 查询异常则抛出 UserSearchError("用户检索失败,请刷新页面后重试")
        """
        try:
            user = self.db.query(User).filter(User.id == user_id).first()
            if not user:
                raise UserSearchError("用户状态异常,请重新登录")
            return user
        except Exception as e:
            raise UserSearchError("用户检索失败,请刷新页面后重试")

    def db_save(self, user):
        try:
            self.db.commit()
            self.db.refresh(user)
        except Exception as e:
            self.db.rollback()
            raise

    # sql function

    def user_exists(self, username: str) -> bool:
        """
        用户账号是否存在
        :param username:
        :return:
        """
        return (
                self.db.query(User)
                .filter(User.username == username)
                .first()
                is not None
        )

    def phone_exists(self, phone: str) -> bool:
        """
        手机号码是否存在
        :param phone:
        :return:
        """
        return (
                self.db.query(User)
                .filter(User.phone == phone)
                .first()
                is not None
        )

    def email_exists(self, email: str) -> bool:
        """
        邮箱是否存在
        :param email:
        :return:
        """
        return (
                self.db.query(User)
                .filter(User.email == email)
                .first()
                is not None
        )

    def user_info_save(self, user: User) -> User:
        """
        用户创建
        :param user:
        :return:
        """
        try:
            self.db.add(user)
            self.db_save(user)
            return user
        except SQLAlchemyError as e:
            print(e)
            raise UserRegisterError("用户信息保存失败") from e

    def user_info_update(self, user_info, user_id) -> User:
        """
        用户信息更新
        :param user_info:
        :param user_id:
        :return:
        """
        user = self.user_search(user_id)
        try:
            user.phone = user_info.phone
            user.email = user_info.email or ""
            user.first_name = user_info.first_name or ""
            user.last_name = user_info.last_name or ""
            user.avatar = user_info.avatar or ""
            user.is_active = bool(user_info.is_active)
            user.is_staff = bool(user_info.is_staff)
            user.is_superuser = bool(user_info.is_superuser)
            user.is_deleted = bool(user_info.is_deleted)

            self.db_save(user)
            return user
        except Exception as e:
            print(e)
            raise UserInfoModifyError("用户信息修改失败")

    def user_del(self, user_id):
        user = self.user_search(user_id)
        try:
            user.is_deleted = True
            self.db_save(user)
        except Exception as e:
            print(e)
            raise UserInfoModifyError('用户删除失败')

    def user_logout(self, user_id: int):
        """
        用户登出
        :param user_id:
        :return:
        """
        user = self.user_search(user_id)
        try:
            user.last_login = datetime.now()
            self.db_save(user)
        except Exception as e:
            raise

    def get_user_detail(self, user_id: int) -> object:
        """
        用户详细信息获取
        :param user_id: 用户id
        :return:
        """
        # 用户检索
        user_detail = self.user_search(user_id)
        try:
            user_detail.password = None
            user_detail.avatar = ("http://" + os.getenv("SERVER_IP") + ":" + os.getenv("SERVER_PORT") + "/" +
                                  user_detail.avatar
                                  .replace("B:\\3_after_end\\medicalBs\\", "")
                                  .replace("\\", "/"))
            # TODO 用户返回数据序列化
            self.db_save(user_detail)
            return user_detail
        except Exception as e:
            raise

    def get_user_list(self, page, page_size, search) -> Any:
        """
        用户列表获取
        :param page:
        :param page_size
        :param search
        :return:
        """
        # 分页计算
        offset = (page - 1) * page_size
        total = self.db.scalar(
            select(func.count()).select_from(User)
        )
        stmt = (
            select(User)
            .offset(offset)
        )
        if search is not None:
            search = f"%{search}%"
            stmt = stmt.where(
                or_(
                    User.username.like(search),
                    User.phone.like(search),
                    User.email.like(search)
                )
            )
        stmt = (stmt.limit(page_size)
                .order_by(User.id.asc())
                .where(User.is_deleted == False))
        try:
            users = self.db.execute(stmt).scalars().all()
        except Exception as e:
            raise UserSearchError("用户列表获取失败!")
        return total, users

    def user_auth(self, user_id):
        # TODO
        pass

    def user_reset_passwd(self, user_id):
        user = self.user_search(user_id)
        try:
            hash_pass = Security.get_password_hash("123456")
            user.password = hash_pass
            self.db_save(user)
        except Exception as e:
            raise

    def update_user_permissions(self, user_id, permission_data):
        """
        更新用户权限
        :param user_id: 用户ID
        :param permission_data: 权限数据，包含 role, is_staff, is_superuser
        :return: 更新后的用户
        """
        user = self.user_search(user_id)
        try:
            # 根据角色设置 is_staff 和 is_superuser
            if hasattr(permission_data, 'role'):
                role = permission_data.role
                if role == 'admin':
                    user.is_staff = True
                    user.is_superuser = False
                elif role == 'superadmin':
                    user.is_staff = True
                    user.is_superuser = True
                else:  # user
                    user.is_staff = False
                    user.is_superuser = False
            
            # 直接设置 is_staff 和 is_superuser (优先级更高)
            if hasattr(permission_data, 'is_staff') and permission_data.is_staff is not None:
                user.is_staff = permission_data.is_staff
            
            if hasattr(permission_data, 'is_superuser') and permission_data.is_superuser is not None:
                user.is_superuser = permission_data.is_superuser

            self.db_save(user)
            return user
        except Exception as e:
            print(e)
            raise UserInfoModifyError("用户权限更新失败")
