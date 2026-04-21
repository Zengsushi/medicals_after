"""Rename permissions to plural form

Revision ID: rename_permissions_plural
Revises:
Create Date: 2026-04-07

"""
from alembic import op
import sqlalchemy as sa

revision = 'rename_permissions_plural'
down_revision = None
branch_labels = None
depends_on = None


OLD_TO_NEW_PERMISSIONS = {
    'user:view': 'users:view',
    'user:create': 'users:create',
    'user:edit': 'users:edit',
    'user:delete': 'users:delete',
    'user:authorize': 'users:authorize',
    'user:resetpwd': 'users:resetpwd',
    'role:view': 'roles:view',
    'role:create': 'roles:create',
    'role:edit': 'roles:edit',
    'role:delete': 'roles:delete',
    'permission:view': 'permissions:view',
    'permission:create': 'permissions:create',
    'permission:edit': 'permissions:edit',
    'permission:delete': 'permissions:delete',
    'menu:view': 'menus:view',
    'menu:create': 'menus:create',
    'menu:edit': 'menus:edit',
    'menu:delete': 'menus:delete',
    'dict:view': 'dicts:view',
    'dict:create': 'dicts:create',
    'dict:edit': 'dicts:edit',
    'dict:delete': 'dicts:delete',
    'source:view': 'sources:view',
    'source:create': 'sources:create',
    'source:edit': 'sources:edit',
    'source:delete': 'sources:delete',
    'visual:view': 'visuals:view',
    'visual:large': 'visuals:large',
    'admin:manage': 'admins:manage',
    'admin:view': 'admins:view',
}


def upgrade():
    connection = op.get_bind()

    for old_code, new_code in OLD_TO_NEW_PERMISSIONS.items():
        connection.execute(
            sa.text("""
                UPDATE permissions
                SET code = :new_code
                WHERE code = :old_code
            """),
            {'old_code': old_code, 'new_code': new_code}
        )
        connection.execute(
            sa.text("""
                UPDATE menus
                SET permission_code = :new_code
                WHERE permission_code = :old_code
            """),
            {'old_code': old_code, 'new_code': new_code}
        )

    connection.commit()


def downgrade():
    connection = op.get_bind()

    for old_code, new_code in OLD_TO_NEW_PERMISSIONS.items():
        connection.execute(
            sa.text("""
                UPDATE permissions
                SET code = :old_code
                WHERE code = :new_code
            """),
            {'old_code': old_code, 'new_code': new_code}
        )
        connection.execute(
            sa.text("""
                UPDATE menus
                SET permission_code = :old_code
                WHERE permission_code = :new_code
            """),
            {'old_code': old_code, 'new_code': new_code}
        )

    connection.commit()
