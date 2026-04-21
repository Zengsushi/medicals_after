"""
安全工具类 - 支持bcrypt密码验证（调试版）
"""
import bcrypt

class Security:
    """安全工具类 - 使用bcrypt密码验证"""

    @classmethod
    def get_password_hash(cls, password: str) -> str:
        """使用bcrypt哈希密码"""
        return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    @classmethod
    def verify_password(cls, password: str, stored_password: str) -> bool:
        """验证密码"""
        print(f"[Security] 验证密码 - 输入: {password[:3]}..., 存储: {stored_password[:20] if stored_password else 'None'}...")
        
        if not stored_password:
            print("[Security] stored_password 为空")
            return False
        
        try:
            if stored_password.startswith('$2'):
                result = bcrypt.checkpw(password.encode('utf-8'), stored_password.encode('utf-8'))
                print(f"[Security] bcrypt验证结果: {result}")
                return result
            else:
                result = password == stored_password
                print(f"[Security] 明文验证结果: {result}")
                return result
        except Exception as e:
            print(f"[Security] 验证异常: {e}")
            return password == stored_password
