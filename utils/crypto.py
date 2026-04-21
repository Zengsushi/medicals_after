import os
from pathlib import Path
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# 加载环境变量
ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=ROOT_DIR / ".env", override=False)
load_dotenv()


class CryptoUtil:
    """加密工具类"""
    
    # 从环境变量获取密钥；若缺失或格式异常，降级为临时密钥（不阻塞系统启动）
    _key = os.getenv('ENCRYPTION_KEY')
    _cipher_suite = None
    if _key:
        try:
            _cipher_suite = Fernet(_key.encode())
        except Exception as e:
            print(f"警告: ENCRYPTION_KEY 格式无效，将使用临时密钥。错误: {e}")
    if _cipher_suite is None:
        _key = Fernet.generate_key().decode()
        _cipher_suite = Fernet(_key.encode())
        print("警告: 未配置或无法使用 ENCRYPTION_KEY，已生成临时密钥。历史密文可能无法解密。")
    
    @classmethod
    def encrypt(cls, data: str) -> str:
        """加密数据"""
        if not data:
            return data
        encrypted = cls._cipher_suite.encrypt(data.encode())
        return encrypted.decode()
    
    @classmethod
    def decrypt(cls, encrypted_data: str) -> str:
        """解密数据"""
        if not encrypted_data:
            return encrypted_data
        decrypted = cls._cipher_suite.decrypt(encrypted_data.encode())
        return decrypted.decode()