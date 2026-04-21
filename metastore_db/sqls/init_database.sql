-- ========================================
-- 医疗数据分析系统 - 数据库初始化脚本
-- ========================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS medicals DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE medicals;

-- ========================================
-- 注意：表结构会通过 SQLAlchemy 自动创建
-- 此脚本仅用于创建数据库和设置初始配置
-- ========================================

-- 显示数据库创建成功
SELECT '数据库 medicals 创建成功！' AS message;

-- 显示下一步操作提示
SELECT '请启动后端服务，表结构会自动创建' AS next_step;
