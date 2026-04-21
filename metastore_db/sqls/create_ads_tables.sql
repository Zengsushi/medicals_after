-- 创建 ads_hospital_ranking 表
CREATE TABLE IF NOT EXISTS medicals.ads_hospital_ranking (
    id INT AUTO_INCREMENT PRIMARY KEY,
    hospital_id VARCHAR(50),
    hospital_name VARCHAR(255),
    ranking INT,
    score DECIMAL(5,2),
    city VARCHAR(100),
    hospital_level VARCHAR(50),
    department_count INT,
    doctor_count INT,
    consultation_count INT,
    dt VARCHAR(8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建 ads_doctor_ranking 表
CREATE TABLE IF NOT EXISTS medicals.ads_doctor_ranking (
    id INT AUTO_INCREMENT PRIMARY KEY,
    doctor_id VARCHAR(50),
    doctor_name VARCHAR(100),
    ranking INT,
    score DECIMAL(5,2),
    hospital_name VARCHAR(255),
    department VARCHAR(100),
    title VARCHAR(50),
    consultation_count INT,
    dt VARCHAR(8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建 ads_disease_analysis 表
CREATE TABLE IF NOT EXISTS medicals.ads_disease_analysis (
    id INT AUTO_INCREMENT PRIMARY KEY,
    disease_category VARCHAR(100),
    disease_name VARCHAR(255),
    consultation_count INT,
    avg_treatment_cost DECIMAL(10,2),
    dt VARCHAR(8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建 ads_region_medical_resource 表
CREATE TABLE IF NOT EXISTS medicals.ads_region_medical_resource (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(100),
    province VARCHAR(100),
    hospital_count INT,
    doctor_count INT,
    bed_count INT,
    dt VARCHAR(8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建 ads_department_service_analysis 表
CREATE TABLE IF NOT EXISTS medicals.ads_department_service_analysis (
    id INT AUTO_INCREMENT PRIMARY KEY,
    department VARCHAR(100),
    doctor_count INT,
    consultation_count INT,
    avg_wait_time DECIMAL(5,2),
    satisfaction_rate DECIMAL(5,2),
    dt VARCHAR(8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建 ads_consultation_trend 表
CREATE TABLE IF NOT EXISTS medicals.ads_consultation_trend (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    consultation_count INT,
    new_patient_count INT,
    return_patient_count INT,
    dt VARCHAR(8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建 ads_overview 表
CREATE TABLE IF NOT EXISTS medicals.ads_overview (
    id INT AUTO_INCREMENT PRIMARY KEY,
    total_hospitals INT,
    total_doctors INT,
    total_consultations INT,
    total_patients INT,
    avg_satisfaction_rate DECIMAL(5,2),
    dt VARCHAR(8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建 ads_doctor_title_analysis 表
CREATE TABLE IF NOT EXISTS medicals.ads_doctor_title_analysis (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(50),
    doctor_count INT,
    consultation_count INT,
    avg_score DECIMAL(5,2),
    dt VARCHAR(8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建 ads_hospital_level_analysis 表
CREATE TABLE IF NOT EXISTS medicals.ads_hospital_level_analysis (
    id INT AUTO_INCREMENT PRIMARY KEY,
    hospital_level VARCHAR(50),
    hospital_count INT,
    doctor_count INT,
    bed_count INT,
    avg_score DECIMAL(5,2),
    dt VARCHAR(8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建 ads_price_range_analysis 表
CREATE TABLE IF NOT EXISTS medicals.ads_price_range_analysis (
    id INT AUTO_INCREMENT PRIMARY KEY,
    price_range VARCHAR(50),
    doctor_count INT,
    consultation_count INT,
    avg_score DECIMAL(5,2),
    dt VARCHAR(8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建 ads_satisfaction_analysis 表
CREATE TABLE IF NOT EXISTS medicals.ads_satisfaction_analysis (
    id INT AUTO_INCREMENT PRIMARY KEY,
    satisfaction_level VARCHAR(50),
    doctor_count INT,
    consultation_count INT,
    percentage DECIMAL(5,2),
    dt VARCHAR(8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 创建 ads_city_medical_comparison 表
CREATE TABLE IF NOT EXISTS medicals.ads_city_medical_comparison (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(100),
    hospital_density DECIMAL(10,2),
    doctor_density DECIMAL(10,2),
    avg_consultation_cost DECIMAL(10,2),
    satisfaction_rate DECIMAL(5,2),
    dt VARCHAR(8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
