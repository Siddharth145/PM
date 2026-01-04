-- Create the main database
CREATE DATABASE IF NOT EXISTS kpi_form_db;
USE kpi_form_db;

-- KPI Logic table - stores saved KPI configurations
CREATE TABLE IF NOT EXISTS kpi_logics (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    title VARCHAR(255) NOT NULL,
    department VARCHAR(255),
    variant VARCHAR(1000) NOT NULL,
    filters JSON,
    calculation JSON,
    risk_operator VARCHAR(50),
    risk_value VARCHAR(100),
    warning_operator VARCHAR(50),
    warning_value VARCHAR(100),
    custom_formula TEXT,
    formula_aggregate VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE KEY unique_kpi_logic (name, department),
    INDEX idx_department (department),
    INDEX idx_active (is_active)
);

-- KPI Execution Log table - tracks KPI executions and results
CREATE TABLE IF NOT EXISTS kpi_executions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    kpi_logic_id INT NOT NULL,
    result_value VARCHAR(255),
    result_unit VARCHAR(50),
    execution_time_ms INT,
    status VARCHAR(50) DEFAULT 'pending',
    error_message TEXT,
    dataset_info JSON,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (kpi_logic_id) REFERENCES kpi_logics(id) ON DELETE CASCADE,
    INDEX idx_kpi_logic_id (kpi_logic_id),
    INDEX idx_executed_at (executed_at),
    INDEX idx_status (status)
);

-- Departments table - lookup table for departments
CREATE TABLE IF NOT EXISTS departments (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Process Models table - stores discovered process models metadata
CREATE TABLE IF NOT EXISTS process_models (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    dataset_name VARCHAR(255),
    case_id_column VARCHAR(100),
    activity_column VARCHAR(100),
    timestamp_column VARCHAR(100),
    total_cases INT,
    total_activities INT,
    unique_variants INT,
    model_type VARCHAR(50),
    model_data LONGTEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_dataset_name (dataset_name)
);

-- Event Log Summary table - caches event log statistics
CREATE TABLE IF NOT EXISTS event_log_summaries (
    id INT PRIMARY KEY AUTO_INCREMENT,
    process_model_id INT,
    total_cases INT,
    total_events INT,
    total_activities INT,
    avg_case_duration_hours DECIMAL(10, 2),
    complexity_score DECIMAL(5, 2),
    rework_percentage DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (process_model_id) REFERENCES process_models(id) ON DELETE CASCADE,
    INDEX idx_process_model_id (process_model_id)
);

-- Variants table - stores process variants
CREATE TABLE IF NOT EXISTS variants (
    id INT PRIMARY KEY AUTO_INCREMENT,
    process_model_id INT,
    variant_path TEXT NOT NULL,
    base_pattern TEXT,
    case_count INT,
    frequency_percentage DECIMAL(5, 2),
    is_cyclic BOOLEAN DEFAULT FALSE,
    cycle_info JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (process_model_id) REFERENCES process_models(id) ON DELETE CASCADE,
    INDEX idx_process_model_id (process_model_id),
    INDEX idx_is_cyclic (is_cyclic)
);

-- Bottlenecks table - stores identified bottleneck transitions
CREATE TABLE IF NOT EXISTS bottlenecks (
    id INT PRIMARY KEY AUTO_INCREMENT,
    process_model_id INT,
    from_activity VARCHAR(255),
    to_activity VARCHAR(255),
    avg_wait_hours DECIMAL(10, 2),
    frequency INT,
    severity VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (process_model_id) REFERENCES process_models(id) ON DELETE CASCADE,
    INDEX idx_process_model_id (process_model_id),
    INDEX idx_severity (severity)
);

-- Case Timeline table - stores individual case event sequences
CREATE TABLE IF NOT EXISTS case_timelines (
    id INT PRIMARY KEY AUTO_INCREMENT,
    process_model_id INT,
    case_id VARCHAR(255),
    event_sequence INT,
    activity VARCHAR(255),
    event_timestamp DATETIME,
    duration_from_previous_hours DECIMAL(10, 2),
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (process_model_id) REFERENCES process_models(id) ON DELETE CASCADE,
    INDEX idx_case_id (case_id),
    INDEX idx_activity (activity),
    INDEX idx_event_timestamp (event_timestamp)
);

-- Network Metrics table - stores graph topology metrics
CREATE TABLE IF NOT EXISTS network_metrics (
    id INT PRIMARY KEY AUTO_INCREMENT,
    process_model_id INT,
    activity_name VARCHAR(255),
    betweenness_centrality DECIMAL(10, 4),
    closeness_centrality DECIMAL(10, 4),
    pagerank_score DECIMAL(10, 4),
    in_degree INT,
    out_degree INT,
    rework_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (process_model_id) REFERENCES process_models(id) ON DELETE CASCADE,
    INDEX idx_process_model_id (process_model_id),
    INDEX idx_activity_name (activity_name)
);

-- KPI Thresholds table - stores risk and warning thresholds
CREATE TABLE IF NOT EXISTS kpi_thresholds (
    id INT PRIMARY KEY AUTO_INCREMENT,
    kpi_logic_id INT,
    threshold_type VARCHAR(50),
    operator VARCHAR(50),
    value VARCHAR(100),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (kpi_logic_id) REFERENCES kpi_logics(id) ON DELETE CASCADE,
    INDEX idx_kpi_logic_id (kpi_logic_id)
);

-- Audit Log table - tracks API calls and data changes
CREATE TABLE IF NOT EXISTS audit_logs (
    id INT PRIMARY KEY AUTO_INCREMENT,
    endpoint VARCHAR(255),
    method VARCHAR(10),
    request_id VARCHAR(255),
    status_code INT,
    execution_time_ms INT,
    error_message TEXT,
    user_info JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_endpoint (endpoint),
    INDEX idx_created_at (created_at)
);