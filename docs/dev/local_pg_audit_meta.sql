-- 公开 demo metadata model
--
-- 该脚本只创建虚构的 demo_meta schema 和数据资产，用于本地演示。
-- 不包含任何生产表、真实字段映射、真实调度任务或内部系统名称。

CREATE SCHEMA IF NOT EXISTS demo_meta;

DROP TABLE IF EXISTS demo_meta.asset_mappings;
DROP TABLE IF EXISTS demo_meta.schema_config;
DROP TABLE IF EXISTS demo_meta.send_jobs;
DROP TABLE IF EXISTS demo_meta.job_dependencies;
DROP TABLE IF EXISTS demo_meta.relations;
DROP TABLE IF EXISTS demo_meta.processes;
DROP TABLE IF EXISTS demo_meta.runtimes;
DROP TABLE IF EXISTS demo_meta.programs;
DROP TABLE IF EXISTS demo_meta.jobs;
DROP TABLE IF EXISTS demo_meta.plans;
DROP TABLE IF EXISTS demo_meta.result_receipts;
DROP TABLE IF EXISTS demo_meta.job_outputs;
DROP TABLE IF EXISTS demo_meta.receive_plans;
DROP TABLE IF EXISTS demo_meta.reference_tables;
DROP TABLE IF EXISTS demo_meta.term_roots;
DROP TABLE IF EXISTS demo_meta.roles;
DROP TABLE IF EXISTS demo_meta.reports;
DROP TABLE IF EXISTS demo_meta.partitions;
DROP TABLE IF EXISTS demo_meta.app_users;

CREATE TABLE demo_meta.term_roots (
    id BIGSERIAL PRIMARY KEY,
    root_code TEXT NOT NULL UNIQUE,
    root_cn TEXT,
    category TEXT,
    status TEXT DEFAULT '启用',
    remark TEXT,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE demo_meta.reference_tables (
    table_name TEXT PRIMARY KEY,
    description TEXT
);

CREATE TABLE demo_meta.receive_plans (
    plan_name TEXT PRIMARY KEY,
    source_system TEXT
);

CREATE TABLE demo_meta.job_outputs (
    job_name TEXT PRIMARY KEY,
    output_path TEXT NOT NULL
);

CREATE TABLE demo_meta.result_receipts (
    table_name TEXT NOT NULL,
    receive_plan TEXT,
    source_system TEXT,
    receive_job_name TEXT,
    source_job_name TEXT,
    PRIMARY KEY (table_name, receive_plan)
);

CREATE TABLE demo_meta.plans (
    plan_name TEXT PRIMARY KEY,
    dependency_text TEXT,
    description TEXT,
    owner TEXT,
    status TEXT,
    calendar TEXT
);

CREATE TABLE demo_meta.jobs (
    plan_name TEXT,
    sequence_name TEXT,
    job_name TEXT PRIMARY KEY,
    description TEXT,
    program_name TEXT,
    domain TEXT,
    priority TEXT,
    option_01 TEXT,
    option_02 TEXT,
    calendar TEXT,
    option_03 TEXT,
    option_04 TEXT,
    option_05 TEXT,
    option_06 TEXT,
    option_07 TEXT,
    option_08 TEXT,
    option_09 TEXT,
    option_10 TEXT,
    option_11 TEXT,
    option_12 TEXT,
    option_13 TEXT,
    option_14 TEXT,
    option_15 TEXT,
    status TEXT DEFAULT '启用',
    option_16 TEXT,
    event_text TEXT,
    option_17 TEXT,
    dependency_text TEXT,
    realtime_flag TEXT DEFAULT 'N'
);

CREATE TABLE demo_meta.programs (
    program_name TEXT PRIMARY KEY,
    program_key TEXT,
    description TEXT,
    language TEXT,
    file_path TEXT,
    target_table TEXT,
    option_01 TEXT,
    option_02 TEXT,
    option_03 TEXT,
    option_04 TEXT,
    status TEXT DEFAULT 'ENABLED'
);

CREATE TABLE demo_meta.runtimes (
    job_name TEXT,
    end_time TIMESTAMP,
    PRIMARY KEY (job_name, end_time)
);

CREATE TABLE demo_meta.processes (
    process_name TEXT PRIMARY KEY,
    script_code TEXT NOT NULL,
    source_name TEXT DEFAULT 'demo_process'
);

CREATE TABLE demo_meta.relations (
    target_table TEXT NOT NULL,
    source_table TEXT NOT NULL,
    PRIMARY KEY (target_table, source_table)
);

CREATE TABLE demo_meta.job_dependencies (
    job_name TEXT NOT NULL,
    dependency_name TEXT NOT NULL,
    PRIMARY KEY (job_name, dependency_name)
);

CREATE TABLE demo_meta.send_jobs (
    send_name TEXT,
    job_name TEXT,
    target_table TEXT,
    field_list TEXT
);

CREATE TABLE demo_meta.roles (
    role_name TEXT PRIMARY KEY
);

CREATE TABLE demo_meta.reports (
    report_name TEXT PRIMARY KEY,
    report_path TEXT
);

CREATE TABLE demo_meta.partitions (
    schema_name TEXT,
    table_name TEXT,
    partition_count INTEGER DEFAULT 0,
    PRIMARY KEY (schema_name, table_name)
);

CREATE TABLE demo_meta.schema_config (
    source_file TEXT,
    config_key TEXT,
    config_value TEXT
);

CREATE TABLE demo_meta.asset_mappings (
    source_table TEXT,
    target_table TEXT,
    source_column TEXT,
    target_column TEXT,
    description TEXT
);

CREATE TABLE demo_meta.app_users (
    id BIGSERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT '启用',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO demo_meta.term_roots(root_code, root_cn, category, remark) VALUES
    ('DEMO', '示例', 'table,field', '公开演示词根'),
    ('CUSTOMER', '客户', 'table,field', '虚构业务对象'),
    ('EVENT', '事件', 'table,field', '虚构业务对象'),
    ('AMOUNT', '金额', 'field', '通用数值字段'),
    ('DT', '日期', 'field', '通用日期字段');

INSERT INTO demo_meta.reference_tables(table_name, description) VALUES
    ('DWS.DEMO_STATUS_MAP', '虚构状态字典'),
    ('DWS.DEMO_RATE_MAP', '虚构费率字典');

INSERT INTO demo_meta.receive_plans(plan_name, source_system) VALUES
    ('DEMO_PLAN_INGEST_DAY', 'DEMO_CATALOG'),
    ('DEMO_PLAN_EXPORT_DAY', 'DEMO_REPORTING');

INSERT INTO demo_meta.job_outputs(job_name, output_path) VALUES
    ('DEMO_JOB_EXPORT', '/demo/output/demo_export.csv');

INSERT INTO demo_meta.result_receipts(table_name, receive_plan, source_system, receive_job_name, source_job_name) VALUES
    ('DWP.DEMO_DWP_EXPORT', 'DEMO_PLAN_EXPORT_DAY', 'DEMO_REPORTING', 'DEMO_JOB_EXPORT', 'DEMO_JOB_EXPORT');

INSERT INTO demo_meta.plans(plan_name, dependency_text, description, owner, status, calendar) VALUES
    ('DEMO_PLAN_DAILY', '', 'daily demo plan', 'demo_owner', 'ENABLED', 'DAILY');

INSERT INTO demo_meta.programs(program_name, program_key, description, language, file_path, target_table) VALUES
    ('DEMO_PROGRAM_SUMMARY', 'DEMO_PROGRAM_SUMMARY', 'builds a fictional summary table', 'python', '/demo/WORKSPACE/DWM/DWM.M_DEMO_SUMMARY/demo_job.py', 'DWM.M_DEMO_SUMMARY'),
    ('DEMO_PROGRAM_EXPORT', 'DEMO_PROGRAM_EXPORT', 'exports a fictional result', 'python', '/demo/WORKSPACE/DWP/DWP.DEMO_DWP_EXPORT/demo_export.py', 'DWP.DEMO_DWP_EXPORT');

INSERT INTO demo_meta.jobs(
    plan_name, sequence_name, job_name, description, program_name, domain, priority,
    calendar, status, event_text, dependency_text, realtime_flag
) VALUES
    ('DEMO_PLAN_DAILY', 'DEMO_SEQ_DAILY', 'DEMO_JOB_SUMMARY', 'build fictional summary', 'DEMO_PROGRAM_SUMMARY', 'DEMO_DOMAIN', '10', 'DAILY', '启用', '', '33:DEMO_JOB_EXPORT', 'N'),
    ('DEMO_PLAN_DAILY', 'DEMO_SEQ_DAILY', 'DEMO_JOB_EXPORT', 'export fictional result', 'DEMO_PROGRAM_EXPORT', 'DEMO_DOMAIN', '10', 'DAILY', '启用', '-outfile:2:outfile=/demo/output/demo_export.csv:0', '', 'N');

INSERT INTO demo_meta.runtimes(job_name, end_time) VALUES
    ('DEMO_JOB_SUMMARY', '2026-01-01 09:00:00'),
    ('DEMO_JOB_EXPORT', '2026-01-01 09:30:00');

INSERT INTO demo_meta.processes(process_name, script_code) VALUES
    ('DEMO_JOB_SUMMARY:DWM.M_DEMO_SUMMARY', 'insert into DWM.M_DEMO_SUMMARY select * from DWD.R_DEMO_EVENT join DWF.F_DEMO_EVENT on 1=1'),
    ('DEMO_JOB_EXPORT:DWP.DEMO_DWP_EXPORT', 'insert into DWP.DEMO_DWP_EXPORT select * from DWM.M_DEMO_SUMMARY');

INSERT INTO demo_meta.relations(target_table, source_table) VALUES
    ('DWM.M_DEMO_SUMMARY', 'DWD.R_DEMO_EVENT'),
    ('DWD.R_DEMO_EVENT', 'DWF.F_DEMO_EVENT'),
    ('DWP.DEMO_DWP_EXPORT', 'DWM.M_DEMO_SUMMARY');

INSERT INTO demo_meta.job_dependencies(job_name, dependency_name) VALUES
    ('DEMO_JOB_SUMMARY', 'DEMO_JOB_EXPORT');

INSERT INTO demo_meta.send_jobs(send_name, job_name, target_table, field_list) VALUES
    ('DEMO_SEND', 'DEMO_JOB_EXPORT', 'DWP.DEMO_DWP_EXPORT', 'customer_id,amount,run_dt');

INSERT INTO demo_meta.roles(role_name) VALUES ('DEMO_VIEWER'), ('DEMO_EDITOR');
INSERT INTO demo_meta.reports(report_name, report_path) VALUES ('DEMO_REPORT', 'examples/reports/demo_report.frm');
INSERT INTO demo_meta.partitions(schema_name, table_name, partition_count) VALUES ('DWM', 'M_DEMO_SUMMARY', 0);
INSERT INTO demo_meta.schema_config(source_file, config_key, config_value) VALUES ('demo.json', 'mode', 'local');
INSERT INTO demo_meta.asset_mappings(source_table, target_table, source_column, target_column, description) VALUES
    ('DWF.F_DEMO_EVENT', 'DWD.R_DEMO_EVENT', 'event_id', 'event_id', 'demo lineage');
