"""
Database service module for KPI Process Mining application.
Handles all database operations for KPI logics, filters, and calculations.
"""

import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Any, Optional, Tuple
import json
from datetime import datetime
import logging
from contextlib import contextmanager
from db_config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseService:
    """Service class for handling database operations related to KPI logics."""
    
    def __init__(self):
        self.connection_config = {
            'host': MYSQL_HOST,
            'port': MYSQL_PORT,
            'user': MYSQL_USER,
            'password': MYSQL_PASSWORD,
            'database': MYSQL_DATABASE,
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci'
        }
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        connection = None
        try:
            connection = mysql.connector.connect(**self.connection_config)
            yield connection
        except Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection and connection.is_connected():
                connection.close()
    
    def create_tables_if_not_exist(self):
        """Create tables if they don't exist. Used for initialization."""
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                
                # Define table creation SQL
                table_sqls = [
                    """
                    CREATE TABLE IF NOT EXISTS kpi_logics (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        name VARCHAR(255) NULL,
                        department VARCHAR(100) NULL,
                        title VARCHAR(500) NOT NULL,
                        variant VARCHAR(1000) NOT NULL,
                        risk_operator VARCHAR(50) NULL,
                        risk_value VARCHAR(255) NULL,
                        warning_operator VARCHAR(50) NULL,
                        warning_value VARCHAR(255) NULL,
                        custom_formula TEXT NULL,
                        formula_aggregate VARCHAR(50) NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE,
                        INDEX idx_name (name),
                        INDEX idx_department (department),
                        INDEX idx_variant (variant(255)),
                        INDEX idx_active (is_active),
                        INDEX idx_created_at (created_at)
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS kpi_calculations (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        kpi_logic_id INT NOT NULL,
                        calculation_type VARCHAR(100) NOT NULL,
                        column_name VARCHAR(255) NOT NULL,
                        custom_formula TEXT NULL,
                        formula_aggregate VARCHAR(50) NULL,
                        FOREIGN KEY (kpi_logic_id) REFERENCES kpi_logics(id) ON DELETE CASCADE,
                        INDEX idx_kpi_logic_id (kpi_logic_id),
                        INDEX idx_calculation_type (calculation_type)
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS kpi_filters (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        kpi_logic_id INT NOT NULL,
                        column_name VARCHAR(255) NOT NULL,
                        operator VARCHAR(50) NOT NULL,
                        filter_value VARCHAR(1000) NOT NULL,
                        filter_order INT DEFAULT 0,
                        FOREIGN KEY (kpi_logic_id) REFERENCES kpi_logics(id) ON DELETE CASCADE,
                        INDEX idx_kpi_logic_id (kpi_logic_id),
                        INDEX idx_column_name (column_name),
                        INDEX idx_operator (operator)
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS kpi_execution_history (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        kpi_logic_id INT NOT NULL,
                        execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        result_value TEXT NULL,
                        result_unit VARCHAR(50) NULL,
                        execution_time_ms INT NULL,
                        status ENUM('success', 'error', 'warning') DEFAULT 'success',
                        error_message TEXT NULL,
                        dataset_info JSON NULL,
                        FOREIGN KEY (kpi_logic_id) REFERENCES kpi_logics(id) ON DELETE CASCADE,
                        INDEX idx_kpi_logic_id (kpi_logic_id),
                        INDEX idx_execution_timestamp (execution_timestamp),
                        INDEX idx_status (status)
                    )
                    """
                ]
                
                for sql in table_sqls:
                    try:
                        cursor.execute(sql)
                        logger.info(f"Table created or verified successfully")
                    except Error as e:
                        logger.error(f"Error creating table: {e}")
                        raise
                
                connection.commit()
                logger.info("All database tables created successfully")
                
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def save_kpi_logic(self, kpi_logic: Dict[str, Any]) -> int:
        """
        Save a KPI logic to the database.
        Returns the ID of the created KPI logic.
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                
                # Insert main KPI logic
                kpi_insert_query = """
                    INSERT INTO kpi_logics (name, department, title, variant, risk_operator, risk_value, 
                                          warning_operator, warning_value, custom_formula, formula_aggregate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                kpi_values = (
                    kpi_logic.get('name'),
                    kpi_logic.get('department'),
                    kpi_logic.get('title', ''),
                    kpi_logic.get('variant', 'All Cases'),
                    kpi_logic.get('risk_operator'),
                    kpi_logic.get('risk_value'),
                    kpi_logic.get('warning_operator'),
                    kpi_logic.get('warning_value'),
                    kpi_logic.get('custom_formula'),
                    kpi_logic.get('formula_aggregate')
                )
                
                cursor.execute(kpi_insert_query, kpi_values)
                kpi_logic_id = cursor.lastrowid
                
                # Insert calculation
                if 'calculation' in kpi_logic:
                    calc = kpi_logic['calculation']
                    calc_insert_query = """
                        INSERT INTO kpi_calculations (kpi_logic_id, calculation_type, column_name, 
                                                    custom_formula, formula_aggregate)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    
                    calc_values = (
                        kpi_logic_id,
                        calc.get('type', ''),
                        calc.get('column', ''),
                        calc.get('custom_formula'),
                        calc.get('formula_aggregate')
                    )
                    
                    cursor.execute(calc_insert_query, calc_values)
                
                # Insert filters
                if 'filters' in kpi_logic:
                    filter_insert_query = """
                        INSERT INTO kpi_filters (kpi_logic_id, column_name, operator, filter_value, filter_order)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    
                    for i, filter_item in enumerate(kpi_logic['filters']):
                        filter_values = (
                            kpi_logic_id,
                            filter_item.get('column', ''),
                            filter_item.get('operator', ''),
                            filter_item.get('value', ''),
                            i
                        )
                        cursor.execute(filter_insert_query, filter_values)
                
                connection.commit()
                logger.info(f"KPI logic saved with ID: {kpi_logic_id}")
                return kpi_logic_id
                
        except Error as e:
            logger.error(f"Error saving KPI logic: {e}")
            raise
    
    def get_all_kpi_logics(self) -> List[Dict[str, Any]]:
        """
        Retrieve all active KPI logics from the database.
        Returns a list of KPI logic dictionaries compatible with the existing JSON format.
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor(dictionary=True)
                
                # Get all KPI logics
                cursor.execute("""
                    SELECT id, name, department, title, variant, risk_operator, risk_value,
                           warning_operator, warning_value, custom_formula, formula_aggregate,
                           created_at, updated_at
                    FROM kpi_logics 
                    WHERE is_active = TRUE 
                    ORDER BY created_at DESC
                """)
                
                kpi_logics = cursor.fetchall()
                result = []
                
                for kpi in kpi_logics:
                    # Get calculation for this KPI
                    cursor.execute("""
                        SELECT calculation_type, column_name, custom_formula, formula_aggregate
                        FROM kpi_calculations 
                        WHERE kpi_logic_id = %s
                    """, (kpi['id'],))
                    
                    calculation = cursor.fetchone()
                    
                    # Get filters for this KPI
                    cursor.execute("""
                        SELECT column_name, operator, filter_value
                        FROM kpi_filters 
                        WHERE kpi_logic_id = %s 
                        ORDER BY filter_order
                    """, (kpi['id'],))
                    
                    filters = cursor.fetchall()
                    
                    # Format the result to match the JSON structure
                    kpi_logic = {
                        'id': kpi['id'],  # Include database ID for future operations
                        'name': kpi['name'],
                        'department': kpi['department'],
                        'title': kpi['title'],
                        'variant': kpi['variant'],
                        'risk_operator': kpi['risk_operator'],
                        'risk_value': kpi['risk_value'],
                        'warning_operator': kpi['warning_operator'],
                        'warning_value': kpi['warning_value'],
                        'custom_formula': kpi['custom_formula'],
                        'formula_aggregate': kpi['formula_aggregate'],
                        'calculation': {},
                        'filters': []
                    }
                    
                    if calculation:
                        kpi_logic['calculation'] = {
                            'type': calculation['calculation_type'],
                            'column': calculation['column_name'],
                            'custom_formula': calculation['custom_formula'],
                            'formula_aggregate': calculation['formula_aggregate']
                        }
                    
                    for filter_item in filters:
                        kpi_logic['filters'].append({
                            'column': filter_item['column_name'],
                            'operator': filter_item['operator'],
                            'value': filter_item['filter_value']
                        })
                    
                    result.append(kpi_logic)
                
                return result
                
        except Error as e:
            logger.error(f"Error retrieving KPI logics: {e}")
            raise
    
    def delete_kpi_logic(self, kpi_logic_id: int) -> bool:
        """
        Delete a KPI logic by ID (soft delete by setting is_active = FALSE).
        Returns True if successful.
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                
                cursor.execute("""
                    UPDATE kpi_logics 
                    SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP 
                    WHERE id = %s
                """, (kpi_logic_id,))
                
                connection.commit()
                
                if cursor.rowcount > 0:
                    logger.info(f"KPI logic {kpi_logic_id} deleted successfully")
                    return True
                else:
                    logger.warning(f"KPI logic {kpi_logic_id} not found")
                    return False
                
        except Error as e:
            logger.error(f"Error deleting KPI logic: {e}")
            raise
    
    def update_kpi_logic(self, kpi_logic_id: int, kpi_logic: Dict[str, Any]) -> bool:
        """
        Update an existing KPI logic.
        Returns True if successful.
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                
                # Update main KPI logic
                update_query = """
                    UPDATE kpi_logics 
                    SET name = %s, department = %s, title = %s, variant = %s, risk_operator = %s, risk_value = %s,
                        warning_operator = %s, warning_value = %s, custom_formula = %s, 
                        formula_aggregate = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s AND is_active = TRUE
                """
                
                update_values = (
                    kpi_logic.get('name'),
                    kpi_logic.get('department'),
                    kpi_logic.get('title', ''),
                    kpi_logic.get('variant', 'All Cases'),
                    kpi_logic.get('risk_operator'),
                    kpi_logic.get('risk_value'),
                    kpi_logic.get('warning_operator'),
                    kpi_logic.get('warning_value'),
                    kpi_logic.get('custom_formula'),
                    kpi_logic.get('formula_aggregate'),
                    kpi_logic_id
                )
                
                cursor.execute(update_query, update_values)
                
                # Delete existing calculation and filters
                cursor.execute("DELETE FROM kpi_calculations WHERE kpi_logic_id = %s", (kpi_logic_id,))
                cursor.execute("DELETE FROM kpi_filters WHERE kpi_logic_id = %s", (kpi_logic_id,))
                
                # Insert new calculation
                if 'calculation' in kpi_logic:
                    calc = kpi_logic['calculation']
                    calc_insert_query = """
                        INSERT INTO kpi_calculations (kpi_logic_id, calculation_type, column_name, 
                                                    custom_formula, formula_aggregate)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    
                    calc_values = (
                        kpi_logic_id,
                        calc.get('type', ''),
                        calc.get('column', ''),
                        calc.get('custom_formula'),
                        calc.get('formula_aggregate')
                    )
                    
                    cursor.execute(calc_insert_query, calc_values)
                
                # Insert new filters
                if 'filters' in kpi_logic:
                    filter_insert_query = """
                        INSERT INTO kpi_filters (kpi_logic_id, column_name, operator, filter_value, filter_order)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    
                    for i, filter_item in enumerate(kpi_logic['filters']):
                        filter_values = (
                            kpi_logic_id,
                            filter_item.get('column', ''),
                            filter_item.get('operator', ''),
                            filter_item.get('value', ''),
                            i
                        )
                        cursor.execute(filter_insert_query, filter_values)
                
                connection.commit()
                
                if cursor.rowcount > 0:
                    logger.info(f"KPI logic {kpi_logic_id} updated successfully")
                    return True
                else:
                    logger.warning(f"KPI logic {kpi_logic_id} not found for update")
                    return False
                
        except Error as e:
            logger.error(f"Error updating KPI logic: {e}")
            raise
    
    def log_kpi_execution(self, kpi_logic_id: int, result_value: Any, result_unit: str, 
                         execution_time_ms: int, status: str = 'success', 
                         error_message: str = None, dataset_info: Dict = None):
        """
        Log KPI execution for auditing and performance tracking.
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                
                insert_query = """
                    INSERT INTO kpi_execution_history 
                    (kpi_logic_id, result_value, result_unit, execution_time_ms, status, 
                     error_message, dataset_info)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                
                values = (
                    kpi_logic_id,
                    str(result_value) if result_value is not None else None,
                    result_unit,
                    execution_time_ms,
                    status,
                    error_message,
                    json.dumps(dataset_info) if dataset_info else None
                )
                
                cursor.execute(insert_query, values)
                connection.commit()
                
        except Error as e:
            logger.error(f"Error logging KPI execution: {e}")
            # Don't raise here as this is logging functionality
    
    def get_kpi_execution_history(self, kpi_logic_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get execution history for a specific KPI logic.
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor(dictionary=True)
                
                cursor.execute("""
                    SELECT execution_timestamp, result_value, result_unit, execution_time_ms, 
                           status, error_message, dataset_info
                    FROM kpi_execution_history 
                    WHERE kpi_logic_id = %s 
                    ORDER BY execution_timestamp DESC 
                    LIMIT %s
                """, (kpi_logic_id, limit))
                
                return cursor.fetchall()
                
        except Error as e:
            logger.error(f"Error retrieving KPI execution history: {e}")
            return []
    
    def check_duplicate_kpi(self, kpi_logic: Dict[str, Any]) -> bool:
        """
        Check if a KPI logic with similar characteristics already exists.
        This helps prevent duplicate KPIs as done in the original JSON implementation.
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                
                # Create a signature based on title, variant, department and calculation type
                title = kpi_logic.get('title', '')
                variant = kpi_logic.get('variant', 'All Cases')
                department = kpi_logic.get('department')
                calculation_type = kpi_logic.get('calculation', {}).get('type', '') if 'calculation' in kpi_logic else ''
                
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM kpi_logics kl
                    LEFT JOIN kpi_calculations kc ON kl.id = kc.kpi_logic_id
                    WHERE kl.is_active = TRUE 
                    AND kl.title = %s 
                    AND kl.variant = %s 
                    AND (kl.department = %s OR (kl.department IS NULL AND %s IS NULL))
                    AND (kc.calculation_type = %s OR %s = '')
                """, (title, variant, department, department, calculation_type, calculation_type))
                
                result = cursor.fetchone()
                return result[0] > 0 if result else False
                
        except Error as e:
            logger.error(f"Error checking for duplicate KPI: {e}")
            return False
    
    def get_departments(self) -> List[str]:
        """
        Get a list of all unique departments from active KPI logics.
        Returns a sorted list of department names (excluding NULL).
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                
                cursor.execute("""
                    SELECT DISTINCT department
                    FROM kpi_logics
                    WHERE is_active = TRUE AND department IS NOT NULL
                    ORDER BY department
                """)
                
                results = cursor.fetchall()
                return [row[0] for row in results if row[0]]
                
        except Error as e:
            logger.error(f"Error retrieving departments: {e}")
            return []
    
    def get_kpi_logics_by_department(self, department: str) -> List[Dict[str, Any]]:
        """
        Retrieve all active KPI logics for a specific department.
        Returns a list of KPI logic dictionaries compatible with the existing JSON format.
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor(dictionary=True)
                
                # Get all KPI logics for the department
                cursor.execute("""
                    SELECT id, name, department, title, variant, risk_operator, risk_value,
                           warning_operator, warning_value, custom_formula, formula_aggregate,
                           created_at, updated_at
                    FROM kpi_logics 
                    WHERE is_active = TRUE AND department = %s
                    ORDER BY created_at DESC
                """, (department,))
                
                kpi_logics = cursor.fetchall()
                result = []
                
                for kpi in kpi_logics:
                    # Get calculation for this KPI
                    cursor.execute("""
                        SELECT calculation_type, column_name, custom_formula, formula_aggregate
                        FROM kpi_calculations 
                        WHERE kpi_logic_id = %s
                    """, (kpi['id'],))
                    
                    calculation = cursor.fetchone()
                    
                    # Get filters for this KPI
                    cursor.execute("""
                        SELECT column_name, operator, filter_value
                        FROM kpi_filters 
                        WHERE kpi_logic_id = %s 
                        ORDER BY filter_order
                    """, (kpi['id'],))
                    
                    filters = cursor.fetchall()
                    
                    # Format the result to match the JSON structure
                    kpi_logic = {
                        'id': kpi['id'],
                        'name': kpi['name'],
                        'department': kpi['department'],
                        'title': kpi['title'],
                        'variant': kpi['variant'],
                        'risk_operator': kpi['risk_operator'],
                        'risk_value': kpi['risk_value'],
                        'warning_operator': kpi['warning_operator'],
                        'warning_value': kpi['warning_value'],
                        'custom_formula': kpi['custom_formula'],
                        'formula_aggregate': kpi['formula_aggregate'],
                        'calculation': {},
                        'filters': []
                    }
                    
                    if calculation:
                        kpi_logic['calculation'] = {
                            'type': calculation['calculation_type'],
                            'column': calculation['column_name'],
                            'custom_formula': calculation['custom_formula'],
                            'formula_aggregate': calculation['formula_aggregate']
                        }
                    
                    for filter_item in filters:
                        kpi_logic['filters'].append({
                            'column': filter_item['column_name'],
                            'operator': filter_item['operator'],
                            'value': filter_item['filter_value']
                        })
                    
                    result.append(kpi_logic)
                
                logger.info(f"Retrieved {len(result)} KPI logics for department: {department}")
                return result
                
        except Error as e:
            logger.error(f"Error retrieving KPI logics for department {department}: {e}")
            return []

# Singleton instance
db_service = DatabaseService()
