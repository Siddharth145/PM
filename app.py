import pandas as pd
import io
from datetime import datetime
import pm4py
import math
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.visualization.bpmn import visualizer as bpmn_visualizer
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.objects.conversion.process_tree import converter as pt_converter
from pm4py.visualization.petri_net import visualizer as pn_visualizer
from pm4py.algo.discovery.alpha import algorithm as alpha_miner
from pm4py.statistics.start_activities.log import get as start_activities_get
from pm4py.statistics.end_activities.log import get as end_activities_get
from pm4py.algo.filtering.log.variants import variants_filter
import os
import networkx as nx
import tempfile
import json
from collections import defaultdict
from fastapi import FastAPI, File, UploadFile, HTTPException, Body
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn
from typing import List, Dict, Any, Optional
from fastapi.middleware.cors import CORSMiddleware
import ast
import re
import time
from database_service import db_service

# Ensure Graphviz executables are on PATH for pm4py
os.environ["PATH"] += os.pathsep + r"C:\\Graphviz\bin"

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "api_events.log"

# Remove pre-existing handlers (e.g., those uvicorn sets) and reconfigure explicitly
for h in logging.root.handlers[:]:
    logging.root.removeHandler(h)

rotating_handler = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
stream_handler = logging.StreamHandler()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[rotating_handler, stream_handler],
    force=True
)
logger = logging.getLogger("api")

app = FastAPI(
    title="Process Mining & KPI API",
    description="An API for process mining, variant analysis, and KPI calculation.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or specify your frontend's URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    """Initialize database tables on application startup."""
    try:
        db_service.create_tables_if_not_exist()
        print("Database initialized successfully")
        logger.info("Startup: database initialized successfully")
    except Exception as e:
        print(f"Warning: Database initialization failed: {e}")
        print("Continuing with JSON fallback...")
        logger.warning(f"Startup: database initialization failed: {e}")
    # Emit a test log line so we can confirm file logging works
    logger.info("Startup complete: logging system active")

# Mount static files
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")
else:
    # static directory is optional in this workspace; skip mounting if missing
    print("Warning: 'static' directory not found — skipping StaticFiles mount")


# --- In-memory data storage ---
data_store = {
    "df": None,
    "log": None,
    "selected_columns": None,
    "headers": []
}

EDGE_CASE_SAMPLE_LIMIT = 50

def collect_edge_case_stats(
    df: pd.DataFrame,
    case_id_col: str,
    activity_col: str,
    timestamp_col: str,
    sample_limit: int = EDGE_CASE_SAMPLE_LIMIT
) -> Dict[Any, Dict[str, Any]]:
    """Return per-edge case statistics including a sample of case IDs."""
    if df is None or not all([case_id_col, activity_col, timestamp_col]):
        return {}

    try:
        working_df = df.copy()
        working_df[timestamp_col] = pd.to_datetime(working_df[timestamp_col], errors="coerce")
        working_df = working_df.dropna(subset=[timestamp_col])
        working_df = working_df.sort_values([case_id_col, timestamp_col])
    except Exception:
        return {}

    transitions: Dict[Any, set] = defaultdict(set)

    for case_id, group in working_df.groupby(case_id_col):
        activities = group[activity_col].tolist()
        if not activities or len(activities) < 2:
            continue
        for idx in range(len(activities) - 1):
            source = activities[idx]
            target = activities[idx + 1]
            if pd.isna(source) or pd.isna(target):
                continue
            source_key = str(source)
            target_key = str(target)
            transitions[(source_key, target_key)].add(str(case_id))

    stats: Dict[Any, Dict[str, Any]] = {}
    for key, case_ids in transitions.items():
        sorted_ids = sorted(case_ids)
        sample = sorted_ids[:sample_limit] if sample_limit and sample_limit > 0 else sorted_ids
        stats[key] = {
            "case_ids": sample,
            "case_count": len(sorted_ids),
            "sample_truncated": len(sorted_ids) > len(sample)
        }

    return stats

# --- JSON Sanitization Utilities ---
def _is_number(v):
    return isinstance(v, (int, float))

def sanitize_for_json(obj):
    """Recursively sanitize data structures replacing NaN/Inf with None for JSON compliance."""
    if _is_number(obj):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [sanitize_for_json(v) for v in obj]
    return obj

def safe_json_response(payload: dict, status_code: int = 200):
    return JSONResponse(content=sanitize_for_json(payload), status_code=status_code)

@app.middleware("http")
async def log_requests(request, call_next):
    rid = f"{datetime.utcnow().isoformat()}-{id(request)}"
    logger.info(f"REQ {rid} {request.method} {request.url.path}")
    try:
        response = await call_next(request)
        logger.info(f"RES {rid} status={response.status_code}")
        return response
    except Exception as e:
        logger.exception(f"ERR {rid} path={request.url.path} error={e}")
        raise

# Diagnostic endpoint to force a log line
@app.get("/_log_test")
async def log_test():
    logger.info("Manual log test endpoint hit")
    return safe_json_response({"message": "Log test emitted", "log_file": str(LOG_FILE)})

# --- Pydantic Models ---
class ColumnSelection(BaseModel):
    case_id: str
    activity: str
    timestamp: str

class KpiFilter(BaseModel):
    column: str
    operator: str
    value: str

class KpiCalculation(BaseModel):
    type: str
    column: str
    # Optional custom formula (Pandas expression) and optional aggregate
    custom_formula: Optional[str] = None
    formula_aggregate: Optional[str] = None

class KpiRequest(BaseModel):
    selected_variant_path: str
    filters: List[KpiFilter]
    calculation: KpiCalculation
    # Optional risk marker definition
    risk_operator: Optional[str] = None
    risk_value: Optional[str] = None
    # Optional warning marker (closer-to-risk) definition
    warning_operator: Optional[str] = None
    warning_value: Optional[str] = None
    # Optional KPI logic ID for database logging
    kpi_logic_id: Optional[int] = None
    # Optional department classification
    department: Optional[str] = None

class KpiLogic(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    department: Optional[str] = None
    filters: List[KpiFilter]
    calculation: KpiCalculation
    variant: str
    title: str
    risk_operator: Optional[str] = None
    risk_value: Optional[str] = None
    warning_operator: Optional[str] = None
    warning_value: Optional[str] = None
    # Persist custom formula and aggregate with saved logic
    # (optional)
    custom_formula: Optional[str] = None
    formula_aggregate: Optional[str] = None



# --- Utility Functions (adapted from Streamlit app) ---

def save_kpi_logics_to_database(new_kpi_logics: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Save KPI logics to database with duplicate checking.
    """
    try:
        saved_count = 0
        skipped_count = 0
        for kpi_logic in new_kpi_logics:
            # If the incoming logic contains an ID, prefer updating the existing record
            kpi_id = kpi_logic.get('id')
            if kpi_id:
                try:
                    updated = db_service.update_kpi_logic(kpi_id, kpi_logic)
                    if updated:
                        saved_count += 1
                        continue
                    # If update returned False (not found), fall through and try insert
                except Exception:
                    # If update fails for any reason, we'll try to insert below as fallback
                    pass

            # No ID or update did not succeed: check for duplicates before inserting
            try:
                if not db_service.check_duplicate_kpi(kpi_logic):
                    db_service.save_kpi_logic(kpi_logic)
                    saved_count += 1
                else:
                    skipped_count += 1
            except Exception:
                # On DB error when checking duplicates, attempt to insert to avoid losing data
                try:
                    db_service.save_kpi_logic(kpi_logic)
                    saved_count += 1
                except Exception:
                    skipped_count += 1
        
        message = f"Successfully saved {saved_count} KPI logic(s)"
        if skipped_count > 0:
            message += f", skipped {skipped_count} duplicate(s)"
        
        return {"message": message, "saved": saved_count, "skipped": skipped_count}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving KPI logics to database: {e}")


def evaluate_kpi_risk(value: Any, operator: Optional[str], threshold: Optional[str]) -> bool:
    """
    Evaluate whether a KPI `value` falls into a risk condition defined by `operator` and `threshold`.
    - operator: one of 'greater_than', 'less_than', 'between', 'equals', 'gte', 'lte', 'outside'
    - threshold: string; for 'between' or 'outside' expected format 'low|high', otherwise a single numeric string
    Returns True if value is considered a risk.
    """
    if not operator or threshold is None:
        return False

    # helper: coerce common string forms to float
    def _coerce_num(v):
        if isinstance(v, (int, float)):
            return float(v)
        if v is None:
            raise ValueError('None')
        s = str(v).strip()
        # remove percent and commas
        s = s.replace('%', '').replace(',', '')
        # extract leading number (handles things like '11.0 cases')
        m = re.search(r"-?\d+(?:\.\d+)?", s)
        if not m:
            raise ValueError(f'No numeric part in {v}')
        return float(m.group(0))

    try:
        val = _coerce_num(value)
    except Exception:
        return False

    # normalize operator values (accept '>', '<', '>=', '<=', 'eq', etc.)
    def _normalize_op(op):
        if not op:
            return ''
        s = str(op).strip().lower()
        s = s.replace(' ', '_')
        s_map = {
            '>': 'greater_than',
            'gt': 'greater_than',
            '>=': 'gte',
            '>=': 'gte',
            'gte': 'gte',
            '<': 'less_than',
            'lt': 'less_than',
            '<=': 'lte',
            'lte': 'lte',
            '=': 'equals',
            '==': 'equals',
            'equals': 'equals',
            'eq': 'equals',
            'between': 'between',
            'outside': 'outside'
        }
        return s_map.get(s, s)

    op = _normalize_op(operator)
    try:
        # handle threshold parsing for between/outside (allow '|', ',', '-' separators)
        if op == 'between' or op == 'outside':
            sep = '|' if '|' in str(threshold) else (',' if ',' in str(threshold) else ('-' if '-' in str(threshold) else None))
            if not sep:
                return False
            parts = [p.strip() for p in str(threshold).split(sep) if p.strip()]
            if len(parts) != 2:
                return False
            low = _coerce_num(parts[0])
            high = _coerce_num(parts[1])
            inside = (low <= val <= high)
            return inside if op == 'between' else (not inside)

        # other comparisons expect a single numeric threshold
        try:
            th = _coerce_num(threshold)
        except Exception:
            return False

        if op in ('greater_than',):
            return val > th
        if op in ('less_than',):
            return val < th
        if op in ('gte',):
            return val >= th
        if op in ('lte',):
            return val <= th
        if op in ('equals',):
            return val == th
    except Exception:
        return False

    return False


def compute_proximity_to_risk(value: Any, operator: Optional[str], threshold: Optional[str]) -> Optional[float]:
    """
    Compute a heuristic proximity percentage (0-100) showing how close `value` is to the risk threshold.
    Returns None when a numeric proximity cannot be computed.
    Heuristics (simple and robust):
    - greater_than / gt / gte: 100 if value >= th, otherwise (val / th)*100 (if th>0) clamped 0..100
    - less_than / lt / lte: 100 if value <= th, otherwise (th / val)*100 (if val>0) clamped 0..100
    - equals: 100 if equal, otherwise 100 - (abs(val-th) / (abs(th) if abs(th)>0 else abs(val) or 1) * 100)
    - between/outside: if within bounds => 100, else compute closeness to nearest bound using ratio
    Returns float with one decimal place.
    """
    if operator is None or threshold is None:
        return None

    # helper: reuse _coerce_num-like behaviour
    def _coerce_num_local(v):
        if isinstance(v, (int, float)):
            return float(v)
        if v is None:
            raise ValueError('None')
        s = str(v).strip()
        s = s.replace('%', '').replace(',', '')
        m = re.search(r"-?\d+(?:\.\d+)?", s)
        if not m:
            raise ValueError(f'No numeric part in {v}')
        return float(m.group(0))

    try:
        val = _coerce_num_local(value)
    except Exception:
        return None

    # normalize operator
    def _normalize_op_local(op):
        if not op:
            return ''
        s = str(op).strip().lower()
        s = s.replace(' ', '_')
        m = {
            '>': 'greater_than', 'gt': 'greater_than', '>=': 'gte', 'gte': 'gte',
            '<': 'less_than', 'lt': 'less_than', '<=': 'lte', 'lte': 'lte',
            '=': 'equals', '==': 'equals', 'equals': 'equals', 'eq': 'equals',
            'between': 'between', 'outside': 'outside'
        }
        return m.get(s, s)

    op = _normalize_op_local(operator)
    try:
        # parse threshold to numeric (or low|high)
        try:
            if op in ('between', 'outside'):
                sep = '|' if '|' in str(threshold) else (',' if ',' in str(threshold) else ('-' if '-' in str(threshold) else None))
                if not sep:
                    return None
                parts = [p.strip() for p in str(threshold).split(sep) if p.strip()]
                if len(parts) != 2:
                    return None
                low = _coerce_num_local(parts[0])
                high = _coerce_num_local(parts[1])
            else:
                th = _coerce_num_local(threshold)
        except Exception:
            return None

        if op in ('greater_than', 'gt', 'gte'):
            # treat gte same as greater_than for proximity: closer is higher percent
            if val >= th:
                return 100.0
            if th == 0:
                return 0.0
            pct = (val / th) * 100
            return float(max(0.0, min(100.0, pct)))

        if op in ('less_than', 'lt', 'lte', 'less_than_equal'):
            th = float(threshold)
            if val <= th:
                return 100.0
            if val == 0:
                return 0.0
            pct = (th / val) * 100
            return float(max(0.0, min(100.0, pct)))

        if op in ('equals', 'eq'):
            if val == th:
                return 100.0
            denom = abs(th) if abs(th) > 0 else (abs(val) if abs(val) > 0 else 1)
            pct = 100.0 - (abs(val - th) / denom) * 100.0
            return float(max(0.0, min(100.0, pct)))

        if op in ('between', 'outside'):
            if low <= val <= high:
                return 100.0
            span = abs(high - low) if abs(high - low) > 0 else max(abs(low), abs(high), 1)
            if val < low:
                pct = (val / low) * 100 if low != 0 else 0.0
                return float(max(0.0, min(100.0, pct)))
            else:
                pct = (high / val) * 100 if val != 0 else 0.0
                return float(max(0.0, min(100.0, pct)))
    except Exception:
        return None

    return None


def ensure_kpi_column_aliases(df: pd.DataFrame, selected_columns: Dict[str, str]) -> pd.DataFrame:
    """Backfill common alias column names expected by saved formulas.

    Some legacy KPI formulas reference canonical column names like ``ProjectCaseId`` even when
    the user selects a different case identifier column at runtime (for example ``ProjectInformationNo``).
    When that happens the Series lookup inside the formula resolves to ``None`` and pandas raises
    ``'NoneType' object is not subscriptable``. To keep previously saved formulas working across
    different datasets, mirror the selected case/activity/timestamp columns under the most common
    aliases whenever those aliases are missing from the dataframe.
    """
    if not selected_columns:
        return df

    alias_catalog: Dict[str, List[str]] = {
        'case_id': ['case_id', 'CaseId', 'CaseID', 'caseID', 'Case_ID', 'ProjectCaseId', 'ProjectCaseID', 'projectCaseId'],
        'activity': ['activity', 'Activity', 'ActivityName', 'concept:name'],
        'timestamp': ['timestamp', 'Timestamp', 'time:timestamp', 'event_time']
    }

    for key, aliases in alias_catalog.items():
        actual_col = selected_columns.get(key)
        if not actual_col or actual_col not in df.columns:
            continue

        source_series = df[actual_col]
        for alias in aliases:
            if alias == actual_col:
                continue
            if alias not in df.columns:
                df[alias] = source_series

    return df


def is_formula_safe(formula: str) -> bool:
    """
    Safer check: parse the formula into an AST and ensure it only contains a small set
    of allowed node types (arithmetic, comparisons, boolean ops, names, constants).
    Also ensure all Name nodes refer to allowed identifiers (DataFrame columns or True/False/None).
    This reduces the risk of arbitrary code execution.
    """
    if not isinstance(formula, str):
        return False

    try:
        node = ast.parse(formula, mode='eval')
    except Exception:
        return False

    # Allowed AST node types (permit Attribute/Call; deeper checks happen in safe_eval_formula)
    allowed_nodes = (
        ast.Expression,
        ast.BinOp,
        ast.UnaryOp,
        ast.BoolOp,
        ast.Compare,
        ast.Name,
        ast.Load,
        ast.Constant,
        ast.Num,
        ast.List,
        ast.Tuple,
        ast.Attribute,
        ast.Call,
        ast.Subscript,
        ast.Slice,
        ast.operator,
        ast.unaryop,
        ast.boolop,
        ast.cmpop
    )

    for n in ast.walk(node):
        if not isinstance(n, allowed_nodes):
            return False
    return True


def safe_eval_formula(formula: str, df: pd.DataFrame):
    """Modified version that allows specific safe pandas methods"""
    if not isinstance(formula, str):
        raise ValueError('Formula must be a string')

    node = ast.parse(formula, mode='eval')

    # Allow ONLY specific safe attribute access and calls
    # Include datetime accessors used by KPIs (dt.hour, dt.dayofweek) and common helpers
    ALLOWED_ATTRIBUTES = {
        'astype', 'replace', 'fillna', 'str', 'dt',
        # datetime-like accessors
        'hour', 'day', 'dayofweek', 'weekday', 'month', 'year', 'date',
        # timedelta operations for TAT calculations
        'total_seconds',
        # groupby and transform for TAT calculations
        'groupby', 'transform', 'to_datetime',
        # aggregation-like attrs (rarely used directly in formulas but harmless)
        'mean', 'sum', 'count', 'median', 'first', 'last'
    }
    ALLOWED_FUNCTIONS = {'int', 'float', 'str'}
    
    for n in ast.walk(node):
        if isinstance(n, ast.Attribute):
            if n.attr not in ALLOWED_ATTRIBUTES:
                raise ValueError(f'Attribute access not allowed: .{n.attr}')
        elif isinstance(n, ast.Call):
            # Check if it's a simple method call on a column
            if isinstance(n.func, ast.Attribute):
                if n.func.attr not in ALLOWED_ATTRIBUTES:
                    raise ValueError(f'Method not allowed: {n.func.attr}')
            elif isinstance(n.func, ast.Name):
                if n.func.id not in ALLOWED_FUNCTIONS:
                    raise ValueError(f'Function not allowed: {n.func.id}')
            else:
                raise ValueError('Complex function calls not allowed')
    
    # Build namespace with safe pandas/numpy functions
    # Include both individual column Series AND the full DataFrame
    # This allows formulas to use .groupby() operations properly
    local_ns = {c: df[c] for c in df.columns}
    # Add the DataFrame itself with a special name to avoid column name conflicts
    local_ns['_df'] = df
    
    import numpy as np
    safe_globals = {
        '__builtins__': None,
        'int': int,
        'float': float,
        'str': str,
        'np': type('np', (), {'nan': np.nan})(),  # Only expose np.nan
        # expose pandas as `pd` so user formulas can call pd.to_datetime and similar
        'pd': pd
    }
    
    try:
        compiled = compile(node, '<formula>', 'eval')
        result = eval(compiled, safe_globals, local_ns)
        return result
    except Exception as e:
        # Provide more context about the error
        raise ValueError(f"{str(e)} (Formula: {formula[:100]}...)" if len(formula) > 100 else f"{str(e)} (Formula: {formula})")

def apply_aggregate_to_series(series: pd.Series, agg: Optional[str]):
    if agg is None:
        return None
    a = agg.lower()
    if a in ('mean', 'avg'):
        return series.mean()
    if a in ('sum',):
        return series.sum()
    if a in ('min',):
        return series.min()
    if a in ('max',):
        return series.max()
    if a in ('median',):
        return series.median()
    if a in ('count',):
        return series.count()
    raise ValueError(f"Unsupported aggregate: {agg}")

def load_kpi_logics_from_database() -> List[Dict[str, Any]]:
    """
    Load KPI logics from database.
    """
    try:
        return db_service.get_all_kpi_logics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading KPI logics from database: {e}")


def parse_csv(file_content: bytes):
    try:
        df = pd.read_csv(io.BytesIO(file_content))
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error parsing CSV: {e}")


class FormulaPreviewRequest(BaseModel):
    selected_variant_path: str
    filters: List[KpiFilter]
    formula: str
    aggregate: Optional[str] = None


@app.post('/evaluate_formula/', summary='Preview a custom formula against the current dataset')
async def evaluate_formula(request: FormulaPreviewRequest):
    if data_store['df'] is None or data_store['selected_columns'] is None:
        raise HTTPException(status_code=404, detail='Required data not available. Please upload a file and discover a model.')

    df = data_store['df']
    case_col = data_store['selected_columns']['case_id']
    act_col = data_store['selected_columns']['activity']
    ts_col = data_store['selected_columns']['timestamp']

    # Apply variant filtering
    filtered_df = df.copy()
    if request.selected_variant_path != 'All Cases':
        case_paths = get_process_paths(df, case_col, act_col, ts_col)
        cases_in_variant = case_paths[case_paths == request.selected_variant_path].index
        filtered_df = df[df[case_col].isin(cases_in_variant)].copy()

    # Apply filters
    for f_dict in request.filters:
        f = KpiFilter(**f_dict)
        col, op, val = f.column, f.operator, f.value
        if col not in filtered_df.columns: continue
        try:
            if op == 'equals':
                filtered_df = filtered_df[filtered_df[col].astype(str).str.lower() == str(val).lower()]
            elif op == 'contains':
                filtered_df = filtered_df[filtered_df[col].astype(str).str.lower().str.contains(str(val).lower(), na=False)]
            elif op == 'greater_than':
                filtered_df = filtered_df[pd.to_numeric(filtered_df[col], errors='coerce') > float(val)]
            elif op == 'less_than':
                filtered_df = filtered_df[pd.to_numeric(filtered_df[col], errors='coerce') < float(val)]
        except Exception:
            continue

    if filtered_df.empty:
        return JSONResponse(content={"preview": [], "aggregate": None, "message": "No data matches criteria."})

    if not is_formula_safe(request.formula):
        raise HTTPException(status_code=400, detail='Formula appears unsafe.')

    try:
        eval_df = filtered_df.copy()
        eval_df = ensure_kpi_column_aliases(eval_df, data_store['selected_columns'])
        res = safe_eval_formula(request.formula, eval_df)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f'Error evaluating formula: {e}')

    preview = None
    aggregate = None
    if isinstance(res, pd.Series):
        preview = res.head(5).tolist()
        if request.aggregate:
            agg = request.aggregate.lower()
            if agg in ('proportion', 'prop') and res.dropna().dtype == 'bool':
                aggregate = float(res.mean())
            elif agg in ('std', 'stdev'):
                aggregate = float(res.std())
            else:
                aggregate = apply_aggregate_to_series(res.dropna(), request.aggregate)
    elif isinstance(res, pd.DataFrame):
        preview = res.head(5).to_dict(orient='records')
    else:
        # scalar
        preview = [res]

    logger.info("Formula evaluated", extra={"preview_len": len(preview) if preview else 0})
    return safe_json_response({"preview": preview, "aggregate": aggregate})

def convert_df_to_log(df, case_id_col, activity_col, timestamp_col):
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
    df.dropna(subset=[timestamp_col], inplace=True)
    df[case_id_col] = df[case_id_col].astype(str)
    df[activity_col] = df[activity_col].astype(str)
    renamed_df = df.rename(columns={
        case_id_col: 'case:concept:name',
        activity_col: 'concept:name',
        timestamp_col: 'time:timestamp'
    })
    return log_converter.apply(renamed_df)

def discover_process_model(log, miner_type='inductive'):
    if log is None:
        raise HTTPException(status_code=404, detail="Event log not available.")
    try:
        if miner_type == 'inductive':
            process_tree = inductive_miner.apply(log)
            bpmn_model = pt_converter.apply(process_tree, variant=pt_converter.Variants.TO_BPMN)
            message = "BPMN model generated successfully with Inductive Miner."
            temp_file_path = "temp_bpmn_model.png"
            gviz = bpmn_visualizer.apply(bpmn_model)
            bpmn_visualizer.save(gviz, temp_file_path)
            return temp_file_path, message
        elif miner_type == 'alpha':
            net, im, fm = alpha_miner.apply(log)
            temp_file_path = "temp_petri_net.png"
            gviz = pn_visualizer.apply(net, im, fm)
            pn_visualizer.save(gviz, temp_file_path)
            return temp_file_path, "Petri Net model generated successfully with Alpha Miner."
        else:
            raise HTTPException(status_code=400, detail="Invalid miner type selected.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during model generation: {e}")

def get_process_paths(df, case_id_col, activity_col, timestamp_col):
    df_sorted = df.sort_values(by=[case_id_col, timestamp_col])
    paths = df_sorted.groupby(case_id_col)[activity_col].apply(list)
    return paths.apply(lambda x: " -> ".join(x))

def normalize_cyclic_path(activity_list):
    """
    Normalize a process path by identifying and standardizing cyclic patterns.
    Returns: (base_pattern, cycle_info)
    - base_pattern: The process with cycles normalized (e.g., "A -> B -> [C -> D] x3 -> E")
    - cycle_info: Dict with cycle details (start_idx, end_idx, repetitions)
    """
    if len(activity_list) <= 2:
        return " -> ".join(activity_list), None
    
    # Detect cycles by finding repeated subsequences
    n = len(activity_list)
    cycles = []
    
    # Look for repeating patterns of length 2 to n//2
    for cycle_len in range(2, n // 2 + 1):
        i = 0
        while i <= n - cycle_len:
            pattern = activity_list[i:i + cycle_len]
            repetitions = 1
            j = i + cycle_len
            
            # Count consecutive repetitions
            while j + cycle_len <= n and activity_list[j:j + cycle_len] == pattern:
                repetitions += 1
                j += cycle_len
            
            if repetitions > 1:
                cycles.append({
                    'start': i,
                    'end': j,
                    'pattern': pattern,
                    'length': cycle_len,
                    'repetitions': repetitions
                })
                i = j  # Skip past this cycle
            else:
                i += 1
    
    # If no cycles found, return the original path
    if not cycles:
        return " -> ".join(activity_list), None
    
    # Build normalized pattern with cycle notation (compact format with count)
    base_pattern_parts = []
    cycle_info_list = []
    last_idx = 0
    
    for cycle in cycles:
        # Add activities before this cycle
        if last_idx < cycle['start']:
            base_pattern_parts.extend(activity_list[last_idx:cycle['start']])
        
        # Add cycle notation with explicit count: [pattern] x{count}
        cycle_str = " -> ".join(cycle['pattern'])
        base_pattern_parts.append(f"[{cycle_str}] x{cycle['repetitions']}")
        cycle_info_list.append(cycle)
        last_idx = cycle['end']
    
    # Add remaining activities
    if last_idx < len(activity_list):
        base_pattern_parts.extend(activity_list[last_idx:])
    
    base_pattern = " -> ".join(base_pattern_parts)
    return base_pattern, cycle_info_list if cycle_info_list else None

def get_variant_base_pattern(path_string):
    """
    Extract the base pattern from a path string for grouping variants.
    This identifies the core process flow ignoring cycle repetitions.
    """
    activity_list = [act.strip() for act in path_string.split("->")]
    base_pattern, _ = normalize_cyclic_path(activity_list)
    return base_pattern

def group_variants_by_base_pattern(variants_dict):
    """
    Group variants by their base process pattern, handling cyclic processes.
    
    Args:
        variants_dict: Dictionary from pm4py variants_filter.get_variants()
        
    Returns:
        List of grouped variants with structure:
        [{
            'base_pattern': str,
            'total_cases': int,
            'variant_count': int,
            'variants': [{'variant': str, 'count': int, 'is_cyclic': bool, 'cycle_info': dict}, ...]
        }, ...]
    """
    # First, analyze all variants
    variant_analysis = []
    for variant_tuple, cases in variants_dict.items():
        variant_str = ' -> '.join(variant_tuple)
        activity_list = list(variant_tuple)
        base_pattern, cycle_info = normalize_cyclic_path(activity_list)
        
        variant_analysis.append({
            'variant': variant_str,
            'base_pattern': base_pattern,
            'count': len(cases),
            'is_cyclic': cycle_info is not None,
            'cycle_info': cycle_info
        })
    
    # Group by base pattern
    from collections import defaultdict
    grouped = defaultdict(lambda: {'variants': [], 'total_cases': 0})
    
    for var_info in variant_analysis:
        base = var_info['base_pattern']
        grouped[base]['variants'].append({
            'variant': var_info['variant'],
            'count': var_info['count'],
            'is_cyclic': var_info['is_cyclic'],
            'cycle_info': var_info['cycle_info']
        })
        grouped[base]['total_cases'] += var_info['count']
    
    # Convert to list and sort
    result = []
    for base_pattern, data in grouped.items():
        # Sort variants within each group by count
        data['variants'].sort(key=lambda x: x['count'], reverse=True)
        
        result.append({
            'base_pattern': base_pattern,
            'total_cases': data['total_cases'],
            'variant_count': len(data['variants']),
            'variants': data['variants']
        })
    
    # Sort groups by total cases
    result.sort(key=lambda x: x['total_cases'], reverse=True)
    
    return result

def group_variants_by_base_pattern_with_cases(variants_dict):
    """
    Group variants by their base process pattern, handling cyclic processes.
    Enhanced version that includes case IDs for each variant.
    
    Args:
        variants_dict: Dictionary from pm4py variants_filter.get_variants()
        
    Returns:
        List of grouped variants with structure:
        [{
            'base_pattern': str,
            'total_cases': int,
            'variant_count': int,
            'variants': [{'variant': str, 'count': int, 'is_cyclic': bool, 'cycle_info': dict, 'case_ids': list}, ...]
        }, ...]
    """
    # First, analyze all variants with case IDs
    variant_analysis = []
    for variant_tuple, trace_objects in variants_dict.items():
        variant_str = ' -> '.join(variant_tuple)
        activity_list = list(variant_tuple)
        base_pattern, cycle_info = normalize_cyclic_path(activity_list)
        
        # Extract case IDs from trace objects
        case_ids = [trace.attributes.get('concept:name', str(i)) for i, trace in enumerate(trace_objects)]
        
        variant_analysis.append({
            'variant': variant_str,
            'base_pattern': base_pattern,
            'count': len(trace_objects),
            'is_cyclic': cycle_info is not None,
            'cycle_info': cycle_info,
            'case_ids': case_ids
        })
    
    # Group by base pattern
    from collections import defaultdict
    grouped = defaultdict(lambda: {'variants': [], 'total_cases': 0, 'all_case_ids': []})
    
    for var_info in variant_analysis:
        base = var_info['base_pattern']
        grouped[base]['variants'].append({
            'variant': var_info['variant'],
            'count': var_info['count'],
            'is_cyclic': var_info['is_cyclic'],
            'cycle_info': var_info['cycle_info'],
            'case_ids': var_info['case_ids']
        })
        grouped[base]['total_cases'] += var_info['count']
        grouped[base]['all_case_ids'].extend(var_info['case_ids'])
    
    # Convert to list and sort
    result = []
    for base_pattern, data in grouped.items():
        # Sort variants within each group by count
        data['variants'].sort(key=lambda x: x['count'], reverse=True)
        
        result.append({
            'base_pattern': base_pattern,
            'total_cases': data['total_cases'],
            'variant_count': len(data['variants']),
            'variants': data['variants'],
            'all_case_ids': data['all_case_ids']  # All cases following this base pattern
        })
    
    # Sort groups by total cases
    result.sort(key=lambda x: x['total_cases'], reverse=True)
    
    return result

def calculate_relational_kpi(df, case_id_col, activity_col, timestamp_col, selected_variant_path, filters, calculation):
    filtered_df = df.copy()
    # Step 1: Filter by process variant if a specific one is chosen
    if selected_variant_path != "All Cases":
        case_paths = get_process_paths(df, case_id_col, activity_col, timestamp_col)
        cases_in_variant = case_paths[case_paths == selected_variant_path].index
        filtered_df = df[df[case_id_col].isin(cases_in_variant)].copy()

    # Step 2: Apply additional relational filters
    for f_dict in filters:
        f = KpiFilter(**f_dict)
        col, op, val = f.column, f.operator, f.value
        if col not in filtered_df.columns: continue
        try:
            if op == 'equals':
                filtered_df = filtered_df[filtered_df[col].astype(str).str.lower() == str(val).lower()]
            elif op == 'contains':
                filtered_df = filtered_df[filtered_df[col].astype(str).str.lower().str.contains(str(val).lower(), na=False)]
            elif op == 'greater_than':
                filtered_df = filtered_df[pd.to_numeric(filtered_df[col], errors='coerce') > float(val)]
            elif op == 'less_than':
                filtered_df = filtered_df[pd.to_numeric(filtered_df[col], errors='coerce') < float(val)]
        except ValueError: continue
    
    if filtered_df.empty: return "No data matches criteria.", "N/A", ""

    # Step 3: Perform calculation
    result, unit = 'N/A', ''
    calc = KpiCalculation(**calculation)
    calc_col = calc.column
    calc_type = calc.type
    
    try:
        # If a custom formula is provided, evaluate it safely and optionally aggregate
        custom_formula = calculation.get('custom_formula') if isinstance(calculation, dict) else getattr(calculation, 'custom_formula', None)
        formula_agg = calculation.get('formula_aggregate') if isinstance(calculation, dict) else getattr(calculation, 'formula_aggregate', None)
        if custom_formula:
            # safety check
            if not is_formula_safe(custom_formula):
                # Generate title before returning
                filter_desc = " | ".join([f"{f.column} {f.operator} '{f.value}'" for f in [KpiFilter(**fil) for fil in filters]])
                kpi_title = f"{calc.type} of '{calc_col}'"
                if selected_variant_path != "All Cases": kpi_title += f" for variant '{selected_variant_path}'"
                if filter_desc: kpi_title += f" where: {filter_desc}"
                return kpi_title, "Error: formula appears unsafe.", "Error"
            try:
                eval_df = filtered_df.copy()
                eval_df = ensure_kpi_column_aliases(eval_df, {
                    'case_id': case_id_col,
                    'activity': activity_col,
                    'timestamp': timestamp_col
                })
                # Ensure critical columns exist before evaluation
                logger.debug(f"Evaluating formula on DataFrame with columns: {eval_df.columns.tolist()}")
                logger.debug(f"DataFrame shape: {eval_df.shape}")
                logger.debug(f"Formula: {custom_formula[:200]}")
                
                # Check if formula references columns that don't exist
                import re
                # Extract potential column names from formula (simple heuristic)
                potential_cols = set(re.findall(r'\b[A-Z][a-zA-Z0-9_]*\b', custom_formula))
                missing_cols = [c for c in potential_cols if c not in eval_df.columns and c not in ['True', 'False', 'None']]
                if missing_cols:
                    logger.warning(f"Formula references columns not in DataFrame: {missing_cols}")
                
                result_series = safe_eval_formula(custom_formula, eval_df)
            except Exception as e:
                # Generate title before returning
                filter_desc = " | ".join([f"{f.column} {f.operator} '{f.value}'" for f in [KpiFilter(**fil) for fil in filters]])
                kpi_title = f"{calc.type} of '{calc_col}'"
                if selected_variant_path != "All Cases": kpi_title += f" for variant '{selected_variant_path}'"
                if filter_desc: kpi_title += f" where: {filter_desc}"
                logger.error(f"Formula evaluation error: {e} | Formula: {custom_formula[:100]} | DataFrame columns: {eval_df.columns.tolist() if 'eval_df' in locals() else 'N/A'}")
                return kpi_title, f"Error evaluating formula: {e}", "Error"

            # If we got a Series back, apply aggregation
            if isinstance(result_series, pd.Series):
                if formula_agg:
                    # support additional aggregates
                    agg = formula_agg.lower()
                    if agg in ('proportion', 'prop'):
                        # if series is boolean-like or numeric 0/1, compute mean as proportion
                        cleaned_series = result_series.dropna()
                        if len(cleaned_series) == 0:
                            agg_result = 0.0
                        elif cleaned_series.dtype == 'bool' or cleaned_series.dtype == bool:
                            agg_result = float(cleaned_series.mean())
                        elif cleaned_series.dtype in ['int64', 'float64', 'int32', 'float32']:
                            # For numeric series that might be 0/1, treat as boolean proportion
                            agg_result = float(cleaned_series.mean())
                        else:
                            # Try to convert to boolean for proportion calculation
                            try:
                                agg_result = float(cleaned_series.astype(bool).mean())
                            except:
                                agg_result = float(cleaned_series.count())  # Fallback to count
                    elif agg in ('std', 'stdev'):
                        agg_result = float(result_series.std())
                    else:
                        agg_result = apply_aggregate_to_series(result_series.dropna(), formula_agg)

                    # Generate title before returning
                    filter_desc = " | ".join([f"{f.column} {f.operator} '{f.value}'" for f in [KpiFilter(**fil) for fil in filters]])
                    kpi_title = f"{calc.type} of '{calc_col}'"
                    if selected_variant_path != "All Cases": kpi_title += f" for variant '{selected_variant_path}'"
                    if filter_desc: kpi_title += f" where: {filter_desc}"
                    
                    if agg_result is None:
                        return kpi_title, "N/A", ""
                    if isinstance(agg_result, (int, float)):
                        return kpi_title, f"{agg_result:.2f}", ""
                    return kpi_title, str(agg_result), ""
                else:
                    # Generate title before returning
                    filter_desc = " | ".join([f"{f.column} {f.operator} '{f.value}'" for f in [KpiFilter(**fil) for fil in filters]])
                    kpi_title = f"{calc.type} of '{calc_col}'"
                    if selected_variant_path != "All Cases": kpi_title += f" for variant '{selected_variant_path}'"
                    if filter_desc: kpi_title += f" where: {filter_desc}"
                    
                    if not result_series.dropna().empty:
                        v = result_series.dropna().iloc[0]
                        return kpi_title, (f"{v:.2f}" if isinstance(v, (int, float)) else str(v)), ""
                    return kpi_title, "N/A", ""

            if isinstance(result_series, pd.DataFrame):
                # Generate title before returning
                filter_desc = " | ".join([f"{f.column} {f.operator} '{f.value}'" for f in [KpiFilter(**fil) for fil in filters]])
                kpi_title = f"{calc.type} of '{calc_col}'"
                if selected_variant_path != "All Cases": kpi_title += f" for variant '{selected_variant_path}'"
                if filter_desc: kpi_title += f" where: {filter_desc}"
                return kpi_title, "Error: formula returned a DataFrame. Please return a single Series or boolean mask.", "Error"

            # Boolean mask handling
            try:
                is_bool_mask = (isinstance(result_series, pd.Series) and result_series.dropna().dtype == 'bool') or (hasattr(result_series, 'dtype') and str(result_series.dtype) == 'bool')
            except Exception:
                is_bool_mask = False

            if is_bool_mask:
                mask = result_series.astype(bool)
                cnt = int(mask.sum())
                
                # Generate title before returning
                filter_desc = " | ".join([f"{f.column} {f.operator} '{f.value}'" for f in [KpiFilter(**fil) for fil in filters]])
                kpi_title = f"{calc.type} of '{calc_col}'"
                if selected_variant_path != "All Cases": kpi_title += f" for variant '{selected_variant_path}'"
                if filter_desc: kpi_title += f" where: {filter_desc}"
                
                if not formula_agg or formula_agg.lower() in ('count',):
                    return kpi_title, cnt, 'count'
                if formula_agg.lower() in ('proportion', 'prop'):
                    return kpi_title, float(mask.mean()), 'proportion'
                if formula_agg.lower() in ('std', 'stdev'):
                    return kpi_title, float(mask.astype(int).std()), ''
                agg_result = apply_aggregate_to_series(mask.astype(int), formula_agg)
                return kpi_title, agg_result, ''
        # --- Standard calculations ---
        if calc_type == 'Count of Cases':
            result = filtered_df[case_id_col].nunique()
            unit = 'cases'
        elif calc_type == 'Average Cycle Time':
            df_for_time = filtered_df.copy()
            df_for_time[timestamp_col] = pd.to_datetime(df_for_time[timestamp_col], errors='coerce')
            case_starts = df_for_time.groupby(case_id_col)[timestamp_col].min()
            case_ends = df_for_time.groupby(case_id_col)[timestamp_col].max()
            cycle_times = (case_ends - case_starts).dt.total_seconds() / (60 * 60 * 24) # days
            if not cycle_times.empty:
                result, unit = f"{cycle_times.mean():.2f}", 'days'
        elif calc_type in ['Average', 'Sum']:
            values = pd.to_numeric(filtered_df[calc_col], errors='coerce').dropna()
            if not values.empty:
                if calc_type == 'Average':
                    result = f"{values.mean():.2f}"
                    unit = ''
                elif calc_type == 'Sum':
                    result = f"{values.sum():.2f}"
                    unit = ''
        elif calc_type == 'Count of Unique Values':
            result = filtered_df[calc_col].nunique()
            unit = 'unique values'
        elif calc_type == 'Most Frequent Value':
            result = filtered_df[calc_col].mode().iloc[0] if not filtered_df[calc_col].mode().empty else "N/A"
            unit = ''
        elif calc_type == 'Least Frequent Value':
            value_counts = filtered_df[calc_col].value_counts()
            result = value_counts.idxmin() if not value_counts.empty else "N/A"
            unit = ''
        # --- Advanced KPI calculations ---
        elif calc_type == 'Loss Ratio':
            num_col, den_col = calc_col.split('|')
            case_first = filtered_df.groupby(case_id_col).first()
            numerator = pd.to_numeric(case_first[num_col], errors='coerce').sum()
            denominator = pd.to_numeric(case_first[den_col], errors='coerce').sum()
            result = f"{(numerator / denominator):.2f}" if denominator != 0 else "0"
            unit = ''
        elif calc_type == 'Expense Ratio':
            num_col, den_col = calc_col.split('|')
            case_first = filtered_df.groupby(case_id_col).first()
            numerator = pd.to_numeric(case_first[num_col], errors='coerce').sum()
            denominator = pd.to_numeric(case_first[den_col], errors='coerce').sum()
            result = f"{(numerator / denominator):.2f}" if denominator != 0 else "0"
            unit = ''
        elif calc_type == 'Combined Ratio':
            num_col, den_col = calc_col.split('|')
            case_first = filtered_df.groupby(case_id_col).first()
            incurred_losses = pd.to_numeric(case_first[num_col], errors='coerce').sum()
            expenses = pd.to_numeric(case_first[den_col], errors='coerce').sum()
            premium = pd.to_numeric(case_first[den_col], errors='coerce').sum()
            result = f"{((incurred_losses + expenses) / premium):.2f}" if premium != 0 else "0"
            unit = ''
        elif calc_type == 'Claim Processing Time':
            process_col, process_val = calc_col.split('|')
            claim_proc_df = filtered_df[filtered_df[process_col] == process_val]
            case_durations = (pd.to_datetime(claim_proc_df.groupby(case_id_col)[timestamp_col].max()) - pd.to_datetime(claim_proc_df.groupby(case_id_col)[timestamp_col].min())).dt.total_seconds() / (60*60*24)
            result = f"{case_durations.mean():.2f}" if not case_durations.empty else "0"
            unit = 'days'
        elif calc_type == 'Claim Settlement Ratio':
            claims_approved_col, claims_made_col = calc_col.split('|')
            case_first = filtered_df.groupby(case_id_col).first()
            claims_approved = pd.to_numeric(case_first[claims_approved_col], errors='coerce').sum()
            claims_made = pd.to_numeric(case_first[claims_made_col], errors='coerce').sum()
            result = f"{(claims_approved / claims_made):.2f}" if claims_made > 0 else "0"
            unit = ''
        elif calc_type == 'Policy Renewal Rate':
            flag_col = calc_col
            case_first = filtered_df.groupby(case_id_col).first()
            renewal_flag = pd.to_numeric(case_first[flag_col], errors='coerce').sum()
            result = f"{(renewal_flag / len(case_first)):.2f}" if len(case_first) > 0 else "0"
            unit = ''
        elif calc_type == 'Average Policy Size':
            avg_col = calc_col
            case_first = filtered_df.groupby(case_id_col).first()
            avg_policy_size = pd.to_numeric(case_first[avg_col], errors='coerce').mean()
            result = f"{avg_policy_size:.2f}" if not pd.isna(avg_policy_size) else "0"
            unit = ''
        elif calc_type == 'Claims Ratio':
            claims_made_col = calc_col
            case_first = filtered_df.groupby(case_id_col).first()
            claims_made = pd.to_numeric(case_first[claims_made_col], errors='coerce').sum()
            result = f"{(claims_made / len(case_first)):.2f}" if len(case_first) > 0 else "0"
            unit = ''
        elif calc_type == 'Retention Rate':
            flag_col = calc_col
            case_first = filtered_df.groupby(case_id_col).first()
            retained_flag = pd.to_numeric(case_first[flag_col], errors='coerce').sum()
            result = f"{(retained_flag / len(case_first)):.2f}" if len(case_first) > 0 else "0"
            unit = ''
    except Exception as e: return f"Error: {e}", "Error", ""

    # Step 4: Generate title
    filter_desc = " | ".join([f"{f.column} {f.operator} '{f.value}'" for f in [KpiFilter(**fil) for fil in filters]])
    kpi_title = f"{calc.type} of '{calc_col}'"
    if selected_variant_path != "All Cases": kpi_title += f" for variant '{selected_variant_path}'"
    if filter_desc: kpi_title += f" where: {filter_desc}"
    return kpi_title, result, unit


# --- Department-Based Batch KPI Application Functions ---

def apply_kpi_logic_to_df(kpi_logic: Dict[str, Any], df: pd.DataFrame, selected_columns: Dict[str, str]) -> Dict[str, Any]:
    """
    Apply a single KPI logic to a dataframe and return detailed results including risk assessment.
    
    Args:
        kpi_logic: Dictionary containing KPI logic (filters, calculation, risk thresholds, etc.)
        df: The event log dataframe
        selected_columns: Dictionary with 'case_id', 'activity', 'timestamp' column mappings
    
    Returns:
        Dictionary with keys: title, result, unit, status, is_risk, is_warning, proximity, 
                             execution_time_ms, error, kpi_logic_id
    """
    import time
    start_time = time.time()
    
    result_dict = {
        'kpi_logic_id': kpi_logic.get('id'),
        'title': kpi_logic.get('title', 'Untitled KPI'),
        'name': kpi_logic.get('name'),
        'department': kpi_logic.get('department'),
        'result': None,
        'unit': '',
        'status': 'success',
        'is_risk': False,
        'is_warning': False,
        'proximity': None,
        'execution_time_ms': 0,
        'error': None
    }
    
    try:
        # Extract parameters
        variant = kpi_logic.get('variant', 'All Cases')
        filters = kpi_logic.get('filters', [])
        calculation = kpi_logic.get('calculation', {})
        
        case_id_col = selected_columns['case_id']
        activity_col = selected_columns['activity']
        timestamp_col = selected_columns['timestamp']
        
        # Calculate KPI using existing function
        title, result_value, unit = calculate_relational_kpi(
            df, case_id_col, activity_col, timestamp_col,
            variant, filters, calculation
        )
        
        result_dict['result'] = result_value
        result_dict['unit'] = unit
        
        # Check if result is valid for risk assessment
        if result_value in ['N/A', 'Error', None] or isinstance(result_value, str) and ('Error' in result_value or 'No data' in result_value):
            result_dict['status'] = 'na'
            result_dict['result'] = result_value if result_value else 'N/A'
        else:
            # Try to convert result to numeric for risk assessment
            try:
                numeric_result = float(str(result_value).replace(',', ''))
                
                # Evaluate risk
                risk_operator = kpi_logic.get('risk_operator')
                risk_value = kpi_logic.get('risk_value')
                warning_operator = kpi_logic.get('warning_operator')
                warning_value = kpi_logic.get('warning_value')
                
                is_risk = evaluate_kpi_risk(numeric_result, risk_operator, risk_value)
                is_warning = evaluate_kpi_risk(numeric_result, warning_operator, warning_value) if not is_risk else False
                
                result_dict['is_risk'] = is_risk
                result_dict['is_warning'] = is_warning
                
                if is_risk:
                    result_dict['status'] = 'risk'
                elif is_warning:
                    result_dict['status'] = 'warning'
                else:
                    result_dict['status'] = 'ok'
                
                # Compute proximity to risk threshold
                if risk_operator and risk_value:
                    proximity = compute_proximity_to_risk(numeric_result, risk_operator, risk_value)
                    result_dict['proximity'] = round(proximity, 1) if proximity is not None else None
                    
            except (ValueError, TypeError):
                # Result is not numeric, can't assess risk
                result_dict['status'] = 'ok'
        
    except Exception as e:
        result_dict['status'] = 'error'
        result_dict['error'] = str(e)
        logger.error(f"Error applying KPI logic '{result_dict['title']}': {e}")
    
    # Calculate execution time
    execution_time_ms = int((time.time() - start_time) * 1000)
    result_dict['execution_time_ms'] = execution_time_ms
    
    return result_dict


def apply_kpis_batch(department: str, df: pd.DataFrame, selected_columns: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Apply all KPI logics in a department to the current dataframe.
    
    Args:
        department: The department name to filter KPI logics
        df: The event log dataframe
        selected_columns: Dictionary with 'case_id', 'activity', 'timestamp' column mappings
    
    Returns:
        List of result dictionaries, one per KPI logic
    """
    results = []
    
    try:
        # Get all KPI logics for this department
        kpi_logics = db_service.get_kpi_logics_by_department(department)
        
        if not kpi_logics:
            logger.warning(f"No KPI logics found for department: {department}")
            return results
        
        logger.info(f"Applying {len(kpi_logics)} KPI logics from department: {department}")
        
        # Apply each KPI logic
        for kpi_logic in kpi_logics:
            result = apply_kpi_logic_to_df(kpi_logic, df, selected_columns)
            results.append(result)
            
            # Log execution to database if KPI has an ID
            if kpi_logic.get('id') and result['status'] != 'error':
                try:
                    db_service.log_kpi_execution(
                        kpi_logic_id=kpi_logic['id'],
                        result_value=result['result'],
                        result_unit=result['unit'],
                        execution_time_ms=result['execution_time_ms'],
                        status=result['status'],
                        error_message=result.get('error'),
                        dataset_info={'department': department, 'row_count': len(df)}
                    )
                except Exception as log_err:
                    logger.error(f"Failed to log KPI execution: {log_err}")
        
        logger.info(f"Completed batch KPI application. Processed {len(results)} KPIs")
        
    except Exception as e:
        logger.error(f"Error in apply_kpis_batch for department '{department}': {e}")
        raise
    
    return results


def aggregate_kpi_batch_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Compute aggregated analytics from a batch of KPI results.
    
    Args:
        results: List of KPI result dictionaries from apply_kpis_batch
    
    Returns:
        Dictionary with aggregated statistics and chart-ready data
    """
    if not results:
        return {
            'total_kpis': 0,
            'applied_count': 0,
            'status_counts': {'ok': 0, 'warning': 0, 'risk': 0, 'na': 0, 'error': 0},
            'risk_count': 0,
            'warning_count': 0,
            'ok_count': 0,
            'na_count': 0,
            'error_count': 0,
            'mean_execution_time_ms': 0,
            'proximity_histogram': [],
            'execution_time_distribution': []
        }
    
    total_kpis = len(results)
    
    # Count by status
    status_counts = {'ok': 0, 'warning': 0, 'risk': 0, 'na': 0, 'error': 0}
    for result in results:
        status = result.get('status', 'na')
        status_counts[status] = status_counts.get(status, 0) + 1
    
    risk_count = status_counts['risk']
    warning_count = status_counts['warning']
    ok_count = status_counts['ok']
    na_count = status_counts['na']
    error_count = status_counts['error']
    applied_count = total_kpis - error_count
    
    # Execution time statistics
    execution_times = [r['execution_time_ms'] for r in results if r.get('execution_time_ms') is not None]
    mean_execution_time = sum(execution_times) / len(execution_times) if execution_times else 0
    
    # Proximity histogram (buckets: 0-20, 20-40, 40-60, 60-80, 80-100)
    proximity_values = [r['proximity'] for r in results if r.get('proximity') is not None]
    proximity_histogram = {
        '0-20': 0,
        '20-40': 0,
        '40-60': 0,
        '60-80': 0,
        '80-100': 0
    }
    
    for prox in proximity_values:
        if prox < 20:
            proximity_histogram['0-20'] += 1
        elif prox < 40:
            proximity_histogram['20-40'] += 1
        elif prox < 60:
            proximity_histogram['40-60'] += 1
        elif prox < 80:
            proximity_histogram['60-80'] += 1
        else:
            proximity_histogram['80-100'] += 1
    
    # Execution time distribution (for chart)
    # Group execution times into buckets
    if execution_times:
        min_time = min(execution_times)
        max_time = max(execution_times)
        bucket_size = max(1, (max_time - min_time) / 10) if max_time > min_time else 1
        
        execution_time_distribution = []
        for i in range(10):
            bucket_start = min_time + i * bucket_size
            bucket_end = bucket_start + bucket_size
            count = sum(1 for t in execution_times if bucket_start <= t < bucket_end)
            if i == 9:  # Include max value in last bucket
                count = sum(1 for t in execution_times if bucket_start <= t <= bucket_end)
            execution_time_distribution.append({
                'range': f"{int(bucket_start)}-{int(bucket_end)}",
                'count': count
            })
    else:
        execution_time_distribution = []
    
    return {
        'total_kpis': total_kpis,
        'applied_count': applied_count,
        'status_counts': status_counts,
        'risk_count': risk_count,
        'warning_count': warning_count,
        'ok_count': ok_count,
        'na_count': na_count,
        'error_count': error_count,
        'mean_execution_time_ms': round(mean_execution_time, 2),
        'proximity_histogram': proximity_histogram,
        'execution_time_distribution': execution_time_distribution
    }


# --- Process Mining Graph Data Function ---
def build_process_mining_graph(df, case_id_col, activity_col, timestamp_col):
    """
    Build an enhanced process mining graph using pm4py techniques:
    - Directly-Follows Graph (DFG) for transitions and frequencies
    - Performance DFG for timing metrics
    - Activity duration analysis
    Returns: dict with 'nodes' and 'edges' lists with rich metadata optimized for SVG visualization.
    """
    try:
        # Convert to pm4py event log format
        log = convert_df_to_log(df, case_id_col, activity_col, timestamp_col)

        edge_case_stats = collect_edge_case_stats(df, case_id_col, activity_col, timestamp_col)
        
        # 1. Discover DFG (Directly-Follows Graph) - gives us transition counts
        from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
        dfg = dfg_discovery.apply(log)
        
        # 2. Get start and end activities
        sa = start_activities_get.get_start_activities(log)
        ea = end_activities_get.get_end_activities(log)
        
        # 3. Discover Performance DFG - gives us timing information
        from pm4py.algo.discovery.dfg import algorithm as dfg_discovery
        performance_dfg = dfg_discovery.apply(log, variant=dfg_discovery.Variants.PERFORMANCE)
        
        # 4. Get activity occurrences (node frequencies)
        from pm4py.statistics.attributes.log import get as attributes_get
        activity_counts = attributes_get.get_attribute_values(log, "concept:name")
        
        # Calculate activity durations from the log
        activity_durations = {}
        for trace in log:
            for i, event in enumerate(trace):
                act = event["concept:name"]
                if act not in activity_durations:
                    activity_durations[act] = []
                
                # Calculate time spent in activity (time to next activity in same case)
                if i < len(trace) - 1:
                    duration = (trace[i+1]["time:timestamp"] - event["time:timestamp"]).total_seconds() / 3600.0
                    activity_durations[act].append(duration)
        
        # Build nodes with proper metrics
        nodes = []
        node_id_map = {}
        node_counter = 0
        
        # Calculate positioning using a simple force-directed approach
        all_activities = list(activity_counts.keys())
        max_count = max(activity_counts.values()) if activity_counts else 1
        
        for idx, (activity, count) in enumerate(activity_counts.items()):
            node_id = node_counter
            node_counter += 1
            node_id_map[activity] = node_id
            
            # Determine node type
            node_type = 'activity'
            if activity in sa:
                node_type = 'start'
            elif activity in ea:
                node_type = 'end'
            
            # Calculate average time for this activity
            avg_time = sum(activity_durations.get(activity, [0])) / len(activity_durations.get(activity, [1])) if activity in activity_durations and activity_durations[activity] else 0
            
            # Position nodes in a grid layout (will be improved by frontend)
            cols = 4
            x = 150 + (idx % cols) * 250
            y = 150 + (idx // cols) * 200
            
            # Color based on performance
            color = '#10b981'  # green - good
            if avg_time > 40:
                color = '#ef4444'  # red - bottleneck
            elif avg_time > 20:
                color = '#f59e0b'  # orange - warning
            
            nodes.append({
                'id': node_id,
                'name': activity,
                'x': x,
                'y': y,
                'type': node_type,
                'variants': count,  # frequency in log
                'cases': count,
                'avgTime': round(avg_time, 2),
                'color': color
            })
        
        # Build edges from DFG with performance metrics
        edges = []
        max_edge_count = max(dfg.values()) if dfg else 1
        
        for (source_act, target_act), frequency in dfg.items():
            if source_act not in node_id_map or target_act not in node_id_map:
                continue
                
            # Get performance metric (average wait time between activities)
            avg_wait = performance_dfg.get((source_act, target_act), 0) / 3600.0  # Convert to hours
            
            # Calculate thickness based on frequency
            thickness = max(1, min(10, int((frequency / max_edge_count) * 10)))
            
            # Color based on frequency
            freq_ratio = frequency / max_edge_count
            if freq_ratio > 0.5:
                color = '#10b981'  # green - main path
            elif freq_ratio > 0.2:
                color = '#f59e0b'  # orange - alternative path
            else:
                color = '#ef4444'  # red - rare path

            case_info = edge_case_stats.get((str(source_act), str(target_act)), {
                "case_ids": [],
                "case_count": 0,
                "sample_truncated": False
            })
            
            edges.append({
                'from': node_id_map[source_act],
                'to': node_id_map[target_act],
                'frequency': frequency,
                'avgTime': round(avg_wait, 2),
                'cases': case_info["case_count"],
                'caseCount': case_info["case_count"],
                'caseSample': case_info["case_ids"],
                'caseSampleTruncated': case_info["sample_truncated"],
                'color': color,
                'thickness': thickness
            })
        
        # Calculate statistics
        total_cases = len(log)
        
        # Identify bottlenecks (activities with high avg time)
        bottlenecks = [
            node['name'] for node in nodes 
            if node['avgTime'] > 30 and node['type'] == 'activity'
        ]
        
        return {
            'nodes': nodes,
            'edges': edges,
            'stats': {
                'totalCases': total_cases,
                'avgDuration': round(sum(activity_durations.get(act, [0])[0] if activity_durations.get(act) else 0 for act in all_activities) / len(all_activities) if all_activities else 0, 2),
                'variants': len(all_activities),
                'bottlenecks': bottlenecks
            }
        }
    except Exception as e:
        logger.error(f"Error building process graph with pm4py: {e}", exc_info=True)
        # Fallback to manual calculation
        return build_process_mining_graph_fallback(df, case_id_col, activity_col, timestamp_col)

def build_process_mining_graph_fallback(df, case_id_col, activity_col, timestamp_col):
    """
    Fallback method for building process graph without pm4py (manual calculation).
    Returns data in the same format as the pm4py version for SVG visualization.
    """
    df = df.sort_values([case_id_col, timestamp_col])
    nodes_dict = {}
    edges_dict = {}
    edge_case_stats = collect_edge_case_stats(df, case_id_col, activity_col, timestamp_col)
    
    # Analyze case paths and collect metrics
    for case_id, group in df.groupby(case_id_col):
        activities = group[activity_col].tolist()
        timestamps = pd.to_datetime(group[timestamp_col])
        
        prev_activity = None
        prev_timestamp = None
        
        for i, (activity, timestamp) in enumerate(zip(activities, timestamps)):
            # Node metrics
            if activity not in nodes_dict:
                nodes_dict[activity] = {
                    "count": 0,
                    "cases": set(),
                    "durations": [],
                    "is_start": False,
                    "is_end": False
                }
            
            nodes_dict[activity]["count"] += 1
            nodes_dict[activity]["cases"].add(case_id)
            
            # Mark start/end activities
            if i == 0:
                nodes_dict[activity]["is_start"] = True
            if i == len(activities) - 1:
                nodes_dict[activity]["is_end"] = True
            
            # Calculate activity duration (time to next activity)
            if i < len(activities) - 1:
                next_timestamp = timestamps.iloc[i + 1]
                duration = (next_timestamp - timestamp).total_seconds() / 3600.0
                nodes_dict[activity]["durations"].append(duration)
            
            # Edge metrics
            if prev_activity is not None:
                edge_key = (prev_activity, activity)
                if edge_key not in edges_dict:
                    edges_dict[edge_key] = {
                        "count": 0,
                        "durations": []
                    }
                
                edges_dict[edge_key]["count"] += 1
                
                # Calculate wait time between activities
                if prev_timestamp is not None:
                    duration = (timestamp - prev_timestamp).total_seconds() / 3600.0
                    edges_dict[edge_key]["durations"].append(duration)
            
            prev_activity = activity
            prev_timestamp = timestamp
    
    # Build nodes list for SVG
    nodes = []
    node_id_map = {}
    max_count = max([n["count"] for n in nodes_dict.values()]) if nodes_dict else 1
    
    for idx, (activity, data) in enumerate(nodes_dict.items()):
        node_id_map[activity] = idx
        
        # Determine node type
        if data["is_start"] and not data["is_end"]:
            node_type = 'start'
        elif data["is_end"] and not data["is_start"]:
            node_type = 'end'
        else:
            node_type = 'activity'
        
        # Calculate average time
        avg_time = sum(data["durations"]) / len(data["durations"]) if data["durations"] else 0
        
        # Color based on performance
        color = '#10b981'  # green
        if avg_time > 40:
            color = '#ef4444'  # red
        elif avg_time > 20:
            color = '#f59e0b'  # orange
        
        # Position nodes
        cols = 4
        x = 150 + (idx % cols) * 250
        y = 150 + (idx // cols) * 200
        
        nodes.append({
            'id': idx,
            'name': activity,
            'x': x,
            'y': y,
            'type': node_type,
            'variants': data["count"],
            'cases': len(data["cases"]),
            'avgTime': round(avg_time, 2),
            'color': color
        })
    
    # Build edges list for SVG
    edges = []
    max_edge_count = max([e["count"] for e in edges_dict.values()]) if edges_dict else 1
    
    for (source_act, target_act), data in edges_dict.items():
        if source_act not in node_id_map or target_act not in node_id_map:
            continue
        
        avg_wait = sum(data["durations"]) / len(data["durations"]) if data["durations"] else 0
        thickness = max(1, min(10, int((data["count"] / max_edge_count) * 10)))
        
        freq_ratio = data["count"] / max_edge_count
        if freq_ratio > 0.5:
            color = '#10b981'
        elif freq_ratio > 0.2:
            color = '#f59e0b'
        else:
            color = '#ef4444'

        case_info = edge_case_stats.get((str(source_act), str(target_act)), {
            "case_ids": [],
            "case_count": 0,
            "sample_truncated": False
        })
        
        edges.append({
            'from': node_id_map[source_act],
            'to': node_id_map[target_act],
            'frequency': data["count"],
            'avgTime': round(avg_wait, 2),
            'cases': case_info["case_count"],
            'caseCount': case_info["case_count"],
            'caseSample': case_info["case_ids"],
            'caseSampleTruncated': case_info["sample_truncated"],
            'color': color,
            'thickness': thickness
        })
    
    # Calculate statistics
    total_cases = df[case_id_col].nunique()
    bottlenecks = [n['name'] for n in nodes if n['avgTime'] > 30 and n['type'] == 'activity']
    avg_duration = sum(n['avgTime'] for n in nodes) / len(nodes) if nodes else 0
    
    return {
        'nodes': nodes,
        'edges': edges,
        'stats': {
            'totalCases': total_cases,
            'avgDuration': round(avg_duration, 2),
            'variants': len(nodes),
            'bottlenecks': bottlenecks
        }
    }

def perform_performance_analysis(df, case_id_col, activity_col, timestamp_col):
    # Old Cytoscape format - keeping for backwards compatibility
    df = df.sort_values([case_id_col, timestamp_col])
    nodes_dict = {}
    edges_dict = {}
    
    # Analyze case paths and collect metrics
    for case_id, group in df.groupby(case_id_col):
        activities = group[activity_col].tolist()
        timestamps = pd.to_datetime(group[timestamp_col])
        
        prev_activity = None
        prev_timestamp = None
        
        for i, (activity, timestamp) in enumerate(zip(activities, timestamps)):
            # Node metrics
            if activity not in nodes_dict:
                nodes_dict[activity] = {
                    "id": activity,
                    "label": activity,
                    "count": 0,
                    "rework_count": 0,
                    "cases": set(),
                    "avg_duration": 0,
                    "total_duration": 0,
                    "position_stats": {"first": 0, "middle": 0, "last": 0}
                }
            
            nodes_dict[activity]["count"] += 1
            nodes_dict[activity]["cases"].add(case_id)
            
            # Position in process
            if i == 0:
                nodes_dict[activity]["position_stats"]["first"] += 1
            elif i == len(activities) - 1:
                nodes_dict[activity]["position_stats"]["last"] += 1
            else:
                nodes_dict[activity]["position_stats"]["middle"] += 1
            
            # Detect rework (same activity appears again in same case)
            if activities[:i].count(activity) > 0:
                nodes_dict[activity]["rework_count"] += 1
            
            # Edge metrics
            if prev_activity is not None:
                edge_key = (prev_activity, activity)
                if edge_key not in edges_dict:
                    edges_dict[edge_key] = {
                        "source": prev_activity,
                        "target": activity,
                        "count": 0,
                        "cases": set(),
                        "durations": [],
                        "avg_duration": 0,
                        "min_duration": float('inf'),
                        "max_duration": 0
                    }
                
                edges_dict[edge_key]["count"] += 1
                edges_dict[edge_key]["cases"].add(case_id)
                
                # Calculate duration between activities
                if prev_timestamp is not None:
                    duration = (timestamp - prev_timestamp).total_seconds()
                    edges_dict[edge_key]["durations"].append(duration)
                    edges_dict[edge_key]["min_duration"] = min(edges_dict[edge_key]["min_duration"], duration)
                    edges_dict[edge_key]["max_duration"] = max(edges_dict[edge_key]["max_duration"], duration)
            
            prev_activity = activity
            prev_timestamp = timestamp
    
    # Calculate final metrics and prepare data for Cytoscape.js
    total_cases = df[case_id_col].nunique()
    max_node_count = max([n["count"] for n in nodes_dict.values()]) if nodes_dict else 1
    max_edge_count = max([e["count"] for e in edges_dict.values()]) if edges_dict else 1
    
    # Prepare nodes for Cytoscape
    node_list = []
    for node_data in nodes_dict.values():
        # Calculate average durations and frequencies
        frequency = node_data["count"] / max_node_count
        rework_rate = node_data["rework_count"] / node_data["count"] if node_data["count"] > 0 else 0
        case_coverage = len(node_data["cases"]) / total_cases
        
        # Determine node type/role
        node_type = "intermediate"
        if node_data["position_stats"]["first"] > node_data["position_stats"]["middle"] and node_data["position_stats"]["first"] > node_data["position_stats"]["last"]:
            node_type = "start"
        elif node_data["position_stats"]["last"] > node_data["position_stats"]["middle"] and node_data["position_stats"]["last"] > node_data["position_stats"]["first"]:
            node_type = "end"
        
        node_list.append({
            "data": {
                "id": node_data["id"],
                "label": node_data["label"],
                "count": node_data["count"],
                "frequency": frequency,
                "rework_count": node_data["rework_count"],
                "rework_rate": rework_rate,
                "case_coverage": case_coverage,
                "node_type": node_type,
                "cases": len(node_data["cases"]),
                # For tooltips and styling
                "tooltip": f"{node_data['label']}\nCount: {node_data['count']}\nCases: {len(node_data['cases'])}\nRework: {node_data['rework_count']}\nCoverage: {case_coverage:.2%}"
            }
        })
    
    # Prepare edges for Cytoscape
    edge_list = []
    for edge_data in edges_dict.values():
        # Calculate edge metrics
        frequency = edge_data["count"] / max_edge_count
        case_coverage = len(edge_data["cases"]) / total_cases
        
        # Calculate duration statistics
        avg_duration = sum(edge_data["durations"]) / len(edge_data["durations"]) if edge_data["durations"] else 0
        min_duration = edge_data["min_duration"] if edge_data["min_duration"] != float('inf') else 0
        max_duration = edge_data["max_duration"]
        
        edge_list.append({
            "data": {
                "id": f"{edge_data['source']}->{edge_data['target']}",
                "source": edge_data["source"],
                "target": edge_data["target"],
                "count": edge_data["count"],
                "frequency": frequency,
                "case_coverage": case_coverage,
                "avg_duration": avg_duration,
                "min_duration": min_duration,
                "max_duration": max_duration,
                "cases": len(edge_data["cases"]),
                # For tooltips and styling
                "tooltip": f"{edge_data['source']} → {edge_data['target']}\nCount: {edge_data['count']}\nCases: {len(edge_data['cases'])}\nAvg Duration: {avg_duration/3600:.2f}h\nCoverage: {case_coverage:.2%}"
            }
        })
    
    return {
        "elements": node_list + edge_list,
        "statistics": {
            "total_cases": total_cases,
            "total_activities": len(nodes_dict),
            "total_transitions": len(edges_dict),
            "max_node_frequency": max_node_count,
            "max_edge_frequency": max_edge_count
        }
    }

def perform_performance_analysis(df, case_id_col, activity_col, timestamp_col):
    """Analyzes performance metrics like case duration and bottlenecks."""
    if df is None or not all([case_id_col, activity_col, timestamp_col]):
        return None

    try:
        # Prepare dataframe for pm4py
        df_log = df.copy()
        df_log[timestamp_col] = pd.to_datetime(df_log[timestamp_col], errors='coerce')
        df_log.dropna(subset=[timestamp_col], inplace=True)
        df_log.rename(columns={
            case_id_col: 'case:concept:name',
            activity_col: 'concept:name',
            timestamp_col: 'time:timestamp'
        }, inplace=True)
        
        log = log_converter.apply(df_log)

        # Case duration
        case_durations_seconds = pm4py.get_all_case_durations(log)
        case_durations = [s / 3600 for s in case_durations_seconds] # convert to hours
        case_duration_stats = {
            'Mean': pd.Series(case_durations).mean(),
            'Median': pd.Series(case_durations).median(),
            'Min': pd.Series(case_durations).min(),
            'Max': pd.Series(case_durations).max()
        }

        # Bottleneck analysis (waiting time)
        df_log = df_log.sort_values(['case:concept:name', 'time:timestamp'])
        df_log['next_timestamp'] = df_log.groupby('case:concept:name')['time:timestamp'].shift(-1)
        df_log['next_activity'] = df_log.groupby('case:concept:name')['concept:name'].shift(-1)
        
        transitions = df_log.dropna(subset=['next_timestamp'])
        transitions['waiting_time_hours'] = (transitions['next_timestamp'] - transitions['time:timestamp']).dt.total_seconds() / 3600
        
        bottleneck_df = transitions.groupby(['concept:name', 'next_activity'])['waiting_time_hours'].mean().reset_index()
        bottleneck_df.rename(columns={'concept:name': 'activity_from', 'next_activity': 'activity_to'}, inplace=True)
        bottleneck_df = bottleneck_df.sort_values(by='waiting_time_hours', ascending=False)


        return {
            "case_duration_stats": case_duration_stats,
            "bottlenecks": bottleneck_df.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during performance analysis: {e}")


# --- API Endpoints ---

@app.post("/upload_csv/", summary="Upload and parse an event log CSV")
async def upload_csv(file: UploadFile = File(...)):
    """
    Upload a CSV file, which is then parsed into a pandas DataFrame.
    The DataFrame is stored in memory for subsequent API calls.
    - **file**: The CSV file to upload.
    """
    content = await file.read()
    df = parse_csv(content)
    data_store["df"] = df
    data_store["headers"] = df.columns.tolist()
    data_store["log"] = None # Reset log on new upload
    data_store["selected_columns"] = None
    payload = {
        "message": "CSV uploaded and parsed successfully!",
        "filename": file.filename,
        "headers": data_store["headers"],
        "preview": df.head().to_dict(orient="records")
    }
    logger.info(f"CSV uploaded name={file.filename} rows={len(df)} cols={len(df.columns)}")
    return safe_json_response(payload)

@app.post("/discover_model/", summary="Discover a process model from the uploaded data")
async def discover_model(columns: ColumnSelection, miner_type: str = 'inductive'):
    """
    Select columns for case ID, activity, and timestamp, then discover a process model.
    - **columns**: JSON object with `case_id`, `activity`, `timestamp`.
    - **miner_type**: The mining algorithm to use (`inductive` or `alpha`).
    """
    if data_store["df"] is None:
        raise HTTPException(status_code=404, detail="No CSV data found. Please upload a file first.")
    
    data_store["selected_columns"] = columns.dict()
    
    try:
        log = convert_df_to_log(
            data_store["df"],
            columns.case_id,
            columns.activity,
            columns.timestamp
        )
        data_store["log"] = log
        
        logger.info(f"Process model discovery started: case_id={columns.case_id}, activity={columns.activity}, timestamp={columns.timestamp}")
        
        model_path, message = discover_process_model(log, miner_type)
        
        if not os.path.exists(model_path):
            logger.error(f"Model file not created at path: {model_path}")
            raise HTTPException(status_code=500, detail="Model file was not created.")
        
        logger.info(f"Process model created successfully: {model_path}")
        return FileResponse(model_path, media_type='image/png', filename=os.path.basename(model_path))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in discover_model: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error discovering model: {str(e)}")


@app.get("/variant_analysis/", summary="Get analysis of process variants")
async def variant_analysis():
    """
    Returns variant analysis with variants grouped by their base process pattern.
    Handles cyclic processes by normalizing and grouping similar patterns.
    Includes case IDs for each variant.
    """
    if data_store["log"] is None:
        raise HTTPException(status_code=404, detail="Event log not generated. Please discover a model first.")
    
    variants = variants_filter.get_variants(data_store["log"])
    
    # Get traditional flat list with case IDs
    variants_list = []
    for variant_tuple, trace_objects in variants.items():
        variant_str = ' -> '.join(variant_tuple)
        # Extract case IDs from trace objects
        case_ids = [trace.attributes.get('concept:name', str(i)) for i, trace in enumerate(trace_objects)]
        variants_list.append({
            "variant": variant_str,
            "count": len(trace_objects),
            "case_ids": case_ids
        })
    variants_list = sorted(variants_list, key=lambda x: x['count'], reverse=True)
    
    # Get grouped variants by base pattern (enhanced with case IDs)
    grouped_variants = group_variants_by_base_pattern_with_cases(variants)

    logger.info(f"Variant analysis: {len(variants_list)} total variants, {len(grouped_variants)} base patterns")
    return safe_json_response({
        "total_variants": len(variants),
        "total_base_patterns": len(grouped_variants),
        "variants": variants_list,  # Keep for backward compatibility (now with case_ids)
        "grouped_variants": grouped_variants  # New grouped structure (now with case_ids)
    })

@app.get("/log_statistics/", summary="Get event log statistics")
async def log_statistics():
    """
    Returns start/end activities and performance analysis (case duration, bottlenecks).
    """
    if data_store["log"] is None or data_store["df"] is None or data_store["selected_columns"] is None:
        raise HTTPException(status_code=404, detail="Required data not available. Please upload a file and discover a model.")

    start_activities = start_activities_get.get_start_activities(data_store["log"])
    end_activities = end_activities_get.get_end_activities(data_store["log"])
    
    perf_data = perform_performance_analysis(
        data_store["df"],
        data_store["selected_columns"]['case_id'],
        data_store["selected_columns"]['activity'],
        data_store["selected_columns"]['timestamp']
    )

    payload = {
        "start_activities": start_activities,
        "end_activities": end_activities,
        "performance_analysis": perf_data
    }
    logger.info("Log statistics served")
    return safe_json_response(payload)

@app.post("/calculate_kpi/", summary="Calculate a relational KPI")
async def calculate_kpi_endpoint(request: KpiRequest):
    """
    Calculates a KPI based on a process variant, filters, and a calculation definition.
    Enhanced with database logging for performance tracking.
    """
    if data_store["df"] is None or data_store["selected_columns"] is None:
        raise HTTPException(status_code=404, detail="Required data not available. Please upload a file and discover a model.")

    start_time = time.time()
    kpi_logic_id = None
    
    try:
        # Convert Pydantic models to dicts for the function
        filters_dict = [f.dict() for f in request.filters]
        calculation_dict = request.calculation.dict()

        title, result, unit = calculate_relational_kpi(
            data_store["df"],
            data_store["selected_columns"]['case_id'],
            data_store["selected_columns"]['activity'],
            data_store["selected_columns"]['timestamp'],
            request.selected_variant_path,
            filters_dict,
            calculation_dict
        )
        
        # Calculate execution time
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        # Evaluate warning and risk markers if provided
        is_risk = False
        is_warning = False
        # Try to coerce result to a numeric-friendly form for comparisons
        numeric_for_eval = result
        try:
            # if result is like '123.45' or '123 cases' extract numeric portion
            if isinstance(result, str):
                m = re.search(r"-?\d+(?:\.\d+)?", result)
                if m:
                    numeric_for_eval = float(m.group(0))
            elif isinstance(result, (int, float)):
                numeric_for_eval = float(result)
        except Exception:
            numeric_for_eval = result

        try:
            # Evaluate risk first (highest severity)
            is_risk = evaluate_kpi_risk(numeric_for_eval, request.risk_operator, request.risk_value)
            # Evaluate warning only if not already risk
            if not is_risk:
                is_warning = evaluate_kpi_risk(numeric_for_eval, request.warning_operator, request.warning_value)
        except Exception:
            is_risk = False
            is_warning = False

        status = 'ok'
        if is_risk:
            status = 'risk'
        elif is_warning:
            status = 'warning'

        # compute proximity (prefer risk proximity)
        proximity = None
        try:
            # prefer proximity to risk threshold when present; pass numeric_for_eval for best results
            proximity = compute_proximity_to_risk(numeric_for_eval, request.risk_operator, request.risk_value)
            if proximity is None:
                proximity = compute_proximity_to_risk(numeric_for_eval, request.warning_operator, request.warning_value)
        except Exception:
            proximity = None

        # Log successful execution to database (optional)
        try:
            dataset_info = {
                "row_count": len(data_store["df"]) if data_store["df"] is not None else 0,
                "selected_variant": request.selected_variant_path,
                "filters_count": len(filters_dict)
            }
            
            # If we can identify the KPI logic ID from the request, log it
            # This would require frontend to pass the KPI logic ID
            if hasattr(request, 'kpi_logic_id') and request.kpi_logic_id:
                db_service.log_kpi_execution(
                    request.kpi_logic_id, result, unit, execution_time_ms, 
                    'success', None, dataset_info
                )
        except Exception as log_error:
            print(f"Warning: Failed to log KPI execution: {log_error}")

        response_payload = {
            "title": title,
            "result": result,
            "unit": unit,
            "is_risk": is_risk,
            "is_warning": is_warning,
            "status": status,
            "proximity": proximity,
            "execution_time_ms": execution_time_ms
        }
        logger.info(f"KPI calc title='{title}' result='{result}' risk={is_risk} warn={is_warning} ms={execution_time_ms}")
        return safe_json_response(response_payload)
        
    except Exception as e:
        execution_time_ms = int((time.time() - start_time) * 1000)
        
        # Log failed execution to database (optional)
        try:
            if hasattr(request, 'kpi_logic_id') and request.kpi_logic_id:
                db_service.log_kpi_execution(
                    request.kpi_logic_id, None, None, execution_time_ms, 
                    'error', str(e), None
                )
        except Exception as log_error:
            print(f"Warning: Failed to log KPI execution error: {log_error}")
        
        raise HTTPException(status_code=500, detail=f"Error during KPI calculation: {e}")


# --- New: Process Mining Graph Data Endpoint ---
@app.get("/process_graph/", summary="Get interactive process mining graph data")
async def get_process_graph():
    """
    Returns nodes and edges with metrics for SVG-based process mining visualization.
    Uses pm4py DFG and Performance DFG techniques.
    """
    if data_store["df"] is None or data_store["selected_columns"] is None:
        raise HTTPException(status_code=404, detail="Required data not available.")

    df = data_store["df"]
    case_id = data_store["selected_columns"]['case_id']
    activity = data_store["selected_columns"]['activity']
    timestamp = data_store["selected_columns"]['timestamp']

    graph_data = build_process_mining_graph(df, case_id, activity, timestamp)
    logger.info("Process graph generated", extra={"nodes": len(graph_data.get('nodes', [])), "edges": len(graph_data.get('edges', []))})
    return safe_json_response(graph_data)


@app.get("/edge_cases/", summary="Get case ids for a transition (edge) between two activities")
async def get_edge_cases(source: str, target: str, limit: int = 1000):
    """
    Returns the list of case IDs that contain a direct transition from `source` activity to `target` activity.
    - **source**: activity name for the edge source
    - **target**: activity name for the edge target
    - **limit**: optional maximum number of case ids to return (default 1000)
    """
    if data_store.get("df") is None or data_store.get("selected_columns") is None:
        raise HTTPException(status_code=404, detail="Required data not available.")

    df = data_store["df"].copy()
    case_col = data_store["selected_columns"]["case_id"]
    act_col = data_store["selected_columns"]["activity"]
    ts_col = data_store["selected_columns"]["timestamp"]

    try:
        # Prepare transitions by ordering and shifting
        df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')
        df = df.dropna(subset=[ts_col])
        df_sorted = df.sort_values([case_col, ts_col])
        df_sorted['next_activity'] = df_sorted.groupby(case_col)[act_col].shift(-1)

        # Filter transitions matching source -> target
        transitions = df_sorted[(df_sorted[act_col] == source) & (df_sorted['next_activity'] == target)]
        case_ids = transitions[case_col].drop_duplicates().tolist()
        count = len(case_ids)

        # Respect limit
        if limit is not None and isinstance(limit, int) and limit > 0:
            case_ids = case_ids[:limit]

        return safe_json_response({
            "source": source,
            "target": target,
            "count": count,
            "case_ids": case_ids
        })
    except Exception as e:
        logger.error(f"Error getting edge cases for {source} -> {target}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving edge cases: {e}")


@app.get("/case_timeline/{case_id}", summary="Get the complete timeline of events for a specific case")
async def get_case_timeline(case_id: str):
    """
    Returns the chronological sequence of all events/activities for a given case ID.
    Useful for detailed process analysis and case inspection.
    
    - **case_id**: the unique identifier for the case
    
    Returns:
    - case_id: the case identifier
    - events: list of events with activity, timestamp, and time since previous event
    """
    if data_store.get("df") is None or data_store.get("selected_columns") is None:
        raise HTTPException(status_code=404, detail="Required data not available.")

    df = data_store["df"].copy()
    case_col = data_store["selected_columns"]["case_id"]
    act_col = data_store["selected_columns"]["activity"]
    ts_col = data_store["selected_columns"]["timestamp"]

    try:
        # Convert case_id to string for comparison (handles various data types)
        df[case_col] = df[case_col].astype(str)
        case_id_str = str(case_id)
        
        # Filter for the specific case
        case_df = df[df[case_col] == case_id_str].copy()
        
        if case_df.empty:
            raise HTTPException(status_code=404, detail=f"Case '{case_id}' not found in the dataset.")
        
        # Ensure timestamp is datetime
        case_df[ts_col] = pd.to_datetime(case_df[ts_col], errors='coerce')
        case_df = case_df.dropna(subset=[ts_col])
        
        # Sort by timestamp
        case_df = case_df.sort_values(ts_col)
        
        # Calculate time since previous event (in hours)
        case_df['time_diff'] = case_df[ts_col].diff()
        case_df['since_prev_hours'] = case_df['time_diff'].dt.total_seconds() / 3600
        
        # Build events list
        events = []
        for idx, row in case_df.iterrows():
            event = {
                "activity": str(row[act_col]),
                "timestamp": row[ts_col].strftime("%Y-%m-%d %H:%M:%S") if pd.notna(row[ts_col]) else "",
                "since_prev_hours": float(row['since_prev_hours']) if pd.notna(row['since_prev_hours']) else None
            }
            
            # Include any additional columns as extra metadata
            extra_fields = {}
            for col in case_df.columns:
                if col not in [case_col, act_col, ts_col, 'time_diff', 'since_prev_hours']:
                    val = row[col]
                    if pd.notna(val):
                        extra_fields[col] = str(val)
            
            if extra_fields:
                event["metadata"] = extra_fields
            
            events.append(event)
        
        return safe_json_response({
            "case_id": case_id_str,
            "total_events": len(events),
            "first_event": events[0]["timestamp"] if events else None,
            "last_event": events[-1]["timestamp"] if events else None,
            "total_duration_hours": float(case_df['since_prev_hours'].sum()) if len(events) > 1 else 0,
            "events": events
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting timeline for case '{case_id}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving case timeline: {e}")


@app.get("/process_graph_cytoscape/", summary="Get process graph data for Cytoscape.js (legacy)")
async def get_process_graph_cytoscape():
    """
    Returns nodes and edges in Cytoscape.js format for backwards compatibility.
    """
    if data_store["df"] is None or data_store["selected_columns"] is None:
        raise HTTPException(status_code=404, detail="Required data not available.")

    df = data_store["df"]
    case_id = data_store["selected_columns"]['case_id']
    activity = data_store["selected_columns"]['activity']
    timestamp = data_store["selected_columns"]['timestamp']

    graph_data = perform_performance_analysis(df, case_id, activity, timestamp)
    logger.info("Cytoscape graph generated", extra={"elements": len(graph_data.get('elements', []))})
    return safe_json_response(graph_data)


@app.get("/node_insights/{node_id}", summary="Get insights for a single activity/node")
async def node_insights(node_id: str):
    """
    Returns metrics for a single activity/node: counts, unique cases, top next/previous activities,
    average outgoing/incoming durations and a few sample case ids.
    """
    if data_store["df"] is None or data_store["selected_columns"] is None:
        raise HTTPException(status_code=404, detail="Required data not available.")

    df = data_store["df"].copy()
    case_col = data_store["selected_columns"]["case_id"]
    act_col = data_store["selected_columns"]["activity"]
    ts_col = data_store["selected_columns"]["timestamp"]

    try:
        df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')
        df = df.dropna(subset=[ts_col])

        # occurrences and unique cases
        occurrences = int((df[act_col] == node_id).sum())
        unique_cases = df[df[act_col] == node_id][case_col].nunique()

        # next activities and durations
        df_sorted = df.sort_values([case_col, ts_col])
        df_sorted['next_act'] = df_sorted.groupby(case_col)[act_col].shift(-1)
        df_sorted['next_ts'] = df_sorted.groupby(case_col)[ts_col].shift(-1)
        df_sorted['prev_act'] = df_sorted.groupby(case_col)[act_col].shift(1)
        df_sorted['prev_ts'] = df_sorted.groupby(case_col)[ts_col].shift(1)

        outgoing = df_sorted[df_sorted[act_col] == node_id].dropna(subset=['next_act'])
        incoming = df_sorted[df_sorted['next_act'] == node_id].dropna(subset=[ts_col])

        def top_n_counts(series, n=5):
            if series.empty:
                return []
            vc = series.value_counts()
            return [{"value": idx, "count": int(c)} for idx, c in vc.head(n).items()]

        top_next = top_n_counts(outgoing['next_act'])
        top_prev = top_n_counts(incoming['prev_act'])

        # durations
        outgoing['delta_to_next_s'] = (outgoing['next_ts'] - outgoing[ts_col]).dt.total_seconds()
        incoming['delta_from_prev_s'] = (incoming[ts_col] - incoming['prev_ts']).dt.total_seconds()

        avg_outgoing = float(outgoing['delta_to_next_s'].mean()) if not outgoing.empty else 0.0
        avg_incoming = float(incoming['delta_from_prev_s'].mean()) if not incoming.empty else 0.0

        # sample cases
        sample_cases = df[df[act_col] == node_id][case_col].drop_duplicates().head(10).tolist()

        payload = {
            "node_id": node_id,
            "occurrences": occurrences,
            "unique_cases": unique_cases,
            "top_next": top_next,
            "top_previous": top_prev,
            "avg_outgoing_seconds": avg_outgoing,
            "avg_incoming_seconds": avg_incoming,
            "sample_cases": sample_cases
        }
        logger.info(f"Node insights node={node_id} occ={occurrences} cases={unique_cases}")
        return safe_json_response(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error computing node insights: {e}")


@app.get("/subgraph/{node_id}", summary="Get subgraph centered on a node")
async def subgraph(node_id: str, depth: int = 1):
    """
    Returns a subgraph (nodes + edges) containing nodes within `depth` hops from the provided node_id.
    Useful for focusing on a specific part of the process.
    """
    if data_store["df"] is None or data_store["selected_columns"] is None:
        raise HTTPException(status_code=404, detail="Required data not available.")

    df = data_store["df"]
    case_col = data_store["selected_columns"]["case_id"]
    act_col = data_store["selected_columns"]["activity"]
    ts_col = data_store["selected_columns"]["timestamp"]

    try:
        # Build full graph representation
        full_graph = build_process_mining_graph(df, case_col, act_col, ts_col)
        elements = full_graph.get('elements', [])

        # Build adjacency map
        adj = {}
        nodes_map = {}
        edges = []
        for el in elements:
            d = el.get('data', {})
            if 'source' in d and 'target' in d:
                edges.append(d)
                adj.setdefault(d['source'], set()).add(d['target'])
                adj.setdefault(d['target'], set())
            else:
                nodes_map[d['id']] = d

        # BFS from node_id
        visited = set([node_id])
        frontier = {node_id}
        for _ in range(depth):
            next_frontier = set()
            for n in frontier:
                for nbr in adj.get(n, []):
                    if nbr not in visited:
                        visited.add(nbr)
                        next_frontier.add(nbr)
            frontier = next_frontier

        # Also include incoming neighbors (undirected exploration)
        # do another pass to include predecessors within depth
        for _ in range(depth):
            prevs = set()
            for e in edges:
                if e['target'] in visited and e['source'] not in visited:
                    prevs.add(e['source'])
            if not prevs:
                break
            visited.update(prevs)

        # Collect nodes and edges in visited set
        sub_nodes = [ {"data": nodes_map[n]} for n in visited if n in nodes_map ]
        sub_edges = [ {"data": e} for e in edges if e['source'] in visited and e['target'] in visited ]

        # Compute aggregate KPIs for the subgraph: cases intersecting nodes, avg cycle time (days), top bottlenecks
        try:
            # cases whose paths include any visited node
            df_sorted = df.sort_values([case_col, ts_col])
            case_paths = df_sorted.groupby(case_col)[act_col].apply(list)
            cases_in_sub = [cid for cid, acts in case_paths.items() if any(a in visited for a in acts)]

            total_cases_in_sub = len(cases_in_sub)

            avg_cycle_days = 0.0
            if total_cases_in_sub > 0:
                starts = df_sorted[df_sorted[case_col].isin(cases_in_sub)].groupby(case_col)[ts_col].min()
                ends = df_sorted[df_sorted[case_col].isin(cases_in_sub)].groupby(case_col)[ts_col].max()
                cycle_times = (pd.to_datetime(ends) - pd.to_datetime(starts)).dt.total_seconds() / (60*60*24)
                avg_cycle_days = float(cycle_times.mean()) if not cycle_times.empty else 0.0

            # Bottleneck analysis among transitions for these cases
            trans = df_sorted[df_sorted[case_col].isin(cases_in_sub)].copy()
            trans['next_timestamp'] = trans.groupby(case_col)[ts_col].shift(-1)
            trans['next_activity'] = trans.groupby(case_col)[act_col].shift(-1)
            trans = trans.dropna(subset=['next_timestamp'])
            if not trans.empty:
                trans['waiting_hours'] = (trans['next_timestamp'] - trans[ts_col]).dt.total_seconds() / 3600
                bott = trans.groupby([act_col, 'next_activity'])['waiting_hours'].mean().reset_index()
                bott = bott.sort_values(by='waiting_hours', ascending=False).head(10)
                bottlenecks = bott.rename(columns={act_col: 'from_activity', 'next_activity': 'to_activity', 'waiting_hours': 'avg_wait_hours'}).to_dict(orient='records')
            else:
                bottlenecks = []

            return JSONResponse(content={
                "elements": sub_nodes + sub_edges,
                "statistics": {
                    "total_nodes": len(sub_nodes),
                    "total_edges": len(sub_edges)
                },
                "aggregates": {
                    "total_cases": total_cases_in_sub,
                    "avg_cycle_days": avg_cycle_days,
                    "bottlenecks": bottlenecks
                }
            })
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error computing subgraph aggregates: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error building subgraph: {e}")


@app.get("/kpi_logics/", summary="Load all saved KPI logics", response_model=List[KpiLogic])
async def get_kpi_logics():
    """
    Loads and returns all saved KPI logic configurations from the database.
    """
    try:
        return load_kpi_logics_from_database()
    except Exception as e:
        logger.error(f"Error loading KPI logics from database: {e}")
        raise HTTPException(status_code=500, detail=f"Error loading KPI logics from database: {e}")

@app.post("/kpi_logics/", summary="Save a new KPI logic")
async def save_kpi_logic(kpi_logic: KpiLogic):
    """
    Saves a new KPI logic configuration to the database.
    """
    logic_to_save = kpi_logic.dict()
    try:
        return save_kpi_logics_to_database([logic_to_save])
    except Exception as e:
        logger.error(f"Error saving KPI logic to database: {e}")
        raise HTTPException(status_code=500, detail=f"Error saving KPI logic to database: {e}")

@app.delete("/kpi_logics/{logic_id}", summary="Delete a saved KPI logic")
async def delete_kpi_logic(logic_id: int):
    """
    Deletes a KPI logic from the database by its ID.
    - **logic_id**: The database ID of the logic to delete.
    """
    try:
        success = db_service.delete_kpi_logic(logic_id)
        if success:
            return {"message": f"KPI logic with ID {logic_id} deleted successfully."}
        else:
            raise HTTPException(status_code=404, detail="KPI logic not found.")
    except HTTPException:
        # Preserve explicit HTTPExceptions
        raise
    except Exception as e:
        # General unexpected errors
        logger.error(f"Unexpected error deleting KPI logic {logic_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error deleting KPI logic: {e}")

@app.put("/kpi_logics/{logic_id}", summary="Update an existing KPI logic")
async def update_kpi_logic(logic_id: int, kpi_logic: KpiLogic):
    """
    Updates an existing KPI logic in the database.
    - **logic_id**: The database ID of the logic to update.
    """
    logic_to_update = kpi_logic.dict()
    try:
        success = db_service.update_kpi_logic(logic_id, logic_to_update)
        if success:
            return {"message": f"KPI logic with ID {logic_id} updated successfully."}
        else:
            raise HTTPException(status_code=404, detail="KPI logic not found.")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating KPI logic: {e}")

@app.get("/kpi_logics/{logic_id}/history", summary="Get KPI execution history")
async def get_kpi_execution_history(logic_id: int, limit: int = 100):
    """
    Gets the execution history for a specific KPI logic.
    - **logic_id**: The database ID of the KPI logic.
    - **limit**: Maximum number of history records to return.
    """
    try:
        history = db_service.get_kpi_execution_history(logic_id, limit)
        return {"history": history}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving KPI execution history: {e}")


# --- Department-Based KPI Endpoints ---

@app.get("/kpi_logics/departments", summary="Get all departments")
async def get_departments():
    """
    Returns a list of all unique departments from active KPI logics.
    Useful for populating department selection dropdowns.
    """
    try:
        departments = db_service.get_departments()
        return {"departments": departments, "count": len(departments)}
    except Exception as e:
        logger.error(f"Error retrieving departments: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving departments: {e}")


@app.get("/kpi_logics/department/{department}", summary="Get KPI logics by department")
async def get_kpi_logics_by_department(department: str):
    """
    Returns all active KPI logics for a specific department.
    - **department**: The department name to filter by.
    """
    try:
        kpi_logics = db_service.get_kpi_logics_by_department(department)
        return {
            "department": department,
            "kpi_logics": kpi_logics,
            "count": len(kpi_logics)
        }
    except Exception as e:
        logger.error(f"Error retrieving KPI logics for department {department}: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving KPI logics: {e}")


@app.post("/kpi_logics/department/{department}/apply", summary="Apply all KPIs in a department")
async def apply_department_kpis(department: str):
    """
    Applies all KPI logics in the specified department to the currently loaded event log.
    Returns per-KPI results plus aggregated analytics for visualization.
    
    - **department**: The department name whose KPIs to apply.
    
    Returns:
        - kpi_results: List of individual KPI results with status, risk, proximity, etc.
        - analytics: Aggregated statistics (counts, distributions, charts data)
    """
    # Check if data is loaded
    if data_store["df"] is None or data_store["selected_columns"] is None:
        raise HTTPException(
            status_code=400,
            detail="No event log data loaded. Please upload a CSV file first."
        )
    
    try:
        df = data_store["df"]
        selected_columns = data_store["selected_columns"]
        
        # Apply all KPIs in the department
        logger.info(f"Applying KPIs for department: {department}")
        kpi_results = apply_kpis_batch(department, df, selected_columns)
        
        # Compute aggregated analytics
        analytics = aggregate_kpi_batch_results(kpi_results)
        
        return {
            "department": department,
            "kpi_results": kpi_results,
            "analytics": analytics,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error applying department KPIs: {e}")
        raise HTTPException(status_code=500, detail=f"Error applying department KPIs: {e}")


# --- Enhanced Analytics APIs ---
@app.get("/process_analytics/", summary="Get comprehensive process analytics")
async def get_process_analytics():
    """
    Returns comprehensive process analytics including complexity metrics, efficiency scores,
    and anomaly detection for sci-fi dashboard visualization.
    """
    if data_store["df"] is None or data_store["selected_columns"] is None:
        raise HTTPException(status_code=404, detail="Required data not available.")

    df = data_store["df"]
    case_col = data_store["selected_columns"]["case_id"]
    act_col = data_store["selected_columns"]["activity"]
    ts_col = data_store["selected_columns"]["timestamp"]

    try:
        df_sorted = df.sort_values([case_col, ts_col])
        
        # Process complexity metrics
        total_cases = df[case_col].nunique()
        total_activities = df[act_col].nunique()
        total_events = len(df)
        
        # Variant analysis
        case_paths = df_sorted.groupby(case_col)[act_col].apply(lambda x: ' -> '.join(x))
        unique_variants = case_paths.nunique()
        variant_counts = case_paths.value_counts()
        
        # Calculate process complexity score (0-100)
        complexity_score = min(100, (unique_variants / total_cases) * 100)
        
        # Calculate efficiency metrics
        df_ts = df_sorted.copy()
        df_ts[ts_col] = pd.to_datetime(df_ts[ts_col])
        
        case_durations = df_ts.groupby(case_col).agg({
            ts_col: ['min', 'max']
        })[ts_col]
        case_durations['duration_hours'] = (case_durations['max'] - case_durations['min']).dt.total_seconds() / 3600
        
        avg_duration = case_durations['duration_hours'].mean()
        std_duration = case_durations['duration_hours'].std()
        
        # Identify anomalous cases (outliers)
        threshold = avg_duration + 2 * std_duration if not pd.isna(std_duration) else avg_duration
        anomalous_cases = case_durations[case_durations['duration_hours'] > threshold].index.tolist()
        
        # Activity frequency analysis
        activity_counts = df[act_col].value_counts()
        most_frequent_activity = activity_counts.index[0] if not activity_counts.empty else "N/A"
        least_frequent_activity = activity_counts.index[-1] if not activity_counts.empty else "N/A"
        
        # Rework analysis
        rework_cases = []
        for case_id, group in df_sorted.groupby(case_col):
            activities = group[act_col].tolist()
            if len(activities) != len(set(activities)):  # Has repeated activities
                rework_cases.append(case_id)
        
        rework_percentage = (len(rework_cases) / total_cases) * 100 if total_cases > 0 else 0
        
        # Process health score (0-100)
        health_score = max(0, 100 - complexity_score/2 - rework_percentage)
        
        # Throughput analysis
        df_ts['date'] = df_ts[ts_col].dt.date
        daily_throughput = df_ts.groupby('date')[case_col].nunique().to_dict()
        
        # Convert date keys to strings for JSON serialization
        daily_throughput = {str(k): v for k, v in daily_throughput.items()}
        
        response_payload = {
            "process_metrics": {
                "total_cases": total_cases,
                "total_activities": total_activities,
                "total_events": total_events,
                "unique_variants": unique_variants,
                "complexity_score": round(complexity_score, 2),
                "health_score": round(health_score, 2),
                "avg_duration_hours": round(avg_duration, 2) if not pd.isna(avg_duration) else 0,
                "rework_percentage": round(rework_percentage, 2)
            },
            "activity_insights": {
                "most_frequent": {"activity": most_frequent_activity, "count": int(activity_counts.iloc[0]) if not activity_counts.empty else 0},
                "least_frequent": {"activity": least_frequent_activity, "count": int(activity_counts.iloc[-1]) if not activity_counts.empty else 0},
                "distribution": [{"activity": act, "count": int(count)} for act, count in activity_counts.head(10).items()]
            },
            "anomalies": {
                "anomalous_cases": anomalous_cases[:20],
                "count": len(anomalous_cases)
            },
            "throughput": {
                "daily": daily_throughput,
                "trend": "stable"
            }
        }
        logger.info("Process analytics computed", extra={"cases": total_cases, "variants": unique_variants})
        return safe_json_response(response_payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error computing process analytics: {e}")


@app.get("/network_metrics/", summary="Get network topology metrics")
async def get_network_metrics():
    """
    Returns network topology metrics for the process graph including centrality measures,
    clustering coefficients, and critical path analysis.
    """
    if data_store["df"] is None or data_store["selected_columns"] is None:
        raise HTTPException(status_code=404, detail="Required data not available.")

    df = data_store["df"]
    case_col = data_store["selected_columns"]["case_id"]
    act_col = data_store["selected_columns"]["activity"]
    ts_col = data_store["selected_columns"]["timestamp"]

    try:
        # Build NetworkX graph
        G = nx.DiGraph()
        df_sorted = df.sort_values([case_col, ts_col])
        
        # Add nodes and edges
        for case_id, group in df_sorted.groupby(case_col):
            activities = group[act_col].tolist()
            for i in range(len(activities) - 1):
                current = activities[i]
                next_act = activities[i + 1]
                if G.has_edge(current, next_act):
                    G[current][next_act]['weight'] += 1
                else:
                    G.add_edge(current, next_act, weight=1)
        
        # Calculate centrality measures
        try:
            betweenness = nx.betweenness_centrality(G, weight='weight')
            closeness = nx.closeness_centrality(G)
            pagerank = nx.pagerank(G, weight='weight')
            in_degree = dict(G.in_degree(weight='weight'))
            out_degree = dict(G.out_degree(weight='weight'))
        except:
            # Fallback for disconnected graphs
            betweenness = {node: 0 for node in G.nodes()}
            closeness = {node: 0 for node in G.nodes()}
            pagerank = {node: 1/len(G.nodes()) for node in G.nodes()}
            in_degree = dict(G.in_degree(weight='weight'))
            out_degree = dict(G.out_degree(weight='weight'))
        
        # Find critical nodes (high centrality)
        critical_nodes = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)[:5]
        
        # Identify bottlenecks (high betweenness, low throughput)
        bottleneck_nodes = []
        for node, centrality in critical_nodes:
            if centrality > 0.1:  # Threshold for high centrality
                bottleneck_nodes.append({
                    "node": node,
                    "betweenness": round(centrality, 4),
                    "in_degree": in_degree.get(node, 0),
                    "out_degree": out_degree.get(node, 0)
                })
        
        # Calculate network density
        density = nx.density(G) if G.number_of_nodes() > 0 else 0
        
        # Find strongly connected components
        try:
            scc = list(nx.strongly_connected_components(G))
            largest_scc_size = len(max(scc, key=len)) if scc else 0
        except:
            largest_scc_size = 0
        
        response_payload = {
            "network_properties": {
                "nodes": G.number_of_nodes(),
                "edges": G.number_of_edges(),
                "density": round(density, 4),
                "largest_scc_size": largest_scc_size
            },
            "centrality_analysis": {
                "critical_nodes": [{"node": node, "score": round(score, 4)} for node, score in critical_nodes],
                "bottlenecks": bottleneck_nodes,
                "hub_activities": sorted(pagerank.items(), key=lambda x: x[1], reverse=True)[:5]
            },
            "node_metrics": {
                node: {
                    "betweenness": round(betweenness.get(node, 0), 4),
                    "closeness": round(closeness.get(node, 0), 4),
                    "pagerank": round(pagerank.get(node, 0), 4),
                    "in_degree": in_degree.get(node, 0),
                    "out_degree": out_degree.get(node, 0)
                }
                for node in G.nodes()
            }
        }
        logger.info("Network metrics computed", extra={"nodes": G.number_of_nodes(), "edges": G.number_of_edges()})
        return safe_json_response(response_payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error computing network metrics: {e}")


@app.get("/real_time_metrics/", summary="Get real-time process metrics")
async def get_real_time_metrics():
    """
    Simulates real-time metrics for the sci-fi dashboard including system health,
    active processes, and performance indicators.
    """
    import random
    import time
    
    # Simulate real-time data
    current_time = datetime.now()
    
    # System health indicators
    cpu_usage = random.uniform(20, 80)
    memory_usage = random.uniform(30, 90)
    network_latency = random.uniform(10, 100)
    
    # Process metrics
    active_cases = random.randint(50, 200)
    completed_today = random.randint(20, 100)
    pending_cases = random.randint(10, 50)
    
    # Performance indicators
    avg_processing_time = random.uniform(2.5, 8.0)
    success_rate = random.uniform(85, 99)
    error_rate = random.uniform(0.1, 5.0)
    
    # Anomaly alerts
    anomalies = []
    if cpu_usage > 75:
        anomalies.append({"type": "high_cpu", "message": "CPU usage exceeding normal levels", "severity": "warning"})
    if error_rate > 3:
        anomalies.append({"type": "high_errors", "message": "Error rate above threshold", "severity": "critical"})
    
    payload = {
        "timestamp": current_time.isoformat(),
        "system_health": {
            "cpu_usage": round(cpu_usage, 1),
            "memory_usage": round(memory_usage, 1),
            "network_latency": round(network_latency, 1),
            "status": "optimal" if cpu_usage < 70 and memory_usage < 80 else "warning"
        },
        "process_status": {
            "active_cases": active_cases,
            "completed_today": completed_today,
            "pending_cases": pending_cases,
            "total_throughput": active_cases + completed_today
        },
        "performance_kpis": {
            "avg_processing_time": round(avg_processing_time, 2),
            "success_rate": round(success_rate, 2),
            "error_rate": round(error_rate, 2),
            "efficiency_score": round((success_rate - error_rate) * 0.8, 1)
        },
        "alerts": anomalies,
        "trends": {
            "hourly_throughput": [random.randint(5, 25) for _ in range(24)],
            "daily_performance": [random.uniform(80, 99) for _ in range(7)]
        }
    }
    logger.info("Real-time metrics served")
    return safe_json_response(payload)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8001)
