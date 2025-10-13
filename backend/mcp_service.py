import pathlib
from typing import Any, Dict
import pandas as pd
import json
import logging
import io
import sys
import time
import uuid
import traceback
import signal
import os
from contextlib import redirect_stdout, redirect_stderr

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Infer better data types for schema
def infer_better_type(series):
    """Infer a more descriptive data type for a pandas Series."""
    # Remove nulls for analysis
    non_null = series.dropna()

    if len(non_null) == 0:
        return "string (empty)"

    # Check current dtype first
    dtype_str = str(series.dtype)

    # If already a good type, keep it
    if 'int' in dtype_str:
        return dtype_str
    if 'float' in dtype_str:
        return dtype_str
    if 'bool' in dtype_str:
        return 'boolean'
    if 'datetime' in dtype_str:
        return 'datetime'

    # Try to infer better types for 'object' columns
    if dtype_str == 'object':
        # Try boolean
        if non_null.isin([0, 1, '0', '1', True, False, 'True', 'False', 'true', 'false']).all():
            return 'boolean'

        # Try integer
        try:
            converted = pd.to_numeric(non_null, errors='raise')
            if (converted == converted.astype(int)).all():
                return 'integer'
        except (ValueError, TypeError):
            pass

        # Try float
        try:
            pd.to_numeric(non_null, errors='raise')
            return 'float'
        except (ValueError, TypeError):
            pass

        # Try datetime
        try:
            pd.to_datetime(non_null, errors='raise')
            return 'datetime'
        except (ValueError, TypeError):
            pass

        return 'string'

    return dtype_str


def _posix_time_limit(seconds: float):
    """POSIX-only wall clock timeout using signals; noop elsewhere."""
    class _TL:
        def __enter__(self_):
            self_.posix = (os.name == "posix" and hasattr(signal, "setitimer"))
            if not self_.posix:
                return
            self_.old_handler = signal.getsignal(signal.SIGALRM)
            def _raise(_sig, _frm):
                raise TimeoutError("time limit exceeded")
            signal.signal(signal.SIGALRM, _raise)
            signal.setitimer(signal.ITIMER_REAL, float(seconds))
        def __exit__(self_, exc_type, exc, tb):
            if self_.posix:
                signal.setitimer(signal.ITIMER_REAL, 0.0)
                signal.signal(signal.SIGALRM, self_.old_handler)
            return False
    return _TL()


def format_csv_response(filepath: pathlib.Path, df: Any) -> str:
    """
    Generate standardized response format for CSV data files.

    Args:
        filepath: Path to the saved CSV file
        df: DataFrame that was saved

    Returns:
        Formatted string with file info, schema, sample data, and Python snippet
    """
    
    # Log input parameters
    logger.info(f"format_csv_response called with filepath: {filepath}")
    logger.info(f"DataFrame shape before processing: {df.shape if hasattr(df, 'shape') else 'No shape attribute'}")
    logger.info(f"DataFrame type: {type(df)}")
    
    try:
        # Log DataFrame length explicitly
        df_len = len(df) if hasattr(df, '__len__') else 'Unknown'
        logger.info(f"DataFrame length: {df_len}")
        
        # Log DataFrame columns if available
        if hasattr(df, 'columns'):
            logger.info(f"DataFrame columns: {list(df.columns)}")
        else:
            logger.warning("DataFrame has no 'columns' attribute")

        # Get file size
        logger.info("Getting file size...")
        file_size_bytes = filepath.stat().st_size
        logger.info(f"File size: {file_size_bytes} bytes")
        
        if file_size_bytes < 1024:
            size_str = f"{file_size_bytes} bytes"
        elif file_size_bytes < 1024 * 1024:
            size_str = f"{file_size_bytes / 1024:.1f} KB"
        else:
            size_str = f"{file_size_bytes / (1024 * 1024):.1f} MB"
        
        logger.info(f"Formatted file size: {size_str}")

        # Get filename only (relative to CSV_PATH)
        filename = filepath.name
        logger.info(f"Filename: {filename}")

        # Build schema JSON with inferred types
        logger.info("Building schema...")
        schema = {col: infer_better_type(df[col]) for col in df.columns}
        schema_json = json.dumps(schema, indent=2)
        logger.info(f"Schema generated with {len(schema)} columns")

        # Generate sample data (first row) as markdown table
        logger.info("Generating sample data table...")
        if len(df) > 0:
            sample_df = df.head(1)
            # Create markdown table manually for better control
            headers = list(sample_df.columns)
            values = [str(v) for v in sample_df.iloc[0].values]

            # Truncate long values for display
            values = [v[:50] + "..." if len(v) > 50 else v for v in values]

            # Build markdown table
            header_row = "| " + " | ".join(headers) + " |"
            separator = "|" + "|".join(["-" * (len(h) + 2) for h in headers]) + "|"
            value_row = "| " + " | ".join(values) + " |"

            sample_table = f"{header_row}\n{separator}\n{value_row}"
            logger.info("Sample table generated successfully")
        else:
            sample_table = "(empty dataset)"
            logger.warning("DataFrame is empty, using placeholder for sample table")

        # Create Python snippet
        python_snippet = f"""import pandas as pd
df = pd.read_csv('data/mcp-polygon/{filename}')
print(df.info())
print(df.head())"""

        # Build final response
        logger.info("Building final response...")
        response = f"""✓ Data saved to CSV

File: {filename}
Rows: {len(df)}
Size: {size_str}

Schema (JSON):
{schema_json}

Sample (first row):
{sample_table}

Python snippet to load:
```python
{python_snippet}
```"""

        logger.info(f"Response generated successfully. Response length: {len(response)} characters")
        return response

    except Exception as e:
        logger.error(f"Error in format_csv_response: {str(e)}")
        logger.error(f"Exception type: {type(e)}")
        # Re-raise the exception to maintain original behavior
        raise


def register_py_eval(local_mcp_instance, csv_dir):
    """Register the py_eval tool for Python code execution"""
    @local_mcp_instance.tool()
    def py_eval(code: str, timeout_sec: float = 5.0) -> Dict[str, Any]:
        """
        Execute Python code with pandas/numpy pre-loaded and access to CSV folder.

        Parameters:
            code (str): Python code to execute
            timeout_sec (float): Execution timeout in seconds (default: 5.0)

        Returns:
            dict: Execution results with stdout, stderr, duration_ms, and error info

        Available variables in execution environment:
            - pd: pandas library
            - np: numpy library
            - CSV_PATH: path to data/mcp-polygon folder for reading/writing CSV files
        """
        logger.info(f"py_eval invoked with {len(code)} characters of code")

        # Capture output
        buf_out, buf_err = io.StringIO(), io.StringIO()
        started = time.time()

        try:
            # Import scientific libraries in execution environment
            import pandas as pd
            import numpy as np

            # Create execution environment
            env = {
                "__builtins__": __builtins__,
                "pd": pd,
                "np": np,
                "CSV_PATH": str(csv_dir),
            }

            with redirect_stdout(buf_out), redirect_stderr(buf_err), _posix_time_limit(timeout_sec):
                exec(code, env, env)
            ok, error = True, None

        except TimeoutError as e:
            ok, error = False, f"Timeout: {e}"
        except Exception:
            ok, error = False, traceback.format_exc()

        duration_ms = int((time.time() - started) * 1000)

        result = {
            "ok": ok,
            "stdout": buf_out.getvalue(),
            "stderr": buf_err.getvalue(),
            "error": error,
            "duration_ms": duration_ms,
            "csv_path": str(csv_dir)
        }

        logger.info(f"py_eval completed: ok={ok}, duration={duration_ms}ms")
        return result