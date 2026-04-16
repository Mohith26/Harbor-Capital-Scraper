"""engine/loaders.py — file loading, sheet detection, and table segmentation."""
import os
import re
import pandas as pd

MAX_DATA_ROWS = 500

# --- HEADER DETECTION KEYWORDS ---
HEADER_KEYWORDS = {
    'address', 'city', 'tenant', 'buyer', 'seller', 'date', 'price', 'sqft',
    'size', 'rate', 'term', 'commencement', 'lessee', 'sf', 'psf', 'rent',
    'building', 'property', 'lease', 'cap', 'closing', 'year', 'type',
    'height', 'notes', 'acres', 'acreage', 'submarket', 'class', 'vintage',
    'structure', 'nnn', 'gross', 'transaction', 'sale', 'landlord', 'occupant',
}


def get_sheet_names(file_path):
    """Return list of sheet names for Excel files, or [None] for CSV."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext in ('.xlsx', '.xls'):
        import openpyxl
        wb = openpyxl.load_workbook(file_path, read_only=True)
        names = wb.sheetnames
        wb.close()
        return names
    return [None]


def detect_table_segments(file_path, sheet_name=None, max_rows=None):
    """Split an Excel/CSV sheet into multiple table segments at gap rows.
    Returns list of dicts: [{'df': DataFrame, 'segment_title': str|None, 'segment_index': int}, ...]
    Each df has its own header detected and applied. Single-table sheets return a 1-element list."""
    if max_rows is None:
        max_rows = MAX_DATA_ROWS
    ext = os.path.splitext(file_path)[1].lower()

    # Read the entire sheet raw
    try:
        read_kwargs = {'header': None}
        if ext in ('.xlsx', '.xls'):
            read_kwargs['engine'] = 'openpyxl'
            if sheet_name is not None:
                read_kwargs['sheet_name'] = sheet_name
            df_raw = pd.read_excel(file_path, **read_kwargs)
        else:
            df_raw = pd.read_csv(file_path, **read_kwargs)
    except Exception:
        return []

    if df_raw.empty:
        return []

    # Identify gap rows (entirely empty or whitespace-only)
    gap_mask = df_raw.apply(
        lambda row: row.isna().all() or row.astype(str).str.strip().eq('').all(), axis=1
    )

    # Group consecutive non-gap rows into segments
    raw_segments = []
    current_start = None
    for idx in range(len(df_raw)):
        if gap_mask.iloc[idx]:
            if current_start is not None:
                raw_segments.append((current_start, idx))
                current_start = None
        else:
            if current_start is None:
                current_start = idx
    if current_start is not None:
        raw_segments.append((current_start, len(df_raw)))

    if not raw_segments:
        return []

    # Process each segment: detect header, build DataFrame
    results = []
    pending_title = None

    for seg_start, seg_end in raw_segments:
        seg_raw = df_raw.iloc[seg_start:seg_end].reset_index(drop=True)

        # Trim leading empty columns
        seg_raw = _trim_leading_empty_columns(seg_raw)
        if seg_raw.empty or len(seg_raw.columns) == 0:
            continue

        # Score rows for header keywords
        best_row_idx, max_score = -1, 0
        for ridx, row in seg_raw.iterrows():
            row_text = " ".join(row.dropna().astype(str)).lower()
            score = sum(1 for k in HEADER_KEYWORDS if k in row_text)
            if score > max_score:
                max_score, best_row_idx = score, ridx

        # If score is too low, this might be a title-only row
        if max_score < 2:
            if len(seg_raw) <= 2:
                # Capture as title for next segment
                title_text = " ".join(seg_raw.iloc[0].dropna().astype(str)).strip()
                if title_text:
                    pending_title = title_text
                continue
            # No clear header — skip this segment
            continue

        # Build DataFrame with detected header
        header_row = seg_raw.iloc[best_row_idx]
        col_names = [str(v).strip() if pd.notna(v) and str(v).strip() else f"Unnamed_{i}"
                     for i, v in enumerate(header_row)]
        data_rows = seg_raw.iloc[best_row_idx + 1:].copy()
        data_rows.columns = col_names

        # Merge split headers
        data_rows = _merge_split_headers(data_rows)
        data_rows = data_rows.dropna(how='all')

        # Truncate
        if max_rows and len(data_rows) > max_rows:
            data_rows = data_rows.head(max_rows)

        # Trim leading empty columns again after header assignment
        data_rows = _trim_leading_empty_columns(data_rows)

        # Deduplicate column names
        seen = {}
        new_cols = []
        for col in data_rows.columns:
            col_str = str(col)
            if col_str in seen:
                seen[col_str] += 1
                new_cols.append(f"{col_str}_{seen[col_str]}")
            else:
                seen[col_str] = 1
                new_cols.append(col_str)
        data_rows.columns = new_cols

        if data_rows.empty:
            continue

        results.append({
            'df': data_rows.reset_index(drop=True),
            'segment_title': pending_title,
            'segment_index': len(results),
        })
        pending_title = None

    return results


def robust_load_file_segmented(file_path, sheet_name=None, max_rows=None):
    """Return list of segment dicts for a sheet. Falls back to robust_load_file()."""
    segments = detect_table_segments(file_path, sheet_name=sheet_name, max_rows=max_rows)
    if segments:
        return segments
    # Fallback to original loader
    df = robust_load_file(file_path, max_rows=max_rows, sheet_name=sheet_name)
    if df is not None and not df.empty:
        return [{'df': df, 'segment_title': None, 'segment_index': 0}]
    return []


def robust_load_file(file_path, max_rows=None, sheet_name=None):
    """Load CSV or Excel file with intelligent header detection and split-header merging."""
    if max_rows is None:
        max_rows = MAX_DATA_ROWS
    ext = os.path.splitext(file_path)[1].lower()

    # Build common kwargs for Excel reads
    excel_kwargs = {'engine': 'openpyxl'}
    if sheet_name is not None:
        excel_kwargs['sheet_name'] = sheet_name

    # Read first 30 rows to find header
    try:
        if ext in ('.xlsx', '.xls'):
            df_raw = pd.read_excel(file_path, header=None, nrows=30, **excel_kwargs)
        else:
            df_raw = pd.read_csv(file_path, header=None, nrows=30)
    except Exception as e:
        print(f"   > Error reading file: {e}")
        return None

    # Score each row to find the best header
    best_row_idx, max_score = -1, 0
    for idx, row in df_raw.iterrows():
        row_text = " ".join(row.dropna().astype(str)).lower()
        score = sum(1 for k in HEADER_KEYWORDS if k in row_text)
        if score > max_score:
            max_score, best_row_idx = score, idx

    # Require minimum score of 2 to accept a header row
    if best_row_idx == -1 or max_score < 2:
        try:
            if ext in ('.xlsx', '.xls'):
                return pd.read_excel(file_path, **excel_kwargs)
            return pd.read_csv(file_path)
        except:
            return None

    # Reload with detected header
    try:
        if ext in ('.xlsx', '.xls'):
            df = pd.read_excel(file_path, header=best_row_idx, **excel_kwargs)
        else:
            df = pd.read_csv(file_path, header=best_row_idx)
    except:
        return None

    # Check for split/multi-row headers (merge up to 2 sub-header rows)
    df = _merge_split_headers(df)
    df = df.dropna(how='all')

    # Truncate large files
    if max_rows and len(df) > max_rows:
        df = df.head(max_rows)

    # Deduplicate column names (append _2, _3, etc.)
    seen = {}
    new_cols = []
    for col in df.columns:
        col_str = str(col)
        if col_str in seen:
            seen[col_str] += 1
            new_cols.append(f"{col_str}_{seen[col_str]}")
        else:
            seen[col_str] = 1
            new_cols.append(col_str)
    df.columns = new_cols

    return df


def _is_data_row(row_text):
    """Heuristic: returns True if a row looks like data rather than a header."""
    if '$' in row_text:
        return True
    if re.search(r'\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}', row_text):
        return True
    digits = sum(c.isdigit() for c in row_text)
    if len(row_text) > 0 and (digits / len(row_text)) > 0.3:
        return True
    return False


def _merge_split_headers(df):
    """If the first row(s) after the header look like sub-headers, merge them into column names."""
    if len(df) < 1:
        return df

    for _ in range(2):  # Check up to 2 sub-header rows
        if len(df) < 1:
            break
        sub_row = df.iloc[0]
        row_text = " ".join(sub_row.dropna().astype(str))

        if _is_data_row(row_text):
            break

        non_empty = sub_row.dropna().astype(str).str.strip().apply(len).gt(0).sum()
        if non_empty <= len(df.columns) * 0.3:
            break

        # Merge sub-header into column names
        new_columns = []
        for col, sub in zip(df.columns, sub_row):
            col_str = str(col).strip()
            sub_str = str(sub).strip()
            if "Unnamed" in col_str:
                col_str = ""
            if sub_str.lower() == "nan" or not sub_str:
                sub_str = ""
            combined = f"{col_str} {sub_str}".strip()
            new_columns.append(combined if combined else "Unknown")

        df.columns = new_columns
        df = df.iloc[1:].reset_index(drop=True)

    return df


def _trim_leading_empty_columns(df):
    """Drop leading columns that are entirely NaN/empty."""
    cols_to_drop = []
    for col in df.columns:
        col_data = df[col]
        if col_data.isna().all() or col_data.astype(str).str.strip().eq('').all():
            cols_to_drop.append(col)
        else:
            break
    return df.drop(columns=cols_to_drop) if cols_to_drop else df
