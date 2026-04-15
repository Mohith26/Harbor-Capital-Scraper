"""engine/cleaning.py — header cleaning, column profiling, and rate logic."""
import re
import pandas as pd
import numpy as np
from dateutil.parser import parse

HOUSTON_RATE_THRESHOLD = 4.0  # Configurable: rates <= this are assumed monthly


def clean_header(header):
    text = str(header).lower()
    # Normalize whitespace: newlines, tabs, multiple spaces → single space
    text = re.sub(r'[\n\r\t]+', ' ', text)
    text = text.replace('_', ' ').replace('.', ' ')
    text = re.sub(r'[^\w\s/]', '', text).strip()
    text = re.sub(r'\s+', ' ', text)
    return text


def get_column_profile(series):
    """Profile a column's data type from a sample of values."""
    # Handle duplicate column names returning a DataFrame
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]
    sample = series.dropna().astype(str).head(20).tolist()
    # Filter out placeholder values
    sample = [s for s in sample if s.strip() not in ('', '-', '_', '--', 'N/A', 'n/a', 'nan', 'None')]
    if not sample:
        return 'empty'

    joined = " ".join(sample).lower()
    has_money = '$' in joined

    # Try numeric detection — use _to_float for more robust parsing
    numeric_count = 0
    for x in sample:
        cleaned = re.sub(r'[$,%]', '', x).strip()
        try:
            float(cleaned.replace(',', ''))
            numeric_count += 1
        except ValueError:
            # Also count percentage values and rate strings as numeric
            if re.match(r'^[\d.]+\s*%', cleaned) or re.match(r'^[\d,.]+\s*/\s*\w+', x.strip()):
                numeric_count += 1

    is_numeric = (numeric_count / len(sample)) > 0.5 if sample else False

    # Date detection
    if not is_numeric:
        date_count = 0
        for val in sample[:8]:
            try:
                parse(val, fuzzy=False)
                if any(c in val for c in ['/', '-', ',']):
                    date_count += 1
            except (ValueError, OverflowError):
                pass
        if date_count >= 2:
            return 'date'

    if is_numeric:
        return 'numeric_money' if has_money else 'numeric_clean'
    return 'text'


def _detect_rate_unit_from_header(rate_header):
    """Parse the original column header to determine if rate is monthly or annual."""
    if rate_header is None:
        return None
    h = str(rate_header).lower()
    monthly_hints = [
        'monthly', '/mo', 'per month', ' mo ', ' mo.',
        'psf/mo', 'per sf per month', '$/sf/mo', 'per mo', '/month', 'per sf/mo',
    ]
    annual_hints = [
        'annual', 'yearly', '/yr', 'per year', ' yr ', ' yr.',
        'annually', 'psf/yr', 'per sf per year', '$/sf/yr',
        'per annum', '/year', 'per sf/yr', 'p.a.',
        'nnn/sf/yr', 'per acre per year',
    ]
    for hint in monthly_hints:
        if hint in h:
            return 'monthly'
    for hint in annual_hints:
        if hint in h:
            return 'annual'
    return None


def apply_rate_logic(clean_df, rate_header=None, threshold=HOUSTON_RATE_THRESHOLD):
    """Split rate_psf into rate_monthly and rate_annually.
    First checks header text for unit hints, then falls back to magnitude heuristic."""
    clean_df['rate_monthly'] = None
    clean_df['rate_annually'] = None
    clean_df['rate_basis'] = None

    if 'rate_psf' not in clean_df.columns:
        return clean_df

    header_unit = _detect_rate_unit_from_header(rate_header)

    # If no header hint, use column-level median to decide uniformly
    if header_unit is None:
        float_vals = [_to_float(v) for v in clean_df['rate_psf']]
        valid_vals = [v for v in float_vals if v is not None and v > 0]
        if valid_vals:
            median_val = sorted(valid_vals)[len(valid_vals) // 2]
            header_unit = 'monthly' if median_val <= threshold else 'annual'

    monthly_list, annual_list, basis_list = [], [], []

    for val in clean_df['rate_psf']:
        f_val = _to_float(val)
        if f_val is None:
            monthly_list.append(None)
            annual_list.append(None)
            basis_list.append(None)
            continue

        if header_unit == 'monthly':
            monthly_list.append(round(f_val, 2))
            annual_list.append(round(f_val * 12, 2))
            basis_list.append('monthly_from_header')
        elif header_unit == 'annual':
            annual_list.append(round(f_val, 2))
            monthly_list.append(round(f_val / 12, 2))
            basis_list.append('annual_from_header')
        else:
            # Fallback: magnitude heuristic
            if f_val <= threshold:
                monthly_list.append(round(f_val, 2))
                annual_list.append(round(f_val * 12, 2))
                basis_list.append('monthly_inferred')
            else:
                annual_list.append(round(f_val, 2))
                monthly_list.append(round(f_val / 12, 2))
                basis_list.append('annual_inferred')

    clean_df['rate_monthly'] = monthly_list
    clean_df['rate_annually'] = annual_list
    clean_df['rate_basis'] = basis_list
    return clean_df


def _to_float(v):
    """Convert a value to float, handling currency strings, percentages, and junk text."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    s = str(v).strip()
    # Reject obvious non-numeric placeholders
    if s in ('', '-', '_', '--', 'N/A', 'n/a', 'nan', 'None', 'TBD', 'tbd'):
        return None
    # Strip currency symbols and common suffixes
    s = s.replace(',', '').replace('$', '').replace('sf', '').replace('SF', '')
    # Handle percentage values like "5.3%" or "6.43% (Yr 3)"
    pct_match = re.match(r'^([\d.]+)\s*%', s)
    if pct_match:
        try:
            return float(pct_match.group(1))
        except ValueError:
            return None
    # Handle rate strings like "$5,900/acre Gross" — extract the first number
    slash_match = re.match(r'^([\d.]+)\s*/\s*\w+', s)
    if slash_match:
        try:
            return float(slash_match.group(1))
        except ValueError:
            pass
    # Try direct conversion
    try:
        return float(s)
    except (ValueError, TypeError):
        pass
    # Last resort: extract first number-like sequence, with guards against noise
    num_match = re.search(r'[\d,]+\.?\d*', s)
    if num_match:
        matched = num_match.group()
        remaining = s.replace(matched, '', 1).strip()
        # Reject single-digit matches surrounded by much longer text (e.g., "Building 4 at Park")
        if len(matched) >= 2 or len(remaining) <= len(matched) * 2:
            try:
                return float(matched.replace(',', ''))
            except (ValueError, TypeError):
                pass
    return None
