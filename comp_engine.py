"""Backward-compat facade. New code should import from engine.* directly.

This file exists so app.py's existing imports remain valid after Phase 2 extraction.
Every name is re-exported from an engine/ submodule.
"""
from engine.loaders import (
    get_sheet_names,
    detect_table_segments,
    robust_load_file,
    robust_load_file_segmented,
)
from engine.cleaning import (
    clean_header,
    get_column_profile,
    apply_rate_logic,
    _to_float,
    HOUSTON_RATE_THRESHOLD,
)
from engine.mapping import (
    get_embeddings,
    classify_file_type,
    generate_standardized_df,
    LEASE_SCHEMA,
    SALE_SCHEMA,
    BASE_OVERRIDES,
    LEASE_OVERRIDES,
    SALE_OVERRIDES,
)
from engine.geocoding import fetch_google_data

import pandas as pd


def process_file_to_clean_output(df, filename, sheet_name=None):
    """Full pipeline: classify → map columns → apply rate logic → return (df, confidence)."""
    ftype = classify_file_type(df.columns, filename, sheet_name=sheet_name)
    schema = LEASE_SCHEMA if ftype == "LEASE" else SALE_SCHEMA
    if ftype in ("BOTH", "UNKNOWN"):
        schema = {**LEASE_SCHEMA, **SALE_SCHEMA}

    clean_df, confidence, mappings = generate_standardized_df(df, schema, ftype)

    # Clean all numeric columns — convert messy strings to proper floats and round
    for col_name, col_info in schema.items():
        if col_info['type'] in ('numeric_money', 'numeric_clean') and col_name in clean_df.columns:
            clean_df[col_name] = clean_df[col_name].apply(_to_float)
            clean_df[col_name] = pd.to_numeric(clean_df[col_name], errors='coerce').round(2)

    # Calculate price_per_sf for sales if missing
    if ftype == "SALE" and 'sale_price' in clean_df.columns and 'building_size' in clean_df.columns:
        calc_psf = clean_df['sale_price'] / clean_df['building_size']
        clean_df['price_per_sf'] = clean_df['price_per_sf'].fillna(calc_psf.round(2))

    # Apply Houston lease rate logic — use actual mapped column header for rate detection
    if ftype in ("LEASE", "BOTH"):
        rate_header = mappings.get('rate_psf')
        clean_df = apply_rate_logic(clean_df, rate_header=rate_header)

    clean_df['latitude'] = None
    clean_df['longitude'] = None
    clean_df['city'] = None
    clean_df['zip_code'] = None
    clean_df['source_type'] = ftype
    clean_df['source_file'] = filename
    return clean_df, confidence, mappings


def process_all_sheets(file_path, filename, selected_sheets=None):
    """Process all (or selected) sheets in an Excel file and concatenate results.
    Returns (combined_df, merged_confidence, all_mappings, errors)."""
    sheets = get_sheet_names(file_path)

    if selected_sheets is not None:
        sheets = [s for s in sheets if s in selected_sheets]

    all_dfs = []
    all_confidences = {}
    all_mappings = {}
    errors = []

    for sheet in sheets:
        try:
            segments = robust_load_file_segmented(file_path, sheet_name=sheet)
            if not segments:
                errors.append(f"Sheet '{sheet}': could not read or empty")
                continue
            for seg in segments:
                df_input = seg['df']
                if df_input.empty:
                    continue
                base_label = sheet or "Sheet1"
                if len(segments) == 1:
                    seg_label = base_label
                else:
                    title = seg.get('segment_title')
                    sec_num = seg['segment_index'] + 1
                    seg_label = f"{base_label} - {title or f'Section {sec_num}'}"

                result_df, conf, seg_mappings = process_file_to_clean_output(df_input, filename, sheet_name=sheet)
                result_df['source_sheet'] = seg_label
                all_dfs.append(result_df)
                all_mappings[seg_label] = seg_mappings
                for k, v in conf.items():
                    key = f"{seg_label}: {k}" if len(sheets) > 1 else k
                    all_confidences[key] = v
        except Exception as e:
            errors.append(f"Sheet '{sheet}': {e}")

    if not all_dfs:
        return None, None, {}, errors

    combined = pd.concat(all_dfs, ignore_index=True)
    return combined, all_confidences, all_mappings, errors


def apply_manual_mapping(input_df, mapping_dict, schema_dict, file_type, filename):
    """Re-map columns using a user-provided mapping dict. Skips the AI/Hungarian step.
    mapping_dict: {target_field: input_column_name_or_None}
    Returns (clean_df, confidence_dict)."""
    out = pd.DataFrame()
    confidence = {}

    for target in schema_dict:
        src_col = mapping_dict.get(target)
        if src_col and src_col in input_df.columns:
            col_data = input_df[src_col]
            if isinstance(col_data, pd.DataFrame):
                col_data = col_data.iloc[:, 0]
            out[target] = col_data.values
            confidence[target] = 1.0  # user override = full confidence
        else:
            out[target] = None
            confidence[target] = 0.0

    # Build raw_address_data from address-related columns
    addr_col = mapping_dict.get('address')
    if addr_col and addr_col in input_df.columns:
        out['raw_address_data'] = input_df[addr_col].astype(str)
    else:
        out['raw_address_data'] = ""

    # Clean numeric columns
    for col_name, col_info in schema_dict.items():
        if col_info['type'] in ('numeric_money', 'numeric_clean') and col_name in out.columns:
            out[col_name] = out[col_name].apply(_to_float)
            out[col_name] = pd.to_numeric(out[col_name], errors='coerce').round(2)

    # Calculate price_per_sf for sales if missing
    if file_type == "SALE" and 'sale_price' in out.columns and 'building_size' in out.columns:
        calc_psf = out['sale_price'] / out['building_size']
        out['price_per_sf'] = out['price_per_sf'].fillna(calc_psf.round(2))

    # Apply rate logic for leases
    if file_type in ("LEASE", "BOTH"):
        rate_header = mapping_dict.get('rate_psf')
        out = apply_rate_logic(out, rate_header=rate_header)

    out['latitude'] = None
    out['longitude'] = None
    out['city'] = None
    out['zip_code'] = None
    out['source_type'] = file_type
    out['source_file'] = filename
    return out, confidence


def generate_kml(df):
    """Generate KML string from a DataFrame with latitude/longitude columns."""
    kml = ['<?xml version="1.0" encoding="UTF-8"?>']
    kml.append('<kml xmlns="http://www.opengis.net/kml/2.2">')
    kml.append('<Document>')
    for _, row in df.iterrows():
        if pd.notnull(row.get('latitude')) and pd.notnull(row.get('longitude')):
            kml.append('<Placemark>')
            kml.append(f"<name>{row.get('address', 'Unknown Property')}</name>")
            desc = f"Size: {row.get('building_size') or row.get('leased_sf') or 'N/A'}\n"
            desc += f"Price/Rate: {row.get('sale_price') or row.get('rate_monthly') or 'N/A'}"
            kml.append(f"<description>{desc}</description>")
            kml.append('<Point>')
            kml.append(f"<coordinates>{row['longitude']},{row['latitude']},0</coordinates>")
            kml.append('</Point>')
            kml.append('</Placemark>')
    kml.append('</Document>')
    kml.append('</kml>')
    return "\n".join(kml)


__all__ = [
    "get_sheet_names", "detect_table_segments", "robust_load_file", "robust_load_file_segmented",
    "clean_header", "get_column_profile", "apply_rate_logic", "HOUSTON_RATE_THRESHOLD",
    "get_embeddings", "classify_file_type", "generate_standardized_df",
    "LEASE_SCHEMA", "SALE_SCHEMA", "BASE_OVERRIDES", "LEASE_OVERRIDES", "SALE_OVERRIDES",
    "fetch_google_data", "generate_kml",
    "process_file_to_clean_output", "process_all_sheets", "apply_manual_mapping",
]
