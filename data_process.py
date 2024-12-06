import pandas as pd
import re

def ensure_unique_columns(columns):
    """
    Säkerställer att kolumnnamnen är unika genom att lägga till suffix.
    """
    seen = {}
    unique_columns = []
    for col in columns:
        if col not in seen:
            seen[col] = 0
            unique_columns.append(col)
        else:
            seen[col] += 1
            unique_columns.append(f"{col}_{seen[col]}")
    return unique_columns


def clean_column_values(data, column_name):
    if column_name in data.columns:
        data[column_name] = data[column_name].apply(
            lambda x: re.sub(r'\s+', ' ', str(x)).strip() if pd.notnull(x) else x
        )
    return data


def filter_empty_rows(data):
    """
    Filtrerar bort rader som saknar viktiga kolumnvärden.
    """
    filtered_data = data.dropna(subset=["peName", "peNetprice", "peGrossprice"], how="any")
    return filtered_data


def process_sheets(file_obj):
    """
    Bearbetar alla sheets i en Excel-fil från en uppladdad fil.
    """
    vds_columns = [
        "peParentID", "peTillvalParent", "peType", "peType2", "peBrand",
        "peArtnr", "peName", "peModelyear", "peNetprice",
        "peGrossprice", "peGrosspriceVat", "peRetailerMargin",
        "peWeight", "peValidFrom", "peValidTo"
    ]

    excel_data = pd.ExcelFile(file_obj)
    all_sheets_data = []

    for sheet_name in excel_data.sheet_names:
        org_data = excel_data.parse(sheet_name, header=None)

        # Hitta raden med "Deutschland" som startpunkt
        try:
            deutschland_row_index = org_data[org_data.apply(
                lambda row: row.astype(str).str.contains("Deutschland", case=False).any(), axis=1
            )].index[0]
        except IndexError:
            continue

        # Extrahera data efter "Deutschland"
        relevant_data = org_data.iloc[deutschland_row_index + 1:].reset_index(drop=True)
        section_indices = relevant_data[relevant_data.apply(
            lambda row: row.astype(str).str.contains("Modell", case=False).any(), axis=1
        )].index.tolist()

        sheet_data = []
        for i, start_idx in enumerate(section_indices):
            end_idx = section_indices[i + 1] if i + 1 < len(section_indices) else len(relevant_data)
            section_data = relevant_data.iloc[start_idx:end_idx].reset_index(drop=True)

            try:
                header_row_index = section_data[section_data.apply(
                    lambda row: row.astype(str).str.contains("Modell", case=False).any(), axis=1
                )].index[0]
            except IndexError:
                continue

            table_data = section_data.iloc[header_row_index + 1:].reset_index(drop=True)
            table_data.columns = section_data.iloc[header_row_index].astype(str).apply(
                lambda x: re.sub(r'[^\w\s]', '', x).strip().lower()
            )
            table_data.columns = ensure_unique_columns(table_data.columns)
            table_data = clean_column_values(table_data, "modell")
            sheet_data.append(table_data)

        if sheet_data:
            sheet_combined_data = pd.concat(sheet_data, ignore_index=True)
            all_sheets_data.append(sheet_combined_data)

    if not all_sheets_data:
        return pd.DataFrame(columns=vds_columns)

    combined_data = pd.concat(all_sheets_data, ignore_index=True)

    processed_data = pd.DataFrame(columns=vds_columns)
    processed_data["peParentID"] = None
    processed_data["peTillvalParent"] = None
    processed_data["peType"] = None
    processed_data["peType2"] = None
    processed_data["peBrand"] = "Concorde"
    processed_data["peArtnr"] = None
    processed_data["peName"] = combined_data.get("modell", None)
    processed_data["peModelyear"] = 2025
    processed_data["peGrosspriceVat"] = combined_data.get("vk inkl 19 mwst", None)
    processed_data["peGrossprice"] = combined_data.get("vk netto", None)
    processed_data["peNetprice"] = combined_data.get("hek netto", None)
    processed_data["peRetailerMargin"] = (
        processed_data["peGrossprice"].astype(float) - processed_data["peNetprice"].astype(float)
    ).round(2)
    processed_data["peWeight"] = None
    processed_data["peValidFrom"] = None
    processed_data["peValidTo"] = None

    return filter_empty_rows(processed_data)



