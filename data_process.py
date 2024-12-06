import pandas as pd
import re
import os

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
        data[column_name] = data[column_name].apply(lambda x: re.sub(r'\s+', ' ', str(x)).strip() if pd.notnull(x) else x)
    return data

def filter_empty_rows(data):
    filtered_data = data.dropna(subset=["peName", "peNetprice", "peGrossprice"], how="any")
    return filtered_data

def generate_versioned_filename(base_name, extension, folder="."):
    version = 1
    while True:
        file_name = f"{base_name}_v{version}{extension}"
        if not os.path.exists(os.path.join(folder, file_name)):
            return os.path.join(folder, file_name)
        version += 1
        
def filter_invalid_rows(data):
    
    filtered_data = data.dropna(subset=["peName", "peNetprice", "peGrossprice"], how="any")
    filtered_data = filtered_data[
        (filtered_data["peNetprice"].apply(lambda x: isinstance(x, (int, float)) and x > 0)) &
        (filtered_data["peGrossprice"].apply(lambda x: isinstance(x, (int, float)) and x > 0))
    ]
    return filtered_data


def process_sheets(file_path):
    """
    Bearbetar alla sheets i en Excel-fil, identifierar data efter "Deutschland",
    och strukturerar det enligt specifikationen.
    """
    
    vds_columns = [
        "peParentID", "peTillvalParent", "peType", "peType2", "peBrand", 
        "peArtnr", "peName", "peModelyear", "peNetprice", 
        "peGrossprice", "peGrosspriceVat", "peRetailerMargin", 
        "peWeight", "peValidFrom", "peValidTo"
    ]

    
    excel_data = pd.ExcelFile(file_path)
    all_sheets_data = []

    for sheet_name in excel_data.sheet_names:
        print(f"Bearbetar sheet: {sheet_name}")
        org_data = excel_data.parse(sheet_name, header=None)

        # Leta upp "Deutschland" och börja samla data under det
        try:
            deutschland_row_index = org_data[org_data.apply(lambda row: row.astype(str).str.contains("Deutschland", case=False).any(), axis=1)].index[0]
            print(f"Deutschland hittades i sheet {sheet_name} på rad {deutschland_row_index + 1} (nollindex: {deutschland_row_index}).")
        except IndexError:
            print(f"Inget 'Deutschland' hittades i sheet {sheet_name}. Hoppar över.")
            continue

        # Extrahera data efter "Deutschland"
        relevant_data = org_data.iloc[deutschland_row_index + 1:].reset_index(drop=True)

    
        section_indices = relevant_data[relevant_data.apply(lambda row: row.astype(str).str.contains("Modell", case=False).any(), axis=1)].index.tolist()

        # Samla data från varje sektion
        sheet_data = []
        for i, start_idx in enumerate(section_indices):
            end_idx = section_indices[i + 1] if i + 1 < len(section_indices) else len(relevant_data)
            section_data = relevant_data.iloc[start_idx:end_idx].reset_index(drop=True)

            try:
                header_row_index = section_data[section_data.apply(lambda row: row.astype(str).str.contains("Modell", case=False).any(), axis=1)].index[0]
            except IndexError:
                print(f"Ingen tabell hittades i sektionen på sheet {sheet_name}. Hoppar över.")
                continue

            table_data = section_data.iloc[header_row_index + 1:].reset_index(drop=True)
            table_data.columns = section_data.iloc[header_row_index].astype(str).apply(lambda x: re.sub(r'[^\w\s]', '', x).strip().lower())

            
            table_data.columns = ensure_unique_columns(table_data.columns)

            table_data = clean_column_values(table_data, "modell")

            sheet_data.append(table_data)

        if sheet_data:
            sheet_combined_data = pd.concat(sheet_data, ignore_index=True)
            all_sheets_data.append(sheet_combined_data)

    if not all_sheets_data:
        print("Ingen data bearbetades från filen.")
        return pd.DataFrame(columns=vds_columns)

    combined_data = pd.concat(all_sheets_data, ignore_index=True)

    # Strukturera om kolumnerna för att matcha VDS
    processed_data = pd.DataFrame(columns=vds_columns)
    processed_data["peParentID"] = None  # Lämnas tom för manuell fyllning
    processed_data["peTillvalParent"] = None
    processed_data["peType"] = None
    processed_data["peType2"] = None
    processed_data["peBrand"] = "Concorde"  # Fast värde
    processed_data["peArtnr"] = None
    processed_data["peName"] = combined_data.get("modell", None)
    processed_data["peModelyear"] = 2025  # Fast värde
    processed_data["peGrosspriceVat"] = combined_data.get("vk inkl 19 mwst", None)
    processed_data["peGrossprice"] = combined_data.get("vk netto", None)
    processed_data["peNetprice"] = combined_data.get("hek netto", None)
    
    # Beräkna peRetailerMargin som peGrossprice - peNetprice
    processed_data["peRetailerMargin"] = (
        processed_data["peGrossprice"].astype(float) - processed_data["peNetprice"].astype(float)
    ).round(2)

    processed_data["peWeight"] = None
    processed_data["peValidFrom"] = None
    processed_data["peValidTo"] = None

    # Filtrera bort rader utan priser eller namn
    processed_data = filter_empty_rows(processed_data)

    return processed_data


file_path = "Concorde 2025-1 ORG Vehicles.xlsx"

output_file = generate_versioned_filename(base_name="processed_all_sheets_data", extension=".xlsx")

processed_data = process_sheets(file_path)
processed_data.to_excel(output_file, index=False)
print(f"Processed data saved to {output_file}.")




