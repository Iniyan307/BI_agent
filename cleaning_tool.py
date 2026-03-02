import pandas as pd
import numpy as np
import re

def clean_data(data):
    response_json = data  
    df = pd.DataFrame(response_json["data"])

    # Replace spaces and (,) in column names with _
    df.columns = (df.columns.str.strip().str.lower().str.replace("[ ()/]", "_", regex=True))

    # Convert empty space to nan
    df = df.replace(r'^\s*$', np.nan, regex=True)

    # Convert date columns to py datetime object
    date_columns = ["data_delivery_date", "date_of_po_loi", "probable_start_date", "collection_date", "last_invoice_date", "tentative_close_date", "created_date"]
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Clean Numeric columns
    numeric_columns = ['amount_in_rupees__excl_of_gst___masked_','amount_in_rupees__incl_of_gst___masked_','billed_value_in_rupees__excl_of_gst.___masked_','billed_value_in_rupees__incl_of_gst.___masked_','collected_amount_in_rupees__incl_of_gst.___masked_','amount_to_be_billed_in_rs.__exl._of_gst___masked_','amount_to_be_billed_in_rs.__incl._of_gst___masked_','amount_receivable__masked_','quantity_by_ops','quantity_billed__till_date_','balance_in_quantity']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].fillna(0)
            df[col] = (df[col].astype(str).str.replace(",", "", regex=False))
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove Duplicates
    if "work_order_number" in df.columns:
        df = df.drop_duplicates(subset=["work_order_number"],keep="first")
    elif "serial_number" in df.columns:
        df = df.drop_duplicates(subset=["serial_number"],keep="first")

    # Clean quantities_as_per_po column
    if "quantities_as_per_po" in df.columns:
        col_name = "quantities_as_per_po"
        df[col_name] = df[col_name].astype(str).str.strip().replace(["NA", "N/A", "na", "", "None"],np.nan).str.replace(",", "", regex=False)

        # Extract numeric part
        df["quantities_as_per_po_value"] = (df[col_name].str.extract(r'(\d+\.?\d*)'))
        df["quantities_as_per_po_value"] = pd.to_numeric(df["quantities_as_per_po_value"],errors="coerce")

        # Extract everything after number (unit OR notes)
        df["quantities_as_per_po_unit_or_notes"] = (df[col_name].str.extract(r'^\d+\.?\d*\s*(.*)')[0].str.strip().str.lower())

        # Clean common unit variations
        unit_map = {"ha": "hectare", "acr": "acre", "acres": "acre", "acers": "acre", "km": "km","rkm": "km","mw": "mw","months": "month","month": "month","days": "day","day": "day"}
        df["quantities_as_per_po_unit_or_notes"] = (df["quantities_as_per_po_unit_or_notes"].replace(unit_map))

    res = df.to_json(orient='records')
    return {'status': 'success', "count":len(df),"data" : res}


if __name__ == "__main__":
    data = {'status': 'success', 'count': 5, 'data': [{'Deal name masked': 'Scooby-Doo', 'Serial #': 'SDPLDEAL-075', 'Nature of Work': 'One time Project', 'Amount to be billed in Rs. (Incl. of GST) (Masked)': '311989.7344'}, {'Deal name masked': 'Appa', 'Serial #': 'SDPLDEAL-101', 'Nature of Work': 'Proof of Concept', 'Amount to be billed in Rs. (Incl. of GST) (Masked)': '181897'}, {'Deal name masked': 'Sakura', 'Serial #': 'SDPLDEAL-002', 'Nature of Work': 'Monthly Contract', 'Amount to be billed in Rs. (Incl. of GST) (Masked)': '3492962.459'}, {'Deal name masked': 'Sakura', 'Serial #': 'SDPLDEAL-003', 'Nature of Work': 'One time Project', 'Amount to be billed in Rs. (Incl. of GST) (Masked)': '0'}, {'Deal name masked': 'SpongeBob', 'Serial #': 'SDPLDEAL-004', 'Nature of Work': 'One time Project', 'Amount to be billed in Rs. (Incl. of GST) (Masked)': '-97830.60937'}]}
    print(clean_data(data))