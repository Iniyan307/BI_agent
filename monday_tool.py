import requests
import os
from dotenv import load_dotenv

load_dotenv()
MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
DEAL_FUNNEL_DATA = os.getenv("DEAL_FUNNEL_DATA")
WORK_ORDER_TRACKER = os.getenv("WORK_ORDER_TRACKER")

url = "https://api.monday.com/v2"

headers = {
    "Authorization": f"{MONDAY_API_KEY}",
    "Content-Type": "application/json"
}

MAPPING1 = {'Deal name masked': 'name', 'Customer Name Code': 'dropdown_mm10ve5y', 'Serial #': 'dropdown_mm10y1bq', 'Nature of Work': 'color_mm10267r', 'Last executed month of recurring project': 'color_mm10t62q', 'Execution Status': 'color_mm10p9x0', 'Data Delivery Date': 'date_mm10dbsg', 'Date of PO/LOI': 'date_mm10rdh3', 'Document Type': 'color_mm10j4ka', 'Probable Start Date': 'date_mm104r02', 'Probable End Date': 'date_mm10c0zz', 'BD/KAM Personnel code': 'color_mm10ewqy', 'Sector': 'color_mm10b0nh', 'Type of Work': 'color_mm109cwt', 'Is any Skylark software platform part of the client deliverables in this deal?': 'color_mm10gsxm', 'Last invoice date': 'date_mm10yzpx', 'latest invoice no.': 'dropdown_mm103wya', 'Amount in Rupees (Excl of GST) (Masked)': 'numeric_mm10fg6b', 'Amount in Rupees (Incl of GST) (Masked)': 'numeric_mm10de9z', 'Billed Value in Rupees (Excl of GST.) (Masked)': 'numeric_mm10enaa', 'Billed Value in Rupees (Incl of GST.) (Masked)': 'numeric_mm1076e', 'Collected Amount in Rupees (Incl of GST.) (Masked)': 'numeric_mm10n4f0', 'Amount to be billed in Rs. (Exl. of GST) (Masked)': 'numeric_mm10edpv', 'Amount to be billed in Rs. (Incl. of GST) (Masked)': 'numeric_mm10qzej', 'Amount Receivable (Masked)': 'numeric_mm10jva0', 'AR Priority account': 'color_mm10c4ee', 'Quantity by Ops': 'numeric_mm101e9n', 'Quantities as per PO': 'dropdown_mm10f93v', 'Quantity billed (till date)': 'numeric_mm10xz8g', 'Balance in quantity': 'numeric_mm10k5wb', 'Invoice Status': 'color_mm10ecwc', 'Expected Billing Month': 'text_mm10jqkg', 'Actual Billing Month': 'color_mm10kw72', 'Actual Collection Month': 'text_mm10jthb', 'WO Status (billed)': 'color_mm10b37d', 'Collection status': 'text_mm108cd8', 'Collection Date': 'text_mm10x4cz', 'Billing Status': 'color_mm10z392'}
MAPPING2 = {'Deal name': 'name', 'Owner code': 'color_mm105n81', 'Client Code': 'dropdown_mm10v74v', 'Deal Status': 'color_mm10e648', 'Close Date (A)': 'date_mm10b7bd', 'Closure Probability': 'color_mm102ncf', 'Masked Deal value': 'numeric_mm10g3qs', 'Tentative Close Date': 'date_mm104j80', 'Deal Stage': 'color_mm10ver1', 'Product deal': 'color_mm1044n3', 'Sector/service': 'color_mm108bq8', 'Created Date': 'date_mm10ese1'}
def convert_titles_to_ids(columns: list[str], board_name: str) -> list[str]:
    """
    Converting column titles to monday.com column IDs.
    """
    if board_name == "WORK_ORDER_TRACKER":
        column_mapping = MAPPING1
    elif board_name == "DEAL_FUNNEL_DATA":
        column_mapping = MAPPING2

    ids = []
    for col in columns:
        if col in column_mapping:
            ids.append(column_mapping[col])
        else:
            raise ValueError(f"Column '{col}' not found in mapping {board_name}")
    return ids


def fetch_monday_data(columns: list, board_name: str):
    column_ids = convert_titles_to_ids(columns, board_name)
    column_map = dict(zip(column_ids, columns)) 
    column_ids_str = '","'.join(column_ids)

    if board_name == "WORK_ORDER_TRACKER":
        board_id = WORK_ORDER_TRACKER
        row_title = "Deal name masked"
    elif board_name == "DEAL_FUNNEL_DATA":
        board_id = DEAL_FUNNEL_DATA
        row_title = "Deal name"
    else:
        return {"status": "error", "message": "Invalid board name"}

    query = f"""
    query {{
      boards(ids: {board_id}) {{
        items_page(limit: 5) {{
          items {{
            name
            column_values(ids: ["{column_ids_str}"]) {{
              id
              text
            }}
          }}
        }}
      }}
    }}
    """

    payload = {"query": query}
    response = requests.post(url, json=payload, headers=headers)
    data = response.json()

    items = data["data"]["boards"][0]["items_page"]["items"]
    formatted_items = []
    for item in items:
        row = {row_title: item["name"]}

        for col in item["column_values"]:
            col_title = column_map.get(col["id"], col["id"])
            row[col_title] = col["text"]

        formatted_items.append(row)

    return {
        "status": "success",
        "count": len(formatted_items),
        "data": formatted_items
    }


if __name__ == "__main__":
    print(fetch_monday_data(["Nature of Work" , "Serial #", "Amount to be billed in Rs. (Incl. of GST) (Masked)"], "WORK_ORDER_TRACKER"))