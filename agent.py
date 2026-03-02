from typing import List, Any, Dict, Optional, TypedDict
from dotenv import load_dotenv
from langgraph.graph import StateGraph, END, MessagesState
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, ToolMessage, SystemMessage
from langchain_ollama import ChatOllama
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.tools import tool
from langgraph.prebuilt import ToolNode
from datetime import datetime
from langchain_core.runnables import Runnable
import streamlit as st

from cleaning_tool import clean_data
from monday_tool import fetch_monday_data

import pandas as pd
import json

load_dotenv()

# =========== GET API key from Streamlit secrets 

def get_env(key: str):
    # if key in st.secrets:
        return st.secrets[key]
    # # return os.getenv(key)
    # return None

GOOGLE_API_KEY = get_env("GOOGLE_API_KEY")

# ================= MODEL =================
try:
    llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", google_api_key=GOOGLE_API_KEY, temperature=0)
    # gemini-2.5-flash-lite, gemini-2.5-flash, gemini-2.5-pro , gemini-3-flash, gemini-2.0-flash
    # llm = ChatOllama(model="qwen2.5:7b", temperature=0)
    # qwen2.5:3b, qwen2.5:7b, llama3.2:3b
except Exception as e:
    raise ValueError(f"❌ Failed to initialize LLM: {e}")


# ============ STATE ==========================

class BIState(MessagesState):
    dataset: Optional[list]

# ================= TOOLS =================

@tool
def monday_tool(columns: List[str], board_name: str) -> dict:
    """
    Get the requested data from monday.com and clean the data 
    ARGS: 
        columns: list of columns to be fetched
        borad_name: name of the table (either WORK_ORDER_TRACKER or DEAL_FUNNEL_DATA)
    """
    data = fetch_monday_data(columns, board_name)
    # print(data)
    cleaned_data = clean_data(data)
    # print(cleaned_data)

    return cleaned_data


@tool
def filter_tool(data: List[Dict[str, Any]], column: str, value: str, operator: str) -> Dict:
    """
    Filters list of dictionaries based on a column condition.
    operator:
        - equals
        - not_equals
        - contains
    """
    # print(data, column, value, operator)
    if not isinstance(data, list):
        return {"status": "error", "message": "Data must be a list of dictionaries"}
    if not data:
        return {"status": "error", "message": "Data is not provided"}

    operator = operator.lower()
    valid_operators = {"equals", "not_equals", "contains"}
    if operator not in valid_operators:
        return {"status": "error", "message": f"Invalid operator: {operator}"}

    def match(row):
        cell = row.get(column)

        if cell is None:
            return {"status": "failure","message":"No data present"}

        cell_str = str(cell).lower()
        value_str = str(value).lower()

        if operator == "equals":
            return cell_str == value_str

        if operator == "not_equals":
            return cell_str != value_str

        if operator == "contains":
            return value_str in cell_str

        return {"status": "failure","message":"Not correct operator"}

    filtered = [row for row in data if match(row)]

    return json.loads(json.dumps({
        "status": "success",
        "filtered_count": len(filtered),
        "data": filtered
    }))

@tool
def aggregate_tool(data: List[Dict[str, Any]],column: str,operation: str) -> Dict:
    """
        Perform aggregation on numeric column.
        data: List of numeric values that need to be aggregated.
        operation:
            - sum
            - avg
            - count
            - max
            - min
    """
    # print(data, column, operation)
    if not isinstance(data, list) or not data:
        return {"status": "error", "message": "Invalid or empty data"}

    operation = operation.lower()
    valid_ops = {"sum", "avg", "count", "max", "min"}

    if operation not in valid_ops:
        return {"status": "error", "message": f"Invalid operation: {operation}"}

    numeric_values = []

    for row in data:
        value = row.get(column)
        try:
            numeric_values.append(float(value))
        except (TypeError, ValueError):
            continue

    if not numeric_values and operation != "count":
        return {"status": "error", "message": "No numeric data found"}

    if operation == "sum":
        result = sum(numeric_values)

    elif operation == "avg":
        result = sum(numeric_values) / len(numeric_values) if numeric_values else 0

    elif operation == "count":
        result = len(data)

    elif operation == "max":
        result = max(numeric_values)

    elif operation == "min":
        result = min(numeric_values)

    return {
        "status": "success",
        "operation": operation,
        "column": column,
        "result": result
    }

@tool
def ranking_tool(data: List[Dict[str, Any]],column: str,order: str,top_n: int) -> Dict:
    """ 
    Rank records based on numeric column. 
    data: List of numeric values that need to be ranked. 
    order: 
        - asc 
        - desc 
    """
    if not isinstance(data, list) or not data:
        return {"status": "error", "message": "Invalid or empty data"}

    order = order.lower()
    if order not in {"asc", "desc"}:
        return {"status": "error", "message": "Order must be 'asc' or 'desc'"}

    sortable_rows = []

    for row in data:
        try:
            value = float(row.get(column))
            sortable_rows.append((value, row))
        except (TypeError, ValueError):
            continue

    reverse = True if order == "desc" else False

    sorted_rows = sorted(sortable_rows, key=lambda x: x[0], reverse=reverse)

    top_rows = [row for _, row in sorted_rows[:top_n]]

    return {
        "status": "success",
        "column": column,
        "order": order,
        "top_n": top_n,
        "data": top_rows
    }

@tool
def get_current_date() -> Dict:
    """
    Returns current year and quarter.
    """
    today = datetime.today()

    return {
        "today": today.strftime("%Y-%m-%d")
    }

@tool
def get_current_quarter() -> Dict:
    """
    Returns current year and quarter.
    """
    today = datetime.today()
    month = today.month
    quarter = (month - 1) // 3 + 1

    return {
        "year": today.year,
        "quarter": f"Q{quarter}"
    }

@tool
def date_range_filter_tool(data: List[Dict[str, Any]],column: str,start_date: str,end_date: str) -> Dict:
    """
    Filters rows where column date is between start_date and end_date (inclusive).
    Dates must be in 'YYYY-MM-DD' format.
    """

    def convert_to_datetime(raw_date):
        if isinstance(raw_date, (int, float)):
            if raw_date > 10_000_000_000:  # milliseconds
                raw_date = raw_date / 1000
            return datetime.utcfromtimestamp(raw_date)
        return datetime.strptime(str(raw_date), "%Y-%m-%d")

    def format_date(dt):
        return dt.strftime("%Y-%m-%d")

    if not isinstance(data, list):
        return {"status": "error", "message": "Data must be list of dictionaries"}

    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return {"status": "error", "message": "Invalid date format. Use YYYY-MM-DD"}

    filtered = []

    for row in data:
        raw_date = row.get(column)
        if raw_date is None:
            continue

        try:
            row_date = convert_to_datetime(raw_date)
        except Exception:
            continue

        if start <= row_date <= end:
            safe_row = {**row}
            safe_row[column] = format_date(row_date)
            filtered.append(safe_row)

    return {
        "status": "success",
        "column": column,
        "filtered_count": len(filtered),
        "data": filtered
    }


monday_tools = [monday_tool]
llm_with_monday_tools = llm.bind_tools(monday_tools)

# bi_tools = [filter_tool, aggregate_tool, ranking_tool, kpi_tool, trend_tool, pandas_query_tool]
bi_tools = [filter_tool, aggregate_tool, ranking_tool, get_current_quarter, get_current_date, date_range_filter_tool]
llm_with_bi_tools = llm.bind_tools(bi_tools)

monday_tool_node = ToolNode(monday_tools)
BI_tool_node = ToolNode(bi_tools)
# =====================================================================================

WORK_ORDER_TRACKER = ['Name', 'Customer Name Code', 'Serial #', 'Nature of Work', 'Last executed month of recurring project', 'Execution Status', 'Data Delivery Date', 'Date of PO/LOI', 'Document Type', 'Probable Start Date', 'Probable End Date', 'BD/KAM Personnel code', 'Sector', 'Type of Work', 'Is any Skylark software platform part of the client deliverables in this deal?', 'Last invoice date', 'latest invoice no.', 'Amount in Rupees (Excl of GST) (Masked)', 'Amount in Rupees (Incl of GST) (Masked)', 'Billed Value in Rupees (Excl of GST.) (Masked)', 'Billed Value in Rupees (Incl of GST.) (Masked)', 'Collected Amount in Rupees (Incl of GST.) (Masked)', 'Amount to be billed in Rs. (Exl. of GST) (Masked)', 'Amount to be billed in Rs. (Incl. of GST) (Masked)', 'Amount Receivable (Masked)', 'AR Priority account', 'Quantity by Ops', 'Quantities as per PO', 'Quantity billed (till date)', 'Balance in quantity', 'Invoice Status', 'Expected Billing Month', 'Actual Billing Month', 'Actual Collection Month', 'WO Status (billed)', 'Collection status', 'Collection Date', 'Billing Status']
DEAL_FUNNEL_DATA = ['Name', 'Owner code', 'Client Code', 'Deal Status', 'Close Date (A)', 'Closure Probability', 'Masked Deal value', 'Tentative Close Date', 'Deal Stage', 'Product deal', 'Sector/service', 'Created Date']

# ----------------------------
# 1️⃣ Intent Extraction
# ----------------------------
def intent_node(state: BIState):

    messages = state["messages"]

    system_prompt = f"""
    You are a Business Intelligence Agent. 
    User will give you a query. Answer only business related queries. You can perform calculation and analysis based on the data for the given query.
    Based on the user query, you can choose to ask clarifying questions when necessary or choose to call the monday_tool if the details are enough. 
        Extract:
            columns → List of exact column names requested by the user
            board_name → Either "WORK_ORDER_TRACKER" or "DEAL_FUNNEL_DATA"

        Available Columns: 
            In WORK_ORDER_TRACKER board : {WORK_ORDER_TRACKER},
            Example data from WORK_ORDER_TRACKER board : "Deal name masked": "Scooby-Doo","Customer Name Code": "WOCOMPANY_002","Serial #": "SDPLDEAL-075","Nature of Work": "One time Project","Last executed month of recurring project": "June","Execution Status": "Completed","Data Delivery Date": "27-09-2025","Date of PO/LOI": "29-10-2025","Document Type": "Purchase Order","Probable Start Date": "31-05-2025","Probable End Date": "03-06-2025","BD/KAM Personnel code": "OWNER_003","Sector": "Mining","Type of Work": "Raw images/videography","Is any Skylark software platform part of the client deliverables in this deal?": "NONE","Last invoice date": "14-01-2026","latest invoice no.": "SDPL/FY25-26/916","Amount in Rupees (Excl of GST) (Masked)": 184980,"Amount in Rupees (Incl of GST) (Masked)": 218276.4,"Billed Value in Rupees (Excl of GST.) (Masked)": 184980,"Billed Value in Rupees (Incl of GST.) (Masked)": 218276.4,"Collected Amount in Rupees (Incl of GST.) (Masked)": 66880.1356,"Amount to be billed in Rs. (Exl. of GST) (Masked)": 0,"Amount to be billed in Rs. (Incl. of GST) (Masked)": 0,"Amount Receivable (Masked)": 151396.2644,"AR Priority account": "Priority","Quantity by Ops": 2173.17,"Quantities as per PO": 600,"Quantity billed (till date)": 815.61,"Balance in quantity": 600,"Invoice Status": "Not billed yet","Expected Billing Month": "","Actual Billing Month": "November","Actual Collection Month": "","WO Status (billed)": "Open","Collection status": "","Collection Date": "","Billing Status": "BIlled"
            In DEAL_FUNNEL_DATA board : {DEAL_FUNNEL_DATA}.
            Example data from DEAL_FUNNEL_DATA board : "Deal Name": "Naruto","Owner code": "OWNER_001","Client Code": "COMPANY089","Deal Status": "Open","Close Date (A)": "","Closure Probability": "High","Masked Deal value": 489360,"Tentative Close Date": "2026-02-26","Deal Stage": "B. Sales Qualified Leads","Product deal": "Service + Spectra","Sector/service": "Mining","Created Date": "2025-12-26"

        If information is complete, call the monday_tool.
        Pass all the required column names to the monday_tool as per the query.
        Ask a clarification question, when needed or if query is ambigous.

        * Map the user requested column to the Available Columns
        ** IMPORTANT ** Always use the exact name as per the Available Columns names given in the query and board name while calling the monday_tool. *
        ** Do not use the column names as per the user query.**
        Always extract all the columns mentioned in the query using monday_tool.

        You can call the tool multiple times for getting data from other tables
    """

    response = llm_with_monday_tools.invoke(
        [SystemMessage(content=system_prompt)] + messages
    )

    # If tool call present → return it
    if response.tool_calls:
        return {"messages": [response]}

    # Otherwise it's clarification question
    return {"messages": [response]}

def bi_node(state: BIState):
    messages = state["messages"]
    dataset = state.get("dataset")
    compact_dataset = json.dumps(dataset, separators=(",", ":"))

    latest_user_question = next(
        (m.content for m in reversed(messages) if isinstance(m, HumanMessage)),
        "Analyze the dataset."
    )

    bi_system_message = SystemMessage(
    content=f"""
        You are a business intelligence analyst.

        User Question:
        {latest_user_question}

        Available tools:
        - filter_tool (Use when the query asks to filter based on specific conditions.)
        - aggregate_tool (Use when the query asks to aggregate values.)
        - ranking_tool (Use when the query asks to rank data of a numeric column.)
        - get_current_quarter (Use when the user needs details related current quarter)
        - get_current_date (Use when the user needs details related to current date)
        - date_range_filter_tool (Use when the user needs to filter the data based on dates)

        Rules:
        - Always pass the latest dataset as `data`.
        - Only use existing column names.
        - When computation is complete → provide final business insight.
        When calling a tool, output ONLY valid JSON arguments.
        Do NOT include explanation text.
        Do NOT wrap JSON in markdown.
    """     
    )

    response = llm_with_bi_tools.invoke(
        [
            bi_system_message,
            SystemMessage(content=f"Current Dataset:\n{compact_dataset}"),
            *messages
        ]
    )

    return {
        "messages": [response]
    }

def route_after_intent(state: MessagesState):
    last_message = state["messages"][-1]
    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        return "monday_tools"

    return END

def route_after_bi(state: MessagesState):
    last_message = state["messages"][-1]
    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        return "bi_tools"

    return END

def tool_output_handler(state: BIState):
    last_msg = state["messages"][-1]

    if isinstance(last_msg, ToolMessage):
        content = last_msg.content

        # If tool returned list (like monday_tool)
        if isinstance(content, list):
            return {"dataset": content}

        # If tool returned dict with data key (like filter_tool)
        if isinstance(content, dict) and "data" in content:
            return {"dataset": content["data"]}

    return {}


builder = StateGraph(BIState)

builder.add_node("intent", intent_node)
builder.add_node("monday_tools", monday_tool_node)
builder.add_node("bi_tool_handler", tool_output_handler)

builder.add_node("bi", bi_node)
builder.add_node("bi_tools", BI_tool_node)


builder.set_entry_point("intent")

# ---------- Intent Routing ----------
builder.add_conditional_edges(
    "intent",
    route_after_intent,
    {
        "monday_tools": "monday_tools",
        END: END,
    },
)

builder.add_edge("monday_tools", "bi_tool_handler")
builder.add_edge("bi_tool_handler", "bi")

# ---------- BI Routing ----------
builder.add_conditional_edges(
    "bi",
    route_after_bi,
    {
        "bi_tools": "bi_tools",
        END: END,
    },
)

builder.add_edge("bi_tools", "bi_tool_handler")


graph = builder.compile()
