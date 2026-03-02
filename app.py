import streamlit as st
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
from agent import graph

st.caption(
    "⚠️ Powered by a free-tier Gemini API key. Limited usage — may become unavailable once quota is exhausted."
)

st.set_page_config(page_title="BI Agent", layout="wide")
st.title("📊 BI Conversational Agent")

# -----------------------------
# 🔹 Session State Initialization
# -----------------------------
if "messages" not in st.session_state:
    st.session_state.messages = []

# -----------------------------
# 🔹 Helper: Normalize AI Output
# -----------------------------
def normalize_ai_content(content):
    """
    Ensures AI output is always clean string.
    Handles list-style structured outputs.
    """
    if isinstance(content, list):
        extracted_text = ""
        for item in content:
            if isinstance(item, dict) and "text" in item:
                extracted_text += item["text"] + "\n"
        return extracted_text.strip()

    return str(content)


# -----------------------------
# 🔹 Display Chat History
# -----------------------------
for msg in st.session_state.messages:

    if msg.type == "human":
        with st.chat_message("user"):
            st.markdown(msg.content)

    elif msg.type == "tool":
        with st.chat_message("assistant"):
            st.info(f"🔧 Tool Used: {msg.name}")
            try:
                st.json(msg.content)
            except:
                st.write(msg.content)

    elif msg.type == "ai":
        with st.chat_message("assistant"):
            clean_content = normalize_ai_content(msg.content)
            st.markdown(clean_content)


# -----------------------------
# 🔹 Chat Input
# -----------------------------
user_input = st.chat_input("Ask about business performance...")

if user_input:

    # 1️⃣ Add user message immediately
    user_msg = HumanMessage(content=user_input)
    st.session_state.messages.append(user_msg)

    # 2️⃣ Show user instantly
    with st.chat_message("user"):
        st.markdown(user_input)

    # 3️⃣ Loader
    loader = st.empty()
    loader.markdown("⏳ **Analyzing data...**")

    # 4️⃣ Run Graph Streaming (NO rendering inside loop)
    stream = graph.stream(
        {
            "messages": st.session_state.messages,
            "dataset": None,  # You can inject dataset here
        },
        stream_mode="values"
    )

    final_messages = None

    for event in stream:
        final_messages = event["messages"]

    # 5️⃣ Remove loader
    loader.empty()

    # 6️⃣ Update session state
    if final_messages:
        st.session_state.messages = final_messages

    # 7️⃣ Rerun app to render in perfect chronological order
    st.rerun()
