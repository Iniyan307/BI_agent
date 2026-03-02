import streamlit as st
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
from agent import graph

st.caption(
    "⚠️ Powered by a free-tier Gemini API key. Limited usage — may become unavailable once quota is exhausted."
)

st.set_page_config(page_title="BI Agent", layout="wide")
st.title("📊 BI Conversational Agent")

if "messages" not in st.session_state:
    st.session_state.messages = []

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

user_input = st.chat_input("Ask about business performance...")

if user_input:

    user_msg = HumanMessage(content=user_input)
    st.session_state.messages.append(user_msg)

    with st.chat_message("user"):
        st.markdown(user_input)

    loader = st.empty()
    loader.markdown("⏳ **Analyzing data...**")

    try: 
        stream = graph.stream(
            {
                "messages": st.session_state.messages,
                "dataset": None, 
            },
            stream_mode="values"
        )
    
        final_messages = None
    
        for event in stream:
            final_messages = event["messages"]
        loader.empty()

        if final_messages:
            st.session_state.messages = final_messages
        st.rerun()

    except Exception as e:
        loader.empty()
        error_text = str(e).lower()
        # Detect Gemini quota / API error
        if "quota" in error_text or "rate" in error_text or "api" in error_text:
            st.error("🚨 Google Gemini API key quota exhausted. Please try again later or switch to local LLM.")
        else:
            st.error("⚠️ Something went wrong. Please try again.")
