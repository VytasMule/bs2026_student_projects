import math
import json
import urllib.request
import streamlit as st


QUICK_PICKS = {
    "⚛️ J/psi (Jpsimumu)": "Jpsimumu",
    "⚛️ Z Boson (Zmumu)": "Zmumu",
    "⚛️ Upsilon (Ymumu)": "Ymumu",
    "⚛️ DoubleMu (Mixed)": "DoubleMu",
}


@st.cache_data(ttl=3600)
def get_cern_data(query, only_csv=True):
    try:
        api_url = f"https://opendata.cern.ch/api/records/?q={query.replace(' ', '+')}&size=15"
        if only_csv:
            api_url += "&f=file_format:CSV&f=type:Dataset"
        req = urllib.request.Request(api_url, headers={'User-Agent': 'Mozilla/5.0'})
        response = urllib.request.urlopen(req, timeout=10)
        return json.loads(response.read().decode('utf-8'))
    except Exception as e:
        return {"error": str(e)}


def format_size(size_bytes):
    if not size_bytes or size_bytes == 0:
        return "Unknown Size"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    return f"{round(size_bytes / p, 2)} {size_name[i]}"
