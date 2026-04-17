import streamlit as st
from lib.ui_utils import apply_branding
from lib.exploration.cern_api import get_cern_data, QUICK_PICKS
from lib.exploration.file_renderer import render_csv_files, render_root_files

st.set_page_config(page_title="CERN Explorer | Portal", page_icon="⚛️", layout="wide")
apply_branding()

st.markdown("""
    <style>
    .metric-card { background: rgba(255,255,255,0.03); padding: 15px; border-radius: 8px; text-align: center; }
    </style>
""", unsafe_allow_html=True)

# --- Session state defaults ---
if 'search_query' not in st.session_state:
    st.session_state.search_query = "Jpsimumu"
if 'active_preview_id' not in st.session_state:
    st.session_state.active_preview_id = None
if 'nav_to_analysis' not in st.session_state:
    st.session_state.nav_to_analysis = False

if st.session_state.nav_to_analysis:
    st.session_state.nav_to_analysis = False
    st.switch_page("pages/1_Analysis.py")

# --- Search UI ---
st.title("🔍 Explore CERN Open Data")
st.write("Search the live CERN Open Data portal for authentic particle physics datasets.")

col_search, col_filter = st.columns([3, 1])
with col_search:
    st.text_input("Enter search terms (e.g., 'Jpsimumu', 'DoubleMu', 'Run 2011')", key="search_query")
with col_filter:
    st.write("")
    only_csv = st.checkbox("Only CSV-compatible", value=True)

st.write("🎯 **Quick Picks (Guaranteed CSVs):**")
qp_cols = st.columns(4)
for i, (label, query) in enumerate(QUICK_PICKS.items()):
    with qp_cols[i]:
        st.button(label, key=f"qp_{i}",
                  on_click=lambda q=query: st.session_state.update(search_query=q))

# --- Results ---
if st.session_state.search_query:
    result_data = get_cern_data(st.session_state.search_query, only_csv)

    if "error" in result_data:
        st.error(f"Error querying CERN API: {result_data['error']}")
    else:
        hits = result_data.get('hits', {}).get('hits', [])
        if not hits:
            st.warning("No datasets found for this search.")
        else:
            st.subheader(f"Found {len(hits)} matching records")
            for hit in hits:
                metadata = hit.get('metadata', {})
                title = metadata.get('title', 'Unknown Title')
                rec_id = hit.get('id')
                is_active = st.session_state.active_preview_id == rec_id

                with st.expander(f"📚 {title} (ID: {rec_id})", expanded=is_active):
                    m1, m2, m3, m4 = st.columns(4)
                    with m1:
                        st.metric("Experiment", ", ".join(metadata.get('experiment', ['N/A'])))
                    with m2:
                        st.metric("Type", metadata.get('type', {}).get('secondary', ['Dataset'])[0])
                    with m3:
                        st.metric("Year", metadata.get('date_published', 'N/A'))
                    with m4:
                        st.metric("Run", ", ".join(metadata.get('run_period', ['N/A'])))

                    st.markdown("---")
                    st.write(metadata.get('description', 'Detailed information available on the CERN Open Data portal.'))

                    if 'methodology' in metadata:
                        with st.expander("🔬 Methodology & Selection Criteria"):
                            st.write(metadata['methodology'].get('description', ''), unsafe_allow_html=True)

                    st.markdown(f"**Official Record:** [opendata.cern.ch/record/{rec_id}](https://opendata.cern.ch/record/{rec_id})")

                    files = metadata.get('_files', [])
                    compatible = [f for f in files if f.get('key', '').lower().endswith(('.csv', '.txt', '.json'))]
                    root_files = [f for f in files if f.get('key', '').lower().endswith('.root')]

                    if compatible:
                        render_csv_files(compatible, rec_id, is_active)
                    if root_files:
                        render_root_files(root_files, rec_id)
                    if not compatible and not root_files:
                        formats = metadata.get('distribution', {}).get('formats', [])
                        fmt = ", ".join(formats).upper() if formats else "ROOT/DST"
                        st.warning(f"⚠️ This dataset uses **{fmt}** format, which is not supported for direct browser preview.")

st.info("💡 **Navigation:** Use the sidebar on the left to head back to the 'Analysis' module with your own local data.")
