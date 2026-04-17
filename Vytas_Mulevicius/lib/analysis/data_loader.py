import streamlit as st
import polars as pl


@st.cache_data
def load_data(path: str) -> pl.DataFrame:
    """
    Loads a CSV or ROOT file into a Polars DataFrame.

    Accepts local paths (.csv, .root), HTTP URLs, and XRootD paths starting with
    'root://'. ROOT files are opened via uproot; the first tree whose name contains
    'Events', 'DecayTree', 'mini', or 'tree' is selected, then converted to Polars
    via Arrow. Calls st.stop() on unrecognized formats.
    """
    if path.lower().split('?')[0].endswith('.csv'):
        return pl.read_csv(path)

    if path.lower().split('?')[0].endswith('.root') or path.startswith('root://'):
        import uproot
        import awkward as ak
        with uproot.open(path) as f:
            all_trees = [k for k, v in f.items(recursive=True) if hasattr(v, "arrays")]
            if not all_trees:
                st.error(f"No TTree found in {path}")
                st.stop()
            tree_name = next(
                (k for k in all_trees if any(p in k for p in ('Events', 'DecayTree', 'mini', 'tree'))),
                all_trees[0]
            )
            st.info(f"📍 Loading tree: `{tree_name}`")
            tree = f[tree_name]
            ak_array = tree.arrays()
            arrow_table = ak.to_arrow_table(ak_array, extensionarray=False)
            return pl.from_arrow(arrow_table)

    st.error(f"Unsupported file format: {path}")
    st.stop()
