import streamlit as st
import polars as pl


def map_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Detects experiment format (LHCb, ATLAS, CMS) and normalizes columns.
    Returns a DataFrame with standardized columns including 'Calculated_M'.
    """
    cols = df.columns

    if 'muplus_PX' in cols and 'muminus_PX' in cols:
        df = _map_lhcb(df, cols)
        cols = df.columns

    if 'lep_pt' in cols and 'lep_eta' in cols:
        df = _map_atlas(df, cols)
        cols = df.columns

    if 'Calculated_M' not in df.columns:
        df = _compute_invariant_mass(df, cols)

    return df


def _map_lhcb(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    st.info("💡 **Format Detected:** LHCb NTuple. Mapping particle branches...")
    with st.spinner("Refactoring LHCb columnar data..."):
        mapping = {
            'muplus_PX': 'px1', 'muplus_PY': 'py1', 'muplus_PZ': 'pz1', 'muplus_PT': 'pt1',
            'muminus_PX': 'px2', 'muminus_PY': 'py2', 'muminus_PZ': 'pz2', 'muminus_PT': 'pt2',
        }
        for old_name, new_name in mapping.items():
            if old_name in cols:
                df = df.with_columns((pl.col(old_name) / 1000.0).alias(new_name))

        if 'muplus_PE' in cols:
            df = df.with_columns((pl.col('muplus_PE') / 1000.0).alias('E1'))
        if 'muminus_PE' in cols:
            df = df.with_columns((pl.col('muminus_PE') / 1000.0).alias('E2'))

        m_mu = 0.105658
        if 'E1' not in df.columns:
            df = df.with_columns(
                ((pl.col('px1')**2 + pl.col('py1')**2 + pl.col('pz1')**2 + m_mu**2).sqrt()).alias('E1')
            )
        if 'E2' not in df.columns:
            df = df.with_columns(
                ((pl.col('px2')**2 + pl.col('py2')**2 + pl.col('pz2')**2 + m_mu**2).sqrt()).alias('E2')
            )

        if 'eta1' not in df.columns:
            df = df.with_columns([
                (pl.col('pz1') / (pl.col('px1')**2 + pl.col('py1')**2 + pl.col('pz1')**2).sqrt()).clip(-0.999, 0.999).arctanh().alias('eta1'),
                (pl.col('pz2') / (pl.col('px2')**2 + pl.col('py2')**2 + pl.col('pz2')**2).sqrt()).clip(-0.999, 0.999).arctanh().alias('eta2'),
            ])

        if 'muplus_ID' in cols:
            df = df.with_columns([
                pl.col('muplus_ID').alias('Q1'),
                pl.col('muminus_ID').alias('Q2'),
            ])

        parent_mass_col = next(
            (c for c in cols if c.endswith(('_MM', '_M')) and not any(p in c for p in ('muplus', 'muminus', 'Kplus'))),
            None
        )
        if parent_mass_col:
            st.success(f"💎 **High Precision Branch Found:** Using `{parent_mass_col}` (converted to GeV) for mass distribution.")
            df = df.with_columns((pl.col(parent_mass_col) / 1000.0).alias('Calculated_M'))

    return df


def _map_atlas(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    with st.spinner("Extracting ATLAS leptons and converting MeV -> GeV..."):
        df = df.with_columns([
            (pl.col('lep_pt').list.get(0) / 1000.0).alias('pt1'),
            (pl.col('lep_pt').list.get(1) / 1000.0).alias('pt2'),
            pl.col('lep_eta').list.get(0).alias('eta1'),
            pl.col('lep_eta').list.get(1).alias('eta2'),
            pl.col('lep_phi').list.get(0).alias('phi1'),
            pl.col('lep_phi').list.get(1).alias('phi2'),
        ])
        if 'lep_E' in cols:
            df = df.with_columns([
                (pl.col('lep_E').list.get(0) / 1000.0).alias('E1'),
                (pl.col('lep_E').list.get(1) / 1000.0).alias('E2'),
            ])
        if 'met_et' in cols:
            df = df.with_columns([
                (pl.col('met_et') / 1000.0).alias('met'),
                pl.col('met_phi').alias('met_phi_val'),
            ])
        if 'lep_charge' in cols:
            df = df.with_columns([
                pl.col('lep_charge').list.get(0).alias('Q1'),
                pl.col('lep_charge').list.get(1).alias('Q2'),
            ])
        df = df.drop_nulls(subset=['pt1', 'pt2'])
    return df


def _compute_invariant_mass(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    if 'E1' in cols and 'px1' in cols:
        df = df.with_columns([
            (pl.col('E1') + pl.col('E2')).alias('E_tot'),
            (pl.col('px1') + pl.col('px2')).alias('px_tot'),
            (pl.col('py1') + pl.col('py2')).alias('py_tot'),
            (pl.col('pz1') + pl.col('pz2')).alias('pz_tot'),
        ])
        df = df.with_columns(
            ((pl.col('E_tot')**2 - (pl.col('px_tot')**2 + pl.col('py_tot')**2 + pl.col('pz_tot')**2)).clip(lower_bound=0).sqrt()).alias('Calculated_M')
        )

    elif 'pt1' in cols and 'eta1' in cols:
        m_mu = 0.105658
        df = df.with_columns([
            (pl.col('pt1') * pl.col('phi1').cos()).alias('px1'),
            (pl.col('pt1') * pl.col('phi1').sin()).alias('py1'),
            (pl.col('pt1') * pl.col('eta1').sinh()).alias('pz1'),
        ])
        df = df.with_columns(
            ((pl.col('px1')**2 + pl.col('py1')**2 + pl.col('pz1')**2 + m_mu**2).sqrt()).alias('E1')
        )
        df = df.with_columns([
            (pl.col('pt2') * pl.col('phi2').cos()).alias('px2'),
            (pl.col('pt2') * pl.col('phi2').sin()).alias('py2'),
            (pl.col('pt2') * pl.col('eta2').sinh()).alias('pz2'),
        ])
        df = df.with_columns(
            ((pl.col('px2')**2 + pl.col('py2')**2 + pl.col('pz2')**2 + m_mu**2).sqrt()).alias('E2')
        )
        df = df.with_columns([
            (pl.col('E1') + pl.col('E2')).alias('E_tot'),
            (pl.col('px1') + pl.col('px2')).alias('px_tot'),
            (pl.col('py1') + pl.col('py2')).alias('py_tot'),
            (pl.col('pz1') + pl.col('pz2')).alias('pz_tot'),
        ])
        df = df.with_columns(
            ((pl.col('E_tot')**2 - (pl.col('px_tot')**2 + pl.col('py_tot')**2 + pl.col('pz_tot')**2)).clip(lower_bound=0).sqrt()).alias('Calculated_M')
        )

    elif 'pt' in cols and 'MET' in cols:
        st.info("💡 **Format Detected:** Single-particle event with Missing Transverse Energy (MET). Calculating **Transverse Mass ($M_T$)**.")
        df = df.with_columns(
            ((2 * pl.col('pt') * pl.col('MET') * (1 - (pl.col('phi') - pl.col('phiMET')).cos())).sqrt()).alias('Calculated_M')
        )

    elif 'M' in cols:
        df = df.with_columns(pl.col('M').alias('Calculated_M'))

    else:
        st.error(f"Dataset columns {df.columns} are not recognized for invariant mass calculation.")
        st.stop()

    return df
