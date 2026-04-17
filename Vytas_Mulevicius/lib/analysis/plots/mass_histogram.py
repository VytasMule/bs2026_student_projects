import io
import pandas as pd
import streamlit as st
import numpy as np
import plotly.graph_objects as go
from lib.analysis.plot_mass import generate_publication_plot


def render_mass_histogram(
    filtered_df: pd.DataFrame,
    particle_name: str,
    expected_mass: float,
    mass_range: tuple[float, float],
    bins: int,
    mass_label: str,
) -> None:
    """
    Renders an interactive Plotly mass histogram with a dashed PDG reference line.

    mass_label controls the axis title and hover text: use 'Invariant Mass' for
    dimuon events or 'Transverse Mass ($M_T$)' for single-lepton + MET events.
    Also renders a publication export button backed by generate_publication_plot().
    """
    st.subheader(f"Invariant Mass Distribution ({particle_name} → μμ)")

    counts, bin_edges = np.histogram(filtered_df['Calculated_M'], bins=bins, range=mass_range)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=bin_edges[:-1],
        y=counts,
        width=np.diff(bin_edges),
        offset=0,
        marker_color='royalblue',
        marker_line_width=0,
        name='Events',
        hovertemplate=f"{mass_label}: %{{x:.3f}} - %{{customdata:.3f}} GeV/c²<br>Events: %{{y}}<extra></extra>",
        customdata=bin_edges[1:],
    ))
    fig.add_vline(x=expected_mass, line_width=2, line_dash='dash', line_color='red')
    fig.add_trace(go.Scatter(
        x=[None], y=[None], mode='lines',
        line=dict(color='red', width=2, dash='dash'),
        name=f'{particle_name} Mass ({expected_mass:.3f} GeV/c²)',
    ))
    fig.update_layout(
        title=f'{mass_label} of Events ({particle_name})',
        xaxis_title=f'{mass_label} [GeV/c²]',
        yaxis_title='Number of Events',
        bargap=0,
        hovermode='x unified',
        template='plotly_white',
        height=600,
        legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99),
    )
    st.plotly_chart(fig, width='stretch')

    st.markdown("---")
    st.subheader("🎓 Publication Export")
    st.caption("Generate a high-DPI static plot using Matplotlib (legacy engine integration).")
    if st.button("Generate Publication-Ready Plot"):
        with st.spinner("Rendering high-res PNG..."):
            plot_buf = generate_publication_plot(
                filtered_df,
                particle_name=particle_name,
                expected_mass=expected_mass,
                mass_range=mass_range,
            )
            st.image(plot_buf, caption=f"High-Res {particle_name} Histogram (300 DPI)")
            st.download_button(
                label="📥 Download PNG for Paper",
                data=plot_buf,
                file_name=f"{particle_name}_publication_plot.png",
                mime="image/png",
            )
