import polars as pl


def apply_kinematic_filters(df, mass_range, pt_min, eta_max, require_opposite_charge):
    cols = df.columns

    filter_exprs = [
        (pl.col('Calculated_M') >= mass_range[0]) & (pl.col('Calculated_M') <= mass_range[1])
    ]

    if 'pt1' in cols:
        filter_exprs.append((pl.col('pt1') >= pt_min) & (pl.col('pt2') >= pt_min))
        filter_exprs.append((pl.col('eta1').abs() <= eta_max) & (pl.col('eta2').abs() <= eta_max))
    elif 'pt' in cols:
        filter_exprs.append(pl.col('pt') >= pt_min)
        filter_exprs.append(pl.col('eta').abs() <= eta_max)

    if require_opposite_charge and 'Q1' in cols and 'Q2' in cols:
        filter_exprs.append(pl.col('Q1') != pl.col('Q2'))

    for expr in filter_exprs:
        df = df.filter(expr)

    return df
