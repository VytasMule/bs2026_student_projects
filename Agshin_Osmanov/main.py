from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


def save_top_barh(series, title, xlabel, outpath, top_n=10):
    s = series.head(top_n).sort_values()
    plt.figure()
    s.plot(kind="barh")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.tight_layout()
    plt.savefig(outpath, dpi=160)
    plt.close()


def main():
    project_root = Path(__file__).resolve().parent
    data_path = project_root / "data" / "winners_f1_1950_2025_v2.csv"
    outdir = project_root / "results"
    outdir.mkdir(parents=True, exist_ok=True)

    print("SCRIPT:", __file__)
    print("DATA PATH:", data_path)
    df = pd.read_csv(data_path)

    # Basic cleanup
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["decade"] = (df["year"] // 10) * 10

    # Core counts
    wins_by_driver = df["winner_name"].value_counts()
    wins_by_team = df["team"].value_counts()
    wins_by_continent = df["continent"].value_counts()
    wins_by_year = df.groupby("year").size().sort_index()
    wins_by_decade = df.groupby("decade").size().sort_index()
    wins_by_circuit = df["circuit"].value_counts()
    wins_by_gp = df["grand_prix"].value_counts()

    # Save CSV outputs
    wins_by_driver.head(50).rename("wins").to_csv(outdir / "top_drivers.csv")
    wins_by_team.head(50).rename("wins").to_csv(outdir / "top_teams.csv")
    wins_by_year.rename("wins").to_csv(outdir / "wins_by_year.csv")
    wins_by_decade.rename("wins").to_csv(outdir / "wins_by_decade.csv")
    wins_by_continent.rename("wins").to_csv(outdir / "wins_by_continent.csv")

    # Write a short summary (nice for report/presentation)
    summary = []
    summary.append(f"Total races (rows): {len(df)}")
    summary.append(f"Years covered: {int(df['year'].min())}–{int(df['year'].max())}")
    summary.append(f"Unique winners: {df['winner_name'].nunique()}")
    summary.append(f"Unique teams: {df['team'].nunique()}")
    summary.append("")
    summary.append(f"Top winner: {wins_by_driver.index[0]} ({wins_by_driver.iloc[0]} wins)")
    summary.append(f"Top team: {wins_by_team.index[0]} ({wins_by_team.iloc[0]} wins)")
    summary.append(f"Most common Grand Prix: {wins_by_gp.index[0]} ({wins_by_gp.iloc[0]} races)")
    summary.append(f"Most common circuit: {wins_by_circuit.index[0]} ({wins_by_circuit.iloc[0]} races)")
    (outdir / "summary.txt").write_text("\n".join(summary), encoding="utf-8")

    # Plots
    save_top_barh(wins_by_driver, "Top 10 Drivers by Wins (1950–2025)", "Wins", outdir / "top_drivers.png")
    save_top_barh(wins_by_team, "Top 10 Teams by Wins (1950–2025)", "Wins", outdir / "top_teams.png")

    # Wins by year (trend)
    plt.figure()
    wins_by_year.plot()
    plt.title("Number of Races per Year (1950–2025)")
    plt.xlabel("Year")
    plt.ylabel("Races")
    plt.tight_layout()
    plt.savefig(outdir / "wins_by_year.png", dpi=160)
    plt.close()

    # Wins by decade
    plt.figure()
    wins_by_decade.plot(kind="bar")
    plt.title("Number of Races per Decade")
    plt.xlabel("Decade")
    plt.ylabel("Races")
    plt.tight_layout()
    plt.savefig(outdir / "wins_by_decade.png", dpi=160)
    plt.close()

    # Wins by continent
    plt.figure()
    wins_by_continent.sort_values().plot(kind="barh")
    plt.title("Races by Continent")
    plt.xlabel("Races")
    plt.ylabel("Continent")
    plt.tight_layout()
    plt.savefig(outdir / "wins_by_continent.png", dpi=160)
    plt.close()

    print("Done ✅ Check outputs in:", outdir)


if __name__ == "__main__":
    main()