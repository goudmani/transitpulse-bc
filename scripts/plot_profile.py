"""Render README charts from the CSVs in data/processed/.

    python scripts/plot_profile.py

Reads delay_distribution.csv and delay_by_hour.csv, writes PNGs into img/.
Regenerate whenever the underlying queries are re-run -- the numbers in the
figures are only as current as the CSVs.

The charts carry a solid light background on purpose: a transparent PNG renders
as dark-on-dark for anyone reading the repo in GitHub's dark theme.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "processed"
OUT = ROOT / "img"

# Validated palette. Blue<->red with a gray midpoint is the documented diverging
# pair; blue/orange are categorical slots 1-2. Do not substitute by eye -- swap
# the whole set and re-validate if you change design system.
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"

EARLY, ONTIME, LATE = "#2a78d6", "#898781", "#e34948"
S1, S2 = "#2a78d6", "#eb6834"

# hour_of_day in the source is UTC (derived from observed_arrival_ts). Vancouver
# is UTC-7 in summer. Charts show local hours because that is what a reader can
# reason about; the model's feature is still the UTC value.
UTC_OFFSET = -7

mpl.rcParams.update(
    {
        "font.family": "sans-serif",
        "font.sans-serif": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "text.color": INK,
        "axes.labelcolor": INK2,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "axes.edgecolor": AXIS,
        "axes.linewidth": 0.8,
        "grid.color": GRID,
        "grid.linewidth": 0.8,
        "xtick.major.size": 0,
        "ytick.major.size": 0,
        "font.size": 10,
    }
)


def strip(ax, keep_bottom: bool = True) -> None:
    """Recessive chrome: no box, hairline grid on the value axis only."""
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_visible(keep_bottom)
    ax.set_axisbelow(True)


def delay_distribution() -> None:
    df = pd.read_csv(DATA / "delay_distribution.csv")
    df["label"] = df["bucket"].str.replace(r"^\d\.\s*", "", regex=True)
    total = df["n"].sum()
    df["pct"] = df["n"] / total * 100

    colors = [EARLY if "early" in b else ONTIME if "on time" in b else LATE for b in df["bucket"]]

    fig, ax = plt.subplots(figsize=(8, 3.6))
    y = range(len(df))[::-1]
    ax.barh(list(y), df["pct"], color=colors, height=0.68)

    for yi, pct, n in zip(y, df["pct"], df["n"], strict=True):
        ax.text(pct + 0.7, yi, f"{pct:.1f}%", va="center", fontsize=9, color=INK2)
        ax.text(0.5, yi, f"{n:,}", va="center", fontsize=8, color=SURFACE)

    ax.set_yticks(list(y), df["label"], fontsize=9, color=INK2)
    ax.set_xlim(0, df["pct"].max() * 1.16)
    ax.set_xticks([])
    strip(ax, keep_bottom=False)

    early = df.loc[df["bucket"].str.contains("early"), "pct"].sum()
    late = df.loc[df["bucket"].str.contains("late"), "pct"].sum()
    ax.set_title(
        f"Arrival delay against the published schedule\n"
        f"{total:,} stop arrivals — {late:.0f}% late, {early:.0f}% early",
        loc="left",
        fontsize=12,
        fontweight="semibold",
        color=INK,
        pad=14,
    )

    fig.tight_layout()
    fig.savefig(OUT / "delay_distribution.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {OUT / 'delay_distribution.png'}")


def hourly_profile() -> None:
    df = pd.read_csv(DATA / "delay_by_hour.csv")
    df["hour"] = (df["hour_of_day"] + UTC_OFFSET) % 24
    df = df.sort_values("hour")

    # Two measures on different scales, so two stacked axes sharing x --
    # never a second y-axis on the same plot.
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(8, 5.6), sharex=True, gridspec_kw={"height_ratios": [1, 1.15]}
    )

    ax1.bar(df["hour"], df["events"] / 1000, color=S1, width=0.62)
    ax1.set_ylabel("arrivals (thousands)", fontsize=9)
    ax1.grid(axis="y")
    strip(ax1)
    ax1.set_title(
        "Service volume and delay do not move together",
        loc="left",
        fontsize=12,
        fontweight="semibold",
        color=INK,
        pad=12,
    )

    ax2.plot(df["hour"], df["p90_delay"], color=S2, lw=2, marker="o", ms=4, label="90th percentile")
    ax2.plot(df["hour"], df["avg_delay"], color=S1, lw=2, marker="o", ms=4, label="mean")
    ax2.set_ylabel("delay (seconds)", fontsize=9)
    ax2.set_xlabel("hour of day — Vancouver local time", fontsize=9)
    ax2.set_xticks(range(0, 24, 3), [f"{h:02d}" for h in range(0, 24, 3)])
    ax2.grid(axis="y")
    ax2.legend(frameon=False, fontsize=9, loc="upper left", labelcolor=INK2)
    strip(ax2)

    peak = df.loc[df["p90_delay"].idxmax()]
    ax2.annotate(
        f"{int(peak['p90_delay'])}s",
        xy=(peak["hour"], peak["p90_delay"]),
        xytext=(6, 2),
        textcoords="offset points",
        fontsize=9,
        color=INK2,
    )

    fig.tight_layout()
    fig.savefig(OUT / "hourly_profile.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {OUT / 'hourly_profile.png'}")


if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    delay_distribution()
    hourly_profile()
