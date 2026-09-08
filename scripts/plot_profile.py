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

# No UTC shift any more. hour_of_day used to be derived from observed_arrival_ts
# in UTC, so these charts corrected it by a hard-coded -7 -- which was wrong twice
# over: it silently assumed PDT for a window that crosses the PST boundary, and it
# only fixed the picture, never the feature the model trained on. gold_features.py
# now derives the hour with from_utc_timestamp(..., "America/Vancouver"), so the
# CSVs must be exported from transitpulse.training_features and used as-is.
# See docs/adr/005-timezone-boundaries.md.

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
    df["hour"] = df["hour_of_day"]
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


def model_vs_baselines() -> None:
    """Where the model earns its keep, hour by hour.

    Reads docs/demo-data.json rather than a CSV: those numbers were produced by
    scoring the registered model artifact against the held-out test split, so the
    chart cannot drift from the evaluation the registry gate actually ran.
    """
    import json

    payload = json.loads((ROOT / "docs" / "demo-data.json").read_text())
    df = pd.DataFrame(payload["by_hour"]).sort_values("hour")
    head = payload["headline"]

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(8, 6.2), sharex=True, gridspec_kw={"height_ratios": [1.25, 1]}
    )

    ax1.plot(df["hour"], df["schedule"], color=MUTED, lw=1.6, ls="--", label="published schedule")
    ax1.plot(df["hour"], df["persistence"], color=S2, lw=1.8, label="persistence")
    ax1.plot(df["hour"], df["historical"], color=LATE, lw=1.8, label="historical median")
    ax1.plot(df["hour"], df["model"], color=S1, lw=2.6, label="XGBoost")
    ax1.set_ylabel("mean absolute error (seconds)", fontsize=9)
    ax1.grid(axis="y")
    ax1.legend(frameon=False, fontsize=9, loc="upper left", ncol=2, labelcolor=INK2)
    strip(ax1)
    ax1.set_title(
        "The model's advantage grows with congestion\n"
        f"{head['n_scored']:,} held-out arrivals, "
        f"{head['first_day']} to {head['last_day']}",
        loc="left",
        fontsize=12,
        fontweight="semibold",
        color=INK,
        pad=14,
    )

    # Gain over the STRONGEST baseline at each hour, which is the honest claim --
    # not the gap to whichever baseline happens to look worst.
    best = df[["persistence", "historical", "schedule"]].min(axis=1)
    gain = (best - df["model"]) / best * 100
    colors = [S1 if value >= 0 else LATE for value in gain]
    ax2.bar(df["hour"], gain, color=colors, width=0.66)
    ax2.axhline(0, color=AXIS, lw=0.8)
    ax2.set_ylabel("% better than best baseline", fontsize=9)
    ax2.set_xlabel("hour of day — Vancouver local time", fontsize=9)
    ax2.set_xticks(range(0, 24, 3), [f"{h:02d}" for h in range(0, 24, 3)])
    ax2.grid(axis="y")
    strip(ax2)

    ax2.annotate(
        "wins the commute, loses the quiet hours",
        xy=(0.99, 0.06),
        xycoords="axes fraction",
        ha="right",
        fontsize=8.5,
        color=MUTED,
    )

    fig.tight_layout()
    fig.savefig(OUT / "model_vs_baselines.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {OUT / 'model_vs_baselines.png'}")


def feature_importance() -> None:
    import json

    payload = json.loads((ROOT / "docs" / "demo-data.json").read_text())
    df = pd.DataFrame(payload["importance"]).head(12).iloc[::-1]

    fig, ax = plt.subplots(figsize=(8, 4.2))
    y = range(len(df))
    ax.barh(list(y), df["gain"] * 100, color=S1, height=0.66)
    for yi, value in zip(y, df["gain"], strict=True):
        ax.text(value * 100 + 0.6, yi, f"{value * 100:.1f}%", va="center", fontsize=8.5, color=INK2)
    ax.set_yticks(list(y), df["feature"], fontsize=9, color=INK2)
    ax.set_xticks([])
    ax.set_xlim(0, df["gain"].max() * 118)
    strip(ax, keep_bottom=False)
    ax.set_title(
        "One feature carries the model\n"
        "share of total split gain, XGBoost, 15 boosting rounds",
        loc="left",
        fontsize=12,
        fontweight="semibold",
        color=INK,
        pad=14,
    )

    fig.tight_layout()
    fig.savefig(OUT / "feature_importance.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {OUT / 'feature_importance.png'}")


if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    delay_distribution()
    hourly_profile()
    model_vs_baselines()
    feature_importance()
