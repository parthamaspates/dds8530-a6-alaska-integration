"""Figures for the notebook and the report (Matplotlib + Seaborn)."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from . import config

sns.set_theme(style="whitegrid", context="paper", font_scale=1.35)
PAL = {"camera": "#1F5FA8", "monitor": "#D97706", "notebook": "#2E8B57"}

def save(fig, name):
    fig.tight_layout()
    path = config.FIG_DIR / f"{name}.png"
    fig.savefig(path, dpi=200)
    return path

def sources_overview(spec_counts, attrs_per_spec):
    """spec_counts: vertical, source, origin, n ; attrs_per_spec: vertical, n_attributes."""
    fig, ax = plt.subplots(1, 2, figsize=(11, 4.2), gridspec_kw={"width_ratios": [1.5, 1]})
    top = spec_counts.sort_values("n", ascending=False).groupby("vertical").head(6)
    top = top.assign(label=top.source.str.replace("www.", "", regex=False) + " (" + top.vertical.str[:3] + ")")
    sns.barplot(data=top, y="label", x="n", hue="origin", dodge=False, ax=ax[0])
    ax[0].set(xlabel="Listings", ylabel="", title="Largest sources per vertical and where they are stored")
    ax[0].legend(title="Stored in", fontsize=8)
    sns.boxplot(data=attrs_per_spec, x="vertical", y="n_attributes", hue="vertical", palette=PAL, showfliers=False, legend=False, ax=ax[1])
    ax[1].set(xlabel="", ylabel="Attributes per listing", title="Schema heterogeneity")
    return save(fig, "fig1_sources")

def quality(before_after):
    """before_after: field, stage, pct_missing."""
    fig, ax = plt.subplots(figsize=(7.5, 3.6))
    sns.barplot(data=before_after, x="field", y="pct_missing", hue="stage", ax=ax, palette=["#B0B7C3", "#1F5FA8"])
    ax.set(xlabel="", ylabel="% of listings missing", title="Unified-schema completeness before and after integration")
    ax.legend(title="", loc="upper right", fontsize=8)
    for c in ax.containers:
        ax.bar_label(c, fmt="%.0f", fontsize=8)
    return save(fig, "fig2_quality")

def accuracy(sm, er_):
    fig, ax = plt.subplots(1, 2, figsize=(11, 3.8), sharey=True)
    for a, df, title in [(ax[0], sm, "Schema matching"), (ax[1], er_, "Entity resolution (pairwise)")]:
        m = df.melt(id_vars="vertical", value_vars=["precision", "recall", "f1"], var_name="metric")
        sns.barplot(data=m, x="vertical", y="value", hue="metric", ax=a, palette="Set2")
        a.set(xlabel="", ylabel="Score", ylim=(0, 1.05), title=title)
        for c in a.containers:
            a.bar_label(c, fmt="%.2f", fontsize=7)
    return save(fig, "fig3_accuracy")

def performance(perf):
    """perf: task, engine, seconds."""
    fig, ax = plt.subplots(figsize=(8, 3.8))
    sns.barplot(data=perf, x="task", y="seconds", hue="engine", ax=ax, palette="Set1")
    ax.set(xlabel="", ylabel="Wall-clock seconds", title="Pandas vs. Dask vs. Spark (same machine)")
    for c in ax.containers:
        ax.bar_label(c, fmt="%.1f", fontsize=8)
    return save(fig, "fig4_performance")

def streaming(metrics, windows):
    fig, ax = plt.subplots(1, 2, figsize=(11, 3.8))
    ax[0].bar(metrics.t, metrics.rows, width=3.5, color="#B0B7C3", label="listings in micro-batch")
    ax[0].bar(metrics.t, metrics.matched, width=3.5, color="#1F5FA8", label="matched to catalog entity")
    ax[0].set(xlabel="Seconds since producer start", ylabel="Listings", title="Kafka → Spark micro-batches")
    ax[0].legend(fontsize=8)
    if len(windows):
        w = windows.copy()
        w["t"] = (w.window_start - w.window_start.min()).dt.total_seconds()
        sns.lineplot(data=w, x="t", y="count", hue="vertical", palette=PAL, marker="o", ax=ax[1])
    ax[1].set(xlabel="Event time (s, 10-s tumbling windows)", ylabel="Listings per window", title="Windowed arrivals by vertical")
    return save(fig, "fig5_streaming")

def screen_sizes(unified):
    d = unified.dropna(subset=["screen_size_in", "brand"])
    d = d[d.vertical.isin(["monitor", "notebook"])]
    top = d.groupby("brand").size().sort_values(ascending=False).head(8).index
    d = d[d.brand.isin(top)]
    fig, ax = plt.subplots(figsize=(9, 3.8))
    sns.boxplot(data=d, x="brand", y="screen_size_in", hue="vertical", palette=PAL, showfliers=False, ax=ax)
    ax.set(xlabel="", ylabel="Screen size (in)", title="Integrated data: screen size by brand")
    return save(fig, "fig6_screen_sizes")
