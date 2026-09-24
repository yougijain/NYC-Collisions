"""Render the repository's social preview card.

The image GitHub shows when the repository link is pasted into a chat, a
post or an email. Without one the link renders as a grey placeholder, and
the first thing anybody sees of this project is a default.

It quotes the same two figures the README leads with, so it is generated
from the same artifacts rather than exported once from a design tool and
left to rot. The weekly refresh reruns it.

GitHub wants 1280x640 and under 1 MB.

Usage:
    python scripts/build_social_card.py
"""
import json, sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "app"))
import palette

facts = json.loads(Path(ROOT / "docs" / "dataset_facts.json").read_text())
wl = json.loads(Path(ROOT / "data" / "clean" / "watchlist_summary.json").read_text())

fig = plt.figure(figsize=(12.8, 6.4), dpi=100)
fig.patch.set_facecolor(palette.SURFACE)
ax = fig.add_axes([0, 0, 1, 1]); ax.axis("off")

ax.add_patch(plt.Rectangle((0, 0), 0.016, 1, color=palette.FLAGGED,
                           transform=ax.transAxes))

t = dict(transform=ax.transAxes, ha="left", va="baseline")
ax.text(0.075, 0.845, "NYC COLLISIONS", color=palette.INK_MUTED,
        fontsize=17, fontweight="bold", **t)

ax.text(0.075, 0.585, f"{wl['sites']:,} intersections", color=palette.INK,
        fontsize=72, fontweight="bold", **t)
ax.text(0.075, 0.435, "injure people more often than their own crashes explain",
        color=palette.INK, fontsize=27, **t)

ax.text(0.075, 0.275,
        f"{facts['rows']:,} crashes  ·  calibrated injury-risk model  "
        f"·  rebuilt weekly, unattended",
        color=palette.INK_SECONDARY, fontsize=18, **t)

ax.text(0.075, 0.135, "github.com/yougijain/NYC-Collisions",
        color=palette.LINK, fontsize=18, fontweight="bold", **t)

out = ROOT / "docs" / "img" / "social-preview.png"
fig.savefig(out, facecolor=palette.SURFACE)
print(f"{out}  {out.stat().st_size / 1024:.0f} KB")
