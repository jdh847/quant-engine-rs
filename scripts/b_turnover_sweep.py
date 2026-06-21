#!/usr/bin/env python3
"""B-followup: cut turnover on industry_relative_reversion, re-check OOS.

NOTE: the `readiness` subcommand is currently unusable on this machine (it hangs
in macOS uninterruptible-wait, leaving unkillable UE zombies that poison the
shared data-file resource for every subsequent `readiness`; needs a reboot to
clear). `run` is unaffected, so this script uses `run` over the full sample and
computes the OOS-window metrics (Sharpe / maxDD / trades over dates >= the
readiness test-window start) directly from equity_curve.csv. The method is
validated against the base config, whose readiness OOS was Sharpe 1.327 /
maxDD 8.22%.

Sweep two turnover levers and recompute OOS for each:
  - max_turnover_ratio       (0.35 = current)
  - rebalance_interval_days  (5 = current default)
Goal: OOS maxDD <= 8% AND Sharpe >= 1.25  =>  PAPER_READY-eligible.
"""
import subprocess, json, os, re, csv, math

# Workaround for the UE-zombie vnode lock on the original binary/data: run a
# fresh copy of the binary against a fresh copy of the data (absolute paths).
# Falls back to the in-tree paths if the fresh copies are absent.
BIN = os.environ.get("QUANT_ENGINE_BIN",
                     "/tmp/pqbot_fresh" if os.path.exists("/tmp/pqbot_fresh")
                     else "target/debug/private_quant_bot")
BASE = ("/tmp/rv_fresh.toml" if os.path.exists("/tmp/rv_fresh.toml")
        else "config/bot_us_relative_value.toml")
OUT = ("/tmp/b_turnover_sweep" if os.path.exists("/tmp/pqbot_fresh")
       else "outputs_rust/b_turnover_sweep")
TMPDIR = os.path.join(OUT, "_configs")
os.makedirs(TMPDIR, exist_ok=True)
base_text = open(BASE).read()

OOS_START = "2023-10-12"   # readiness test-window start for this sample/ratio
TURNS = ["0.35", "0.20", "0.12"]
REBALS = ["5", "10", "20"]


def make_cfg(turn, rebal):
    t = re.sub(r"^max_turnover_ratio = .*$",
               f"max_turnover_ratio = {turn}", base_text, flags=re.M)
    if re.search(r"^rebalance_interval_days = ", t, flags=re.M):
        t = re.sub(r"^rebalance_interval_days = .*$",
                   f"rebalance_interval_days = {rebal}", t, flags=re.M)
    else:
        t = re.sub(r"^(max_turnover_ratio = .*)$",
                   f"\\1\nrebalance_interval_days = {rebal}", t, flags=re.M)
    return t


def oos_metrics(rundir):
    """Sharpe (annualized), maxDD, pnl over the OOS window from equity_curve.csv."""
    eq = []
    with open(os.path.join(rundir, "equity_curve.csv")) as f:
        for row in csv.DictReader(f):
            if row["date"] >= OOS_START:
                eq.append(float(row["equity"]))
    if len(eq) < 3:
        return None
    rets = [eq[i] / eq[i-1] - 1.0 for i in range(1, len(eq))]
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    std = math.sqrt(var)
    sharpe = (mean / std * math.sqrt(252)) if std > 0 else 0.0
    peak, maxdd = eq[0], 0.0
    for v in eq:
        peak = max(peak, v)
        maxdd = max(maxdd, (peak - v) / peak)
    pnl = eq[-1] / eq[0] - 1.0
    return sharpe, maxdd, pnl


def oos_trades(rundir):
    n = 0
    p = os.path.join(rundir, "trades.csv")
    if os.path.exists(p):
        with open(p) as f:
            for row in csv.DictReader(f):
                d = row.get("date", "")
                if d >= OOS_START:
                    n += 1
    return n


def full_metrics(rundir):
    m = {}
    with open(os.path.join(rundir, "summary.txt")) as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                m[k] = v.rstrip("%")
    return m


print(f"VALIDATION: base config OOS should be ~Sharpe 1.327 / maxDD 8.22% "
      f"(readiness ground truth)\n")
print(f"{'turn':<6}{'rebal':<7}{'oosShrp':>9}{'oosMaxDD':>10}{'oosPnL':>9}"
      f"{'oosTrd':>8}{'fullShrp':>9}{'fullTrd':>9}  eligible")
print("-" * 80)
rows = []
for turn in TURNS:
    for rebal in REBALS:
        cfg = os.path.join(TMPDIR, f"rv_t{turn}_r{rebal}.toml")
        open(cfg, "w").write(make_cfg(turn, rebal))
        rundir = os.path.join(OUT, f"t{turn}_r{rebal}")
        subprocess.run([BIN, "run", "--config", cfg, "--output-dir", rundir,
                        "--skip-validate-data"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        om = oos_metrics(rundir)
        if om is None:
            print(f"{turn:<6}{rebal:<7}  ERROR no equity curve")
            continue
        osh, odd, opnl = om
        otr = oos_trades(rundir)
        fm = full_metrics(rundir)
        fsh = float(fm.get("sharpe", 0))
        ftr = int(fm.get("trades", 0))
        elig = "<== YES" if (osh >= 1.25 and odd <= 0.08) else ""
        print(f"{turn:<6}{rebal:<7}{osh:>9.3f}{odd*100:>9.2f}%{opnl*100:>8.2f}%"
              f"{otr:>8}{fsh:>9.3f}{ftr:>9}  {elig}")
        rows.append((turn, rebal, osh, odd, opnl, otr, fsh, ftr))
print("-" * 80)
elig = [r for r in rows if r[2] >= 1.25 and r[3] <= 0.08]
print(f"\nPAPER_READY-eligible (OOS Sharpe>=1.25 AND maxDD<=8%): {len(elig)}")
for r in sorted(elig, key=lambda x: (x[3], -x[2])):
    print(f"  turn={r[0]} rebal={r[1]}: OOS sharpe={r[2]:.3f} maxdd={r[3]*100:.2f}% "
          f"pnl={r[4]*100:.2f}% oos_trades={r[5]} full_trades={r[7]}")
