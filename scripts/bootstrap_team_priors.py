# scripts/bootstrap_team_priors.py
import os, pandas as pd

DATA_DIR = "data"
IN  = os.path.join(DATA_DIR, "xg_metrics_hybrid.csv")
OUT = os.path.join(DATA_DIR, "teams_master.csv")

def clamp(v, lo, hi): 
    try: v=float(v)
    except: return (lo+hi)/2
    return max(lo, min(hi, v))

def main():
    if not os.path.exists(IN):
        print("[WARN] xg_metrics_hybrid.csv missing; writing generic teams_master.csv"); 
        pd.DataFrame(columns=["team","gk_rating","setpiece_rating","crowd_index"]).to_csv(OUT, index=False); return

    df = pd.read_csv(IN)
    # Heuristics:
    # - setpiece_rating: 0.55 + 0.10*sign(xgd90), clamped [0.50, 0.85]
    # - gk_rating: 0.80 - 0.15*max(0, xga per match proxy), clamped [0.55, 0.90]
    # - crowd_index default 0.70 (you can hand-tune fortresses in stadiums.csv name_map)
    rows=[]
    for r in df.itertuples(index=False):
        xgd90 = getattr(r, "xgd90_hybrid", None)
        xga   = getattr(r, "xga_hybrid", None)
        setp  = 0.55 + 0.10*(1 if (pd.notna(xgd90) and xgd90>0) else (-1 if (pd.notna(xgd90) and xgd90<0) else 0))
        gk    = 0.80 - 0.15*max(0.0, (xga/34.0) if pd.notna(xga) else 0.0)  # rough per-match
        rows.append({
            "team": r.team,
            "gk_rating": clamp(gk, 0.55, 0.90),
            "setpiece_rating": clamp(setp, 0.50, 0.85),
            "crowd_index": 0.70
        })
    pd.DataFrame(rows).drop_duplicates("team").to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT}")

if __name__ == "__main__":
    main()
