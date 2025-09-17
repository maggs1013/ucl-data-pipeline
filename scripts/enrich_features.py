import os, math
import pandas as pd

DATA_DIR = "data"

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2*R*math.asin(math.sqrt(a))

def safe_read(path):
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()

def merge_team_master(df, teams):
    if teams.empty: return df
    df = df.merge(teams.add_prefix("home_"), left_on="home_team", right_on="home_team", how="left")
    df = df.merge(teams.add_prefix("away_"), left_on="away_team", right_on="away_team", how="left")
    # If single crowd_index column is missing or null, copy from home team default
    if "crowd_index" not in df.columns or df["crowd_index"].isna().all():
        if "home_crowd_index" in df.columns:
            df["crowd_index"] = df["home_crowd_index"]
    # Prefer explicit values if they already exist, else use team defaults
    for side in ["home","away"]:
        for c in ["gk_rating","setpiece_rating"]:
            col = f"{side}_{c}"
            if col in df.columns:
                df[col] = df[col].fillna(df[f"{side}_{c}"])
            else:
                df[col] = df[f"{side}_{c}"]
    return df

def apply_ref_rates(df, refs):
    if "ref_name" in df.columns and not refs.empty:
        df = df.merge(refs, how="left", on="ref_name")
        df["ref_pen_rate"] = df["ref_pen_rate"].fillna(0.30)
    else:
        if "ref_pen_rate" not in df.columns:
            df["ref_pen_rate"] = 0.30
    return df

def apply_injuries(df, inj):
    if inj.empty: return df
    inj["date"] = pd.to_datetime(inj["date"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.merge(inj.rename(columns={"team":"home_team","injury_index":"home_injury_index"}),
                  on=["date","home_team"], how="left")
    df = df.merge(inj.rename(columns={"team":"away_team","injury_index":"away_injury_index"}),
                  on=["date","away_team"], how="left")
    return df

def apply_lineup_flags(df, lu):
    if lu.empty: return df
    lu["date"] = pd.to_datetime(lu["date"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for side in ["home","away"]:
        m = lu.rename(columns={
            "team":f"{side}_team",
            "key_att_out":f"{side}_key_att_out",
            "key_def_out":f"{side}_key_def_out",
            "keeper_changed":f"{side}_keeper_changed"
        })
        df = df.merge(m, on=["date",f"{side}_team"], how="left")
        # Optional: minor adjustments based on flags can be added later
    return df

def compute_travel(df, stad):
    if stad.empty:
        if "home_travel_km" not in df.columns: df["home_travel_km"] = 0.0
        if "away_travel_km" not in df.columns: df["away_travel_km"] = 200.0
        return df
    sH = stad.add_prefix("home_"); sA = stad.add_prefix("away_")
    df = df.merge(sH, how="left", left_on="home_team", right_on="home_team")
    df = df.merge(sA, how="left", left_on="away_team", right_on="away_team")
    def row_dist(r):
        if pd.isna(r["home_lat"]) or pd.isna(r["away_lat"]): return 200.0
        return haversine(r["home_lat"], r["home_lon"], r["away_lat"], r["away_lon"])
    if "home_travel_km" not in df.columns: df["home_travel_km"] = 0.0
    if "away_travel_km" not in df.columns:
        df["away_travel_km"] = df.apply(row_dist, axis=1)
    else:
        mask = df["away_travel_km"].isna()
        df.loc[mask, "away_travel_km"] = df[mask].apply(row_dist, axis=1)
    return df

def enrich_file(path, teams, stad, refs, inj, lu):
    if not os.path.exists(path): return
    df = pd.read_csv(path, parse_dates=["date"])
    df = merge_team_master(df, teams)
    df = apply_injuries(df, inj)
    df = apply_lineup_flags(df, lu)
    df = apply_ref_rates(df, refs)
    df = compute_travel(df, stad)
    df.to_csv(path, index=False)

def main():
    teams = safe_read(os.path.join(DATA_DIR, "teams_master.csv"))
    stad  = safe_read(os.path.join(DATA_DIR, "stadiums.csv"))
    refs  = safe_read(os.path.join(DATA_DIR, "ref_baselines.csv"))
    inj   = safe_read(os.path.join(DATA_DIR, "injuries.csv"))
    lu    = safe_read(os.path.join(DATA_DIR, "lineups.csv"))

    enrich_file(os.path.join(DATA_DIR, "raw_football_data.csv"), teams, stad, refs, inj, lu)
    enrich_file(os.path.join(DATA_DIR, "raw_theodds_fixtures.csv"), teams, stad, refs, inj, lu)
    print("Enrichment complete.")

if __name__ == "__main__":
    main()
