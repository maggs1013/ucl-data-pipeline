import os, pandas as pd
from utils import download_csv

OUT_HIST = "data/raw_football_data.csv"
os.makedirs("data", exist_ok=True)

# Add the CSV endpoints you want to include:
URLS = [
    "https://www.football-data.co.uk/mmz4281/2425/CL.csv",  # UCL 24/25
    # Add leagues if you want to enrich training:
    # "https://www.football-data.co.uk/mmz4281/2425/E0.csv",  # EPL
    # "https://www.football-data.co.uk/mmz4281/2425/D1.csv",  # Bundesliga
]

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Football-Data uses columns like Date, HomeTeam, AwayTeam, FTHG, FTAG, B365H,B365D,B365A (or PSH/PSD/PSA)
    cols = df.columns.str.upper()
    df.columns = cols
    # Try best-known columns across seasons/books:
    oddsH = None
    for c in ["B365H","PSH","WHH","IWH"]:
        if c in df.columns:
            oddsH = c; break
    oddsD = None
    for c in ["B365D","PSD","WHD","IWD"]:
        if c in df.columns:
            oddsD = c; break
    oddsA = None
    for c in ["B365A","PSA","WHA","IWA"]:
        if c in df.columns:
            oddsA = c; break

    keep = {
        "date": "DATE",
        "home_team": "HOMETEAM",
        "away_team": "AWAYTEAM",
        "home_goals": "FTHG",
        "away_goals": "FTAG",
        "home_odds_dec": oddsH,
        "draw_odds_dec": oddsD,
        "away_odds_dec": oddsA
    }
    out = {}
    for k, v in keep.items():
        if v is None or v not in df.columns:
            out[k] = pd.Series([None]*len(df))
        else:
            out[k] = df[v]

    out_df = pd.DataFrame(out).dropna(subset=["date","home_team","away_team"])
    # Standardize date
    out_df["date"] = pd.to_datetime(out_df["date"], dayfirst=True, errors="coerce")
    out_df = out_df.dropna(subset=["date"])
    # Basic defaults for features (you can enrich later)
    out_df["home_rest_days"] = 4
    out_df["away_rest_days"] = 4
    out_df["home_travel_km"] = 200
    out_df["away_travel_km"] = 200
    out_df["home_injury_index"] = 0.3
    out_df["away_injury_index"] = 0.3
    out_df["home_gk_rating"] = 0.6
    out_df["away_gk_rating"] = 0.6
    out_df["home_setpiece_rating"] = 0.6
    out_df["away_setpiece_rating"] = 0.6
    out_df["ref_pen_rate"] = 0.30
    out_df["crowd_index"] = 0.7
    return out_df

def main():
    frames = []
    for u in URLS:
        try:
            df = download_csv(u)
            frames.append(normalize(df))
        except Exception as e:
            print("Failed:", u, e)
    hist = pd.concat(frames, ignore_index=True).sort_values("date")
    hist.to_csv(OUT_HIST, index=False)
    print("Saved", OUT_HIST, len(hist))

if __name__ == "__main__":
    main()
