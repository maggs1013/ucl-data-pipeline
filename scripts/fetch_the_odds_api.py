# scripts/fetch_the_odds_api.py
# Drop-in resilient fetcher:
# 1) Manual override: if data/manual_odds.csv exists, use it and skip the API.
# 2) Otherwise: query The Odds API, auto-detect the UCL sport key if not set,
#    try multiple regions, and FAIL GRACEFULLY by writing an empty CSV (exit 0).

import os
import sys
import requests
import pandas as pd

OUT_UPCOMING = "data/raw_theodds_fixtures.csv"
MANUAL_ODDS = "data/manual_odds.csv"

# Secrets / env (optional helpers)
API_KEY = os.environ.get("THE_ODDS_API_KEY", "").strip()
SPORT = os.environ.get("ODDS_SPORT_KEY", "").strip()     # optional: force a sport key via secret
REGIONS = os.environ.get("ODDS_REGIONS", "eu,uk,us,au").strip()
MARKETS = os.environ.get("ODDS_MARKETS", "h2h").strip()

BASE = "https://api.the-odds-api.com/v4"

def write_empty_and_exit(msg: str) -> None:
    """Write an empty-but-valid CSV so downstream steps continue, then exit 0."""
    cols = ["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec"]
    pd.DataFrame(columns=cols).to_csv(OUT_UPCOMING, index=False)
    print("WARNING:", msg)
    print("Wrote empty", OUT_UPCOMING)
    sys.exit(0)

def use_manual_odds_if_present() -> bool:
    """If data/manual_odds.csv exists and is valid, convert -> OUT_UPCOMING and return True."""
    if not os.path.exists(MANUAL_ODDS):
        return False
    try:
        df = pd.read_csv(MANUAL_ODDS)
    except Exception as e:
        print("manual_odds.csv read error:", e, "— falling back to API...")
        return False

    needed = {"date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec"}
    if not needed.issubset(df.columns) or df.empty:
        print("manual_odds.csv present but invalid columns/empty — falling back to API...")
        return False

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
    df.to_csv(OUT_UPCOMING, index=False)
    print(f"Using manual odds: {MANUAL_ODDS} → wrote {OUT_UPCOMING} ({len(df)} rows)")
    return True

def list_sports(api_key: str):
    url = f"{BASE}/sports/"
    r = requests.get(url, params={"apiKey": api_key}, timeout=60)
    if r.status_code != 200:
        print("Sports list error:", r.status_code, r.text)
        return []
    return r.json()

def fetch_odds(api_key: str, sport_key: str, regions: str, markets: str):
    url = f"{BASE}/sports/{sport_key}/odds"
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": "decimal",
    }
    r = requests.get(url, params=params, timeout=60)
    if r.status_code != 200:
        print("Fetch odds error:", r.status_code, r.text)
        return None
    return r.json()

def main():
    # 0) Manual override path (step 4: use your own odds file if present)
    if use_manual_odds_if_present():
        return

    # 1) API key required for automatic odds
    if not API_KEY:
        write_empty_and_exit("Missing THE_ODDS_API_KEY (GitHub Secret). Using empty upcoming file.")

    # 2) Get available sports and auto-detect a Champions League key if not provided
    sports = list_sports(API_KEY)
    if not sports:
        write_empty_and_exit("Could not retrieve sports list — check API key/plan/connectivity.")

    sport_key = SPORT
    if not sport_key:
        # Print a small subset for debug
        print("Available sports keys (first 25):")
        for s in sports[:25]:
            print("-", s.get("key"), "|", s.get("title"))

        # Try to choose a UCL-looking key automatically
        candidates = [
            s for s in sports
            if "soccer" in (s.get("key") or "")
            and ("uefa" in (s.get("key") or "") or "champ" in (s.get("key") or ""))
        ]
        if candidates:
            sport_key = candidates[0]["key"]
            print("Auto-selected sport_key:", sport_key)
        else:
            # Fallback to EPL so pipeline still runs
            fallback = [s for s in sports if s.get("key") in ("soccer_epl","soccer_uefa_europa_league")]
            if fallback:
                sport_key = fallback[0]["key"]
                print("No UCL key found; using fallback:", sport_key)
            else:
                write_empty_and_exit("No suitable soccer sport_key found for your account/plan.")

    # 3) Fetch odds
    data = fetch_odds(API_KEY, sport_key, REGIONS, MARKETS)
    if data is None:
        write_empty_and_exit(f"Odds fetch failed for sport_key={sport_key}. Writing empty file to continue.")

    # 4) Normalize and write
    rows = []
    for game in data:
        home = game.get("home_team")
        away = game.get("away_team")
        commence = game.get("commence_time")
        home_odds = draw_odds = away_odds = None

        for bm in game.get("bookmakers", []):
            for mk in bm.get("markets", []):
                if mk.get("key") == "h2h":
                    mm = {o.get("name"): float(o.get("price")) for o in mk.get("outcomes", []) if o.get("name") and o.get("price") is not None}
                    home_odds = mm.get(home)
                    away_odds = mm.get(away)
                    draw_odds = mm.get("Draw") or mm.get("Tie")
                    break
            if home_odds or away_odds or draw_odds:
                break

        rows.append({
            "date": commence,
            "home_team": home,
            "away_team": away,
            "home_odds_dec": home_odds,
            "draw_odds_dec": draw_odds,
            "away_odds_dec": away_odds
        })

    upc = pd.DataFrame(rows)
    if upc.empty:
        write_empty_and_exit(f"No odds returned for sport_key={sport_key} in regions={REGIONS}. Writing empty file.")
    upc["date"] = pd.to_datetime(upc["date"], errors="coerce").dt.tz_localize(None)
    upc.to_csv(OUT_UPCOMING, index=False)
    print("Saved", OUT_UPCOMING, len(upc))

if __name__ == "__main__":
    main()
