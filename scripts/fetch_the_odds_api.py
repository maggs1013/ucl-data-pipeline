import os, requests, pandas as pd

OUT_UPCOMING="data/raw_theodds_fixtures.csv"
API_KEY=os.environ.get("THE_ODDS_API_KEY")
SPORT="soccer_uefa_champions_league"
REGION="eu"; MARKETS="h2h"

def main():
    os.makedirs("data",exist_ok=True)
    url=f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds"
    r=requests.get(url,params=dict(apiKey=API_KEY,regions=REGION,markets=MARKETS,oddsFormat="decimal"),timeout=60)
    r.raise_for_status(); js=r.json()
    rows=[]
    for game in js:
        home,away=game["home_team"],game["away_team"]
        commence=game["commence_time"]
        odds=(None,None,None)
        if game.get("bookmakers"):
            bm=game["bookmakers"][0]
            for mk in bm.get("markets",[]):
                if mk["key"]=="h2h":
                    mm={o["name"]:float(o["price"]) for o in mk["outcomes"]}
                    odds=(mm.get(home),mm.get("Draw") or mm.get("Tie"),mm.get(away))
                    break
        rows.append(dict(date=commence,home_team=home,away_team=away,
                         home_odds_dec=odds[0],draw_odds_dec=odds[1],away_odds_dec=odds[2]))
    upc=pd.DataFrame(rows)
    upc["date"]=pd.to_datetime(upc["date"],errors="coerce").dt.tz_localize(None)
    for c in ["home_rest_days","away_rest_days"]: upc[c]=4
    for c in ["home_travel_km","away_travel_km"]: upc[c]=200
    for c in ["home_injury_index","away_injury_index"]: upc[c]=0.3
    for c in ["home_gk_rating","away_gk_rating","home_setpiece_rating","away_setpiece_rating"]: upc[c]=0.6
    upc["ref_pen_rate"]=0.30; upc["crowd_index"]=0.7
    upc.to_csv(OUT_UPCOMING,index=False)
    print("Saved",OUT_UPCOMING,len(upc))

if __name__=="__main__": main()
