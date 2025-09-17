import pandas as pd
import requests
from io import StringIO

def download_csv(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return pd.read_csv(StringIO(r.text))

def decimal_from_fractional(frac: str) -> float:
    # For safety if any site returns fractional like "11/10"
    if isinstance(frac, str) and "/" in frac:
        a,b = frac.split("/")
        return 1.0 + float(a)/float(b)
    try:
        return float(frac)
    except:
        return None
