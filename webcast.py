import asyncio
import re
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin
import aiohttp
from bs4 import BeautifulSoup
from playwright.async_api import Page, async_playwright
from datetime import datetime, date
from zoneinfo import ZoneInfo
import platform

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
)
POST_LOAD_WAIT_MS = 8000
STREAM_PATTERN = re.compile(r"\.m3u8($|\?)", re.IGNORECASE)

# 🔄 Write playlist as SportsWebcastPT.m3u8
OUTPUT_FILE = "SportsWebcastPT.m3u8"

# ⏱️ Convert all displayed times to Pacific Time
LOCAL_TZ = ZoneInfo("America/Los_Angeles")

# ---- BASE URLS ----
NFL_BASE_URL = "https://nflwebcast.top/"
NHL_BASE_URL = "https://nhlstreams.org/nhlstreams-2/"
MLB_BASE_URL = "https://mlbstreams.live/"
MLS_BASE_URL = "https://mlswebcast.com/"
NBA_BASE_URL = "https://nbawebcast.top/"

# Optional 24/7 channels
NFL_CHANNEL_URLS = []
MLB_CHANNEL_URLS = []
NHL_CHANNEL_URLS = []
MLS_CHANNEL_URLS = []

CHANNEL_METADATA = {
    "nflnetwork": {"name": "NFL Network", "id": "NFL.Network.HD.us2", "logo": "https://github.com/tv-logo/tv-logos/blob/main/countries/united-states/nfl-network-hz-us.png?raw=true"},
    "nflredzone": {"name": "NFL RedZone", "id": "NFL.RedZone.HD.us2", "logo": "https://github.com/tv-logo/tv-logos/blob/main/countries/united-states/nfl-red-zone-hz-us.png?raw=true"},
    "espnusa": {"name": "ESPN", "id": "ESPN.HD.us2", "logo": "https://github.com/tv-logo/tv-logos/blob/main/countries/united-states/espn-us.png?raw=true"},
}

# PPV pattern/headers
PPV_STREAM_URL_PATTERN = "https://gg.poocloud.in/{team_name}/tracks-v1a1/mono.ts.m3u8"
PPV_CUSTOM_HEADERS = {
    "origin": "https://ppv.to",
    "referrer": "https://ppv.to/",
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0",
}

# ---------- parsing helpers ----------
MONTH_RE = r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?"
TIME_RE = r"(\d{1,2}):(\d{2})\s*(AM|PM)"
TZ_ABBR_NEAR = r"\b([ECMP][SD]?T|ET|CT|MT|PT)\b"

SITE_DEFAULT_TZ = {
    NFL_BASE_URL: ZoneInfo("America/New_York"),
    NHL_BASE_URL: ZoneInfo("America/New_York"),
    MLB_BASE_URL: ZoneInfo("America/New_York"),
    MLS_BASE_URL: ZoneInfo("America/New_York"),
    NBA_BASE_URL: ZoneInfo("America/New_York"),
}
TZ_ABBR_MAP = {
    "ET": "America/New_York", "EST": "America/New_York", "EDT": "America/New_York",
    "CT": "America/Chicago",  "CST": "America/Chicago",  "CDT": "America/Chicago",
    "MT": "America/Denver",   "MST": "America/Denver",   "MDT": "America/Denver",
    "PT": "America/Los_Angeles","PST": "America/Los_Angeles","PDT": "America/Los_Angeles",
}

def normalize_game_name(original_name: str) -> str:
    cleaned = " ".join(original_name.splitlines()).strip()
    if "@" in cleaned:
        a, b = cleaned.split("@", 1)
        a = a.strip().title()
        b = re.sub(rf"(\b{MONTH_RE}\b.*)$", "", b.strip(), flags=re.IGNORECASE).strip().title()
        return f"{a} @ {b}"
    return " ".join(cleaned.split()).title()

def _strftime_no_pad(fmt_linux: str, fmt_windows: str, dt: datetime) -> str:
    return dt.strftime(fmt_windows if platform.system() == "Windows" else fmt_linux)

def format_game_datetime(game_name: str, dt_local: Optional[datetime]) -> str:
    if not dt_local:
        return game_name
    date_str = _strftime_no_pad("%b %-d", "%b %#d", dt_local)
    time_str = _strftime_no_pad("%-I:%M %p", "%#I:%M %p", dt_local)
    # Use local tz abbreviation (PST/PDT) automatically
    tz_abbr = dt_local.tzname() or LOCAL_TZ.tzname(dt_local)
    return f"{game_name} • {date_str} at {time_str} {tz_abbr}"

def _month_str_to_num(mon: str) -> int:
    mon = mon.lower().strip(".")
    mapping = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"sept":9,"oct":10,"nov":11,"dec":12}
    return mapping.get(mon, 1)

def _guess_year(month: int, day: int, today: date) -> int:
    cand = date(today.year, month, day)
    delta = (cand - today).days
    if delta < -180: return today.year + 1
    if delta > 180:  return today.year - 1
    return today.year

def parse_date_title(text: str, today: date) -> Optional[date]:
    text = " ".join(text.split())
    m = re.search(rf"\b({MONTH_RE})\s+(\d{{1,2}})(?:,\s*(\d{{4}}))?", text, flags=re.IGNORECASE)
    if not m:
        return None
    mon_str = m.group(1)
    day_str = m.group(2)
    year_str = m.group(3) if m.lastindex and m.lastindex >= 3 else None

    mon = _month_str_to_num(mon_str)
    try:
        day = int(day_str)
    except:
        return None

    year = int(year_str) if year_str else _guess_year(mon, day, today)
    try:
        return date(year, mon, day)
    except:
        return None

def parse_time_and_tz(text: str, default_tz: ZoneInfo) -> Tuple[Optional[int], Optional[int], ZoneInfo]:
    m = re.search(TIME_RE, text, flags=re.IGNORECASE)
    if not m:
        return None, None, default_tz
    hh, mm, ap = int(m.group(1)), int(m.group(2)), m.group(3).upper()
    if ap == "PM" and hh != 12: hh += 12
    if ap == "AM" and hh == 12: hh = 0
    start = m.start()
    window = text[max(0, start-16): start+32]
    tz_m = re.search(TZ_ABBR_NEAR, window, flags=re.IGNORECASE)
    tzinfo = default_tz
    if tz_m:
        abbr = tz_m.group(1).upper()
        try:
            tzinfo = ZoneInfo(TZ_ABBR_MAP[abbr])
        except:
            tzinfo = default_tz
    return hh, mm, tzinfo

async def verify_stream_url(session: aiohttp.ClientSession, url: str) -> bool:
    try:
        async with session.head(url, timeout=10, allow_redirects=True) as r:
            return r.status == 200
    except:
        return False

async def find_stream_in_page(page: Page, url: str, session: aiohttp.ClientSession) -> Optional[str]:
    candidates: List[str] = []
    def on_req(req):
        if STREAM_PATTERN.search(req.url) and req.url not in candidates:
            candidates.append(req.url)
    page.on("request", on_req)
    try:
        await page.goto(url, wait_until="load", timeout=30000)
        await page.wait_for_timeout(POST_LOAD_WAIT_MS)
    finally:
        page.remove_listener("request", on_req)

    for s in reversed(candidates):
        if await verify_stream_url(session, s):
            return s
    return None

async def _row_nearest_date_text(row) -> str:
    return await row.evaluate(
        """(el) => {
            let p = el.previousElementSibling;
            while (p) {
              const cls = (p.getAttribute('class')||'').toLowerCase();
              if (cls.includes('mdatetitle')) return (p.innerText||'').trim();
              p = p.previousElementSibling;
            }
            return '';
        }"""
    )

async def _row_time_text(row) -> str:
    sel = [
        "td.timestatus span", "td.timestatus", "td.timeanddate",
        "td.time", "td.timing", "td.status", "td.date", "td.match_time"
    ]
    for s in sel:
        el = row.locator(s)
        if await el.count():
            try:
                t = await el.inner_text()
                if t and re.search(TIME_RE, t, flags=re.IGNORECASE):
                    return t
            except:
                pass
    try:
        return await row.inner_text()
    except:
        return ""

# --- TEAM SLUG MAPS (NFL/NHL) ---
def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())

NFL_TEAM_SLUGS = {
    "arizona cardinals": "arizonacardinals",
    "atlanta falcons": "atlantafalcons",
    "baltimore ravens": "baltimoreravens",
    "buffalo bills": "buffalobills",
    "carolina panthers": "carolinapanthers",
    "chicago bears": "chicagobears",
    "cincinnati bengals": "cincinnatibengals",
    "cleveland browns": "clevelandbrowns",
    "dallas cowboys": "dallascowboys",
    "denver broncos": "denverbroncos",
    "detroit lions": "detroitlions",
    "green bay packers": "greenbaypackers",
    "houston texans": "houstontexans",
    "indianapolis colts": "indianapoliscolts",
    "jacksonville jaguars": "jacksonvillejaguars",
    "kansas city chiefs": "kansascitychiefs",
    "las vegas raiders": "lasvegasraiders",
    "los angeles chargers": "losangeleschargers",
    "los angeles rams": "losangelesrams",
    "miami dolphins": "miamidolphins",
    "minnesota vikings": "minnesotavikings",
    "new england patriots": "newenglandpatriots",
    "new orleans saints": "neworleanssaints",
    "new york giants": "newyorkgiants",
    "new york jets": "newyorkjets",
    "philadelphia eagles": "philadelphiaeagles",
    "pittsburgh steelers": "pittsburghsteelers",
    "san francisco 49ers": "sanfrancisco49ers",
    "seattle seahawks": "seattleseahawks",
    "tampa bay buccaneers": "tampabaybuccaneers",
    "tennessee titans": "tennesseetitans",
    "washington commanders": "washingtoncommanders",
}
NFL_NICK_SLUGS = {
    "cardinals": "arizonacardinals", "falcons": "atlantafalcons",
    "ravens": "baltimoreravens", "bills": "buffalobills",
    "panthers": "carolinapanthers", "bears": "chicagobears",
    "bengals": "cincinnatibengals", "browns": "clevelandbrowns",
    "cowboys": "dallascowboys", "broncos": "denverbroncos",
    "lions": "detroitlions", "packers": "greenbaypackers",
    "texans": "houstontexans", "colts": "indianapoliscolts",
    "jaguars": "jacksonvillejaguars", "chiefs": "kansascitychiefs",
    "raiders": "lasvegasraiders", "chargers": "losangeleschargers",
    "rams": "losangelesrams", "dolphins": "miamidolphins",
    "vikings": "minnesotavikings", "patriots": "newenglandpatriots",
    "saints": "neworleanssaints", "giants": "newyorkgiants",
    "jets": "newyorkjets", "eagles": "philadelphiaeagles",
    "steelers": "pittsburghsteelers", "49ers": "sanfrancisco49ers",
    "niners": "sanfrancisco49ers", "seahawks": "seattleseahawks",
    "buccaneers": "tampabaybuccaneers", "bucs": "tampabaybuccaneers",
    "titans": "tennesseetitans", "commanders": "washingtoncommanders",
}

NHL_TEAM_SLUGS = {
    "anaheim ducks": "anaheimducks", "arizona coyotes": "arizonacoyotes",
    "boston bruins": "bostonbruins", "buffalo sabres": "buffalosabres",
    "calgary flames": "calgaryflames", "carolina hurricanes": "carolinahurricanes",
    "chicago blackhawks": "chicagoblackhawks", "colorado avalanche": "coloradoavalanche",
    "columbus blue jackets": "columbusbluejackets", "dallas stars": "dallasstars",
    "detroit red wings": "detroitredwings", "edmonton oilers": "edmontonoilers",
    "florida panthers": "floridapanthers", "los angeles kings": "losangeleskings",
    "minnesota wild": "minnesotawild", "montreal canadiens": "montrealcanadiens",
    "nashville predators": "nashvillepredators", "new jersey devils": "newjerseydevils",
    "new york islanders": "newyorkislanders", "new york rangers": "newyorkrangers",
    "ottawa senators": "ottawasenators", "philadelphia flyers": "philadelphiaflyers",
    "pittsburgh penguins": "pittsburghpenguins", "san jose sharks": "sanjosesharks",
    "seattle kraken": "seattlekraken", "st. louis blues": "stlouisblues",
    "tampa bay lightning": "tampabaylightning", "toronto maple leafs": "torontomapleleafs",
    "vancouver canucks": "vancouvercanucks", "vegas golden knights": "vegasgoldenknights",
    "washington capitals": "washingtoncapitals", "winnipeg jets": "winnipegjets",
}
NHL_NICK_SLUGS = {
    "ducks": "anaheimducks", "coyotes": "arizonacoyotes",
    "bruins": "bostonbruins", "sabres": "buffalosabres",
    "flames": "calgaryflames", "hurricanes": "carolinahurricanes",
    "blackhawks": "chicagoblackhawks", "avalanche": "coloradoavalanche",
    "bluejackets": "columbusbluejackets", "jackets": "columbusbluejackets",
    "stars": "dallasstars", "redwings": "detroitredwings", "wings": "detroitredwings",
    "oilers": "edmontonoilers", "panthers": "floridapanthers",
    "kings": "losangeleskings", "wild": "minnesotawild",
    "canadiens": "montrealcanadiens", "predators": "nashvillepredators",
    "devils": "newjerseydevils", "islanders": "newyorkislanders",
    "rangers": "newyorkrangers", "senators": "ottawasenators",
    "flyers": "philadelphiaflyers", "penguins": "pittsburghpenguins",
    "sharks": "sanjosesharks", "kraken": "seattlekraken",
    "blues": "stlouisblues", "lightning": "tampabaylightning",
    "mapleleafs": "torontomapleleafs", "leafs": "torontomapleleafs",
    "canucks": "vancouvercanucks", "goldenknights": "vegasgoldenknights", "knights": "vegasgoldenknights",
    "capitals": "washingtoncapitals", "caps": "washingtoncapitals",
    "jets": "winnipegjets",
}

FULL_TEAM_SLUGS_NFL: Set[str] = set(NFL_TEAM_SLUGS.values())
FULL_TEAM_SLUGS_NHL: Set[str] = set(NHL_TEAM_SLUGS.values())

# ------- NBA nickname-only + new nba_{code} support -------
NBA_NICK_TO_CODE: Dict[str, str] = {
    "hawks": "atlantahawks",
    "celtics": "bostonceltics",
    "nets": "brooklynnets",
    "hornets": "charlottehornets",
    "bulls": "chicagobulls",
    "cavaliers": "clevelandcavaliers",
    "mavericks": "dallasmavericks",
    "nuggets": "denvernuggets",
    "pistons": "detroitpistons",
    "warriors": "goldenstatewarriors",
    "rockets": "houstonrockets",
    "pacers": "indianapacers",
    "clippers": "losangelesclippers",
    "lakers": "losangeleslakers",
    "grizzlies": "memphisgrizzlies",
    "heat": "miamiheat",
    "bucks": "milwaukeebucks",
    "timberwolves": "minnesotatimberwolves",
    "pelicans": "neworleanspelicans",
    "knicks": "newyorkknicks",
    "thunder": "oklahomacitythunder",
    "magic": "orlandomagic",
    "sixers": "philadelphiasixers",
    "suns": "phoenixsuns",
    "trailblazers": "portlandtrailblazers",
    "kings": "sacramentokings",
    "spurs": "sanantoniospurs",
    "raptors": "torontoraptors",
    "jazz": "utahjazz",
    "wizards": "washingtonwizards",
}

TWO_WORD_NICKNAMES = {
    "red sox": "redsox",
    "white sox": "whitesox",
    "blue jays": "bluejays",
    "trail blazers": "trailblazers",
}
ALT_NICK_MAP = {
    "76ers": "sixers",
    "seventy sixers": "sixers",
}

def _nickname_only_candidates(team_text: str) -> List[str]:
    txt = (team_text or "").strip().lower()
    if not txt:
        return []
    parts = re.sub(r"\s+", " ", txt).split()

    last_two_phrase = " ".join(parts[-2:]) if len(parts) >= 2 else ""
    last_two = TWO_WORD_NICKNAMES.get(last_two_phrase, "")
    last = parts[-1] if parts else ""

    cands: List[str] = []
    if last_two:
        cands.append(_slug(last_two))
    if last:
        cands.append(_slug(last))

    if any(k in txt for k in ALT_NICK_MAP.keys()):
        for key, val in ALT_NICK_MAP.items():
            if key in txt:
                cands.append(_slug(val))

    out, seen = [], set()
    for c in cands:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out

def _expand_nba_candidates(nick_cands: List[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for n in nick_cands:
        code = NBA_NICK_TO_CODE.get(n)
        if code:
            new_slug = f"nba_{code}"
            if new_slug not in seen:
                out.append(new_slug); seen.add(new_slug)
        if n not in seen:
            out.append(n); seen.add(n)
    return out

def _home_match_candidates(home_text: str, nickname_only: bool) -> Set[str]:
    def general_candidates(txt: str) -> List[str]:
        txt = (txt or "").strip().lower()
        base_norm = re.sub(r"\s+", " ", txt)
        out: List[str] = []

        for m in (NFL_TEAM_SLUGS.get(base_norm), NHL_TEAM_SLUGS.get(base_norm)):
            if m: out.append(m)

        parts = base_norm.split()
        last = _slug(parts[-1]) if parts else ""
        two_last = _slug(" ".join(parts[-2:])) if len(parts) >= 2 else ""
        for m in (NFL_NICK_SLUGS.get(two_last), NFL_NICK_SLUGS.get(last),
                  NHL_NICK_SLUGS.get(two_last), NHL_NICK_SLUGS.get(last)):
            if m and m not in out: out.append(m)

        joined = _slug(base_norm)
        if joined and joined not in out:
            out.append(joined)
        return out

    if nickname_only:
        nick_cands = _nickname_only_candidates(home_text)
        nba_expanded = _expand_nba_candidates(nick_cands)
        return set(nba_expanded)
    else:
        return set(general_candidates(home_text))

async def resolve_team_slug(
    session: aiohttp.ClientSession,
    data_team: Optional[str],
    away_team_text: str,
    home_team_text: str,
    allowed_slugs: Optional[Set[str]] = None,
    nickname_only: bool = False,
) -> Optional[str]:
    def general_candidates(txt: str) -> List[str]:
        txt = (txt or "").strip().lower()
        base_norm = re.sub(r"\s+", " ", txt)
        out: List[str] = []

        for m in (NFL_TEAM_SLUGS.get(base_norm), NHL_TEAM_SLUGS.get(base_norm)):
            if m: out.append(m)

        parts = base_norm.split()
        last = _slug(parts[-1]) if parts else ""
        two_last = _slug(" ".join(parts[-2:])) if len(parts) >= 2 else ""
        for m in (NFL_NICK_SLUGS.get(two_last), NFL_NICK_SLUGS.get(last),
                  NHL_NICK_SLUGS.get(two_last), NHL_NICK_SLUGS.get(last)):
            if m and m not in out: out.append(m)

        joined = _slug(base_norm)
        if joined and joined not in out:
            out.append(joined)
        return out

    def build_candidates(txt: str) -> List[str]:
        if nickname_only:
            nick_cands = _nickname_only_candidates(txt)
            return _expand_nba_candidates(nick_cands)
        else:
            return general_candidates(txt)

    async def first_verified(cands: List[str]) -> Optional[str]:
        for slug in cands:
            if allowed_slugs is not None and slug not in allowed_slugs:
                continue
            test_url = PPV_STREAM_URL_PATTERN.format(team_name=slug)
            if await verify_stream_url(session, test_url):
                return slug
        return None

    home_first = await first_verified(build_candidates(home_team_text))
    if home_first:
        return home_first

    away_ok = await first_verified(build_candidates(away_team_text))
    if away_ok:
        return away_ok

    if data_team:
        dt_slug = _slug(data_team)
        dt_cands: List[str] = []
        if nickname_only:
            if not dt_slug.startswith("nba_"):
                dt_cands = _expand_nba_candidates([dt_slug])
            else:
                dt_cands = [dt_slug]
        else:
            dt_cands = [dt_slug]

        dt_ok = await first_verified(dt_cands)
        if dt_ok:
            return dt_ok

    return None

async def scrape_league(base_url: str, channel_urls: List[str], group_prefix: str, default_id: str, default_logo: str) -> List[Dict]:
    found: Dict[str, Tuple[str, str, Optional[datetime]]] = {}
    results: List[Dict] = []
    site_tz = SITE_DEFAULT_TZ.get(base_url, ZoneInfo("America/New_York"))
    today_local = datetime.now(tz=LOCAL_TZ).date()

    async with async_playwright() as p, aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT)
        try:
            page = await context.new_page()
            await page.goto(base_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(POST_LOAD_WAIT_MS)

            all_rows = page.locator("#mtable tr")
            total = await all_rows.count()
            for i in range(total):
                row = all_rows.nth(i)

                link = row.locator("td.teamvs a")
                if not await link.count():
                    continue

                name = (await link.inner_text()) or ""
                href = await link.get_attribute("href")
                if not name or not href:
                    continue

                date_text = await _row_nearest_date_text(row)
                game_date = parse_date_title(date_text, today_local)

                time_text = await _row_time_text(row)
                hh, mm, tzinfo = parse_time_and_tz(time_text, site_tz)

                dt_local = None
                if game_date and hh is not None and mm is not None:
                    try:
                        site_dt = datetime(game_date.year, game_date.month, game_date.day, hh, mm, tzinfo=tzinfo)
                        dt_local = site_dt.astimezone(LOCAL_TZ)
                    except:
                        dt_local = None

                full_url = urljoin(base_url, href)
                stream_url = await find_stream_in_page(await context.new_page(), full_url, session)
                if stream_url:
                    found[name] = (stream_url, "Live Games", dt_local)

            for url in channel_urls:
                slug = url.strip("/").split("/")[-1]
                stream_url = await find_stream_in_page(await context.new_page(), url, session)
                if stream_url:
                    found[slug] = (stream_url, "24/7 Channels", None)
        finally:
            await browser.close()

    for key, (stream_url, category, dt_local) in sorted(found.items()):
        info = CHANNEL_METADATA.get(key, {})
        base_name = info.get("name") or key
        display = key if "@" in key else base_name
        pretty = normalize_game_name(display)
        pretty = format_game_datetime(pretty, dt_local)
        results.append({
            "name": pretty,
            "url": stream_url,
            "tvg_id": info.get("id", default_id),
            "tvg_logo": info.get("logo", default_logo),
            "group": f"{group_prefix} - {category}",
            "ref": base_url
        })
    return results

def _extract_backup_slug_from_el(el) -> Optional[str]:
    if not el:
        return None
    href = el.get("href") or ""
    m = re.search(r"poocloud\.in/([^/]+)/tracks-[^/]+/mono\.ts\.m3u8", href, flags=re.I)
    if not m:
        m = re.search(r"poocloud\.in/([^/]+)/tracks-v1a1/mono\.ts\.m3u8", href, flags=re.I)
    if m:
        return _slug(m.group(1))
    bt = el.get("data-team")
    return _slug(bt) if bt else None

async def scrape_ppv_league(
    base_url: str,
    group_name: str,
    tvg_id: str,
    default_logo: str,
    allowed_slugs: Optional[Set[str]] = None,
    nickname_only: bool = False,
) -> List[Dict]:
    results: List[Dict] = []
    site_tz = SITE_DEFAULT_TZ.get(base_url, ZoneInfo("America/New_York"))
    today_local = datetime.now(tz=LOCAL_TZ).date()

    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}) as session:
        try:
            async with session.get(base_url, timeout=20) as resp:
                html = await resp.text()
        except:
            return []

        soup = BeautifulSoup(html, "lxml")
        table = None
        for t in soup.find_all("table"):
            cls = " ".join(t.get("class", [])).lower()
            if "schedule_container" in cls:
                table = t
                break
        if not table:
            return []

        current_date: Optional[date] = None

        def get_text(el) -> str:
            return el.get_text(" ", strip=True) if el else ""

        for tr in soup.select("tr"):
            classes = " ".join(tr.get("class", [])).lower()

            if "date" in classes or "title" in classes or "header" in classes:
                d = parse_date_title(get_text(tr), today_local)
                if d:
                    current_date = d
                continue

            tds = tr.find_all("td")
            if not tds:
                continue

            team_cells = [td for td in tds if "teamvs" in " ".join(td.get("class", [])).lower()]
            if len(team_cells) >= 2:
                away_team = get_text(team_cells[0])
                home_team = get_text(team_cells[1])
            else:
                texts = [get_text(td) for td in tds if get_text(td)]
                if len(texts) < 2:
                    continue
                away_team, home_team = texts[0], texts[1]

            logo_cells = [td for td in tds if "teamlogo" in " ".join(td.get("class", [])).lower()]
            if logo_cells and logo_cells[-1].find("img"):
                logo_url = logo_cells[-1].find("img").get("src") or default_logo
            else:
                logo_url = default_logo

            watch_btn = tr.find("button", class_="watch_btn")
            team_key = watch_btn.get("data-team") if watch_btn else None

            backup_el = (
                tr.find("a", class_=re.compile(r"backup", re.I)) or
                tr.find("button", class_=re.compile(r"backup", re.I)) or
                tr.find("a", string=re.compile(r"backup", re.I)) or
                tr.find("button", string=re.compile(r"backup", re.I))
            )
            backup_slug = _extract_backup_slug_from_el(backup_el)

            resolved_slug: Optional[str] = None

            if backup_slug:
                test_url = PPV_STREAM_URL_PATTERN.format(team_name=backup_slug)
                if await verify_stream_url(session, test_url):
                    home_allowlist = _home_match_candidates(home_team, nickname_only)
                    if backup_slug in home_allowlist:
                        resolved_slug = backup_slug

            if not resolved_slug:
                resolved_slug = await resolve_team_slug(
                    session=session,
                    data_team=team_key,
                    away_team_text=away_team,
                    home_team_text=home_team,
                    allowed_slugs=allowed_slugs,
                    nickname_only=nickname_only,
                )
            if not resolved_slug:
                continue

            stream_url = PPV_STREAM_URL_PATTERN.format(team_name=resolved_slug)

            time_cell = None
            for sel in ["td.timestatus span", "td.timestatus", "td.timeanddate", "td.time", "td.timing", "td.status", "td.match_time"]:
                time_cell = tr.select_one(sel)
                if time_cell and re.search(TIME_RE, get_text(time_cell), flags=re.IGNORECASE):
                    break
                time_cell = None

            row_text = get_text(time_cell) if time_cell else get_text(tr)
            hh, mm, tzinfo = parse_time_and_tz(row_text, site_tz)

            inline_date = parse_date_title(row_text, today_local)
            game_date = inline_date or current_date
            dt_local = None
            if game_date and hh is not None and mm is not None:
                try:
                    site_dt = datetime(game_date.year, game_date.month, game_date.day, hh, mm, tzinfo=tzinfo)
                    dt_local = site_dt.astimezone(LOCAL_TZ)
                except:
                    dt_local = None

            match_name = format_game_datetime(normalize_game_name(f"{away_team} @ {home_team}"), dt_local)

            if await verify_stream_url(session, stream_url):
                results.append({
                    "name": match_name,
                    "url": stream_url,
                    "tvg_id": tvg_id,
                    "tvg_logo": logo_url,
                    "group": f"{group_name} - Live Games",
                    "ref": base_url,
                    "custom_headers": PPV_CUSTOM_HEADERS,
                })
    return results

# ---- League-specific wrappers ----
async def scrape_nba_league(default_logo: str) -> List[Dict]:
    return await scrape_ppv_league(
        base_url=NBA_BASE_URL,
        group_name="NBAWebcast",
        tvg_id="NBA.Basketball.Dummy.us",
        default_logo=default_logo,
        allowed_slugs=None,
        nickname_only=True,
    )

async def scrape_nfl_league(default_logo: str) -> List[Dict]:
    return await scrape_ppv_league(
        base_url=NFL_BASE_URL,
        group_name="NFLWebcast",
        tvg_id="NFL.Football.Dummy.us",
        default_logo=default_logo,
        allowed_slugs=FULL_TEAM_SLUGS_NFL,
        nickname_only=False,
    )

async def scrape_nhl_league(default_logo: str) -> List[Dict]:
    return await scrape_ppv_league(
        base_url=NHL_BASE_URL,
        group_name="NHLWebcast",
        tvg_id="NHL.Hockey.Dummy.us",
        default_logo=default_logo,
        allowed_slugs=FULL_TEAM_SLUGS_NHL,
        nickname_only=False,
    )

async def scrape_mlb_league(default_logo: str) -> List[Dict]:
    return await scrape_ppv_league(
        base_url=MLB_BASE_URL,
        group_name="MLBWebcast",
        tvg_id="MLB.Baseball.Dummy.us",
        default_logo=default_logo,
        allowed_slugs=None,
        nickname_only=True,
    )

def write_playlist(streams: List[Dict], filename: str):
    if not streams:
        return
    with open(filename, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for e in streams:
            f.write(f'#EXTINF:-1 tvg-id="{e["tvg_id"]}" tvg-name="{e["name"]}" tvg-logo="{e["tvg_logo"]}" group-title="{e["group"]}",{e["name"]}\n')
            if "custom_headers" in e:
                h = e["custom_headers"]
                f.write(f'#EXTVLCOPT:http-origin={h["origin"]}\n')
                f.write(f'#EXTVLCOPT:http-referrer={h["referrer"]}\n')
                f.write(f'#EXTVLCOPT:http-user-agent={h["user_agent"]}\n')
            else:
                f.write(f'#EXTVLCOPT:http-origin={e["ref"]}\n')
                f.write(f'#EXTVLCOPT:http-referrer={e["ref"]}\n')
                f.write(f"#EXTVLCOPT:http-user-agent={USER_AGENT}\n")
            f.write(e["url"] + "\n")

async def main():
    NBA_DEFAULT_LOGO = "http://drewlive24.duckdns.org:9000/Logos/Basketball.png"
    NFL_DEFAULT_LOGO = "http://drewlive24.duckdns.org:9000/Logos/Football.png"
    NHL_DEFAULT_LOGO = "http://drewlive24.duckdns.org:9000/Logos/Hockey.png"
    MLB_DEFAULT_LOGO = "http://drewlive24.duckdns.org:9000/Logos/MLB.png"

    tasks = [
        scrape_nfl_league(NFL_DEFAULT_LOGO),
        scrape_league(NFL_BASE_URL, NFL_CHANNEL_URLS, "NFLWebcast", "NFL.Dummy.us", "http://drewlive24.duckdns.org:9000/Logos/Maxx.png"),
        scrape_nhl_league(NHL_DEFAULT_LOGO),
        scrape_mlb_league(MLB_DEFAULT_LOGO),
        scrape_league(MLS_BASE_URL, [], "MLSWebcast", "MLS.Soccer.Dummy.us", "http://drewlive24.duckdns.org:9000/Logos/Football2.png"),
        scrape_nba_league(NBA_DEFAULT_LOGO),
    ]
    results = await asyncio.gather(*tasks)
    all_streams = [s for league in results for s in league]
    write_playlist(all_streams, OUTPUT_FILE)

if __name__ == "__main__":
    asyncio.run(main())