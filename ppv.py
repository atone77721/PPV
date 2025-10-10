import asyncio
from playwright.async_api import async_playwright
import aiohttp
from datetime import datetime
from urllib.parse import urljoin

API_URL = "https://ppv.to/api/streams"

CUSTOM_HEADERS = [
    '#EXTVLCOPT:http-origin=https://ppv.to',
    '#EXTVLCOPT:http-referrer=https://ppv.to/',
    '#EXTVLCOPT:http-user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0'
]

ALLOWED_CATEGORIES = {
    "24/7 Streams", "Wrestling", "Football", "Basketball", "Baseball",
    "Combat Sports", "American Football", "Darts", "Motorsports", "Ice Hockey"
}

CATEGORY_LOGOS = {
    "24/7 Streams": "http://drewlive24.duckdns.org:9000/Logos/247.png",
    "Wrestling": "http://drewlive24.duckdns.org:9000/Logos/Wrestling.png",
    "Football": "http://drewlive24.duckdns.org:9000/Logos/Football.png",
    "Basketball": "http://drewlive24.duckdns.org:9000/Logos/Basketball.png",
    "Baseball": "http://drewlive24.duckdns.org:9000/Logos/Baseball.png",
    "American Football": "http://drewlive24.duckdns.org:9000/Logos/NFL3.png",
    "Combat Sports": "http://drewlive24.duckdns.org:9000/Logos/CombatSports2.png",
    "Darts": "http://drewlive24.duckdns.org:9000/Logos/Darts.png",
    "Motorsports": "http://drewlive24.duckdns.org:9000/Logos/Motorsports2.png",
    "Live Now": "http://drewlive24.duckdns.org:9000/Logos/DrewLiveSports.png",
    "Ice Hockey": "http://drewlive24.duckdns.org:9000/Logos/Hockey.png"
}

CATEGORY_TVG_IDS = {
    "24/7 Streams": "24.7.Dummy.us",
    "Wrestling": "PPV.EVENTS.Dummy.us",
    "Football": "Soccer.Dummy.us",
    "Basketball": "Basketball.Dummy.us",
    "Baseball": "MLB.Baseball.Dummy.us",
    "American Football": "NFL.Dummy.us",
    "Combat Sports": "PPV.EVENTS.Dummy.us",
    "Darts": "Darts.Dummy.us",
    "Motorsports": "Racing.Dummy.us",
    "Live Now": "24.7.Dummy.us",
    "Ice Hockey": "Hockey.Dummy.us"
}


async def fetch_m3u8_from_iframe(page, iframe_url):
    await page.goto(iframe_url, wait_until="load")
    await asyncio.sleep(1)  # Give the page time to load if needed
    m3u8_links = await page.eval_on_selector_all(
        "source, video, iframe",
        """elements => elements.map(el => el.src || el.getAttribute('src')).filter(Boolean)"""
    )
    return m3u8_links


async def scrape_streams():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Fetch main streams
        await page.goto(API_URL, wait_until="load")
        streams = await page.evaluate("() => window.streams || []")  # Adjust depending on API
        
        playlist_entries = []

        for i, stream in enumerate(streams, start=1):
            category = stream.get("category", "Unknown")
            if category not in ALLOWED_CATEGORIES:
                continue

            name = stream.get("name", "Unknown Stream")
            start_time = stream.get("start_time") or datetime.utcnow().isoformat()
            dt = datetime.fromisoformat(start_time) if isinstance(start_time, str) else datetime.utcnow()
            game_name = f"{name} ({dt.strftime('%Y-%m-%d %H:%M')})"
            
            iframe_url = stream.get("iframe") or stream.get("url")
            if not iframe_url:
                continue

            m3u8_links = await fetch_m3u8_from_iframe(page, iframe_url)

            # Always include all m3u8 links, even if not live
            for link in m3u8_links:
                entry = [
                    f'#EXTINF:-1 tvg-id="{CATEGORY_TVG_IDS.get(category, "")}" tvg-logo="{CATEGORY_LOGOS.get(category, "")}" group-title="{category}",{game_name}',
                    link
                ]
                playlist_entries.extend(entry)

        await browser.close()
        return playlist_entries


async def main():
    playlist_entries = await scrape_streams()
    with open("PPVLand.m3u8", "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for entry in playlist_entries:
            f.write(entry + "\n")
    print("✅ Playlist saved as PPVLand.m3u8")


if __name__ == "__main__":
    asyncio.run(main())
