import asyncio
import json
import os
import logging
import datetime
from typing import Dict, List, Any, Optional
import httpx

# সুন্দর ও পরিষ্কার লগিং ব্যবস্থা (Logging Configuration)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)

# পরিবেশের সিক্রেট (Environment Variables) থেকে API URL নেওয়া
# গিটহাব সিক্রেটস (GitHub Secrets) না থাকলে ডিফল্ট ফলব্যাক URL কাজ করবে
BASE_URL = os.getenv("API_BASE_URL", "https://api.sorrybrorewards.com/v2/extractor/api")

# সকল মুভি প্রোভাইডারের তালিকা (All Movie Providers List)
PROVIDERS = [
    "moviesmod",
    "topmovies",
    "uhd",
    "moviesdrive",
    "fourkhd",
    "hdhub4u",
    "filmyfly",
    "kat",
    "showbox",
    "castle",
    "allmovieland"
]

# স্ট্যান্ডার্ড রিকোয়েস্ট হেডার
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

# ডাটা সংরক্ষণের ফোল্ডার তৈরি
OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)


async def fetch_json(client: httpx.AsyncClient, endpoint: str, params: Dict[str, Any] = None) -> Any:
    """
    API থেকে নিরাপদে ডাটা ফেচ করার ফাংশন।
    ত্রুটি এড়াতে এতে ৩ বার রিট্রাই মেকানিজম যুক্ত আছে।
    """
    url = f"{BASE_URL}{endpoint}"
    for attempt in range(3):
        try:
            response = await client.get(url, params=params, headers=HEADERS, timeout=15.0)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
        except Exception as e:
            if attempt == 2:
                logging.warning(f"রিকোয়েস্ট ব্যর্থ হয়েছে: {endpoint} | ত্রুটি: {e}")
            await asyncio.sleep(1.5 * (attempt + 1))
    return None


def clean_url(url_str: Optional[str]) -> str:
    """ইউআরএল ফরম্যাট পরিষ্কার করার ফাংশন"""
    if not url_str:
        return ""
    return str(url_str).strip()


async def extract_item_details(client: httpx.AsyncClient, post: Dict[str, Any], provider: str) -> Dict[str, Any]:
    """
    একটি মুভি বা সিরিজের সমস্ত বিবরণ (শিরোনাম, রেজুলেশন, ইমেজ, এপিসোড, MKV এবং ডাউনলোড লিঙ্ক)
    একত্রিত করে এক্সট্র্যাক্ট করার প্রধান ফাংশন।
    """
    post_link = clean_url(post.get("link") or post.get("url") or post.get("id"))
    title = post.get("title") or post.get("name") or "অজানা শিরোনাম"

    # প্রাথমিক অবজেক্ট ডিক্লেয়ারেশন
    item_details = {
        "title": str(title).strip(),
        "provider": provider,
        "image": clean_url(post.get("image") or post.get("poster") or post.get("img")),
        "resolution": post.get("quality") or post.get("resolution") or "Standard HD",
        "category": post.get("category") or post.get("type") or "Movie",
        "post_url": post_link,
        "episodes": [],
        "download_links": [],
        "mkv_links": [],
        "last_updated": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    }

    if not post_link:
        return item_details

    # ৩টি সাব-এন্ডপয়েন্টে সমান্তরালভাবে (Parallel Async) রিকোয়েস্ট পাঠানো
    info_task = fetch_json(client, "/info", {"url": post_link})
    episodes_task = fetch_json(client, "/episodes", {"url": post_link})
    stream_task = fetch_json(client, "/stream", {"url": post_link})

    info_res, ep_res, stream_res = await asyncio.gather(info_task, episodes_task, stream_task)

    # ১. মেটাডাটা ও রেজুলেশন আপডেট
    if info_res and isinstance(info_res, dict):
        item_details["image"] = clean_url(info_res.get("image")) or item_details["image"]
        item_details["resolution"] = info_res.get("quality") or info_res.get("resolution") or item_details["resolution"]
        if "title" in info_res and info_res["title"]:
            item_details["title"] = str(info_res["title"]).strip()

    # ২. এপিসোড প্রসেসিং
    if ep_res:
        if isinstance(ep_res, list):
            item_details["episodes"] = ep_res
        elif isinstance(ep_res, dict) and "episodes" in ep_res:
            item_details["episodes"] = ep_res["episodes"]

    # ৩. স্ট্রিম ও MKV ডাউনলোড লিঙ্ক ফিল্টারিং
    if stream_res:
        links_list = stream_res if isinstance(stream_res, list) else stream_res.get("streams", [])
        for link_obj in links_list:
            if isinstance(link_obj, dict):
                url = clean_url(link_obj.get("url") or link_obj.get("link"))
                quality = link_obj.get("quality") or link_obj.get("name") or "Direct"
                
                if url:
                    link_entry = {"quality": quality, "url": url}
                    item_details["download_links"].append(link_entry)
                    
                    # MKV লিঙ্ক চিহ্নিত করা
                    if ".mkv" in url.lower():
                        item_details["mkv_links"].append(link_entry)
                        
            elif isinstance(link_obj, str):
                url = clean_url(link_obj)
                if url:
                    link_entry = {"quality": "Direct Download", "url": url}
                    item_details["download_links"].append(link_entry)
                    if ".mkv" in url.lower():
                        item_details["mkv_links"].append(link_entry)

    return item_details


async def scrape_provider(client: httpx.AsyncClient, provider: str, max_pages: int = 2) -> List[Dict[str, Any]]:
    """
    এক একক প্রোভাইডারের ডাটা স্ক্রাপ করে পৃথক JSON ফাইলে সেভ করার ফাংশন।
    """
    logging.info(f"▶ স্ক্রাপিং শুরু: [{provider.upper()}]")
    provider_items = []

    for page in range(1, max_pages + 1):
        posts_data = await fetch_json(client, "/posts", {"provider": provider, "filter": "", "page": page})
        
        if not posts_data:
            break

        posts_list = []
        if isinstance(posts_data, list):
            posts_list = posts_data
        elif isinstance(posts_data, dict):
            posts_list = posts_data.get("posts") or posts_data.get("data") or []

        if not posts_list:
            break

        # কনকারেন্টলি সমস্ত মুভির বিস্তারিত বের করা
        tasks = [extract_item_details(client, post, provider) for post in posts_list]
        page_results = await asyncio.gather(*tasks)
        provider_items.extend(page_results)
        
        await asyncio.sleep(0.5)

    # প্রতিটি প্রোভাইডারের জন্য আলাদা JSON ফাইলে ডাটা রাইট করা
    filepath = os.path.join(OUTPUT_DIR, f"{provider}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(provider_items, f, ensure_ascii=False, indent=2)

    logging.info(f"✅ সেভ সফল: [{provider.upper()}] -> মোট {len(provider_items)} টি আইটেম ({filepath})")
    return provider_items


async def main():
    """মূল এক্সিকিউশন ও অল-ইন-ওয়ান সামারি ফাইল তৈরির ফাংশন"""
    start_time = datetime.datetime.now()
    limits = httpx.Limits(max_keepalive_connections=30, max_connections=60)
    
    summary_stat = {
        "last_updated": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "total_items": 0,
        "providers_stat": {}
    }

    async with httpx.AsyncClient(limits=limits, timeout=20.0) as client:
        for provider in PROVIDERS:
            try:
                data = await scrape_provider(client, provider, max_pages=2)
                summary_stat["total_items"] += len(data)
                summary_stat["providers_stat"][provider] = len(data)
            except Exception as e:
                logging.error(f"❌ স্ক্রাপিং ত্রুটি [{provider}]: {e}")

    # সামগ্রিক পরিসংখ্যানে summary.json ফাইল সেভ করা
    with open(os.path.join(OUTPUT_DIR, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary_stat, f, ensure_ascii=False, indent=2)

    duration = (datetime.datetime.now() - start_time).seconds
    logging.info(f"🎉 সকল ডাটা সংগৃহীত হয়েছে! মোট: {summary_stat['total_items']} টি আইটেম | সময়: {duration} সেকেন্ড")


if __name__ == "__main__":
    asyncio.run(main())
