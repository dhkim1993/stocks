import os
import re
import html
import time
import logging
import json
import hashlib
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Dict, List, Optional
from urllib.parse import quote

import pandas as pd
import requests
import schedule
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

try:
    from openai import OpenAI
except Exception:
    OpenAI = None


# =========================
# 1) 사용자 설정 (필수/선택)
# =========================
KEYWORDS = [
    "퓨리오사AI",
    "엑스페릭스",
    "레니게이드 NPU",
    "백준호 대표",
    "엑스페릭스 유상증자",
    "FuriosaAI",
    "Xperix",
]

# 최근 N시간 내 기사/공시만 수집
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))

# 스케줄 실행 시간 (매일, 24시간 형식 HH:MM)
RUN_TIME = os.getenv("RUN_TIME", "09:00")

# 네이버 뉴스 API (선택: 없으면 크롤링 fallback)
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "").strip()
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "").strip()

# OpenAI 요약 (선택)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

# 텔레그램 전송 (선택)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_REALTIME_ENABLED = os.getenv("TELEGRAM_REALTIME_ENABLED", "true").strip().lower() in ("1", "true", "yes")
TELEGRAM_SEND_DIGEST = os.getenv("TELEGRAM_SEND_DIGEST", "true").strip().lower() in ("1", "true", "yes")
TELEGRAM_MAX_ALERTS = int(os.getenv("TELEGRAM_MAX_ALERTS", "20"))

# 결과 저장 폴더
OUTPUT_DIR = os.getenv("OUTPUT_DIR", ".").strip()
STATE_FILE = os.getenv("STATE_FILE", "sent_items.json").strip()


KST = timezone(timedelta(hours=9))
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


def setup_logger() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def now_kst() -> datetime:
    return datetime.now(KST)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_naver_pubdate(pub_date: str) -> Optional[datetime]:
    # 예: "Thu, 21 Mar 2024 16:22:00 +0900"
    if not pub_date:
        return None
    try:
        dt = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %z")
        return dt.astimezone(KST)
    except Exception:
        return None


def parse_relative_or_date_korean(text: str) -> Optional[datetime]:
    if not text:
        return None
    t = text.strip()
    now = now_kst()
    try:
        m = re.search(r"(\d+)\s*분\s*전", t)
        if m:
            return now - timedelta(minutes=int(m.group(1)))
        m = re.search(r"(\d+)\s*시간\s*전", t)
        if m:
            return now - timedelta(hours=int(m.group(1)))
        m = re.search(r"(\d+)\s*일\s*전", t)
        if m:
            return now - timedelta(days=int(m.group(1)))

        # 예: 2026.03.26. 또는 2026-03-26
        t = t.replace(".", "-").replace("/", "-").strip(" -")
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M"):
            try:
                parsed = datetime.strptime(t, fmt)
                return parsed.replace(tzinfo=KST)
            except Exception:
                continue
    except Exception:
        return None
    return None


def is_recent(dt: Optional[datetime], hours: int = LOOKBACK_HOURS) -> bool:
    if dt is None:
        return False
    return dt >= (now_kst() - timedelta(hours=hours))


def safe_get(url: str, headers: Optional[Dict] = None, timeout: int = 10) -> Optional[requests.Response]:
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response
    except Exception as e:
        logging.warning("GET 실패: %s | %s", url, e)
        return None


def fetch_news_by_naver_api(keyword: str, display: int = 50) -> List[Dict]:
    results: List[Dict] = []
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        return results

    url = (
        "https://openapi.naver.com/v1/search/news.json"
        f"?query={quote(keyword)}&display={display}&sort=date"
    )
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("items", []):
            pub_dt = parse_naver_pubdate(item.get("pubDate", ""))
            if not is_recent(pub_dt):
                continue
            title = clean_text(item.get("title", ""))
            origin_link = item.get("originallink") or item.get("link", "")
            link = item.get("link", "") or origin_link
            results.append(
                {
                    "datetime": pub_dt,
                    "source": extract_domain_source(origin_link or link),
                    "title": title,
                    "link": origin_link or link,
                    "summary": "",
                    "keyword": keyword,
                    "category": "news",
                }
            )
    except Exception as e:
        logging.warning("네이버 API 실패 (%s): %s", keyword, e)
    return results


def extract_domain_source(url: str) -> str:
    if not url:
        return "unknown"
    try:
        m = re.search(r"https?://([^/]+)", url)
        if not m:
            return "unknown"
        domain = m.group(1).lower()
        domain = domain.replace("www.", "")
        return domain
    except Exception:
        return "unknown"


def fetch_news_by_naver_scrape(keyword: str, pages: int = 2) -> List[Dict]:
    results: List[Dict] = []
    headers = {"User-Agent": USER_AGENT}

    for page in range(1, pages + 1):
        start = 1 + (page - 1) * 10
        url = (
            "https://search.naver.com/search.naver"
            f"?where=news&sm=tab_jum&query={quote(keyword)}&start={start}"
        )
        resp = safe_get(url, headers=headers, timeout=15)
        if not resp:
            continue

        try:
            soup = BeautifulSoup(resp.text, "html.parser")
            # 구조 변경 대비: 다양한 셀렉터 순차 시도
            items = soup.select("div.news_area")
            if not items:
                items = soup.select("div.sds-comps-vertical-layout")

            for item in items:
                try:
                    a_tag = item.select_one("a.news_tit") or item.select_one("a[href]")
                    if not a_tag:
                        continue
                    title = clean_text(a_tag.get_text(" ", strip=True))
                    link = a_tag.get("href", "").strip()
                    if not title or not link:
                        continue

                    info_text = " ".join(
                        [x.get_text(" ", strip=True) for x in item.select("span.info, div.info_group span")]
                    )
                    pub_dt = parse_relative_or_date_korean(info_text)
                    if not is_recent(pub_dt):
                        continue

                    source = "unknown"
                    source_tag = item.select_one("a.info.press") or item.select_one("span.info.press")
                    if source_tag:
                        source = clean_text(source_tag.get_text(" ", strip=True))
                    if source == "unknown":
                        source = extract_domain_source(link)

                    results.append(
                        {
                            "datetime": pub_dt,
                            "source": source,
                            "title": title,
                            "link": link,
                            "summary": "",
                            "keyword": keyword,
                            "category": "news",
                        }
                    )
                except Exception:
                    continue
        except Exception as e:
            logging.warning("네이버 크롤링 파싱 실패 (%s): %s", keyword, e)
    return results


def try_fetch_krx_disclosures(keywords: List[str]) -> List[Dict]:
    """
    KRX KIND 공시 페이지를 참고하는 보조 수집.
    페이지 구조/차단에 따라 결과가 없을 수 있어 실패 시 빈 리스트 반환.
    """
    results: List[Dict] = []
    headers = {"User-Agent": USER_AGENT}
    candidates = [
        "https://kind.krx.co.kr/disclosuretoday/main.do?method=searchDisclosureTodayMain",
        "https://kind.krx.co.kr/common/disclsviewer.do?method=searchDisclosureMain",
    ]

    for url in candidates:
        resp = safe_get(url, headers=headers, timeout=15)
        if not resp:
            continue
        try:
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table tbody tr")
            if not rows:
                continue
            for row in rows:
                cols = [clean_text(td.get_text(" ", strip=True)) for td in row.select("td")]
                if len(cols) < 3:
                    continue

                row_text = " ".join(cols)
                if not any(k.lower() in row_text.lower() for k in keywords):
                    continue

                title = cols[2] if len(cols) > 2 else row_text
                corp = cols[1] if len(cols) > 1 else "KRX"

                link_tag = row.select_one("a[href]")
                link = ""
                if link_tag and link_tag.get("href"):
                    href = link_tag.get("href")
                    if href.startswith("http"):
                        link = href
                    else:
                        link = f"https://kind.krx.co.kr/{href.lstrip('/')}"

                date_text = cols[0] if cols else ""
                pub_dt = parse_relative_or_date_korean(date_text) or now_kst()
                if not is_recent(pub_dt):
                    continue

                results.append(
                    {
                        "datetime": pub_dt,
                        "source": "KRX",
                        "title": f"[공시] {corp} - {title}",
                        "link": link or url,
                        "summary": "",
                        "keyword": "KRX",
                        "category": "disclosure",
                    }
                )
        except Exception as e:
            logging.warning("KRX 파싱 실패: %s", e)
    return results


def fetch_article_body(url: str) -> str:
    headers = {"User-Agent": USER_AGENT}
    resp = safe_get(url, headers=headers, timeout=15)
    if not resp:
        return ""
    try:
        soup = BeautifulSoup(resp.text, "html.parser")

        selectors = [
            "#dic_area",  # 네이버 뉴스 본문
            "#newsct_article",
            "#articeBody",
            "article",
            "div.article_view",
        ]
        for sel in selectors:
            node = soup.select_one(sel)
            if node:
                text = clean_text(node.get_text(" ", strip=True))
                if len(text) > 80:
                    return text

        body = clean_text(soup.get_text(" ", strip=True))
        return body[:2000]
    except Exception:
        return ""


def simple_3line_summary(text: str) -> str:
    if not text:
        return ""
    # 문장 분리 후 앞 3문장
    sentences = re.split(r"(?<=[.!?다])\s+", text)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 10]
    return "\n".join(sentences[:3])


def openai_3line_summary(text: str) -> str:
    if not OPENAI_API_KEY or OpenAI is None:
        return simple_3line_summary(text)
    if not text:
        return ""
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        prompt = (
            "아래 기사 내용을 한국어로 핵심만 3줄로 요약해 주세요. "
            "과장 없이 사실 중심으로 작성하세요.\n\n"
            f"{text[:5000]}"
        )
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        logging.warning("OpenAI 요약 실패: %s", e)
        return simple_3line_summary(text)


def deduplicate_by_title(items: List[Dict], threshold: float = 0.82) -> List[Dict]:
    deduped: List[Dict] = []
    for item in sorted(items, key=lambda x: x.get("datetime") or now_kst(), reverse=True):
        title = item.get("title", "")
        is_dup = False
        for picked in deduped:
            score = SequenceMatcher(None, title, picked.get("title", "")).ratio()
            if score >= threshold:
                is_dup = True
                break
        if not is_dup:
            deduped.append(item)
    return deduped


def to_dataframe(items: List[Dict]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame(columns=["date", "source", "title", "link", "summary", "keyword", "category"])
    rows = []
    for x in items:
        dt = x.get("datetime")
        rows.append(
            {
                "date": dt.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S") if isinstance(dt, datetime) else "",
                "source": x.get("source", ""),
                "title": x.get("title", ""),
                "link": x.get("link", ""),
                "summary": x.get("summary", ""),
                "keyword": x.get("keyword", ""),
                "category": x.get("category", ""),
            }
        )
    df = pd.DataFrame(rows)
    return df.sort_values("date", ascending=False).reset_index(drop=True)


def send_telegram_message(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
    try:
        requests.post(url, data=payload, timeout=15).raise_for_status()
    except Exception as e:
        logging.warning("텔레그램 전송 실패: %s", e)


def _item_key(item: Dict) -> str:
    raw = f"{item.get('title', '')}|{item.get('link', '')}|{item.get('date', '')}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def load_sent_item_keys() -> set:
    if not os.path.exists(STATE_FILE):
        return set()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(data)
    except Exception as e:
        logging.warning("상태 파일 로드 실패: %s", e)
    return set()


def save_sent_item_keys(keys: set) -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(list(keys)), f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning("상태 파일 저장 실패: %s", e)


def send_realtime_telegram_alerts(df: pd.DataFrame) -> int:
    if df.empty or not TELEGRAM_REALTIME_ENABLED:
        return 0
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return 0

    sent_keys = load_sent_item_keys()
    new_rows = []
    for _, row in df.iterrows():
        key = _item_key(row.to_dict())
        if key not in sent_keys:
            new_rows.append((key, row))

    if not new_rows:
        return 0

    sent_count = 0
    for key, row in new_rows[:TELEGRAM_MAX_ALERTS]:
        msg = (
            "[실시간 뉴스/공시 알림]\n"
            f"- 일시: {row['date']}\n"
            f"- 매체: {row['source']}\n"
            f"- 제목: {row['title']}\n"
            f"- 링크: {row['link']}"
        )
        if row.get("summary"):
            msg += f"\n- 요약: {row['summary']}"
        send_telegram_message(msg)
        sent_keys.add(key)
        sent_count += 1
        time.sleep(0.15)

    save_sent_item_keys(sent_keys)
    return sent_count


def make_telegram_digest(df: pd.DataFrame, top_n: int = 10) -> str:
    if df.empty:
        return "오늘 수집된 퓨리오사AI/엑스페릭스 관련 최신 뉴스/공시가 없습니다."
    lines = ["[일일 뉴스/공시 요약]"]
    for _, row in df.head(top_n).iterrows():
        lines.append(f"- ({row['date']}) [{row['source']}] {row['title']}")
        lines.append(f"  {row['link']}")
    return "\n".join(lines)


def collect_all() -> pd.DataFrame:
    logging.info("수집 시작")
    all_items: List[Dict] = []

    use_api = bool(NAVER_CLIENT_ID and NAVER_CLIENT_SECRET)
    logging.info("네이버 API 사용 여부: %s", use_api)

    for kw in KEYWORDS:
        try:
            if use_api:
                items = fetch_news_by_naver_api(kw)
            else:
                items = fetch_news_by_naver_scrape(kw)
            all_items.extend(items)
            time.sleep(0.2)
        except Exception as e:
            logging.warning("키워드 처리 실패 (%s): %s", kw, e)

    # KRX 공시 참고 수집 (실패해도 전체 흐름 유지)
    try:
        krx_items = try_fetch_krx_disclosures(KEYWORDS)
        all_items.extend(krx_items)
    except Exception as e:
        logging.warning("KRX 수집 실패: %s", e)

    # 24시간 필터 보강
    all_items = [x for x in all_items if is_recent(x.get("datetime"))]
    logging.info("필터 후 건수: %d", len(all_items))

    # 제목 유사도 중복 제거
    all_items = deduplicate_by_title(all_items, threshold=0.82)
    logging.info("중복 제거 후 건수: %d", len(all_items))

    # 요약 생성 (본문 접근 가능할 때)
    for item in all_items:
        try:
            body = fetch_article_body(item.get("link", ""))
            item["summary"] = openai_3line_summary(body)
        except Exception:
            item["summary"] = ""

    df = to_dataframe(all_items)
    return df


def save_report(df: pd.DataFrame) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f"daily_news_report_{now_kst().strftime('%Y%m%d')}.csv"
    output_path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logging.info("저장 완료: %s", output_path)
    return output_path


def run_once(send_msg: bool = True) -> None:
    try:
        df = collect_all()
        output_path = save_report(df)
        logging.info("최종 건수: %d", len(df))

        if send_msg and TELEGRAM_REALTIME_ENABLED:
            cnt = send_realtime_telegram_alerts(df)
            if cnt:
                logging.info("텔레그램 실시간 알림 전송: %d건", cnt)

        if send_msg and TELEGRAM_SEND_DIGEST:
            digest = make_telegram_digest(df, top_n=10)
            send_telegram_message(digest + f"\n\nCSV: {output_path}")
    except Exception as e:
        logging.exception("실행 실패: %s", e)


def run_scheduler() -> None:
    logging.info("스케줄러 시작: 매일 %s 실행", RUN_TIME)
    schedule.every().day.at(RUN_TIME).do(run_once)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    setup_logger()

    # GitHub Actions/CI 환경에서는 1회 실행 후 종료
    if os.getenv("GITHUB_ACTIONS", "").strip().lower() == "true":
        run_once(send_msg=True)
    else:
        # 로컬 실행 시 즉시 1회 수행 후 스케줄러 유지
        run_once(send_msg=True)
        run_scheduler()
