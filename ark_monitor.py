"""
ARK 펀드 포트폴리오 모니터링 배치
- 1분 간격으로 ARK ETF 보유 종목 조회 (기본: arkfunds.io JSON API)
- 공식 ark-funds.com CSV는 URL/파일명 변경으로 404가 잦아 API를 우선 사용
- 이전 데이터와 변경사항 감지 시 텔레그램으로 포트폴리오 전송
"""

import os
import json
import time
import logging
import hashlib
import argparse
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
import schedule
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# ARK ETF 심볼 및 (선택) 공식 CSV URL — CSV는 펀드별 404 가능, 폴백용
# ---------------------------------------------------------------------------
ARK_SYMBOLS = ("ARKK", "ARKQ", "ARKW", "ARKG", "ARKF", "ARKX")

ARK_CSV_FALLBACK = {
    "ARKK": "https://www.ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv",
    "ARKQ": "https://www.ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_AUTONOMOUS_TECHNOLOGY_&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv",
    "ARKW": "https://www.ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv",
    "ARKG": "https://www.ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_GENOMIC_REVOLUTION_MULTISECTOR_ETF_ARKG_HOLDINGS.csv",
    "ARKF": "https://www.ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv",
    "ARKX": "https://www.ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_SPACE_EXPLORATION_%26_INNOVATION_ETF_ARKX_HOLDINGS.csv",
}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
STATE_FILE = os.getenv("ARK_STATE_FILE", "ark_state.json")

# api | csv | api_then_csv
ARK_DATA_SOURCE = os.getenv("ARK_DATA_SOURCE", "api_then_csv").strip().lower()
ARK_HOLDINGS_API_URL = os.getenv(
    "ARK_HOLDINGS_API_URL", "https://arkfunds.io/api/v2/etf/holdings"
).rstrip("/")

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/csv,text/plain,*/*",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 데이터 수집
# ---------------------------------------------------------------------------
def _normalize_api_holding(row: dict) -> dict:
    """API 응답을 포맷터가 기대하는 키 형태로 맞춤."""
    return {
        "ticker": str(row.get("ticker") or "").strip(),
        "company": row.get("company") or "",
        "weight": row.get("weight"),
        "shares": row.get("shares"),
        "market value ($)": row.get("market_value"),
    }


def fetch_ark_holdings_api(fund_name: str) -> list[dict] | None:
    """arkfunds.io 스타일 holdings JSON API (ARK Invest 비공식 커뮤니티 API)."""
    try:
        resp = requests.get(
            ARK_HOLDINGS_API_URL,
            params={"symbol": fund_name},
            headers=REQUEST_HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as e:
        logger.error(f"[{fund_name}] API HTTP 실패: {e}")
        return None
    except ValueError as e:
        logger.error(f"[{fund_name}] API JSON 파싱 실패: {e}")
        return None

    raw = payload.get("holdings") or []
    if not raw:
        logger.warning(f"[{fund_name}] API 응답에 holdings 없음")
        return None

    sorted_rows = sorted(
        raw,
        key=lambda x: (x.get("weight_rank") if x.get("weight_rank") is not None else 9999, str(x.get("ticker") or "")),
    )
    records = [_normalize_api_holding(h) for h in sorted_rows]
    records = [r for r in records if r["ticker"]]
    logger.info(f"[{fund_name}] API로 {len(records)}개 종목 수집")
    return records


def fetch_ark_holdings_csv(fund_name: str, url: str) -> list[dict] | None:
    """ARK 공식 사이트 CSV (경로 변경 시 404 가능)."""
    try:
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"[{fund_name}] CSV HTTP 실패: {e}")
        return None

    try:
        content = resp.text
        if content.lstrip().startswith("<!DOCTYPE") or content.lstrip().startswith("<html"):
            logger.error(f"[{fund_name}] CSV URL이 HTML(404 페이지)을 반환함")
            return None

        lines = content.splitlines()
        header_idx = 0
        for i, line in enumerate(lines):
            if "date" in line.lower() or "fund" in line.lower():
                header_idx = i
                break

        csv_data = "\n".join(lines[header_idx:])
        df = pd.read_csv(StringIO(csv_data))
        df.columns = [str(c).strip().lower() for c in df.columns]
        df = df.dropna(how="all")

        if "ticker" in df.columns:
            df = df[df["ticker"].notna() & (df["ticker"].astype(str).str.strip() != "")]

        records = df.to_dict("records")
        logger.info(f"[{fund_name}] CSV로 {len(records)}개 종목 수집")
        return records
    except Exception as e:
        logger.error(f"[{fund_name}] CSV 파싱 실패: {e}")
        return None


def fetch_ark_holdings(fund_name: str) -> list[dict] | None:
    """ARK_DATA_SOURCE 에 따라 API / CSV / 순차 폴백."""
    src = ARK_DATA_SOURCE
    url = ARK_CSV_FALLBACK.get(fund_name, "")

    if src == "csv":
        if not url:
            return None
        return fetch_ark_holdings_csv(fund_name, url)

    if src == "api":
        return fetch_ark_holdings_api(fund_name)

    # api_then_csv (기본): API가 비어 있거나 실패하면 CSV 시도
    api_rows = fetch_ark_holdings_api(fund_name)
    if api_rows:
        return api_rows
    if url:
        logger.warning(f"[{fund_name}] API 없음/실패 → CSV 폴백 시도")
        return fetch_ark_holdings_csv(fund_name, url)
    return None


# ---------------------------------------------------------------------------
# 상태 관리
# ---------------------------------------------------------------------------
def compute_hash(data: list[dict]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.md5(serialized.encode()).hexdigest()


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"상태 파일 로드 실패: {e}")
    return {}


def save_state(state: dict) -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        logger.error(f"상태 파일 저장 실패: {e}")


# ---------------------------------------------------------------------------
# 텔레그램 전송
# ---------------------------------------------------------------------------
def send_telegram(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("텔레그램 미설정 — 전송 생략")
        return

    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    max_len = 4096

    chunks = [message[i : i + max_len] for i in range(0, len(message), max_len)]
    for idx, chunk in enumerate(chunks, 1):
        try:
            resp = requests.post(
                api_url,
                json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "parse_mode": "HTML"},
                timeout=30,
            )
            resp.raise_for_status()
            if len(chunks) > 1:
                logger.info(f"텔레그램 전송 {idx}/{len(chunks)} 완료")
        except requests.RequestException as e:
            logger.error(f"텔레그램 전송 실패 ({idx}/{len(chunks)}): {e}")


# ---------------------------------------------------------------------------
# 메시지 포맷
# ---------------------------------------------------------------------------
def _safe(val, fmt: str = "") -> str:
    """NaN / None 을 안전하게 문자열로 변환."""
    if val is None:
        return "N/A"
    s = str(val).strip()
    if s.lower() in ("nan", "none", ""):
        return "N/A"
    if fmt == "float":
        try:
            return f"{float(s.replace(',', '')):,.2f}"
        except ValueError:
            return s
    if fmt == "int":
        try:
            return f"{int(float(s.replace(',', ''))):,}"
        except ValueError:
            return s
    return s


def format_portfolio_message(fund_name: str, holdings: list[dict], is_first_run: bool = False) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status_label = "초기 등록" if is_first_run else "변경 감지"

    lines = [
        f"<b>📊 ARK {fund_name} 포트폴리오 {status_label}</b>",
        f"🕐 {now}",
        f"📋 총 {len(holdings)}개 종목",
        "─" * 30,
        "",
    ]

    for i, row in enumerate(holdings, 1):
        ticker = _safe(row.get("ticker") or row.get("symbol"))
        company = _safe(row.get("company") or row.get("name"))
        weight_raw = row.get("weight (%)") or row.get("weight(%)") or row.get("weight")
        shares_raw = row.get("shares")
        mv_raw = (
            row.get("market value ($)")
            or row.get("market value")
            or row.get("market_value")
        )

        weight = _safe(weight_raw, "float")
        shares = _safe(shares_raw, "int")
        mv = _safe(mv_raw, "float")

        line = f"{i:>3}. <b>{ticker}</b>  {company}"
        details = []
        if weight != "N/A":
            details.append(f"비중 {weight}%")
        if mv != "N/A":
            details.append(f"${mv}")
        if shares != "N/A":
            details.append(f"{shares}주")
        if details:
            line += "\n      " + " | ".join(details)
        lines.append(line)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 메인 체크 로직
# ---------------------------------------------------------------------------
def check_and_notify() -> None:
    logger.info("=" * 50)
    logger.info("ARK 펀드 포트폴리오 체크 시작")
    state = load_state()
    state_updated = False

    for fund_name in ARK_SYMBOLS:
        holdings = fetch_ark_holdings(fund_name)
        if holdings is None:
            continue

        current_hash = compute_hash(holdings)
        fund_state = state.get(fund_name, {})
        previous_hash = fund_state.get("hash", "")
        is_first_run = previous_hash == ""

        if current_hash != previous_hash:
            if is_first_run:
                logger.info(f"[{fund_name}] 초기 포트폴리오 등록")
            else:
                logger.info(f"[{fund_name}] ★ 변경사항 감지 → 텔레그램 전송")

            msg = format_portfolio_message(fund_name, holdings, is_first_run=is_first_run)
            send_telegram(msg)

            state[fund_name] = {
                "hash": current_hash,
                "last_updated": datetime.now().isoformat(),
                "holdings_count": len(holdings),
            }
            state_updated = True
        else:
            logger.info(f"[{fund_name}] 변경사항 없음")

    if state_updated:
        save_state(state)

    logger.info("체크 완료")


# ---------------------------------------------------------------------------
# 실행 엔트리포인트
# ---------------------------------------------------------------------------
def run_once() -> None:
    check_and_notify()


def run_scheduler(interval_minutes: int = 1) -> None:
    logger.info(f"스케줄러 시작 ({interval_minutes}분 간격)")
    check_and_notify()
    schedule.every(interval_minutes).minutes.do(check_and_notify)
    while True:
        schedule.run_pending()
        time.sleep(1)


def run_loop(total_minutes: int, interval_minutes: int = 1) -> None:
    """GitHub Actions 등에서 지정된 시간(분) 동안 반복 실행."""
    logger.info(f"루프 모드: {total_minutes}분간 {interval_minutes}분 간격 실행")
    iterations = max(1, total_minutes // interval_minutes)
    for i in range(1, iterations + 1):
        logger.info(f"루프 {i}/{iterations}")
        check_and_notify()
        if i < iterations:
            time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ARK 펀드 포트폴리오 모니터링")
    parser.add_argument(
        "--mode",
        choices=["once", "scheduler", "loop"],
        default="scheduler",
        help="실행 모드: once(1회), scheduler(무한루프), loop(지정시간)",
    )
    parser.add_argument(
        "--loop-minutes",
        type=int,
        default=5,
        help="loop 모드에서 총 실행 시간(분) (기본값: 5)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=1,
        help="체크 간격(분) (기본값: 1)",
    )
    args = parser.parse_args()

    if os.getenv("GITHUB_ACTIONS", "").strip().lower() == "true":
        run_loop(total_minutes=args.loop_minutes, interval_minutes=args.interval)
    elif args.mode == "once":
        run_once()
    elif args.mode == "loop":
        run_loop(total_minutes=args.loop_minutes, interval_minutes=args.interval)
    else:
        run_scheduler(interval_minutes=args.interval)
