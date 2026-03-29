"""
ARK 펀드 포트폴리오 모니터링 배치
- 1분 간격으로 ARK ETF 보유 종목 CSV를 크롤링
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
# ARK ETF 목록 및 CSV URL
# ---------------------------------------------------------------------------
ARK_FUNDS = {
    "ARKK": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv",
    "ARKQ": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_AUTONOMOUS_TECHNOLOGY_&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv",
    "ARKW": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv",
    "ARKG": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_GENOMIC_REVOLUTION_MULTISECTOR_ETF_ARKG_HOLDINGS.csv",
    "ARKF": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv",
    "ARKX": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_SPACE_EXPLORATION_&_INNOVATION_ETF_ARKX_HOLDINGS.csv",
}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
STATE_FILE = os.getenv("ARK_STATE_FILE", "ark_state.json")

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,text/plain,*/*",
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
def fetch_ark_holdings(fund_name: str, url: str) -> list[dict] | None:
    """ARK ETF 보유 종목 CSV를 가져와서 레코드 목록으로 반환."""
    try:
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"[{fund_name}] HTTP 요청 실패: {e}")
        return None

    try:
        content = resp.text
        lines = content.splitlines()

        # CSV 헤더 행 탐색 (date / fund 컬럼이 있는 첫 번째 행)
        header_idx = 0
        for i, line in enumerate(lines):
            if "date" in line.lower() or "fund" in line.lower():
                header_idx = i
                break

        csv_data = "\n".join(lines[header_idx:])
        df = pd.read_csv(StringIO(csv_data))
        df.columns = [str(c).strip().lower() for c in df.columns]
        df = df.dropna(how="all")

        # 빈 ticker 행(합계 행 등) 제거
        if "ticker" in df.columns:
            df = df[df["ticker"].notna() & (df["ticker"].astype(str).str.strip() != "")]

        records = df.to_dict("records")
        logger.info(f"[{fund_name}] {len(records)}개 종목 수집 완료")
        return records
    except Exception as e:
        logger.error(f"[{fund_name}] CSV 파싱 실패: {e}")
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
        mv_raw = row.get("market value ($)") or row.get("market value")

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

    for fund_name, url in ARK_FUNDS.items():
        holdings = fetch_ark_holdings(fund_name, url)
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
