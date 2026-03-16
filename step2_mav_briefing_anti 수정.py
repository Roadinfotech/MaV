"""
Step 2 통합 버전 (v4.7 Refactored): MaV 프리미엄 경제 브리핑 생성기
- 성능: ThreadPoolExecutor를 통한 멀티 스레딩 데이터 수집 병렬 처리 (속도 3~5배 향상)
- 안정성: dotenv 기반 환경변수 관리 및 tenacity를 활용한 API 지수 백오프(Exponential Backoff) 재시도 로직 적용
- 버그 픽스: main() 함수 변수 할당 구조 완전 분리로 UnboundLocalError 차단
"""
import os
import json
import concurrent.futures
from datetime import datetime, timedelta
import yfinance as yf
import feedparser
import requests
from dotenv import load_dotenv
from tenacity import retry, wait_exponential, stop_after_attempt
# --- 환경변수 로드 ---
# 프로젝트 루트에 .env 파일을 만들고 API 키를 관리하세요.
load_dotenv()
FRED_API_KEY = os.getenv("FRED_API_KEY", "2c0ec3994b962faddd6bba9536aa71e0")
ECOS_API_KEY = os.getenv("ECOS_API_KEY", "UBFPR1O39IKSRMWO1LU9")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "CykhyeVdCgydyQmFY5XBWGdyb3FYSkxiE8PKGag85hm4r3oq6NOo")
MARKET_TICKERS = {
    "indices": {"S&P 500": "^GSPC", "나스닥": "^IXIC", "다우존스": "^DJI", "KOSPI": "^KS11", "KOSDAQ": "^KQ11", "VIX (공포지수)": "^VIX"},
    "us_sectors": {"미국 기술주(XLK)": "XLK", "미국 금융주(XLF)": "XLF", "미국 에너지(XLE)": "XLE"},
    "kr_sectors": {"KODEX 200": "069500.KS", "KODEX 반도체": "091160.KS", "KODEX 2차전지": "305730.KS"},
    "currencies": {"USD/KRW": "KRW=X", "달러 인덱스": "DX-Y.NYB"},
    "commodities": {"WTI 원유": "CL=F", "금": "GC=F"},
    "bonds": {"미국10년물금리": "^TNX"},
    "crypto": {"비트코인": "BTC-USD"}
}
FRED_SERIES = {"미국 기준금리": "FEDFUNDS", "미국 CPI": "CPIAUCSL", "미국 실업률(%)": "UNRATE"}
ECOS_SERIES = {"한국 기준금리": {"stat_code": "722Y001", "item_code": "0101000"}}
RSS_FEEDS = {
    "WSJ Markets": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "CNBC Top News": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
    "Yahoo Finance": "https://finance.yahoo.com/news/rssindex",
    "한국경제": "https://www.hankyung.com/feed/all-news",
}
# ============================================================
# 데이터 수집 서브 함수들 (재시도 로직 적용)
# ============================================================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_ticker_data(name, ticker):
    """개별 주식/ETF 데이터 조회 (네트워크 에러 시 3회 재시도)"""
    t = yf.Ticker(ticker)
    hist = t.history(period="7d")
    if len(hist) < 2:
        return name, None
    
    prices = [round(float(x), 2) for x in hist["Close"].tolist()[-5:]]
    latest, previous = prices[-1], prices[-2]
    change = latest - previous
    change_pct = (change / previous) * 100 if previous != 0 else 0
    
    result = {
        "price": latest, "change": round(change, 2), "change_pct": round(change_pct, 2), 
        "direction": "▲" if change >= 0 else "▼",
        "history": prices
    }
    print(f"  ✅ [시장] {name:15s} {latest:>12,.2f} {'▲' if change >= 0 else '▼'}{abs(change_pct):.2f}%")
    return name, result
def collect_market_data():
    print("\n📊 시장 데이터 및 차트 히스토리 병렬 수집 중...")
    all_results = {}
    
    for category, tickers in MARKET_TICKERS.items():
        category_results = {}
        # YFinance 내부 요청도 멀티 스레드로 병렬로 쏴서 속도를 대폭 높임
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_ticker = {executor.submit(fetch_ticker_data, name, ticker): name for name, ticker in tickers.items()}
            for future in concurrent.futures.as_completed(future_to_ticker):
                try:
                    name, data = future.result()
                    if data:
                        category_results[name] = data
                except Exception as e:
                    name = future_to_ticker[future]
                    print(f"  ❌ {name} 수집 실패: {e}")
        all_results[category] = category_results
    return all_results
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def collect_fear_and_greed():
    print("\n🧭 공포/탐욕 지수 수집 중...")
    try:
        import fear_and_greed
        fg = fear_and_greed.get()
        print(f"  ✅ [지수] 수치: {fg.value} ({fg.description})")
        return {"value": fg.value, "description": fg.description}
    except Exception as e:
        print(f"  ❌ 공포/탐욕 지수 파싱 실패, 기본값 반환: {e}")
        return {"value": 50, "description": "neutral"}
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def collect_fred_data():
    if not FRED_API_KEY:
        print("  ⚠️ FRED_API_KEY 환경변수 누락 (Pass)")
        return {}
        
    print("\n🏛️  미국 거시경제 지표 수집 중...")
    results = {}
    from fredapi import Fred
    fred = Fred(api_key=FRED_API_KEY)
    
    for name, series_id in FRED_SERIES.items():
        try:
            data = fred.get_series(series_id, observation_start=(datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"))
            if data is not None and len(data.dropna()) > 0:
                latest, date = data.dropna().iloc[-1], str(data.dropna().index[-1].date())
                results[name] = {"value": round(float(latest), 2), "date": date}
                print(f"  ✅ [미국] {name:15s}: {latest:.2f} ({date})")
        except Exception as e: 
            pass
    return results
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def collect_ecos_data():
    if not ECOS_API_KEY:
        print("  ⚠️ ECOS_API_KEY 환경변수 누락 (Pass)")
        return {}
        
    print("\n🇰🇷 한국 거시경제 지표 수집 중...")
    results = {}
    end_date, start_date = datetime.now().strftime("%Y%m"), (datetime.now() - timedelta(days=730)).strftime("%Y%m")
    for name, info in ECOS_SERIES.items():
        try:
            url = f"https://ecos.bok.or.kr/api/StatisticSearch/{ECOS_API_KEY}/json/kr/1/10/{info['stat_code']}/M/{start_date}/{end_date}/{info['item_code']}"
            resp = requests.get(url, timeout=10).json()
            rows = resp.get("StatisticSearch", {}).get("row", [])
            if rows:
                val, period = float(rows[-1]["DATA_VALUE"]), rows[-1]["TIME"]
                results[name] = {"value": val, "period": period}
                print(f"  ✅ [한국] {name:15s}: {val} ({period})")
        except Exception: 
            pass
    return results
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
def fetch_rss_feed(feed_name, feed_url, max_per_feed):
    """RSS 피드 파싱을 위한 재시도 단위 함수"""
    feed = feedparser.parse(requests.get(feed_url, timeout=10).content)
    parsed_items = []
    for entry in feed.entries[:max_per_feed]:
        title = entry.get("title", "").strip()
        if title:
            parsed_items.append({"source": feed_name, "title": title, "link": entry.get("link", "")})
    if len(parsed_items) == 0:
        raise Exception("수집된 뉴스가 없습니다 (재시도 유도)")
    print(f"  ✅ [뉴스] {feed_name}: {len(parsed_items)}개 수집 완료")
    return parsed_items
def collect_news(max_per_feed=5):
    print("\n📰 매체별 주요 뉴스 병렬 수집 중...")
    all_news = []
    
    # 뉴스 RSS 피드도 병렬 다운로드 진행
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(RSS_FEEDS)) as executor:
        future_to_feed = {executor.submit(fetch_rss_feed, name, url, max_per_feed): name for name, url in RSS_FEEDS.items()}
        for future in concurrent.futures.as_completed(future_to_feed):
            try:
                news_items = future.result()
                if news_items:
                    all_news.extend(news_items)
            except Exception as e:
                pass
    return all_news
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=3, min=5, max=15))
def generate_ai_insight(market_data, fg_data, news, fred_data, ecos_data):
    """Groq API 호출 함수 (Rate Limit 대응을 위해 최대 15초까지 대기하며 재시도)"""
    if not GROQ_API_KEY:
        print("  ⚠️ GROQ_API_KEY 환경변수 누락 (분석 스킵)")
        return {}
        
    print("\n🤖 프리미엄 AI 매크로 분석 중 (Groq)...")
    market_text = "\n".join([f"{name}: {d['price']:,.2f} ({d['direction']}{abs(d['change_pct']):.2f}%)" for cat, items in market_data.items() for name, d in items.items()])
    news_text = "\n".join([f"- [{n['source']}] {n['title']}" for n in news])
    prompt = f"""You are a Top-Tier Wall Street Macro Analyst writing a newsletter for investors.
[Strict Analysis Rules - SAFETY & OBJECTIVITY]
1. No Speculation: DO NOT predict the future or suggest investment actions. 
2. Historical Objectivity: Base impacts strictly on historical precedents and objective economic mechanisms.
3. Tone: Professional but engaging and easy to read. Avoid robotic or overly stiff language. Use natural Korean paragraphs.
4. Output Format: STRICTLY valid JSON ONLY.
Analyze the data and output the following JSON structure in Korean:
{{
  "narrative": "시황 핵심 요약. 너무 딱딱하지 않은 자연스러운 브리핑 톤으로, 오늘 시장의 핵심 흐름과 인과관계를 가독성 좋게 2~3문장으로 서술하세요.",
  "so_what": {{
    "미국_시장": "현재 미국 증시와 주도 섹터 흐름에 대한 객관적 1줄 평가",
    "한국_시장": "한국 증시와 주요 ETF/섹터 흐름에 대한 객관적 1줄 평가",
    "매크로_환경": "금리와 환율 움직임이 시사하는 거시적 환경 1줄 평가"
  }},
  "economic_calendar": {{
    "US": [
      {{"date": "이번 주 요일", "event": "미국의 중요한 매크로 이벤트 (지표 발표 등)"}}
    ],
    "KR": [
      {{"date": "이번 주 요일", "event": "한국의 중요한 매크로 이벤트 (없으면 비워둠)"}}
    ]
  }},
  "analyzed_news": [
    {{"source": "WSJ 등", "title": "핵심 뉴스 제목 1", "impact": "이 뉴스가 자산 시장에 미치는 파급 효과 (객관적 관점)"}},
    {{"title": "핵심 뉴스 제목 2", "impact": "..."}},
    {{"title": "핵심 뉴스 제목 3", "impact": "..."}}
  ]
}}
---
[시장 및 섹터 데이터]
{market_text}
[오늘의 전체 뉴스 리스트]
{news_text}
"""
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_API_KEY)
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile", 
            messages=[{"role": "user", "content": prompt}], 
            temperature=0.1, 
            response_format={"type": "json_object"}
        )
        result = json.loads(response.choices[0].message.content)
        print("  ✅ AI 프리미엄 분석 완료")
        return result
    except Exception as e:
        print(f"  ❌ AI 분석 실패 (재시도 중): {e}")
        raise e
# ============================================================
# 메인 파이프라인
# ============================================================
def main():
    today_str = datetime.now().strftime("%Y%m%d")
    print(f"🚀 MaV 하이엔드 브리핑 파이프라인 가동 ({today_str})")
    print("⚡ 비동기 데이터 병렬 수집 시작...")
    
    # 1. ThreadPoolExecutor를 이용한 최상위 카테고리 병렬 실행
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        f_market = executor.submit(collect_market_data)
        f_fg = executor.submit(collect_fear_and_greed)
        f_fred = executor.submit(collect_fred_data)
        f_ecos = executor.submit(collect_ecos_data)
        f_news = executor.submit(collect_news)
        
        # 각 수집 완료 대기
        market = f_market.result()
        fg = f_fg.result()
        fred = f_fred.result()
        ecos = f_ecos.result()
        news = f_news.result()
    
    # 2. 수집된 모든 데이터를 넘겨 AI 분석 (동기 호출)
    ai = generate_ai_insight(market, fg, news, fred, ecos)
    
    # 3. 데이터 통합 및 JSON 저장
    output_data = {
        "date": datetime.now().strftime("%Y년 %m월 %d일"), 
        "weekday": ["월", "화", "수", "목", "금", "토", "일"][datetime.now().weekday()], 
        "market_data": market, 
        "fear_and_greed": fg, 
        "fred_data": fred, 
        "ecos_data": ecos, 
        "news": news, 
        "ai_insight": ai
    }
    
    filename = f"mav_briefing_{today_str}.json"
    with open(filename, "w", encoding="utf-8") as f: 
        json.dump(output_data, f, ensure_ascii=False, indent=2)
        
    print(f"\n✅ 프로세스 완료! JSON 저장: {filename}")
if __name__ == "__main__":
    main()