"""
Step 2 통합 버전 (v5.2): MaV 하이엔드 경제 브리핑 생성기 (Data Quality & JSON Robustness)
- News Quality: RSS 피드를 '전체'에서 '증권/금융/매크로' 전용 카테고리로 엄격히 제한
- AI Robustness: Groq LLM의 Markdown(`json`) 오염 출력 시 강제 파싱 및 에러 로깅 추가
"""

import os
import yfinance as yf
import feedparser
import requests
import json
from datetime import datetime, timedelta

# [Security] 환경 변수에서 API Key 로드
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
ECOS_API_KEY = os.environ.get("ECOS_API_KEY", "")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

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

# [Quality Fix] 뉴스 피드를 '금융/증권'으로 엄격히 제한
RSS_FEEDS = {
    "WSJ Markets": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "CNBC Finance": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
    "Yahoo Finance": "https://finance.yahoo.com/news/rssindex",
    "한국경제(증권)": "https://www.hankyung.com/feed/stock",
    "매일경제(금융)": "https://www.mk.co.kr/rss/30000016/"
}

def collect_market_data():
    print("\n📊 시장 데이터 및 차트 히스토리 수집 중...")
    all_results = {}
    for category, tickers in MARKET_TICKERS.items():
        category_results = {}
        for name, ticker in tickers.items():
            try:
                t = yf.Ticker(ticker)
                hist = t.history(period="7d")
                if len(hist) < 2: continue
                
                prices = [round(float(x), 2) for x in hist["Close"].tolist()[-5:]]
                latest, previous = prices[-1], prices[-2]
                change = latest - previous
                change_pct = (change / previous) * 100 if previous != 0 else 0
                
                category_results[name] = {
                    "price": latest, "change": round(change, 2), "change_pct": round(change_pct, 2), 
                    "direction": "▲" if change >= 0 else "▼",
                    "history": prices
                }
                print(f"  ✅ {name:15s} {latest:>12,.2f} {'▲' if change >= 0 else '▼'}{abs(change_pct):.2f}%")
            except Exception as e: print(f"  ❌ {name}: {e}")
        all_results[category] = category_results
    return all_results

def collect_fear_and_greed():
    print("\n🧭 공포/탐욕 지수 수집 중...")
    try:
        import fear_and_greed
        fg = fear_and_greed.get()
        print(f"  ✅ 수치: {fg.value} ({fg.description})")
        return {"value": fg.value, "description": fg.description}
    except Exception:
        return {"value": 50, "description": "neutral"}

def collect_fred_data():
    print("\n🏛️  미국 거시경제 지표 수집 중...")
    if not FRED_API_KEY: return print("  ⚠️ FRED_API_KEY 누락됨") or {}
    results = {}
    try:
        from fredapi import Fred
        fred = Fred(api_key=FRED_API_KEY)
        for name, series_id in FRED_SERIES.items():
            try:
                data = fred.get_series(series_id, observation_start=(datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"))
                if data is not None and len(data.dropna()) > 0:
                    latest, date = data.dropna().iloc[-1], str(data.dropna().index[-1].date())
                    results[name] = {"value": round(float(latest), 2), "date": date}
                    print(f"  ✅ {name:15s}: {latest:.2f} ({date})")
            except Exception: pass
    except Exception: pass
    return results

def collect_ecos_data():
    print("\n🇰🇷 한국 거시경제 지표 수집 중...")
    if not ECOS_API_KEY: return print("  ⚠️ ECOS_API_KEY 누락됨") or {}
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
                print(f"  ✅ {name:15s}: {val} ({period})")
        except Exception: pass
    return results

def collect_news(max_per_feed=5):
    print("\n📰 매체별 주요 뉴스 수집 중...")
    all_news = []
    for feed_name, feed_url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(requests.get(feed_url, timeout=5).content)
            count = 0
            for entry in feed.entries[:max_per_feed]:
                title = entry.get("title", "").strip()
                if title:
                    all_news.append({"source": feed_name, "title": title, "link": entry.get("link", "")})
                    count += 1
            print(f"  ✅ [{feed_name}] {count}개 수집 완료")
        except Exception as e:
            print(f"  ❌ [{feed_name}] 수집 실패: {e}")
    return all_news

def generate_ai_insight(market_data, fg_data, news, fred_data, ecos_data):
    print("\n🤖 프리미엄 AI 매크로 분석 중 (Groq)...")
    if not GROQ_API_KEY: return print("  ⚠️ GROQ_API_KEY 누락됨") or {}
    
    market_text = "\n".join([f"{name}: {d['price']:,.2f} ({d['direction']}{abs(d['change_pct']):.2f}%)" for cat, items in market_data.items() for name, d in items.items()])
    news_text = "\n".join([f"- [{n['source']}] {n['title']}" for n in news])

    prompt = f"""You are a Wall Street Macro Analyst writing a newsletter for investors.

[CRITICAL RULES]
1. NEVER output the instructions or prompt descriptions in your JSON values.
2. Only output the final, polished analysis in Korean. 
3. Tone: Professional, highly readable, objective.

Output ONLY valid JSON matching this structure:
{{
  "narrative": "(Write 3 sentences analyzing today's market drivers, focusing on yields, FX, and equities.)",
  "so_what": {{
    "미국_시장": "(1 sentence on US equities and sectors)",
    "한국_시장": "(1 sentence on KR equities and sectors)",
    "매크로_환경": "(1 sentence on yields and FX)"
  }},
  "economic_calendar": {{
    "US": [ {{"date": "(Day of week)", "event": "(Event name)"}} ],
    "KR": [ {{"date": "(Day of week)", "event": "(Event name)"}} ]
  }},
  "analyzed_news": [
    {{"source": "(News source)", "title": "(Actual news title)", "impact": "(Objective market impact of this news)"}}
  ]
}}

---
[Data]
{market_text}
[News]
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
        
        # [Robustness Fix] Markdown 백틱 강제 제거 로직 추가
        raw_content = response.choices[0].message.content.strip()
        if raw_content.startswith("
http://googleusercontent.com/immersive_entry_chip/0
http://googleusercontent.com/immersive_entry_chip/1
http://googleusercontent.com/immersive_entry_chip/2

### ⏭ 다음 단계 (Action)
1. 로컬에서 수정한 `step2_mav_briefing.py`를 덮어쓴 후, `git add .`, `git commit -m "fix: ai json parsing and news quality"`, `git push` 로 GitHub에 업데이트하라.
2. Actions 탭에서 수동 실행하여 빈칸이 채워지는지 즉각 확인하라.
3. 데이터 확장은 현재 구축된 파이프라인이 100% 무결하게 돌아가는 것을 확인한 즉시, 미국 채권 수익률 곡선(2Y/10Y) 역전 데이터와 크립토 Fear/Greed 지표를 추가하는 **콘텐츠 딥다이브** 단계로 넘어가겠다. 실행 결과 피드백을 기다린다.