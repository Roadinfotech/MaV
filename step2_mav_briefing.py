"""
Step 2 통합 버전 (v5.5): MaV 하이엔드 경제 브리핑 생성기 (Prompt Engineering & Variance Fix)
- LLM Tuning: Temperature 0.1 -> 0.5 변경 (기계적 반복 답변 방지)
- Prompt: 당일(Today) 변화량(Delta) 및 특이 뉴스 강제 반영 지시 추가
"""

import os
import re
import json
import yfinance as yf
import feedparser
import requests
from datetime import datetime, timedelta

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

RSS_FEEDS = {
    "WSJ Markets": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "CNBC Finance": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
    "한국경제(금융)": "https://www.hankyung.com/feed/finance",
    "매일경제(경제)": "https://www.mk.co.kr/rss/30000001/"
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
    if not FRED_API_KEY: return {}
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
    if not ECOS_API_KEY: return {}
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

def extract_valid_json(text):
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except: pass
    try:
        return json.loads(text)
    except:
        return None

def generate_ai_insight(market_data, fg_data, news, fred_data, ecos_data):
    print("\n🤖 프리미엄 AI 매크로 분석 중 (Groq)...")
    if not GROQ_API_KEY:
        return {"narrative": "시스템 알림: GROQ_API_KEY가 누락되어 분석이 생략되었습니다."}
        
    market_text = "\n".join([f"{name}: {d['price']:,.2f} ({d['direction']}{abs(d['change_pct']):.2f}%)" for cat, items in market_data.items() for name, d in items.items()])
    news_text = "\n".join([f"- [{n['source']}] {n['title']}" for n in news])

    prompt = f"""You are a Top-Tier Wall Street Macro Analyst. Output ONLY valid JSON.

[CRITICAL RULES]
1. NO REPETITION: Do not use generic boilerplate text. You MUST focus entirely on what specific events, news, or data changes drove today's market.
2. Tone: Highly objective, data-driven, and crisp. Korean language.

{{
  "narrative": "오늘 수집된 뉴스와 지표 변화를 바탕으로 새롭게 발생한 시장의 핵심 트리거를 3문장으로 구체적으로 작성하세요. 매일 똑같은 일반론적 서술 절대 금지.",
  "so_what": {{
    "미국_시장": "오늘 미국 증시 주도 섹터와 특이점 1줄 평가",
    "한국_시장": "오늘 한국 증시 특징 1줄 평가",
    "매크로_환경": "오늘 국채/환율 핵심 동향 1줄 평가"
  }},
  "economic_calendar": {{
    "US": [{{"date": "월/일", "event": "일정"}}],
    "KR": [{{"date": "월/일", "event": "일정"}}]
  }},
  "analyzed_news": [
    {{"source": "WSJ 등", "title": "핵심 뉴스", "impact": "시장 파급효과 1줄"}}
  ]
}}

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
            temperature=0.5 # 기계적 답변 방지를 위해 0.1에서 0.5로 상향
        )
        
        raw_content = response.choices[0].message.content.strip()
        result = extract_valid_json(raw_content)
        
        if result:
            print("  ✅ AI 분석 성공")
            return result
        else:
            print(f"  ⚠️ LLM 원본 출력값 파싱 실패: {raw_content}")
            return {"narrative": "시스템 알림: AI 모델이 올바른 형식을 반환하지 못했습니다."}
            
    except Exception as e:
        print(f"  ❌ AI API 호출 에러: {e}")
        return {"narrative": f"시스템 알림: AI 통신 에러 발생 ({e})"}

def main():
    today_str = datetime.now().strftime("%Y%m%d")
    print(f"🚀 MaV 하이엔드 브리핑 파이프라인 가동 ({today_str})")
    
    market = collect_market_data()
    fg = collect_fear_and_greed()
    fred = collect_fred_data()
    ecos = collect_ecos_data()
    news = collect_news()
    
    ai = generate_ai_insight(market, fg, news, fred, ecos)
    
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
    with open(filename, "w", encoding="utf-8") as f: json.dump(output_data, f, ensure_ascii=False, indent=2)
    print(f"\n✅ JSON 저장 완료: {filename}")

if __name__ == "__main__":
    main()