"""
Step 3: MaV 프리미엄 카드뉴스 생성기 (Chart & Data-Viz 추가)
- matplotlib을 이용해 각 지수별 5일치 꺾은선 그래프(Sparkline) 자동 생성
- 생성된 차트를 HTML 템플릿에 삽입하여 고품질 인포그래픽 스크린샷 렌더링
"""

import os
import sys
import json
import glob
from datetime import datetime
import matplotlib.pyplot as plt
from jinja2 import Environment, FileSystemLoader
from playwright.sync_api import sync_playwright

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# 1. 꺾은선 그래프 (Sparkline) 렌더링 함수
# ============================================================
def create_sparkline(data_list, filename, color):
    """5일치 데이터를 받아 배경이 투명한 미니 꺾은선 그래프 이미지를 생성"""
    if not data_list or len(data_list) < 2:
        return ""
    
    # 그래프 사이즈 설정 (아주 작고 얇게)
    fig, ax = plt.subplots(figsize=(2, 0.5))
    ax.plot(data_list, color=color, linewidth=3)
    
    # 축, 눈금, 배경 모두 제거
    ax.axis('off')
    
    filepath = os.path.join(OUTPUT_DIR, filename)
    plt.savefig(filepath, transparent=True, bbox_inches='tight', pad_inches=0)
    plt.close(fig)
    
    # 로컬 파일을 HTML에서 불러오기 위한 절대경로 반환
    return "file:///" + os.path.abspath(filepath).replace("\\", "/")

def get_color(change_pct):
    if change_pct >= 3:    return "#00C853"
    elif change_pct >= 0:  return "#66BB6A"
    elif change_pct >= -3: return "#EF5350"
    else:                  return "#C62828"

# ============================================================
# 2. 데이터 가공 및 그래프 생성 매핑
# ============================================================
def prepare_card_data(briefing_data):
    market = briefing_data.get("market_data", {})
    ai = briefing_data.get("ai_insight", {})
    
    date_str = briefing_data.get("date", datetime.now().strftime("%Y년 %m월 %d일"))
    weekday = briefing_data.get("weekday", "")
    date_display = f"{date_str} ({weekday})" if weekday else date_str
    
    # 데이터 포맷팅 및 그래프 생성기
    def format_items_with_charts(category_data, prefix="", file_prefix=""):
        items = []
        for name, d in category_data.items():
            price_val = d["price"]
            change_pct = d["change_pct"]
            color = get_color(change_pct)
            
            # 꺾은선 그래프 생성
            chart_filename = f"chart_{file_prefix}_{name.replace(' ', '_').replace('/', '')}.png"
            chart_url = create_sparkline(d.get("history", []), chart_filename, color)
            
            items.append({
                "name": name,
                "price": f"{prefix}{price_val:,.2f}" if prefix else f"{price_val:,.2f}",
                "direction": d["direction"],
                "change_pct": change_pct,
                "color": color,
                "chart_url": chart_url # 템플릿에 전달할 차트 이미지 경로
            })
        return items
    
    print("\n📈 꺾은선 그래프(Sparklines) 생성 중...")
    indices = format_items_with_charts(market.get("indices", {}), file_prefix="idx")
    commodities = format_items_with_charts(market.get("commodities", {}), "$", file_prefix="com")
    cryptos = format_items_with_charts(market.get("crypto", {}), "$", file_prefix="cry")
    
    # 뉴스 처리
    news_list = ai.get("news_summary", [])
    if not isinstance(news_list, list):
        news_list = [{"title": "뉴스 요약 데이터 오류", "insight": str(news_list)}]

    # So What 객체 처리
    so_what = ai.get("so_what", {})
    so_what_lines = []
    if isinstance(so_what, dict):
        for k, v in so_what.items():
            so_what_lines.append(f"<b>[{k}]</b> {v}")
    
    return {
        "date": date_display,
        "main_insight": ai.get("narrative", "시장 요약 데이터 없음"),
        "so_what_lines": so_what_lines,
        "indices": indices,
        "commodities": commodities,
        "cryptos": cryptos,
        "news_list": news_list[:3],
        "risk_signal": ai.get("risk_signal", "특이사항 없음"),
    }

# ============================================================
# 3. HTML 렌더링 (동적으로 템플릿 생성 후 스크린샷)
# ============================================================
def render_cards(card_data):
    print("🎨 카드뉴스 이미지 생성 시작")
    
    # 꺾은선 그래프가 포함된 하이엔드 HTML 템플릿 (Python 내장)
    HTML_TEMPLATE = """
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css');
            body { margin: 0; width: 1080px; height: 1350px; font-family: 'Pretendard', sans-serif; background: #0f172a; color: white; display: flex; flex-direction: column; padding: 80px; box-sizing: border-box; }
            .header { display: flex; justify-content: space-between; align-items: flex-end; border-bottom: 2px solid #334155; padding-bottom: 20px; margin-bottom: 40px; }
            .logo { font-size: 50px; font-weight: 900; letter-spacing: 2px; color: #fff; }
            .logo span { color: #3b82f6; }
            .date { font-size: 24px; color: #94a3b8; font-weight: 500; }
            .section-title { font-size: 24px; font-weight: 700; color: #60a5fa; margin-bottom: 20px; letter-spacing: 1px; }
            
            .box { background: rgba(30, 41, 59, 0.7); border-radius: 20px; padding: 40px; margin-bottom: 30px; border: 1px solid #334155; }
            .narrative { font-size: 32px; font-weight: 600; line-height: 1.6; color: #f8fafc; }
            
            .data-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 40px; }
            .data-card { background: #1e293b; padding: 25px; border-radius: 16px; border-left: 6px solid #334155; display: flex; flex-direction: column; justify-content: space-between;}
            .data-name { font-size: 20px; color: #cbd5e1; margin-bottom: 10px; }
            .data-bottom { display: flex; justify-content: space-between; align-items: flex-end; }
            .data-price { font-size: 34px; font-weight: 800; font-family: 'Courier New', monospace; }
            .data-pct { font-size: 22px; font-weight: 700; padding: 4px 12px; border-radius: 8px; }
            .sparkline { height: 40px; width: 120px; object-fit: contain; }
            
            .news-item { margin-bottom: 25px; padding-bottom: 25px; border-bottom: 1px solid #334155; }
            .news-title { font-size: 26px; font-weight: 700; color: #e2e8f0; margin-bottom: 10px; line-height: 1.4; }
            .news-insight { font-size: 20px; color: #94a3b8; line-height: 1.5; border-left: 3px solid #3b82f6; padding-left: 15px; }
            
            .footer { margin-top: auto; text-align: center; font-size: 20px; color: #64748b; font-weight: 500; }
        </style>
    </head>
    <body>
        <div class="header">
            <div class="logo">MaV<span>.</span></div>
            <div class="date">{{ date }}</div>
        </div>
        
        <div class="section-title">MACRO SUMMARY</div>
        <div class="box">
            <div class="narrative">{{ main_insight }}</div>
        </div>

        <div class="section-title">MARKET TRENDS (5-Day)</div>
        <div class="data-grid">
            {% for item in indices[:4] %}
            <div class="data-card" style="border-color: {{ item.color }};">
                <div class="data-name">{{ item.name }}</div>
                <div class="data-bottom">
                    <div>
                        <div class="data-price">{{ item.price }}</div>
                        <div class="data-pct" style="color: {{ item.color }};">{{ item.direction }}{{ item.change_pct }}%</div>
                    </div>
                    {% if item.chart_url %}
                    <img class="sparkline" src="{{ item.chart_url }}">
                    {% endif %}
                </div>
            </div>
            {% endfor %}
        </div>
        
        <div class="section-title">DEEP DIVE NEWS</div>
        <div class="box" style="padding: 30px 40px 10px 40px;">
            {% for n in news_list[:2] %}
            <div class="news-item">
                <div class="news-title">{{ n.title }}</div>
                <div class="news-insight">{{ n.insight }}</div>
            </div>
            {% endfor %}
        </div>

        <div class="footer">Market At a View | Premium Intelligence</div>
    </body>
    </html>
    """
    
    env = Environment()
    template = env.from_string(HTML_TEMPLATE)
    html_content = template.render(**card_data)
    
    # 디버깅용 HTML 저장
    with open("temp_render.html", "w", encoding="utf-8") as f:
        f.write(html_content)
        
    output_path = f"{OUTPUT_DIR}/mav_premium_report.png"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_viewport_size({"width": 1080, "height": 1350})
        # 로컬 파일 경로(file://) 처리를 위해 set_content 대신 goto 방식 혼용 (Base URI 지정)
        page.goto(f"file:///{os.path.abspath('temp_render.html').replace(chr(92), '/')}")
        page.wait_for_timeout(1000) # 차트 렌더링 대기
        page.screenshot(path=output_path, full_page=False)
        browser.close()
        
    print(f"✅ 프리미엄 인포그래픽 생성 완료: {output_path}")

def main():
    print("=" * 50)
    print("  MaV - Premium Data Visualization")
    print("=" * 50)
    
    json_files = sorted(glob.glob("mav_briefing_*.json"))
    if not json_files: return print("❌ 데이터 파일 없음.")
    
    with open(json_files[-1], "r", encoding="utf-8") as f:
        briefing_data = json.load(f)
    
    card_data = prepare_card_data(briefing_data)
    render_cards(card_data)

if __name__ == "__main__":
    main()