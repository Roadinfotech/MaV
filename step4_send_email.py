"""
Step 4 통합 버전 (v5.0): MaV 하이엔드 이메일 발송기 (CI/CD Security Ready)
- Security: RESEND_API_KEY 환경 변수 분리
"""

import os
import json
import glob
import resend
import urllib.parse
from datetime import datetime

# [Security] 환경 변수에서 API Key 로드
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
FROM_EMAIL = "MaV <onboarding@resend.dev>"
SUBSCRIBERS = ["waymaker179@gmail.com"]

def load_briefing():
    json_files = sorted(glob.glob("mav_briefing_*.json"))
    if not json_files:
        print("  ❌ mav_briefing_*.json 파일 없음")
        return None
    with open(json_files[-1], "r", encoding="utf-8") as f:
        return json.load(f)

def get_sparkline_url(history_data, color_hex):
    if not history_data or len(history_data) < 2: return ""
    is_positive = (color_hex == "#16A34A")
    color = "rgb(22,163,74)" if is_positive else "rgb(220,38,38)"
    bg_color = "rgba(22,163,74,0.15)" if is_positive else "rgba(220,38,38,0.15)"
    
    chart_config = {
        "type": "sparkline",
        "data": {
            "datasets": [{
                "data": history_data,
                "borderColor": color,
                "backgroundColor": bg_color,
                "fill": True,
                "borderWidth": 4
            }]
        },
        "options": {"plugins": {"datalabels": {"display": False}}}
    }
    json_str = json.dumps(chart_config, separators=(',', ':'))
    encoded_cfg = urllib.parse.quote(json_str)
    return f"https://quickchart.io/chart?w=240&h=80&c={encoded_cfg}"

def build_email_html(data):
    ai = data.get("ai_insight", {})
    market = data.get("market_data", {})
    news_all = data.get("news", [])
    date_str = data.get("date", "")
    weekday = data.get("weekday", "")
    
    main_insight = str(ai.get("narrative", "시장 데이터 분석 대기 중")).replace("\n", "<br><br>")
    
    fg = data.get("fear_and_greed", {})
    try:
        fg_val = float(fg.get("value", 50))
    except:
        fg_val = 50
    fg_desc = fg.get("description", "Neutral").upper()
    arrow_pos = max(2, min(98, fg_val))
    
    def build_market_rows(categories):
        rows = ""
        for cat in categories:
            if cat in market:
                for name, d in market[cat].items():
                    color = "#16A34A" if d["change_pct"] >= 0 else "#DC2626"
                    bg_color = "#F0FDF4" if d["change_pct"] >= 0 else "#FEF2F2"
                    sign = "+" if d["change_pct"] > 0 else ""
                    
                    chart_url = get_sparkline_url(d.get("history", []), color)
                    chart_html = f'<img src="{chart_url}" alt="chart" style="width:75px; height:25px; display:block; margin:auto; border:none;">' if chart_url else ""

                    rows += f"""
                    <tr>
                        <td width="32%" style="padding:14px 0; border-bottom:1px solid #E2E8F0; color:#334155; font-size:13px; font-weight:700;">{name}</td>
                        <td width="23%" style="padding:14px 0; border-bottom:1px solid #E2E8F0; text-align:center;">{chart_html}</td>
                        <td width="25%" style="padding:14px 0; border-bottom:1px solid #E2E8F0; color:#0F172A; font-family:'Courier New',monospace; text-align:right; font-size:14px; font-weight:800;">{d['price']:,.2f}</td>
                        <td width="20%" style="padding:14px 0; border-bottom:1px solid #E2E8F0; text-align:right;">
                            <span style="background:{bg_color}; color:{color}; font-family:'Courier New',monospace; font-size:12px; font-weight:800; padding:4px 6px; border-radius:4px; display:inline-block;">{sign}{d['change_pct']:.2f}%</span>
                        </td>
                    </tr>"""
        return rows

    idx_rows = build_market_rows(["indices"])
    us_sec_rows = build_market_rows(["us_sectors"])
    kr_sec_rows = build_market_rows(["kr_sectors"])

    cal_raw = ai.get("economic_calendar", {})
    cal_html = ""
    if isinstance(cal_raw, dict):
        for region, events in cal_raw.items():
            flag = "🇺🇸 미국 일정 (US)" if region == "US" else "🇰🇷 한국 일정 (KR)"
            cal_html += f"<div style='font-size:13px; font-weight:800; color:#1E293B; margin:16px 0 8px 0;'>[{flag}]</div>"
            if isinstance(events, list) and len(events) > 0:
                for c in events:
                    cal_html += f"""
                    <div style="display:flex; padding:8px 0; border-bottom:1px solid #F1F5F9;">
                        <div style="width:70px; color:#2563EB; font-weight:700; font-size:13px;">{c.get('date','')}</div>
                        <div style="color:#334155; font-size:14px; font-weight:500;">{c.get('event','')}</div>
                    </div>
                    """
            else:
                cal_html += "<div style='color:#94A3B8; font-size:13px; padding-bottom:8px;'>예정된 주요 일정 없음</div>"

    analyzed_news_raw = ai.get("analyzed_news", [])
    analyzed_news_html = ""
    if isinstance(analyzed_news_raw, list):
        for n in analyzed_news_raw:
            if isinstance(n, dict):
                title = n.get("title", "")
                impact = n.get("impact", "")
                source = n.get("source", "News")
                analyzed_news_html += f"""
                <div style="margin-bottom:24px;">
                    <div style="color:#2563EB; font-size:11px; font-weight:800; margin-bottom:4px;">{source}</div>
                    <div style="color:#0F172A; font-size:16px; font-weight:700; margin-bottom:6px; line-height:1.4;">{title}</div>
                    <div style="color:#475569; font-size:14px; line-height:1.6; padding-left:12px; border-left:3px solid #94A3B8;">{impact}</div>
                </div>
                """

    news_by_source = {}
    for n in news_all:
        src = n.get("source", "Other")
        if src not in news_by_source: news_by_source[src] = []
        news_by_source[src].append(n)
        
    raw_news_html = ""
    for src, items in news_by_source.items():
        raw_news_html += f"<div style='font-size:13px; font-weight:800; color:#475569; margin:16px 0 8px 0;'>[{src}]</div>"
        for item in items:
            raw_news_html += f"<div style='font-size:13px; color:#64748B; margin-bottom:6px;'>▪ {item['title']}</div>"

    html = f"""
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0; padding:0; background-color:#F8FAFC; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background-color:#F8FAFC;">
<tr><td align="center" style="padding:40px 16px;">
<table width="640" cellpadding="0" cellspacing="0" style="background-color:#FFFFFF; border-radius:12px; border:1px solid #E2E8F0; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);">

    <tr><td style="padding:35px 40px; border-bottom:3px solid #1E293B;">
        <table width="100%" cellpadding="0" cellspacing="0"><tr>
            <td>
                <div style="font-family:'Courier New',monospace; font-size:32px; font-weight:900; color:#0F172A; letter-spacing:1px;">MaV<span style="color:#2563EB;">.</span></div>
            </td>
            <td style="text-align:right;">
                <div style="font-size:15px; font-weight:700; color:#334155;">{date_str}</div>
                <div style="font-size:12px; font-weight:700; color:#2563EB; letter-spacing:1px; margin-top:6px;">DAILY MACRO BRIEFING</div>
            </td>
        </tr></table>
    </td></tr>

    <tr><td style="padding:40px 40px 20px 40px;">
        <div style="font-size:12px; font-weight:800; color:#64748B; letter-spacing:2px; margin-bottom:14px;">THE BOTTOM LINE</div>
        <div style="font-size:17px; font-weight:600; color:#334155; line-height:1.75; border-left:4px solid #2563EB; padding-left:18px; background:#F8FAFC; padding-top:14px; padding-bottom:14px; border-radius:0 8px 8px 0;">
            {main_insight}
        </div>
    </td></tr>

    <tr><td style="padding:10px 40px 30px 40px;">
        <div style="font-size:12px; font-weight:800; color:#64748B; letter-spacing:2px; margin-bottom:16px; border-bottom:2px solid #F1F5F9; padding-bottom:10px;">MARKET SENTIMENT (FEAR & GREED)</div>
        
        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:4px;">
            <tr>
                <td width="{arrow_pos}%" style="text-align:right; padding:0; font-size:14px; color:#0F172A; line-height:1;">▼</td>
                <td width="{100 - arrow_pos}%" style="padding:0;"></td>
            </tr>
        </table>
        <div style="background: linear-gradient(90deg, #EF4444 0%, #F59E0B 50%, #10B981 100%); width:100%; height:12px; border-radius:6px;"></div>
        
        <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:8px;">
            <tr>
                <td width="33%" style="font-size:11px; font-weight:700; color:#EF4444; text-align:left;">Extreme Fear (0)</td>
                <td width="34%" style="font-size:15px; font-weight:800; color:#0F172A; text-align:center;">{fg_val} ({fg_desc})</td>
                <td width="33%" style="font-size:11px; font-weight:700; color:#10B981; text-align:right;">Extreme Greed (100)</td>
            </tr>
        </table>
    </td></tr>

    <tr><td style="padding:10px 40px 10px 40px;">
        <div style="font-size:12px; font-weight:800; color:#64748B; letter-spacing:2px; margin-bottom:12px; border-bottom:2px solid #F1F5F9; padding-bottom:10px;">GLOBAL INDICES (5-DAY)</div>
        <table width="100%" cellpadding="0" cellspacing="0" style="table-layout:fixed;">{idx_rows}</table>
    </td></tr>

    <tr><td style="padding:10px 40px 10px 40px;">
        <div style="font-size:12px; font-weight:800; color:#64748B; letter-spacing:2px; margin-bottom:12px; border-bottom:2px solid #F1F5F9; padding-bottom:10px;">US SECTOR ETF (5-DAY)</div>
        <table width="100%" cellpadding="0" cellspacing="0" style="table-layout:fixed;">{us_sec_rows}</table>
    </td></tr>

    <tr><td style="padding:10px 40px 30px 40px;">
        <div style="font-size:12px; font-weight:800; color:#64748B; letter-spacing:2px; margin-bottom:12px; border-bottom:2px solid #F1F5F9; padding-bottom:10px;">KR MAJOR ETF (5-DAY)</div>
        <table width="100%" cellpadding="0" cellspacing="0" style="table-layout:fixed;">{kr_sec_rows}</table>
    </td></tr>

    <tr><td style="padding:10px 40px 30px 40px;">
        <div style="font-size:12px; font-weight:800; color:#64748B; letter-spacing:2px; margin-bottom:12px; border-bottom:2px solid #F1F5F9; padding-bottom:10px;">ECONOMIC CALENDAR</div>
        {cal_html}
    </td></tr>

    <tr><td style="padding:10px 40px 20px 40px;">
        <div style="font-size:12px; font-weight:800; color:#64748B; letter-spacing:2px; margin-bottom:20px; border-bottom:2px solid #F1F5F9; padding-bottom:10px;">MACRO IMPACT ANALYSIS</div>
        {analyzed_news_html}
    </td></tr>

    <tr><td style="padding:10px 40px 40px 40px;">
        <div style="background:#F8FAFC; border:1px solid #E2E8F0; padding:20px; border-radius:8px;">
            <div style="font-size:11px; font-weight:800; color:#94A3B8; letter-spacing:1px; margin-bottom:10px;">FULL READING LIST</div>
            {raw_news_html}
        </div>
    </td></tr>

    <tr><td style="padding:30px 0; border-top:1px solid #E2E8F0; text-align:center; background-color:#F8FAFC; border-radius:0 0 12px 12px;">
        <div style="font-size:13px; font-weight:700; color:#64748B; letter-spacing:1px;">Market At a View | AI Pipeline</div>
        <div style="font-size:12px; color:#94A3B8; margin-top:6px;">Automated Financial Intelligence</div>
    </td></tr>

</table>
</td></tr>
</table>
</body>
</html>
"""
    return html

def send_email(data):
    print("\n📧 MaV 하이엔드 이메일 발송 준비")
    if not RESEND_API_KEY:
        return print("  ❌ RESEND_API_KEY 환경변수가 설정되지 않았습니다.")
    
    resend.api_key = RESEND_API_KEY.strip()
    html_body = build_email_html(data)
    
    ai = data.get("ai_insight", {})
    narrative = str(ai.get("narrative", ""))
    
    hint = narrative[:35] + "..." if len(narrative) > 35 else narrative
    subject = f"MaV | {data.get('date', '')} - {hint}"
    
    for sub in SUBSCRIBERS:
        try:
            params = {
                "from": FROM_EMAIL, 
                "to": [sub.strip()], 
                "subject": subject, 
                "html": html_body
            }
            resend.Emails.send(params)
            print(f"  ✅ {sub} 발송 성공")
        except Exception as e:
            print(f"  ❌ {sub} 발송 실패: {e}")

if __name__ == "__main__":
    data = load_briefing()
    if data: 
        send_email(data)