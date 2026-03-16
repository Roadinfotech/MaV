"""
Step 5: MaV 프리미엄 릴스 생성기 (UI/BGM Upgrade Version)
- BGM 오디오 자동 믹싱 (bgm.mp3 파일 필요)
- 블룸버그 스타일 상단 타이틀 바 UI 추가
- 가독성을 극대화한 반투명 텍스트 박스 디자인
"""

import os
import json
import glob
import asyncio
from datetime import datetime
import PIL.Image

# [버그 패치] Pillow 호환성
if not hasattr(PIL.Image, 'ANTIALIAS'):
    PIL.Image.ANTIALIAS = PIL.Image.LANCZOS

# [중요] Windows ImageMagick 경로 강제 지정
os.environ["IMAGEMAGICK_BINARY"] = r"C:\ImageMagick-7.1.2-Q16-HDRI\magick.exe"

import edge_tts
from moviepy.editor import ImageClip, TextClip, AudioFileClip, CompositeVideoClip, concatenate_videoclips, ColorClip, CompositeAudioClip
from moviepy.audio.fx.all import audio_loop, volumex

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

async def generate_audio_async(text, filepath):
    voice = "ko-KR-InJoonNeural" # 한국어 남성 아나운서 톤
    communicate = edge_tts.Communicate(text, voice)
    await communicate.save(filepath)

def generate_audio(text, filename):
    filepath = os.path.join(OUTPUT_DIR, filename)
    asyncio.run(generate_audio_async(text, filepath))
    return filepath

def create_text_clip(text, audio_path, font_name='Malgun-Gothic-Bold'):
    audio = AudioFileClip(audio_path)
    
    # 문장이 너무 길면 가독성이 떨어지므로 3~4어절 단위로 줄바꿈
    words = text.split()
    formatted_text = "\n".join([" ".join(words[i:i+4]) for i in range(0, len(words), 4)])
    
    # 디자인 업그레이드: 텍스트 뒤에 어두운 반투명 박스(bg_color) 추가
    txt_clip = TextClip(
        formatted_text, 
        fontsize=85, 
        color='white', 
        font=font_name, 
        align='center',
        bg_color='rgba(0,0,0,0.6)', # 반투명 검은 박스
        size=(950, None),
        method='caption'
    )
    
    txt_clip = txt_clip.set_position('center').set_duration(audio.duration).set_audio(audio)
    return txt_clip

def make_reels():
    print("\n🎬 MaV 릴스 렌더링 파이프라인 가동 (Premium UI)")
    json_files = sorted(glob.glob("mav_briefing_*.json"))
    if not json_files:
        print("❌ JSON 데이터가 없습니다.")
        return
    
    with open(json_files[-1], "r", encoding="utf-8") as f:
        data = json.load(f)
        
    script = data.get("ai_insight", {}).get("reels_script", {})
    if not script:
        print("❌ 릴스 스크립트 데이터가 없습니다.")
        return
        
    sentences = [script.get("hook"), script.get("data"), script.get("context"), script.get("cta")]
    video_clips = []
    
    print("🎙️ 음성 합성 및 자막 클립 생성 중...")
    for i, text in enumerate(sentences):
        if not text: continue
        audio_file = f"temp_audio_{i}.mp3"
        audio_path = generate_audio(text, audio_file)
        clip = create_text_clip(text, audio_path, font_name='Malgun-Gothic-Bold')
        video_clips.append(clip)
        
    final_text_video = concatenate_videoclips(video_clips)
    
    print("🎨 배경 이미지 및 UI 오버레이 합성 중...")
    bg_path = os.path.join(OUTPUT_DIR, "mav_card1_main.png")
    
    # 1. 배경 설정 (히트맵 30% 밝기로 딥다크하게)
    if os.path.exists(bg_path):
        bg_clip = ImageClip(bg_path).resize(width=1080).crop(y1=0, y2=1920, width=1080)
        bg_clip = bg_clip.fl_image(lambda img: img * 0.25).set_duration(final_text_video.duration)
    else:
        bg_clip = ColorClip(size=(1080, 1920), color=(15, 23, 42)).set_duration(final_text_video.duration)

    # 2. 상단 헤더 UI (블룸버그 스타일)
    header_bg = ColorClip(size=(1080, 120), color=(29, 78, 216)).set_opacity(0.9).set_duration(final_text_video.duration)
    header_txt = TextClip("MaV | Market At a View", fontsize=50, color='white', font='Malgun-Gothic-Bold').set_position('center').set_duration(final_text_video.duration)
    header = CompositeVideoClip([header_bg, header_txt]).set_position(('center', 100))

    # 3. 오디오 믹싱 (TTS + BGM)
    final_audio = final_text_video.audio
    if os.path.exists("bgm.mp3"):
        print("🎵 BGM 파일 감지됨. 오디오 믹싱 적용 중...")
        bgm = AudioFileClip("bgm.mp3")
        # BGM을 영상 길이에 맞게 자르거나 루프시키고, 볼륨을 10%로 낮춤
        bgm = audio_loop(bgm, duration=final_text_video.duration)
        bgm = volumex(bgm, 0.1)
        final_audio = CompositeAudioClip([final_audio, bgm])
    else:
        print("⚠️ bgm.mp3 파일이 작업 폴더에 없습니다. 아나운서 음성만 적용됩니다.")

    final_text_video.audio = final_audio

    # 최종 합성
    final_video = CompositeVideoClip([bg_clip, header, final_text_video.set_position("center")])
    output_filename = os.path.join(OUTPUT_DIR, f"mav_reels_premium_{datetime.now().strftime('%Y%m%d')}.mp4")
    
    print("⚙️ 최종 프리미엄 MP4 렌더링 중...")
    final_video.write_videofile(output_filename, fps=30, codec="libx264", audio_codec="aac", preset="ultrafast", logger=None)
    print(f"✅ 릴스 생성 완료: {output_filename}")
    
    # 임시 파일 정리
    for f in glob.glob(os.path.join(OUTPUT_DIR, "temp_audio_*.mp3")):
        os.remove(f)

if __name__ == "__main__":
    make_reels()