import pytesseract
import pyautogui
from PIL import ImageGrab
import time

def print_account_via_screenshot():
    print("📸 正在截取屏幕读取账户信息...")
    
    # 等待2秒，让你有时间把鼠标移开
    time.sleep(2)
    
    # 1. 截取整个屏幕（因为你的窗口在桌面上）
    screenshot = ImageGrab.grab()
    
    # 2. 使用OCR识别图片上的所有文字
    try:
        # 注意：普通识别就能搞定，不需要精准定位
        text = pytesseract.image_to_string(screenshot)
        print("\n📝 识别到的屏幕内容：")
        print("-" * 30)
        
        # 3. 筛选出我们关心的关键词
        keywords = ["总资产", "可用", "余额", "市值", "持仓", "盈亏", "收益率", "200000"]
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            # 只要行里包含关键词，就打印出来
            if any(key in line for key in keywords):
                print(f"✅ {line}")
                
        print("-" * 30)
        print("✅ 读取完成！")
        
    except Exception as e:
        print(f"❌ 读取出错：{e}")
        print("请尝试运行：pip install pytesseract pillow pyautogui")

