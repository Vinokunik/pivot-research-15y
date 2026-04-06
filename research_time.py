import yfinance as yf
import pandas as pd
import numpy as np
import itertools
import os
import json
import gspread
from google.oauth2.service_account import Credentials

# --- НАСТРОЙКИ ---
SHEET_NAME = 'Pivot Vinokunik'
INVESTMENT = 1000
TAX = 0.25
HOLD_PERIOD = 2 
PERIOD = "15y"

def get_combinations():
    return [''.join(p) for p in itertools.product('+-', repeat=6)]

def calculate_max_drawdown(pnl_series):
    """Рассчитывает максимальную просадку для серии сделок"""
    cumulative = pnl_series.cumsum()
    running_max = cumulative.cummax()
    drawdown = cumulative - running_max
    return drawdown.min()

def analyze():
    # 1. АВТОРИЗАЦИЯ
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds_env = os.environ.get('GOOGLE_CREDS')
    if not creds_env:
        print("Ошибка: GOOGLE_CREDS не найдены")
        return
        
    creds_dict = json.loads(creds_env)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    
    sh = gc.open(SHEET_NAME)
    try:
        worksheet = sh.worksheet('SPX500')
    except:
        print("Лист SPX500 не найден, берем первый лист")
        worksheet = sh.get_worksheet(0)
        
    tickers = worksheet.col_values(1)[1:] 
    print(f"Загружено {len(tickers)} тикеров. Начинаем расчет за {PERIOD}...")

    all_trades = []
    combos_list = get_combinations()

    # 2. ЦИКЛ ПО ТИКЕРАМ
    for ticker in tickers:
        try:
            df = yf.download(ticker, period=PERIOD, interval="1wk", progress=False)
            if df.empty or len(df) < 10: continue
            
            # Исправление структуры yfinance (если есть мульти-индекс)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            h, l, o, c, v = df['High'], df['Low'], df['Open'], df['Close'], df['Volume']

            for i in range(4, len(df) - HOLD_PERIOD):
                # Поиск пивота на свече i-2
                is_high = (h.iloc[i-2] > h.iloc[i-4]) and (h.iloc[i-2] > h.iloc[i-3]) and \
                          (h.iloc[i-2] > h.iloc[i-1]) and (h.iloc[i-2] > h.iloc[i])
                is_low = (l.iloc[i-2] < l.iloc[i-4]) and (l.iloc[i-2] < l.iloc[i-3]) and \
                         (l.iloc[i-2] < l.iloc[i-1]) and (l.iloc[i-2] < l.iloc[i])

                if not (is_high or is_low): continue

                # Сбор 6 характеристик (Маска)
                v_win = v.iloc[i-4:i+1].values
                f1 = "+" if v.iloc[i] > v.iloc[i-4] else "-"
                f2 = "+" if ((h.iloc[i-4] < h.iloc[i-3]) and (h.iloc[i-1] > h.iloc[i])) else "-"
                f3 = "+" if v.iloc[i-2] == max(v_win) else "-"
                f4 = "+" if (c.iloc[i] < o.iloc[i]) else "-"
                f5 = "+" if (v.iloc[i-4] < v.iloc[i-3] < v.iloc[i-2]) else "-"
                f6 = "+" if (c.iloc[i] < l.iloc[i-4] if is_high else c.iloc[i] > h.iloc[i-4]) else "-"

                combo = f"{f1}{f2}{f3}{f4}{f5}{f6}"
                
                # Расчет PnL (Вход Open i+1, Выход Close i+HOLD)
                p_in = o.iloc[i+1]
                p_out = c.iloc[i+HOLD_PERIOD]
                
                # Изначально считаем по тренду пивота
                res = (p_out - p_in) / p_in if is_low else (p_in - p_out) / p_in
                
                pnl = res * INVESTMENT
                if pnl > 0: pnl *= (1 - TAX)
                
                all_trades.append({'Ticker': ticker, 'Combo': combo, 'PnL': pnl})
        except Exception as e:
            print(f"Ошибка в {ticker}: {e}")
            continue

    # 3. ФОРМИРОВАНИЕ ТАБЛИЦ
    if not all_trades:
        print("Сделок не найдено.")
        return

    df_results = pd.DataFrame(all_trades)
    
    # Таблица 1: Акции х Комбинации
    tab1 = df_results.pivot_table(index='Ticker', columns='Combo', values='PnL', aggfunc='sum').fillna(0)
    tab1 = tab1.reindex(columns=combos_list, fill_value=0)

    # Таблица 2: Рейтинг комбинаций с доп. метриками
    tab2 = df_results.groupby('Combo')['PnL'].agg([
        ('Total_Profit', 'sum'),
        ('Count', 'count'),
        ('WinRate', lambda x: (x > 0).mean() * 100),
        ('ProfitFactor', lambda x: x[x>0].sum() / abs(x[x<0].sum()) if x[x<0].sum() != 0 else x[x>0].sum()),
        ('MaxDrawdown', calculate_max_drawdown)
    ]).sort_values('Total_Profit', ascending=False)

    # 4. СОХРАНЕНИЕ
    tab1.to_csv("table_tickers.csv")
    tab2.to_csv("table_rating.csv")
    print("Расчет окончен. Файлы готовы.")

if __name__ == "__main__":
    analyze()
