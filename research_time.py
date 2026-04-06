import yfinance as yf
import pandas as pd
import numpy as np
import itertools
import os
import json
import gspread
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# --- НАСТРОЙКИ ---
SHEET_NAME = 'Pivot Vinokunik'
INVESTMENT = 1000
TAX = 0.25
HOLD_PERIOD = 2 # Выход через 2 недели
PERIOD = "15y"

def get_combinations():
    return [''.join(p) for p in itertools.product('+-', repeat=6)]

def analyze():
    # Авторизация Google
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds_dict = json.loads(os.environ.get('GOOGLE_CREDS'))
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open(SHEET_NAME)
    tickers = sh.get_worksheet(0).col_values(1)[1:]

    all_trades = []
    combos_list = get_combinations()

    for ticker in tickers:
        try:
            df = yf.download(ticker, period=PERIOD, interval="1wk", progress=False)
            if len(df) < 10: continue
            
            # Векторизованные проверки (для скорости)
            h = df['High']
            l = df['Low']
            o = df['Open']
            c = df['Close']
            v = df['Volume']

            for i in range(4, len(df) - HOLD_PERIOD):
                # Поиск пивота (на свече i-2)
                is_high = (h.iloc[i-2] > h.iloc[i-4]) and (h.iloc[i-2] > h.iloc[i-3]) and \
                          (h.iloc[i-2] > h.iloc[i-1]) and (h.iloc[i-2] > h.iloc[i])
                is_low = (l.iloc[i-2] < l.iloc[i-4]) and (l.iloc[i-2] < l.iloc[i-3]) and \
                         (l.iloc[i-2] < l.iloc[i-1]) and (l.iloc[i-2] < l.iloc[i])

                if not (is_high or is_low): continue

                # Сбор 6 характеристик
                v_window = v.iloc[i-4:i+1].values
                c_window = c.iloc[i-4:i+1].values
                o_window = o.iloc[i-4:i+1].values
                
                f1 = "+" if v.iloc[i] > v.iloc[i-4] else "-" # Vol+
                f2 = "+" if ((h.iloc[i-4] < h.iloc[i-3]) and (h.iloc[i-1] > h.iloc[i])) else "-" # Step (упрощенно)
                f3 = "+" if v.iloc[i-2] == max(v_window) else "-" # Peak Vol
                f4 = "+" if (c.iloc[i] < o.iloc[i]) else "-" # Cand (последняя медвежья для High)
                f5 = "+" if (v.iloc[i-4] < v.iloc[i-3] < v.iloc[i-2]) else "-" # IdealV
                f6 = "+" if (c.iloc[i] < l.iloc[i-4]) else "-" # STR (для High)

                combo = f"{f1}{f2}{f3}{f4}{f5}{f6}"
                
                # Расчет прибыли
                p_in = o.iloc[i+1]
                p_out = c.iloc[i+HOLD_PERIOD]
                res = (p_out - p_in) / p_in if is_low else (p_in - p_out) / p_in
                
                pnl = res * INVESTMENT
                if pnl > 0: pnl *= (1 - TAX)
                
                all_trades.append({'Ticker': ticker, 'Combo': combo, 'PnL': pnl})
        except: continue

    df_results = pd.DataFrame(all_trades)
    
    # Таблица 1: Акции х Комбинации (Net Profit)
    tab1 = df_results.pivot_table(index='Ticker', columns='Combo', values='PnL', aggfunc='sum').fillna(0)
    tab1 = tab1.reindex(columns=combos_list, fill_value=0)

    # Таблица 2: Рейтинг комбинаций
    tab2 = df_results.groupby('Combo')['PnL'].agg([
        ('Total_Profit', 'sum'),
        ('Count', 'count'),
        ('WinRate', lambda x: (x > 0).mean() * 100)
    ]).sort_values('Total_Profit', ascending=False)

    # Сохранение
    tab1.to_csv("table_tickers.csv")
    tab2.to_csv("table_rating.csv")

if __name__ == "__main__":
    analyze()
