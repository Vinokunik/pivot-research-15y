import yfinance as yf
import pandas as pd
import numpy as np
import itertools
import os
import json
import gspread
from google.oauth2.service_account import Credentials

# --- НАСТРОЙКИ СТРАТЕГИИ ---
SHEET_NAME = 'Pivot Vinokunik'
START_CAPITAL = 100000
RISK_PER_TRADE = 0.10  # 10% от капитала
TP_RATIO = 3.0         # Тейк в 2 раза больше стопа
TAX = 0.25
PERIOD = "15y"

def get_combinations():
    return [''.join(p) for p in itertools.product('+-', repeat=6)]

def analyze_trading():
    # 1. АВТОРИЗАЦИЯ
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds_dict = json.loads(os.environ.get('GOOGLE_CREDS'))
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    
    sh = gc.open(SHEET_NAME)
    try:
        worksheet = sh.worksheet('SPX500')
    except:
        worksheet = sh.get_worksheet(0)
    
    tickers = worksheet.col_values(1)[1:]
    print(f"Запуск торговой модели для {len(tickers)} тикеров...")

    all_trades = []
    combos_list = get_combinations()

    for ticker in tickers:
        try:
            df = yf.download(ticker, period=PERIOD, interval="1wk", progress=False)
            if df.empty or len(df) < 15: continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            h, l, o, c, v = df['High'], df['Low'], df['Open'], df['Close'], df['Volume']
            
            # Локальный баланс для этого тикера (имитация торговли по одной бумаге)
            current_balance = START_CAPITAL

            for i in range(4, len(df) - 2):
                is_high = (h.iloc[i-2] > h.iloc[i-4]) and (h.iloc[i-2] > h.iloc[i-3]) and \
                          (h.iloc[i-2] > h.iloc[i-1]) and (h.iloc[i-2] > h.iloc[i])
                is_low = (l.iloc[i-2] < l.iloc[i-4]) and (l.iloc[i-2] < l.iloc[i-3]) and \
                         (l.iloc[i-2] < l.iloc[i-1]) and (l.iloc[i-2] < l.iloc[i])

                if not (is_high or is_low): continue

                # Характеристики (Маска)
                v_win = v.iloc[i-4:i+1].values
                f1 = "+" if v.iloc[i] > v.iloc[i-4] else "-"
                f2 = "+" if ((h.iloc[i-4] < h.iloc[i-3]) and (h.iloc[i-1] > h.iloc[i])) else "-"
                f3 = "+" if v.iloc[i-2] == max(v_win) else "-"
                f4 = "+" if (c.iloc[i] < o.iloc[i]) else "-"
                f5 = "+" if (v.iloc[i-4] < v.iloc[i-3] < v.iloc[i-2]) else "-"
                f6 = "+" if (c.iloc[i] < l.iloc[i-4] if is_high else c.iloc[i] > h.iloc[i-4]) else "-"
                combo = f"{f1}{f2}{f3}{f4}{f5}{f6}"

                # Параметры сделки
                entry_price = o.iloc[i+1]
                # Стоп ставим по экстремуму свечи-пивота (i-2)
                sl_price = h.iloc[i-2] if is_high else l.iloc[i-2]
                risk_per_share = abs(entry_price - sl_price)
                
                if risk_per_share == 0: continue
                
                tp_price = entry_price - (risk_per_share * TP_RATIO) if is_high else entry_price + (risk_per_share * TP_RATIO)

                # Сумма входа (10% от капитала)
                position_size_dollars = current_balance * RISK_PER_TRADE
                num_shares = position_size_dollars / entry_price
                
                # Симуляция выхода по истории
                pnl = 0
                trade_closed = False
                for j in range(i+1, len(df)):
                    curr_h, curr_l = h.iloc[j], l.iloc[j]
                    
                    # Проверка стопа
                    if (is_high and curr_h >= sl_price) or (not is_high and curr_l <= sl_price):
                        pnl = -num_shares * risk_per_share
                        trade_closed = True
                    # Проверка тейка
                    elif (is_high and curr_l <= tp_price) or (not is_high and curr_h >= tp_price):
                        pnl = num_shares * risk_per_share * TP_RATIO
                        pnl *= (1 - TAX) # Налог на прибыль
                        trade_closed = True
                    
                    if trade_closed:
                        current_balance += pnl
                        all_trades.append({'Ticker': ticker, 'Combo': combo, 'PnL': pnl, 'Balance': current_balance})
                        break
        except: continue

    if not all_trades: return
    
    df_res = pd.DataFrame(all_trades)
    
    # Таблица 1: Итоговый баланс по тикерам и комбинациям
    tab1 = df_res.pivot_table(index='Ticker', columns='Combo', values='PnL', aggfunc='sum').fillna(0)
    
    # Таблица 2: Эффективность комбинаций (Агрегировано)
    tab2 = df_res.groupby('Combo')['PnL'].agg([
        ('Total_PnL', 'sum'),
        ('Final_Avg_Balance', lambda x: START_CAPITAL + x.sum()),
        ('Trade_Count', 'count'),
        ('Win_Rate', lambda x: (x > 0).mean() * 100)
    ]).sort_values('Total_PnL', ascending=False)

    tab1.to_csv("trade_model_tickers.csv")
    tab2.to_csv("trade_model_rating.csv")
    print("Торговая модель рассчитана.")

if __name__ == "__main__":
    analyze_trading()
