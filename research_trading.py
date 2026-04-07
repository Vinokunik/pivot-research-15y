import yfinance as yf
import pandas as pd
import numpy as np
import itertools
import os
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- НАСТРОЙКИ ---
SHEET_NAME = 'Pivot Vinokunik'
START_CAPITAL = 100000
RISK_PER_TRADE = 0.10  # 10% от капитала
TP_RATIO = 2.0
HOLD_PERIOD = 2
TAX = 0.25
PERIOD = "15y"

def analyze_all_strategies():
    # 1. АВТОРИЗАЦИЯ
    try:
        creds_dict = json.loads(os.environ.get('GOOGLE_CREDS'))
        creds = Credentials.from_service_account_info(
            creds_dict, 
            scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        )
        gc = gspread.authorize(creds)
        sh = gc.open(SHEET_NAME)
        worksheet = sh.get_worksheet(0)
        tickers = worksheet.col_values(1)[1:]
        print(f"Загружено тикеров: {len(tickers)}")
    except Exception as e:
        print(f"Ошибка авторизации или доступа к таблице: {e}")
        return

    timestamp = datetime.now().strftime("%y%m%d_%H%M")
    report_name = f"Report_{PERIOD}_{int(START_CAPITAL/1000)}k_TP{TP_RATIO}_{timestamp}"
    report_name = f"Report_{PERIOD}_{int(START_CAPITAL/1000)}k_TP{TP_RATIO}_H{HOLD_PERIOD}_R{int(RISK_PER_TRADE*100)}pct_{timestamp}"

    all_results = []
    # Сумма сделки = 10% от стартового капитала
    investment_amount = START_CAPITAL * RISK_PER_TRADE 

    for ticker in tickers:
        ticker = ticker.strip()
        try:
            df = yf.download(ticker, period=PERIOD, interval="1wk", progress=False)
            if df.empty or len(df) < 20: continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df = df.dropna()
            h, l, o, c, v = df['High'], df['Low'], df['Open'], df['Close'], df['Volume']

            for i in range(5, len(df) - (HOLD_PERIOD + 5)):
                # Поиск пивота (на свече i-2)
                is_high = (h.iloc[i-2] > h.iloc[i-4]) and (h.iloc[i-2] > h.iloc[i-3]) and \
                          (h.iloc[i-2] > h.iloc[i-1]) and (h.iloc[i-2] > h.iloc[i])
                is_low = (l.iloc[i-2] < l.iloc[i-4]) and (l.iloc[i-2] < l.iloc[i-3]) and \
                         (l.iloc[i-2] < l.iloc[i-1]) and (l.iloc[i-2] < l.iloc[i])

                if not (is_high or is_low): continue

                # Характеристики
                v_win = v.iloc[i-4:i+1].values
                f1 = "+" if v.iloc[i] > v.iloc[i-4] else "-"
                f2 = "+" if (h.iloc[i-4] < h.iloc[i-3] and h.iloc[i-1] > h.iloc[i]) else "-"
                f3 = "+" if v.iloc[i-2] == max(v_win) else "-"
                f4 = "+" if (c.iloc[i] < o.iloc[i] if is_high else c.iloc[i] > o.iloc[i]) else "-"
                f5 = "+" if (v.iloc[i-4] < v.iloc[i-3] < v.iloc[i-2]) else "-"
                f6 = "+" if (c.iloc[i] < l.iloc[i-4] if is_high else c.iloc[i] > h.iloc[i-4]) else "-"
                combo = f"{f1}{f2}{f3}{f4}{f5}{f6}"

                entry_p = o.iloc[i+1]
                sl_p = h.iloc[i-2] if is_high else l.iloc[i-2]
                risk_pct = abs(entry_p - sl_p) / entry_p
                if risk_pct == 0: continue

                # --- 1 & 2: TIME EXIT ---
                out_p_time = c.iloc[i + HOLD_PERIOD]
                # Процентное изменение цены
                change = (entry_p - out_p_time) / entry_p if is_high else (out_p_time - entry_p) / entry_p

                all_results.append({'Combo': combo, 'Strategy': 'Trend_Time', 'PnL': change * investment_amount})
                all_results.append({'Combo': combo, 'Strategy': 'Counter_Time', 'PnL': -change * investment_amount})

                # --- 3: TREND SL/TP ---
                tp_p_t = entry_p - (abs(entry_p - sl_p) * TP_RATIO) if is_high else entry_p + (abs(entry_p - sl_p) * TP_RATIO)
                pnl_t = 0
                for j in range(i+1, len(df)):
                    if (is_high and h.iloc[j] >= sl_p) or (not is_high and l.iloc[j] <= sl_p):
                        pnl_t = -investment_amount * risk_pct
                        break
                    if (is_high and l.iloc[j] <= tp_p_t) or (not is_high and h.iloc[j] >= tp_p_t):
                        pnl_t = investment_amount * (risk_pct * TP_RATIO) * (1 - TAX)
                        break
                if pnl_t != 0: all_results.append({'Combo': combo, 'Strategy': 'Trend_SLTP', 'PnL': pnl_t})

                # --- 4: COUNTER SL/TP ---
                # Для контр-тренда: Тейк там где был стоп, Стоп там где был тейк
                tp_p_c = entry_p + abs(entry_p - sl_p) if is_high else entry_p - abs(entry_p - sl_p)
                sl_p_c = entry_p - (abs(entry_p - sl_p) * TP_RATIO) if is_high else entry_p + (abs(entry_p - sl_p) * TP_RATIO)
                pnl_c = 0
                for j in range(i+1, len(df)):
                    if (is_high and l.iloc[j] <= sl_p_c) or (not is_high and h.iloc[j] >= sl_p_c):
                        pnl_c = -investment_amount * (risk_pct * TP_RATIO)
                        break
                    if (is_high and h.iloc[j] >= tp_p_c) or (not is_high and l.iloc[j] <= tp_p_c):
                        pnl_c = investment_amount * risk_pct * (1 - TAX)
                        break
                if pnl_c != 0: all_results.append({'Combo': combo, 'Strategy': 'Counter_SLTP', 'PnL': pnl_c})

        except Exception as e:
            print(f"Ошибка в {ticker}: {e}")

    # СОХРАНЕНИЕ
    if all_results:
        final_df = pd.DataFrame(all_results)
        file_name = f"{report_name}.xlsx"
        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            for strat in ['Trend_Time', 'Counter_Time', 'Trend_SLTP', 'Counter_SLTP']:
                strat_df = final_df[final_df['Strategy'] == strat]
                if not strat_df.empty:
                    res = strat_df.groupby('Combo')['PnL'].agg([
                        ('Total_Profit', 'sum'),
                        ('Win_Rate', lambda x: (x > 0).mean() * 100),
                        ('Count', 'count')
                    ]).sort_values('Total_Profit', ascending=False)
                    res.to_excel(writer, sheet_name=strat)
        print(f"Готово! Файл: {file_name}")
    else:
        print("Сделок не найдено.")

if __name__ == "__main__":
    analyze_all_strategies()
