import yfinance as yf
import pandas as pd
import numpy as np
import itertools
import os
import json
import gspread
import openpyxl
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- НАСТРОЙКИ СТРАТЕГИИ ---
SHEET_NAME = 'Pivot Vinokunik'
START_CAPITAL = 100000
RISK_PER_TRADE = 0.10
TP_RATIO = 4.0
HOLD_PERIOD = 1  # для выхода по времени
TAX = 0.25
PERIOD = "15y"

def get_combinations():
    return [''.join(p) for p in itertools.product('+-', repeat=6)]

def analyze_all_strategies():
    # 1. АВТОРИЗАЦИЯ
    creds_dict = json.loads(os.environ.get('GOOGLE_CREDS'))
    creds = Credentials.from_service_account_info(
        creds_dict, 
        scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    )
    gc = gspread.authorize(creds)
    sh = gc.open(SHEET_NAME)
    try:
        worksheet = sh.worksheet('SPX500')
    except:
        worksheet = sh.get_worksheet(0)
    
    tickers = worksheet.col_values(1)[1:]
    
    # Динамическое имя для идентификации теста
    timestamp = datetime.now().strftime("%y%m%d_%H%M")
    report_name = f"Report_{PERIOD}_{int(START_CAPITAL/1000)}k_TP{TP_RATIO}_H{HOLD_PERIOD}_{timestamp}"
    
    all_results = [] # Список для хранения всех сделок всех типов

    for ticker in tickers:
        try:
            df = yf.download(ticker, period=PERIOD, interval="1wk", progress=False)
            if df.empty or len(df) < 15: continue
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)

            h, l, o, c, v = df['High'], df['Low'], df['Open'], df['Close'], df['Volume']

            for i in range(4, len(df) - max(HOLD_PERIOD, 5)):
                is_high = (h.iloc[i-2] > h.iloc[i-4]) and (h.iloc[i-2] > h.iloc[i-3]) and \
                          (h.iloc[i-2] > h.iloc[i-1]) and (h.iloc[i-2] > h.iloc[i])
                is_low = (l.iloc[i-2] < l.iloc[i-4]) and (l.iloc[i-2] < l.iloc[i-3]) and \
                         (l.iloc[i-2] < l.iloc[i-1]) and (l.iloc[i-2] < l.iloc[i])
                if not (is_high or is_low): continue

                # Характеристики (Маска)
                v_win = v.iloc[i-4:i+1].values
                f = ["+" if v.iloc[i] > v.iloc[i-4] else "-",
                     "+" if (h.iloc[i-4] < h.iloc[i-3] and h.iloc[i-1] > h.iloc[i]) else "-",
                     "+" if v.iloc[i-2] == max(v_win) else "-",
                     "+" if (c.iloc[i] < o.iloc[i]) else "-",
                     "+" if (v.iloc[i-4] < v.iloc[i-3] < v.iloc[i-2]) else "-",
                     "+" if (c.iloc[i] < l.iloc[i-4] if is_high else c.iloc[i] > h.iloc[i-4]) else "-"]
                combo = "".join(f)

                entry_p = o.iloc[i+1]
                sl_p = h.iloc[i-2] if is_high else l.iloc[i-2]
                risk_abs = abs(entry_p - sl_p)
                if risk_abs == 0: continue
                
                # --- ЛОГИКА 4-х СТРАТЕГИЙ ---
                
                # 1. По тренду (Time Exit)
                out_p_time = c.iloc[i + HOLD_PERIOD]
                res_t = (entry_p - out_p_time) / entry_p if is_high else (out_p_time - entry_p) / entry_p
                all_results.append({'Combo': combo, 'Strategy': 'Trend_Time', 'PnL': res_t * INVESTMENT})

                # 2. Против тренда (Time Exit)
                res_ct = -res_t 
                all_results.append({'Combo': combo, 'Strategy': 'Counter_Time', 'PnL': res_ct * INVESTMENT})

                # 3. По тренду (SL/TP)
                tp_p = entry_p - (risk_abs * TP_RATIO) if is_high else entry_p + (risk_abs * TP_RATIO)
                pnl_sl_tp = 0
                for j in range(i+1, len(df)):
                    curr_h, curr_l = h.iloc[j], l.iloc[j]
                    if (is_high and curr_h >= sl_p) or (not is_high and curr_l <= sl_p): # Stop
                        pnl_sl_tp = -INVESTMENT * (risk_abs/entry_p)
                        break
                    if (is_high and curr_l <= tp_p) or (not is_high and curr_h >= tp_p): # Take
                        pnl_sl_tp = INVESTMENT * (risk_abs*TP_RATIO/entry_p) * (1-TAX)
                        break
                all_results.append({'Combo': combo, 'Strategy': 'Trend_SLTP', 'PnL': pnl_sl_tp})

                # 4. Против тренда (SL/TP)
                # Здесь стоп и тейк меняются местами относительно точки входа
                tp_p_c = entry_p + risk_abs if is_high else entry_p - risk_abs # Тейк там, где был стоп
                sl_p_c = entry_p - (risk_abs * TP_RATIO) if is_high else entry_p + (risk_abs * TP_RATIO) # Стоп там, где был тейк
                pnl_c_sltp = 0
                for j in range(i+1, len(df)):
                    curr_h, curr_l = h.iloc[j], l.iloc[j]
                    if (is_high and curr_l <= sl_p_c) or (not is_high and curr_h >= sl_p_c): # Stop
                        pnl_c_sltp = -INVESTMENT * (risk_abs*TP_RATIO/entry_p)
                        break
                    if (is_high and curr_h >= tp_p_c) or (not is_high and curr_l <= tp_p_c): # Take
                        pnl_c_sltp = INVESTMENT * (risk_abs/entry_p) * (1-TAX)
                        break
                all_results.append({'Combo': combo, 'Strategy': 'Counter_SLTP', 'PnL': pnl_c_sltp})

        except: continue

   # --- ПРОВЕРКА НА ПУСТОТУ ПЕРЕД СОХРАНЕНИЕМ ---
    if all_results:
        final_df = pd.DataFrame(all_results)
        
        # Динамическое имя файла (исправлено)
        file_name = f"{report_name}.xlsx"
        
        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            found_any = False
            for strat in ['Trend_Time', 'Counter_Time', 'Trend_SLTP', 'Counter_SLTP']:
                # Фильтруем данные по стратегии
                strat_df = final_df[final_df['Strategy'] == strat]
                
                if not strat_df.empty:
                    report = strat_df.groupby('Combo')['PnL'].agg([
                        ('Total_Profit', 'sum'),
                        ('Win_Rate', lambda x: (x > 0).mean() * 100),
                        ('Count', 'count')
                    ]).sort_values('Total_Profit', ascending=False)
                    
                    report.to_excel(writer, sheet_name=strat)
                    found_any = True
            
            # Если вдруг ни одна стратегия не дала результатов, создаем пустой лист для корректного сохранения
            if not found_any:
                pd.DataFrame([["No data found"]]).to_excel(writer, sheet_name="Empty")
                
        print(f"Исследование завершено успешно: {file_name}")
    else:
        print("ВНИМАНИЕ: Сделок не найдено. Проверьте список тикеров или условия пивотов.")
    analyze_all_strategies()
