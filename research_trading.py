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
        ticker = ticker.strip() # Убираем лишние пробелы
        try:
            df = yf.download(ticker, period=PERIOD, interval="1wk", progress=False)
            if df.empty or len(df) < 15: 
                continue
            
            # Фикс мульти-индекса
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            df = df.dropna()
            h, l, o, c, v = df['High'], df['Low'], df['Open'], df['Close'], df['Volume']

            found_in_ticker = 0 # Счетчик для отладки

            # Смещаем цикл, чтобы всегда было место для проверки HOLD_PERIOD и свечей i-4
            for i in range(5, len(df) - (HOLD_PERIOD + 2)):
                # 1. Поиск пивота (на свече i-2)
                is_high = (h.iloc[i-2] > h.iloc[i-4]) and (h.iloc[i-2] > h.iloc[i-3]) and \
                          (h.iloc[i-2] > h.iloc[i-1]) and (h.iloc[i-2] > h.iloc[i])
                is_low = (l.iloc[i-2] < l.iloc[i-4]) and (l.iloc[i-2] < l.iloc[i-3]) and \
                         (l.iloc[i-2] < l.iloc[i-1]) and (l.iloc[i-2] < l.iloc[i])

                if not (is_high or is_low):
                    continue

                # 2. Упрощенный сбор характеристик (чтобы не упало)
                v_win = v.iloc[i-4:i+1].values
                
                # f1: Объем выше чем 4 свечи назад
                f1 = "+" if v.iloc[i] > v.iloc[i-4] else "-"
                # f2: Ступенька (упрощенно)
                f2 = "+" if (h.iloc[i-4] < h.iloc[i-3] and h.iloc[i-1] > h.iloc[i]) else "-"
                # f3: Пиковый объем на самом пивоте
                f3 = "+" if v.iloc[i-2] == max(v_win) else "-"
                # f4: Направление последней свечи (i)
                f4 = "+" if (c.iloc[i] < o.iloc[i] if is_high else c.iloc[i] > o.iloc[i]) else "-"
                # f5: Нарастающий объем к пивоту
                f5 = "+" if (v.iloc[i-4] < v.iloc[i-3] < v.iloc[i-2]) else "-"
                # f6: Пробой уровня 4-й свечи (Сила)
                f6 = "+" if (c.iloc[i] < l.iloc[i-4] if is_high else c.iloc[i] > h.iloc[i-4]) else "-"
                
                combo = f"{f1}{f2}{f3}{f4}{f5}{f6}"

                # 3. Расчет цен
                entry_p = o.iloc[i+1]
                sl_p = h.iloc[i-2] if is_high else l.iloc[i-2]
                risk_abs = abs(entry_p - sl_p)
                
                if risk_abs < 0.0001: continue # Защита от нулевого риска

                # --- СТРАТЕГИИ ---
                # 1 & 2: TIME EXIT (TREND & COUNTER)
                out_p_time = c.iloc[i + HOLD_PERIOD]
                res_trend_time = (entry_p - out_p_time) / entry_p if is_high else (out_p_time - entry_p) / entry_p
                
                all_results.append({'Combo': combo, 'Strategy': 'Trend_Time', 'PnL': res_trend_time * INVESTMENT})
                all_results.append({'Combo': combo, 'Strategy': 'Counter_Time', 'PnL': -res_trend_time * INVESTMENT})

                # 3: TREND SL/TP
                tp_p_t = entry_p - (risk_abs * TP_RATIO) if is_high else entry_p + (risk_abs * TP_RATIO)
                pnl_t_sltp = 0
                for j in range(i+1, len(df)):
                    if (is_high and h.iloc[j] >= sl_p) or (not is_high and l.iloc[j] <= sl_p):
                        pnl_t_sltp = -INVESTMENT * (risk_abs/entry_p)
                        break
                    if (is_high and l.iloc[j] <= tp_p_t) or (not is_high and h.iloc[j] >= tp_p_t):
                        pnl_t_sltp = INVESTMENT * (risk_abs*TP_RATIO/entry_p) * (1-TAX)
                        break
                all_results.append({'Combo': combo, 'Strategy': 'Trend_SLTP', 'PnL': pnl_t_sltp})

                # 4: COUNTER SL/TP
                tp_p_c = entry_p + risk_abs if is_high else entry_p - risk_abs
                sl_p_c = entry_p - (risk_abs * TP_RATIO) if is_high else entry_p + (risk_abs * TP_RATIO)
                pnl_c_sltp = 0
                for j in range(i+1, len(df)):
                    if (is_high and l.iloc[j] <= sl_p_c) or (not is_high and h.iloc[j] >= sl_p_c):
                        pnl_c_sltp = -INVESTMENT * (risk_abs*TP_RATIO/entry_p)
                        break
                    if (is_high and h.iloc[j] >= tp_p_c) or (not is_high and l.iloc[j] <= tp_p_c):
                        pnl_c_sltp = INVESTMENT * (risk_abs/entry_p) * (1-TAX)
                        break
                all_results.append({'Combo': combo, 'Strategy': 'Counter_SLTP', 'PnL': pnl_c_sltp})
                
                found_in_ticker += 1

            if found_in_ticker > 0:
                print(f"Тикер {ticker}: Найдено пивотов: {found_in_ticker}")

        except Exception as e:
            print(f"Ошибка в тикере {ticker}: {e}")
            continue

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
