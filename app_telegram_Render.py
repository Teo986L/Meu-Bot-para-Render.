"""
Deriv Signal Generator - SISTEMA 6 TIMEFRAMES COMPLETO
VERSÃO RENDER + TELEGRAM COM TIMEFRAME ESPECÍFICO
🔍 ANÁLISE POR TIMEFRAME SELECIONADO VIA TELEGRAM
"""

import json
import time
import threading
import math
import websocket
from datetime import datetime, time as dt_time
import logging
import sys
import csv
import os
from flask import Flask
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import asyncio
import random

# ========== CONFIGURAÇÕES ORIGINAIS COMPLETAS ==========

WS_ENDPOINT = "wss://ws.binaryws.com/websockets/v3?app_id=1089"
API_TOKEN = "1Jd2sESxdZ24Luv"
SYMBOL = "frxXAUUSD"
CANDLE_COUNT = 700

# ✅✅✅ SISTEMA DE MODOS TRADING ATUALIZADO
TRADING_MODE = "PADRÃO"  # "CONSERVADOR" | "PADRÃO" | "AGGRESSIVO"

# ✅✅✅ CONFIGURAÇÕES POR MODO DE TRADING
if TRADING_MODE == "CONSERVADOR":
    PROB_BUY_THRESHOLD = 0.58   # ✅ MAIS RESTRITIVO
    PROB_SELL_THRESHOLD = 0.42  # ✅ MAIS RESTRITIVO
    MIN_CALL_CONFIRMATIONS = 5   # ✅ 5+ de 6 timeframes
    MIN_PUT_CONFIRMATIONS = 4    # ✅ 4+ de 6 timeframes
    print("🎯 MODO CONSERVADOR: Limiares altos, confirmações máximas")
    
elif TRADING_MODE == "PADRÃO":
    PROB_BUY_THRESHOLD = 0.55   # ✅ EQUILIBRADO
    PROB_SELL_THRESHOLD = 0.45  # ✅ EQUILIBRADO  
    MIN_CALL_CONFIRMATIONS = 4   # ✅ 4+ de 6 timeframes
    MIN_PUT_CONFIRMATIONS = 3    # ✅ 3+ de 6 timeframes
    print("🎯 MODO PADRÃO: Limiares equilibrados, confirmações balanceadas")
    
else:  # AGGRESSIVO
    PROB_BUY_THRESHOLD = 0.52   # ✅ MENOS RESTRITIVO
    PROB_SELL_THRESHOLD = 0.48  # ✅ MENOS RESTRITIVO
    MIN_CALL_CONFIRMATIONS = 3   # ✅ 3+ de 6 timeframes  
    MIN_PUT_CONFIRMATIONS = 2    # ✅ 2+ de 6 timeframes
    print("🎯 MODO AGGRESSIVO: Limiares baixos, confirmações mínimas")

# RSI Filter
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# ✅✅✅ CORREÇÃO: FILTRO RSI PARA MERCADOS EXTREMOS
RSI_EXTREME_OVERSOLD = 15   # ✅ RSI < 15 indica ALTA POTENCIAL (OVERSOLD)
RSI_EXTREME_OVERBOUGHT = 85 # ✅ RSI > 85 indica BAIXA POTENCIAL (OVERBOUGHT)

# Risk management
MAX_DAILY_LOSS_PCT = 2.0
MAX_POSITION_SIZE_PCT = 5.0
RISK_PER_TRADE_PCT = 1.0
VOLATILITY_FILTER_MULTIPLIER = 3.0

# Multicultural Analysis Config
VEDIC_CYCLE_PERIOD = 9
THAI_CYCLE_PERIOD = 6

# ✅✅✅ ATUALIZADO: SISTEMA 6 TIMEFRAMES HIERÁRQUICOS
TIMEFRAMES = {
    'M1': 60,      # 🌪️  MICRO-TENDÊNCIA
    'M5': 300,     # 💨  MOMENTUM
    'M15': 900,    # 💦  CURTO PRAZO  
    'M30': 1800,   # 💧  MÉDIO PRAZO
    'H1': 3600,    # 🌊  LONGO PRAZO
    'H4': 14400    # 🌋  TENDÊNCIA PRINCIPAL
}

# Configurações Render + Telegram
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
PORT = int(os.environ.get('PORT', 5000))

app = Flask(__name__)

# ========== SISTEMA DE ARMAZENAMENTO DE CANDLES ==========

class CandleManager:
    def __init__(self):
        self.candles_data = {}
        self.last_cleanup = datetime.now().date()
        self.lock = threading.Lock()
    
    def add_candles(self, timeframe, candles):
        """Adiciona candles para um timeframe específico"""
        with self.lock:
            if timeframe not in self.candles_data:
                self.candles_data[timeframe] = []
            
            # Adiciona novos candles mantendo apenas os mais recentes
            self.candles_data[timeframe] = candles[-CANDLE_COUNT:]
            
            print(f"📊 Candles armazenados para {timeframe}: {len(self.candles_data[timeframe])}")
    
    def get_candles(self, timeframe):
        """Obtém candles para um timeframe específico"""
        with self.lock:
            return self.candles_data.get(timeframe, [])
    
    def cleanup_old_data(self):
        """Limpa todos os dados às 23h de Angola (22h UTC)"""
        now_utc = datetime.utcnow()
        angola_time = now_utc.replace(hour=22, minute=0, second=0, microsecond=0)
        
        if now_utc.date() > self.last_cleanup and now_utc >= angola_time:
            with self.lock:
                self.candles_data = {}
                self.last_cleanup = now_utc.date()
                print("🧹 Dados de candles limpos às 23h de Angola")
                return True
        return False

# Instância global do gerenciador de candles
candle_manager = CandleManager()

# ========== FUNÇÕES ORIGINAIS COMPLETAS ==========

# ✅✅✅ ATUALIZADO: VERIFICAÇÃO DE CONFIRMAÇÃO CALL PARA 6 TIMEFRAMES
def verificar_confirmacao_call(timeframe_signals):
    """✅✅✅ ATUALIZADO: Confirmação com 6 timeframes"""
    bull_confirmations = 0
    bear_confirmations = 0
    
    for tf, data in timeframe_signals.items():
        if data['signal'] == "BULLISH" and data['strength'] > 0.6:
            bull_confirmations += 1
        elif data['signal'] == "BEARISH" and data['strength'] > 0.6:
            bear_confirmations += 1
    
    print(f"✅ Confirmações CALL: {bull_confirmations}/6, PUT: {bear_confirmations}/6")
    
    # ✅ CALL precisa de mais confirmações (4+ em 6 no modo PADRÃO)
    if bull_confirmations >= MIN_CALL_CONFIRMATIONS:
        return True, f"CALL confirmado por {bull_confirmations}/6 timeframes"
    else:
        return False, f"CALL insuficiente: {bull_confirmations}/6 confirmações (mínimo {MIN_CALL_CONFIRMATIONS})"

# ✅✅✅ PATCH DE BALANCEAMENTO
def balancear_thresholds():
    """✅ CORREÇÃO: Balancear thresholds para permitir mais sinais CALL"""
    print("🔧 APLICANDO CORREÇÃO: Balanceando thresholds CALL/PUT")
    print(f"🎯 THRESHOLDS CORRIGIDOS: CALL ≥{PROB_BUY_THRESHOLD:.0%}, PUT ≤{PROB_SELL_THRESHOLD:.0%}")
    print(f"🎯 CONFIRMAÇÕES: CALL precisa de {MIN_CALL_CONFIRMATIONS}+, PUT precisa de {MIN_PUT_CONFIRMATIONS}+")
    
balancear_thresholds()

# ✅✅✅ ATUALIZADO: SISTEMA MARÉ/ONDAS/ESPUMA/MOMENTO/MICRO - Pesos hierárquicos
def get_dynamic_weights(rsi_values):
    """🎯 SISTEMA 6 TIMEFRAMES - Análise Hierárquica Completa"""
    base_weights = {
        'H4': 0.22,   # 🌋 MARÉ PRINCIPAL (22%)
        'H1': 0.20,   # 🌊 ONDAS LONGA (20%)
        'M30': 0.18,  # 💧 ONDAS MÉDIA (18%) 
        'M15': 0.15,  # 💦 ONDAS CURTA (15%)
        'M5': 0.13,   # 💨 MOMENTUM (13%)
        'M1': 0.12    # 🌪️ ALERTA PRECOCE (12%)
    }
    return base_weights

# ✅✅✅ ATUALIZADO: SISTEMA MARÉ/ONDAS PARA 6 TIMEFRAMES
def check_trend_alignment(timeframe_signals):
    """
    ✅✅✅ ATUALIZADO: Agora com 6 timeframes hierárquicos
    """
    # Verificar se temos os timeframes críticos
    critical_tfs = ['H4', 'H1', 'M30', 'M15']
    if not all(tf in timeframe_signals for tf in critical_tfs):
        return "INDETERMINADO", False, "Dados insuficientes"
    
    h4_signal = timeframe_signals['H4']['signal']
    h1_signal = timeframe_signals['H1']['signal']
    m30_signal = timeframe_signals['M30']['signal']
    m15_signal = timeframe_signals['M15']['signal']
    
    h4_strength = timeframe_signals['H4']['strength']
    h1_strength = timeframe_signals['H1']['strength']
    m30_strength = timeframe_signals['M30']['strength']
    m15_strength = timeframe_signals['M15']['strength']
    
    # ✅ CRITÉRIO 1: MARÉ E ONDAS PRINCIPAIS ALINHADAS (MELHOR CENÁRIO)
    if h4_signal == h1_signal == m30_signal and h4_signal != "NEUTRAL":
        if h4_strength > 0.6 and h1_strength > 0.6 and m30_strength > 0.6:
            return "ALINHADO-PERFEITO", True, f"🎯 ALINHAMENTO PERFEITO: Maré (H4), Ondas (H1) e Média (M30) todos {h4_signal}"
        else:
            return "ALINHADO", True, f"✅ ALINHADO: Maré, Ondas e Média todos {h4_signal}"
    
    # ✅ CRITÉRIO 2: MARÉ + ONDAS ALINHADAS (BOM CENÁRIO)
    elif h4_signal == h1_signal and h4_signal != "NEUTRAL":
        if h4_strength > 0.6 and h1_strength > 0.6:
            return "ALINHADO-FORTE", True, f"✅ ALINHAMENTO FORTE: Maré (H4) e Ondas (H1) ambos {h4_signal}"
        else:
            return "ALINHADO", True, f"✅ ALINHADO: Maré (H4) e Ondas (H1) ambos {h4_signal}"
    
    # ✅ CRITÉRIO 3: UM NEUTRO MAS OUTROS FORTES (OPERÁVEL COM CAUTELA)
    elif ((h4_signal == "NEUTRAL" and h1_signal != "NEUTRAL" and h1_strength > 0.7) or
          (h1_signal == "NEUTRAL" and h4_signal != "NEUTRAL" and h4_strength > 0.7)):
        return "PARCIAL", True, f"⚠️ ALINHAMENTO PARCIAL: {h4_signal}(H4) vs {h1_signal}(H1) - Operável com cautela"
    
    # ✅ CRITÉRIO 4: CONTRADIÇÃO FORTE (NÃO OPERAR EM QUALQUER MODO)
    elif h4_signal != h1_signal and h4_signal != "NEUTRAL" and h1_signal != "NEUTRAL":
        if h4_strength > 0.6 and h1_strength > 0.6:
            return "DIVERGENTE", False, f"🚫 BLOQUEADO: Contradição forte - Maré (H4={h4_signal}) vs Ondas (H1={h1_signal})"
        else:
            # Em modo AGGRESSIVO, permite com alerta
            if TRADING_MODE == "AGGRESSIVO":
                return "DIVERGENTE-AGGRESSIVO", True, f"⚠️ MODO AGGRESSIVO: Maré e Ondas opostas - {h4_signal}(H4) vs {h1_signal}(H1)"
            else:
                return "DIVERGENTE", False, f"🚫 BLOQUEADO: Maré e Ondas em direções opostas - {h4_signal}(H4) vs {h1_signal}(H1)"
    
    # ✅✅✅ CRITÉRIO 5: AMBOS NEUTROS (AGGRESSIVO DEVE PERMITIR)
    elif h4_signal == "NEUTRAL" and h1_signal == "NEUTRAL":
        if TRADING_MODE == "AGGRESSIVO":
            # ✅✅✅ ATUALIZADO: Verificar se há tendência forte nos timeframes menores
            strong_trends_medium = sum(1 for tf, data in timeframe_signals.items() 
                                     if data['strength'] > 0.7 and tf in ['M30', 'M15'])
            strong_trends_short = sum(1 for tf, data in timeframe_signals.items() 
                                    if data['strength'] > 0.7 and tf in ['M5', 'M1'])
            
            # Se timeframes menores tem trend forte, permite operar
            if strong_trends_medium >= 2 or strong_trends_short >= 2:
                return "NEUTRO-AGGRESSIVO", True, "🔓 MODO AGGRESSIVO: Trend forte em timeframes menores compensa Maré/Ondas neutras"
            else:
                return "NEUTRO-AGGRESSIVO", True, "🔓 MODO AGGRESSIVO: Operando mesmo com Maré/Ondas neutras"
        elif TRADING_MODE == "PADRÃO":
            # Verificar se trend é forte nos timeframes médios
            strong_trends = sum(1 for tf, data in timeframe_signals.items() 
                              if data['strength'] > 0.7 and tf in ['M30', 'M15', 'M5'])
            if strong_trends >= 3:
                return "NEUTRO-PADRÃO", True, "⚠️ MODO PADRÃO: Trend forte compensa Maré/Ondas neutras"
            else:
                return "NEUTRO", False, "🚫 BLOQUEADO: Maré e Ondas ambos neutros - trend insuficiente"
        else:  # CONSERVADOR
            return "NEUTRO", False, "🚫 BLOQUEADO: Maré e Ondas ambos neutros - mercado indeciso"
    
    # ✅ CRITÉRIO 6: CASOS RESIDUAIS (AVALIAR COM CAUTELA)
    else:
        if TRADING_MODE == "AGGRESSIVO":
            return "FRACO-AGGRESSIVO", True, f"🔓 MODO AGGRESSIVO: Alinhamento fraco - {h4_signal}(H4) vs {h1_signal}(H1)"
        else:
            return "FRACO", True, f"⚠️ ALINHAMENTO FRACO: {h4_signal}(H4) vs {h1_signal}(H1) - Avaliar com cautela"

# ✅✅✅ ATUALIZADO: SISTEMA DE QUALIDADE PARA 6 TIMEFRAMES
def analyze_consensus_quality(timeframe_signals):
    """
    ✅✅✅ ATUALIZADO: Qualidade com 6 timeframes
    Retorna: (status_qualidade, pode_operar, mensagem)
    """
    if not timeframe_signals:
        return "INDETERMINADO", False, "Sem dados"
    
    # Contar sinais nos 6 timeframes
    signals = [data['signal'] for data in timeframe_signals.values()]
    bull_count = signals.count('BULLISH')
    bear_count = signals.count('BEARISH')
    neutral_count = signals.count('NEUTRAL')
    
    total = len(signals)
    
    # ✅✅✅ NOVOS CRITÉRIOS PARA 6 TIMEFRAMES:
    # QUALIDADE ALTÍSSIMA: 5-6 concordantes
    if bull_count >= 5 or bear_count >= 5:
        direction = "BULL" if bull_count >= 5 else "BEAR"
        return "ALTÍSSIMA", True, f"🎯 Qualidade altíssima: {max(bull_count, bear_count)}/6 {direction}"
    
    # QUALIDADE ALTA: 4 concordantes  
    elif bull_count >= 4 or bear_count >= 4:
        direction = "BULL" if bull_count >= 4 else "BEAR"
        return "ALTA", True, f"🎯 Alta qualidade: {max(bull_count, bear_count)}/6 {direction}"
    
    # QUALIDADE MÉDIA: 3 concordantes = OPERÁVEL
    elif bull_count >= 3 or bear_count >= 3:
        direction = "BULL" if bull_count >= 3 else "BEAR" 
        return "MÉDIA", True, f"✅ Qualidade média: {max(bull_count, bear_count)}/6 {direction}"
    
    # QUALIDADE BAIXA: Muitos neutros = NÃO OPERAR
    elif neutral_count >= 3:
        # ✅✅✅ SISTEMA DE MODOS: Em modo AGGRESSIVO, permite com mais neutros
        if TRADING_MODE == "AGGRESSIVO" and (bull_count == 2 or bear_count == 2):
            return "RUÍDO-AGGRESSIVO", True, f"⚠️ MODO AGGRESSIVO: {neutral_count}/6 neutros mas algum sinal"
        else:
            return "RUÍDO", False, f"🚫 Muitos neutros: {neutral_count}/6 - Mercado indeciso"
    
    # QUALIDADE BAIXA: Divergência = NÃO OPERAR
    else:
        # ✅✅✅ SISTEMA DE MODOS: Em modo AGGRESSIVO, permite divergência
        if TRADING_MODE == "AGGRESSIVO" and total >= 4:
            return "DIVERGENTE-AGGRESSIVO", True, f"⚠️ MODO AGGRESSIVO: Divergência Bull:{bull_count}, Bear:{bear_count}"
        else:
            return "BAIXA", False, f"🚫 Divergência: Bull:{bull_count}, Bear:{bear_count}, Neutro:{neutral_count}"

# ✅✅✅ NOVA FUNÇÃO: IDENTIFICAÇÃO VISUAL DOS TIMEFRAMES
def get_timeframe_role(tf):
    """🎯 Identificar cada timeframe no sistema hierárquico"""
    roles = {
        'H4': "🌋 MARÉ PRINCIPAL",
        'H1': "🌊 ONDAS LONGA", 
        'M30': "💧 ONDAS MÉDIA",
        'M15': "💦 ONDAS CURTA",
        'M5': "💨 MOMENTUM",
        'M1': "🌪️ ALERTA PRECOCE"
    }
    return roles.get(tf, tf)

# ✅✅✅ CORREÇÃO CRÍTICA COMPLETA: LÓGICA DE SINAIS CORRIGIDA
def corrigir_logica_sinal(rsi, trend, macd, bears_power, bulls_power, current_price, previous_price):
    """✅✅✅ CORREÇÃO COMPLETA: RSI baixo indica ALTA potencial, RSI alto indica BAIXA potencial"""
    
    # ✅ CONDIÇÃO 1: RSI EXTREMAMENTE BAIXO = ALTA POTENCIAL (OVERSOLD)
    if rsi < RSI_EXTREME_OVERSOLD:
        if trend == "BULLISH" and macd > 0 and bulls_power > 10:
            print(f"  🚨 RSI EXTREMO {rsi:.1f} + BULLISH = SINAL FORTE DE ALTA")
            return "BULLISH", 0.85
        elif trend == "BEARISH":
            return "NEUTRAL", 0.5  # Contradição, melhor não operar
        else:
            return "BULLISH", 0.7  # ✅ Tendência de alta com RSI oversold
    
    # ✅ CONDIÇÃO 2: RSI EXTREMAMENTE ALTO = BAIXA POTENCIAL (OVERBOUGHT)
    elif rsi > RSI_EXTREME_OVERBOUGHT:
        if trend == "BEARISH" and macd < 0 and bears_power < -10:
            print(f"  🚨 RSI EXTREMO {rsi:.1f} + BEARISH = SINAL FORTE DE QUEDA")
            return "BEARISH", 0.85
        elif trend == "BULLISH":
            return "NEUTRAL", 0.5  # Contradição, melhor não operar
        else:
            return "BEARISH", 0.7  # ✅ Tendência de baixa com RSI overbought
    
    # ✅ CONDIÇÃO 3: ALTA RECENTE + BULLISH = FORTE SINAL CALL
    elif current_price > previous_price and trend == "BULLISH":
        if rsi < 60:  # Não está overbought ainda
            return "BULLISH", 0.75
        else:
            return "BULLISH", 0.65
    
    # ✅ CONDIÇÃO 4: QUEDA RECENTE + BEARISH = FORTE SINAL PUT  
    elif current_price < previous_price and trend == "BEARISH":
        if rsi > 40:  # Não está oversold ainda
            return "BEARISH", 0.75
        else:
            return "BEARISH", 0.65
    
    # ✅ CONDIÇÃO 5: MERCADO NORMAL
    elif rsi < RSI_OVERSOLD and trend == "BULLISH":
        return "BULLISH", 0.7
    elif rsi > RSI_OVERBOUGHT and trend == "BEARISH":
        return "BEARISH", 0.7
    elif 40 <= rsi <= 60:
        return trend, 0.6
    else:
        return "NEUTRAL", 0.5

# CSV Log file
LOG_CSV = "deriv_signals_log.csv"

# ---------- WEB SOCKET CLIENT COMPLETO ----------

class DerivClient:
    def __init__(self, token, endpoint=WS_ENDPOINT):
        self.token = token
        self.endpoint = endpoint
        self.ws = None
        self.req_id = 1
        self.lock = threading.Lock()
        self.responses = {}
        self.connected = False
        self.authorized = False
        self.candle_manager = candle_manager

    def connect(self):
        def on_message(ws, message):
            try:
                data = json.loads(message)
                msg_type = data.get('msg_type', 'unknown')
                
                if msg_type == 'authorize':
                    if 'error' not in data:
                        self.authorized = True
                        print("✅ Autorizado com sucesso")
                    else:
                        error_msg = data['error'].get('message', 'Unknown error')
                        print(f"❌ Erro autorização: {error_msg}")
                        self.authorized = False
                
                rid = data.get('echo_req', {}).get('req_id')
                if rid is not None:
                    self.responses[rid] = data
                    
            except Exception as e:
                print(f"❌ Erro processando mensagem: {e}")

        def on_open(ws):
            print("✅ WebSocket conectado")
            self.connected = True
            self.authorize()

        def on_error(ws, error):
            print(f"💥 Erro WebSocket: {error}")
            self.connected = False
            self.authorized = False

        def on_close(ws, close_status_code, close_msg):
            print(f"❌ WebSocket fechado: {close_status_code} - {close_msg}")
            self.connected = False
            self.authorized = False

        self.ws = websocket.WebSocketApp(
            self.endpoint,
            on_message=on_message,
            on_open=on_open,
            on_error=on_error,
            on_close=on_close
        )
        
        self.ws_thread = threading.Thread(target=self.ws.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()
        
        # Aguardar conexão
        for _ in range(30):
            if self.connected:
                break
            time.sleep(0.1)

    def send(self, payload):
        if not self.connected:
            raise Exception("WebSocket não conectado")
        
        with self.lock:
            payload['req_id'] = self.req_id
            req_id = self.req_id
            self.req_id += 1
            message = json.dumps(payload)
            self.ws.send(message)
            return req_id

    def wait_response(self, req_id, timeout=10.0):
        start_time = time.time()
        while time.time() - start_time < timeout:
            if req_id in self.responses:
                return self.responses.pop(req_id)
            time.sleep(0.1)
        print(f"⏰ Timeout esperando resposta req_id: {req_id}")
        return None

    def authorize(self):
        """✅ MANTER VERSÃO ORIGINAL"""
        if not self.connected:
            return False
        
        req = {'authorize': self.token}
        rid = self.send(req)
        resp = self.wait_response(rid, timeout=10)
        
        if resp and 'error' not in resp:
            self.authorized = True
            return True
        else:
            error_msg = resp.get('error', {}).get('message', 'No response') if resp else 'No response'
            print(f"❌ Falha na autorização: {error_msg}")
            return False

    def get_candles(self, symbol, count=100, granularity=3600):
        if not self.authorized:
            print("⚠️ Não autorizado - tentando autorizar...")
            if not self.authorize():
                return None
        
        payload = {
            'ticks_history': symbol,
            'adjust_start_time': 1,
            'count': count,
            'end': 'latest',
            'granularity': granularity,
            'style': 'candles'
        }
        
        rid = self.send(payload)
        resp = self.wait_response(rid, timeout=15)
        
        if resp and 'candles' in resp:
            return resp['candles']
        elif resp and 'error' in resp:
            print(f"❌ Erro API: {resp['error']['message']}")
        else:
            print("❌ Sem resposta ou timeout na requisição de candles")
        return None

    def update_all_timeframes(self):
        """Atualiza candles para todos os timeframes continuamente"""
        while True:
            try:
                if not self.connected or not self.authorized:
                    time.sleep(5)
                    continue
                
                for tf_name, tf_seconds in TIMEFRAMES.items():
                    try:
                        candles = self.get_candles(SYMBOL, count=100, granularity=tf_seconds)
                        if candles:
                            self.candle_manager.add_candles(tf_name, candles)
                            print(f"🔄 {tf_name} atualizado: {len(candles)} candles")
                        time.sleep(1)  # Pequeno delay entre requests
                    except Exception as e:
                        print(f"❌ Erro atualizando {tf_name}: {e}")
                
                # Verificar limpeza de dados às 23h Angola
                candle_manager.cleanup_old_data()
                
                time.sleep(60)  # Atualizar a cada 1 minuto
                
            except Exception as e:
                print(f"❌ Erro no loop de atualização: {e}")
                time.sleep(30)

# ---------- INDICADORES TÉCNICOS CORRIGIDOS E COMPLETOS ----------

def simple_moving_average(prices, period):
    """Calculate SMA without pandas"""
    if len(prices) < period:
        return sum(prices) / len(prices)
    return sum(prices[-period:]) / period

def exponential_moving_average(prices, period):
    """Calculate EMA without pandas"""
    if len(prices) < period:
        return sum(prices) / len(prices)

    ema_values = [sum(prices[:period]) / period]
    multiplier = 2 / (period + 1)
    
    for price in prices[period:]:
        ema = (price * multiplier) + (ema_values[-1] * (1 - multiplier))
        ema_values.append(ema)
    
    return ema_values[-1]

def calculate_rsi(prices, period=14):
    """Calculate RSI without pandas"""
    if len(prices) < period + 1:
        return 50

    gains = []
    losses = []
    
    for i in range(1, len(prices)):
        change = prices[i] - prices[i-1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))
    
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    
    if avg_loss == 0:
        return 100
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_atr(highs, lows, closes, period=14):
    """Calculate Average True Range without pandas"""
    if len(highs) < period + 1:
        return 0

    tr_values = []
    for i in range(1, len(highs)):
        tr1 = highs[i] - lows[i]
        tr2 = abs(highs[i] - closes[i-1])
        tr3 = abs(lows[i] - closes[i-1])
        true_range = max(tr1, tr2, tr3)
        tr_values.append(true_range)
    
    if len(tr_values) < period:
        return sum(tr_values) / len(tr_values)
    
    return sum(tr_values[-period:]) / period

# ✅✅✅ CORREÇÃO CRÍTICA: ADX REALISTA
def calculate_adx(highs, lows, closes, period=14):
    """✅ ADX CORRIGIDO - Cálculo realista"""
    if len(highs) < period * 2:
        return 25.0  # ✅ Valor neutro quando dados insuficientes
    
    try:
        # Cálculo de +DM e -DM
        plus_dm = []
        minus_dm = []
        
        for i in range(1, len(highs)):
            up_move = highs[i] - highs[i-1]
            down_move = lows[i-1] - lows[i]
            
            if up_move > down_move and up_move > 0:
                plus_dm.append(up_move)
            else:
                plus_dm.append(0)
                
            if down_move > up_move and down_move > 0:
                minus_dm.append(down_move)
            else:
                minus_dm.append(0)
        
        # Suavização com EMA
        def ema(data, period):
            if len(data) < period:
                return sum(data) / len(data) if data else 0
            ema_val = sum(data[:period]) / period
            k = 2 / (period + 1)
            for i in range(period, len(data)):
                ema_val = data[i] * k + ema_val * (1 - k)
            return ema_val
        
        # True Range
        tr_values = []
        for i in range(1, len(highs)):
            tr1 = highs[i] - lows[i]
            tr2 = abs(highs[i] - closes[i-1])
            tr3 = abs(lows[i] - closes[i-1])
            tr_values.append(max(tr1, tr2, tr3))
        
        # Médias suavizadas
        atr = ema(tr_values, period)
        plus_di = 100 * ema(plus_dm, period) / atr if atr > 0 else 0
        minus_di = 100 * ema(minus_dm, period) / atr if atr > 0 else 0
        
        # DX e ADX
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di) if (plus_di + minus_di) > 0 else 0
        adx_value = ema([dx] * period, period)  # Simplificado
        
        return min(adx_value, 100)
    
    except Exception:
        return 25.0  # ✅ Fallback seguro

# ✅✅✅ NOVA IMPLEMENTAÇÃO: MACD COMPLETO
def calculate_macd(prices, fast_period=12, slow_period=26, signal_period=9):
    """✅ CALCULAR MACD COMPLETO: MACD Line, Signal Line, Histogram"""
    if len(prices) < slow_period:
        return 0, 0, 0
    
    # Calcular EMAs
    ema_fast = exponential_moving_average(prices, fast_period)
    ema_slow = exponential_moving_average(prices, slow_period)
    
    # MACD Line
    macd_line = ema_fast - ema_slow
    
    # Para Signal Line, precisamos de histórico do MACD
    # Como simplificação, usaremos EMA do MACD atual
    macd_history = []
    for i in range(slow_period, len(prices)):
        ema_f = exponential_moving_average(prices[:i+1], fast_period)
        ema_s = exponential_moving_average(prices[:i+1], slow_period)
        macd_history.append(ema_f - ema_s)
    
    if len(macd_history) >= signal_period:
        signal_line = exponential_moving_average(macd_history, signal_period)
    else:
        signal_line = macd_line * 0.9  # Aproximação
    
    # Histogram
    histogram = macd_line - signal_line
    
    return macd_line, signal_line, histogram

# ✅✅✅ NOVA IMPLEMENTAÇÃO: BEARS/BULLS POWER
def calculate_bears_bulls(highs, lows, closes, period=13):
    """✅ CALCULAR BEARS/BULLS POWER (Força de Ursos e Touros)"""
    if len(closes) < period:
        return 0, 0
    
    # Bears Power = Low - EMA
    ema_period = exponential_moving_average(closes, period)
    current_low = lows[-1]
    bears_power = current_low - ema_period
    
    # Bulls Power = High - EMA  
    current_high = highs[-1]
    bulls_power = current_high - ema_period
    
    return bears_power, bulls_power

# ✅✅✅ NOVA IMPLEMENTAÇÃO: ANÁLISE DE VOLUME
def calculate_volume_analysis(volumes, prices, period=20):
    """✅ ANALISAR VOLUME: Volume médio, relação atual, confirmação"""
    if len(volumes) < period:
        return {
            'volume_ratio': 1.0,
            'volume_trend': 'NEUTRAL',
            'volume_confirmation': 'INSUFFICIENT_DATA'
        }
    
    # Volume médio
    avg_volume = sum(volumes[-period:]) / period
    current_volume = volumes[-1] if volumes else 0
    
    # Razão volume atual vs médio
    volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0
    
    # Tendência do volume (últimos 5 períodos)
    if len(volumes) >= 5:
        recent_volumes = volumes[-5:]
        volume_trend = 'BULLISH' if recent_volumes[-1] > recent_volumes[0] else 'BEARISH'
    else:
        volume_trend = 'NEUTRAL'
    
    # Confirmação por volume
    if volume_ratio > 1.5:
        volume_confirmation = 'STRONG_CONFIRMATION'
    elif volume_ratio > 1.2:
        volume_confirmation = 'MODERATE_CONFIRMATION'
    elif volume_ratio < 0.8:
        volume_confirmation = 'WEAK_CONFIRMATION'
    else:
        volume_confirmation = 'NEUTRAL'
    
    return {
        'volume_ratio': round(volume_ratio, 2),
        'volume_trend': volume_trend,
        'volume_confirmation': volume_confirmation,
        'current_volume': current_volume,
        'avg_volume': round(avg_volume, 2)
    }

# ---------- BULL/BEAR STRENGTH ANALYSIS CORRIGIDO ----------

def compute_bull_bear_strength_from_candle(candle, atr):
    """
    ✅✅✅ CORREÇÃO COMPLETA: Cálculo REALISTA sem viés bearish
    """
    o = float(candle['open'])
    h = float(candle['high'])
    l = float(candle['low'])
    c = float(candle['close'])
    
    body_size = abs(c - o)
    total_range = max(0.001, h - l)
    
    # ✅ Cálculo baseado apenas no candle, sem variações artificiais
    if c > o:  # Candle bullish (verde)
        # Candle verde forte = bull strength alta
        bull_strength = min(0.95, (c - o) / total_range + 0.1)  # +0.1 bonus para verde
        bear_strength = max(0.05, 1.0 - bull_strength)
    else:  # Candle bearish (vermelho)  
        # Candle vermelho = bear strength alta
        bear_strength = min(0.95, (o - c) / total_range)
        bull_strength = max(0.05, 1.0 - bear_strength)
    
    # ✅ Normalização garantindo soma = 1.0
    total = bull_strength + bear_strength
    bull_strength /= total
    bear_strength /= total

    return round(bull_strength, 3), round(bear_strength, 3)

def apply_bull_bear_strength_filter(candles, features, combined_prob, avg_atr):
    """
    ✅✅✅ CORREÇÃO: Filtro com valores REALISTAS
    """
    if not candles or len(candles) < 1:
        return combined_prob, None

    last_candle = candles[-1]
    atr_value = features.get('atr14', 0.001)
    rsi14 = features.get('rsi14', 50)

    bull_strength, bear_strength = compute_bull_bear_strength_from_candle(last_candle, atr_value)
    strength_diff = bull_strength - bear_strength

    # ✅ CORREÇÃO: Thresholds realistas
    strong_threshold = 0.25
    weak_threshold = 0.08
    
    # ✅ CORREÇÃO: Confirmação RSI mais flexível
    rsi_confirms_bull = (rsi14 < 65)  # ✅ Mais flexível
    rsi_confirms_bear = (rsi14 > 35)  # ✅ Mais flexível

    force_decision = None
    
    if abs(strength_diff) > strong_threshold:
        if strength_diff > 0 and rsi_confirms_bull:
            if combined_prob >= 0.45:
                force_decision = 'BUY'
                print(f"🎯 FORCE BUY CONFIRMADO: Bull Strength {bull_strength:.3f}, Prob Base: {combined_prob:.1%}")
            else:
                print(f"🚫 FORCE BUY NEGADO: Probabilidade base insuficiente: {combined_prob:.1%}")
        
        elif strength_diff < 0 and rsi_confirms_bear:
            if combined_prob <= 0.55:
                force_decision = 'SELL'
                print(f"🎯 FORCE SELL CONFIRMADO: Bear Strength {bear_strength:.3f}, Prob Base: {combined_prob:.1%}")
            else:
                print(f"🚫 FORCE SELL NEGADO: Probabilidade base insuficiente: {combined_prob:.1%}")
    
    elif abs(strength_diff) < weak_threshold:
        force_decision = 'HOLD'
        print(f"🎯 FORCE HOLD: Mercado indeciso (Diff: {strength_diff:.3f})")

    print(f"🔎 Bulls/Bears: {bull_strength:.3f}/{bear_strength:.3f} | diff={strength_diff:.3f} | force_decision={force_decision} | RSI={rsi14:.1f}")

    return combined_prob, force_decision

# ---------- ANÁLISE DE TIMEFRAME ESPECÍFICO ----------

def analyze_single_timeframe(client, timeframe):
    """✅ NOVA FUNÇÃO: Análise de timeframe específico"""
    print(f"🔄 Analisando timeframe específico: {timeframe}")
    
    # Obter candles do timeframe específico
    candles = candle_manager.get_candles(timeframe)
    if not candles or len(candles) < 30:
        print(f"⚠️ Dados insuficientes para {timeframe}")
        return None
    
    try:
        closes = [float(c['close']) for c in candles]
        highs = [float(c['high']) for c in candles]
        lows = [float(c['low']) for c in candles]
        
        # ✅ IMPLEMENTAÇÃO: Coletar volumes
        volumes = []
        for c in candles:
            if 'volume' in c and c['volume'] is not None:
                volumes.append(float(c['volume']))
            else:
                volume_synthetic = (float(c['high']) - float(c['low'])) * 1000
                volumes.append(volume_synthetic)
        
        current_price = closes[-1]
        previous_price = closes[-2] if len(closes) > 1 else current_price
        
        # Calcular indicadores técnicos
        sma_20 = simple_moving_average(closes, 20)
        rsi_14 = calculate_rsi(closes, 14)
        atr_14 = calculate_atr(highs, lows, closes, 14)
        adx_14 = calculate_adx(highs, lows, closes, 14)
        macd_line, signal_line, histogram = calculate_macd(closes)
        bears_power, bulls_power = calculate_bears_bulls(highs, lows, closes, 13)
        volume_analysis = calculate_volume_analysis(volumes, closes, 20)
        
        # Detectar tendência REAL
        real_trend = detectar_tendencia_real(closes, current_price)
        
        # ✅ CORREÇÃO CRÍTICA: Usar lógica corrigida para sinais
        signal, strength = corrigir_logica_sinal(
            rsi_14, real_trend, macd_line, bears_power, bulls_power, 
            current_price, previous_price
        )
        
        timeframe_data = {
            timeframe: {
                'signal': signal,
                'strength': strength,
                'price': current_price,
                'rsi': rsi_14,
                'trend': real_trend,
                'atr': atr_14,
                'adx': adx_14,
                'macd_line': macd_line,
                'macd_signal': signal_line,
                'macd_histogram': histogram,
                'bears_power': bears_power,
                'bulls_power': bulls_power,
                'volume_analysis': volume_analysis
            }
        }
        
        # ✅✅✅ ATUALIZADO: Mostrar papel hierárquico
        role = get_timeframe_role(timeframe)
        
        # ✅✅✅ CORREÇÃO: Mostrar alerta para RSI extremo
        if rsi_14 < RSI_EXTREME_OVERSOLD:
            print(f"  🚨 {role} ({timeframe}): {signal} (RSI EXTREMO: {rsi_14:.1f}, Trend: {real_trend})")
        elif rsi_14 > RSI_EXTREME_OVERBOUGHT:
            print(f"  🚨 {role} ({timeframe}): {signal} (RSI EXTREMO: {rsi_14:.1f}, Trend: {real_trend})")
        else:
            print(f"  {role} ({timeframe}): {signal} (RSI: {rsi_14:.1f}, Trend: {real_trend})")
            
        print(f"    📊 MACD: {macd_line:.4f}, Bears: {bears_power:.4f}, Bulls: {bears_power:.4f}")
        print(f"    📈 Volume: {volume_analysis['volume_confirmation']} (Ratio: {volume_analysis['volume_ratio']})")
        
        return timeframe_data
        
    except Exception as e:
        print(f"⚠️ Erro em análise {timeframe}: {e}")
        return None

# ---------- MULTI-TIMEFRAME ANALYSIS CORREÇÃO FINAL ----------

def analyze_multiple_timeframes(client):
    """✅✅✅ ATUALIZADO: Análise com 6 TIMEFRAMES usando dados armazenados"""
    print("🔄 Coletando dados de 6 timeframes do armazenamento...")

    timeframe_signals = {}
    timeframe_data = {}
    successful_timeframes = 0
    rsi_values = {}
    
    for tf_name in TIMEFRAMES.keys():
        try:
            # Obter candles do armazenamento
            candles = candle_manager.get_candles(tf_name)
            if not candles or len(candles) < 30:
                print(f"⚠️ Dados insuficientes para {tf_name}")
                continue
            
            closes = [float(c['close']) for c in candles]
            highs = [float(c['high']) for c in candles]
            lows = [float(c['low']) for c in candles]
            
            # ✅✅✅ IMPLEMENTAÇÃO: Coletar volumes se disponíveis
            volumes = []
            for c in candles:
                if 'volume' in c and c['volume'] is not None:
                    volumes.append(float(c['volume']))
                else:
                    # Se volume não disponível, usar volume sintético baseado no range
                    volume_synthetic = (float(c['high']) - float(c['low'])) * 1000
                    volumes.append(volume_synthetic)
            
            current_price = closes[-1]
            previous_price = closes[-2] if len(closes) > 1 else current_price
            
            # ✅ CORREÇÃO: Usar SMA para evitar problemas com EMA
            sma_20 = simple_moving_average(closes, 20)
            rsi_14 = calculate_rsi(closes, 14)
            atr_14 = calculate_atr(highs, lows, closes, 14)
            
            # ✅✅✅ NOVOS INDICADORES IMPLEMENTADOS
            adx_14 = calculate_adx(highs, lows, closes, 14)
            macd_line, signal_line, histogram = calculate_macd(closes)
            bears_power, bulls_power = calculate_bears_bulls(highs, lows, closes, 13)
            volume_analysis = calculate_volume_analysis(volumes, closes, 20)
            
            # ✅✅✅ CORREÇÃO CRÍTICA: Detectar tendência REAL
            real_trend = detectar_tendencia_real(closes, current_price)
            
            # ✅✅✅ CORREÇÃO CRÍTICA: Usar lógica corrigida para sinais
            signal, strength = corrigir_logica_sinal(
                rsi_14, real_trend, macd_line, bears_power, bulls_power, 
                current_price, previous_price
            )
            
            # Guardar RSI para pesos dinâmicos
            rsi_values[tf_name] = rsi_14
            
            timeframe_signals[tf_name] = {
                'signal': signal,
                'strength': strength,
                'price': current_price,
                'rsi': rsi_14,
                'trend': real_trend,  # ✅✅✅ AGORA COM TREND REAL
                'atr': atr_14,
                'adx': adx_14,
                'macd_line': macd_line,
                'macd_signal': signal_line,
                'macd_histogram': histogram,
                'bears_power': bears_power,
                'bulls_power': bulls_power,
                'volume_analysis': volume_analysis
            }
            
            timeframe_data[tf_name] = {
                'closes': closes,
                'highs': highs, 
                'lows': lows,
                'volumes': volumes,
                'candles': candles
            }
            
            successful_timeframes += 1
            
            # ✅✅✅ ATUALIZADO: Mostrar papel hierárquico
            role = get_timeframe_role(tf_name)
            
            # ✅✅✅ CORREÇÃO: Mostrar alerta para RSI extremo
            if rsi_14 < RSI_EXTREME_OVERSOLD:
                print(f"  🚨 {role} ({tf_name}): {signal} (RSI EXTREMO: {rsi_14:.1f}, Trend: {real_trend})")
            elif rsi_14 > RSI_EXTREME_OVERBOUGHT:
                print(f"  🚨 {role} ({tf_name}): {signal} (RSI EXTREMO: {rsi_14:.1f}, Trend: {real_trend})")
            else:
                print(f"  {role} ({tf_name}): {signal} (RSI: {rsi_14:.1f}, Trend: {real_trend})")
                
            print(f"    📊 MACD: {macd_line:.4f}, Bears: {bears_power:.4f}, Bulls: {bears_power:.4f}")
            print(f"    📈 Volume: {volume_analysis['volume_confirmation']} (Ratio: {volume_analysis['volume_ratio']})")
            
        except Exception as e:
            print(f"⚠️ Erro em {tf_name}: {e}")
            continue
    
    if successful_timeframes < 3:
        print("❌ Dados insuficientes de timeframes")
        return None, None, None
    
    return timeframe_signals, timeframe_data, rsi_values

def get_timeframe_consensus(timeframe_signals, rsi_values):
    """✅✅✅ ATUALIZADO: Consenso com 6 timeframes hierárquicos"""
    if not timeframe_signals:
        return "NEUTRAL", 0.5

    # ✅ Obter pesos hierárquicos para 6 TIMEFRAMES
    dynamic_weights = get_dynamic_weights(rsi_values)
    
    bull_score = 0
    bear_score = 0
    total_weight = 0
    
    for tf, data in timeframe_signals.items():
        weight = dynamic_weights.get(tf, 0.17)  # Default igual para 6 timeframes
        
        if data['signal'] == "BULLISH":
            bull_score += data['strength'] * weight
        elif data['signal'] == "BEARISH":
            bear_score += data['strength'] * weight
        # NEUTRAL não contribui
        
        total_weight += weight
    
    if total_weight == 0:
        return "NEUTRAL", 0.5
    
    # ✅ CORREÇÃO: Cálculo de força correto
    net_score = bull_score - bear_score
    strength = abs(net_score) / total_weight
    
    # ✅ CORREÇÃO CRÍTICA: Limiar reduzido mas coerente
    if net_score > 0.05:
        return "BULLISH", strength
    elif net_score < -0.05:
        return "BEARISH", strength
    else:
        return "NEUTRAL", strength

# ✅✅✅ NOVA FUNÇÃO: DETECTAR TENDÊNCIA REAL
def detectar_tendencia_real(closes, current_price):
    """✅ CORREÇÃO: Detectar tendência baseada no movimento real de preços"""
    if len(closes) < 10:
        return "NEUTRAL"
    
    # Preço atual vs médias móveis
    sma_5 = sum(closes[-5:]) / 5
    sma_10 = sum(closes[-10:]) / 10
    
    # Tendência baseada em preço real
    if current_price < sma_5 and current_price < sma_10:
        return "BEARISH"
    elif current_price > sma_5 and current_price > sma_10:
        return "BULLISH"
    else:
        return "NEUTRAL"

# ---------- ANÁLISE MULTICULTURAL COMPLETA ----------

def vedic_numerology_analysis(price):
    """Vedic numerology analysis based on price"""
    price_str = f"{price:.3f}".replace('.', '')
    digit_sum = sum(int(d) for d in price_str if d.isdigit())

    while digit_sum > 9:
        digit_sum = sum(int(d) for d in str(digit_sum))
    
    vedic_meaning = {
        1: ("SUN", "BULLISH", 0.7),
        2: ("MOON", "NEUTRAL", 0.5),
        3: ("JUPITER", "BULLISH", 0.8),
        4: ("RAHU", "BEARISH", 0.2),
        5: ("MERCURY", "NEUTRAL", 0.5),
        6: ("VENUS", "BULLISH", 0.7),
        7: ("KETU", "BEARISH", 0.3),
        8: ("SATURN", "BEARISH", 0.2),
        9: ("MARS", "BULLISH", 0.9)
    }
    
    planet, direction, strength = vedic_meaning.get(digit_sum, ("UNKNOWN", "NEUTRAL", 0.5))
    return digit_sum, planet, direction, strength

# ✅✅✅ CORREÇÃO #2: ÂNGULOS GANN DINÂMICOS
def vedic_gann_analysis(highs, lows, current_price):
    """✅✅✅ CORREÇÃO: Ângulos Gann dinâmicos e realistas"""
    if len(highs) < 10:
        return {"signal": "NEUTRAL", "angles": []}

    recent_high = max(highs[-10:])
    recent_low = min(lows[-10:])
    range_size = recent_high - recent_low
    
    if range_size == 0:
        return {"signal": "NEUTRAL", "angles": []}
    
    # ✅ CORREÇÃO: Ângulos dinâmicos baseados no range
    price_position = (current_price - recent_low) / range_size
    
    # ✅ CORREÇÃO: Lógica balanceada
    if price_position > 0.7:  # Preço nos 30% superiores
        signal = "BEARISH"
        current_angle = "1x2 (63.75°)"
    elif price_position < 0.3:  # Preço nos 30% inferiores
        signal = "BULLISH" 
        current_angle = "2x1 (26.25°)"
    else:  # Zona neutra
        signal = "NEUTRAL"
        current_angle = "1x1 (45°)"
    
    return {
        "signal": signal,
        "current_angle": current_angle
    }

# ✅✅✅ CORREÇÃO #1: SISTEMA ELEFANTE TAILANDÊS BALANCEADO
def thai_elephant_pattern(highs, lows, closes):
    """✅✅✅ CORREÇÃO COMPLETA: Padrão Elefante com lógica balanceada"""
    if len(closes) < 10:
        return {"pattern": "NO_PATTERN", "signal": "NEUTRAL", "confidence": 0}

    current_high = highs[-1]
    current_low = lows[-1] 
    current_close = closes[-1]
    previous_close = closes[-2] if len(closes) > 1 else current_close
    
    avg_high_5 = sum(highs[-5:]) / 5
    avg_low_5 = sum(lows[-5:]) / 5
    
    elephant_signals_bull = 0
    elephant_signals_bear = 0
    max_signals = 3
    
    # ✅ SINAL BULLISH 1: Forte fechamento acima da média alta
    if current_close > avg_high_5 and current_close > previous_close:
        elephant_signals_bull += 1
        print(f"  🐘✅ Elefante Bullish: Fechamento forte acima da resistência")
    
    # ✅ SINAL BULLISH 2: Suporte mantido na média baixa
    if current_low > avg_low_5 and current_close > avg_low_5:
        elephant_signals_bull += 1
        print(f"  🐘✅ Elefante Bullish: Suporte mantido")
    
    # ✅ SINAL BEARISH 1: Rejeição na resistência
    if current_high > avg_high_5 and current_close < avg_high_5:
        elephant_signals_bear += 1
        print(f"  🐘❌ Elefante Bearish: Rejeição na resistência")
    
    # ✅ SINAL BEARISH 2: Rompimento do suporte
    if current_low < avg_low_5 and current_close < avg_low_5:
        elephant_signals_bear += 1
        print(f"  🐘❌ Elefante Bearish: Suporte rompido")
    
    # ✅ DECISÃO BALANCEADA
    total_signals = elephant_signals_bull + elephant_signals_bear
    
    if total_signals >= 2:
        if elephant_signals_bull > elephant_signals_bear:
            confidence = elephant_signals_bull / max_signals
            print(f"  🐘✅ ELEFANTE BULLISH: {elephant_signals_bull}/{max_signals} sinais")
            return {"pattern": "WHITE_ELEPHANT", "signal": "BULLISH", "confidence": confidence}
        elif elephant_signals_bear > elephant_signals_bull:
            confidence = elephant_signals_bear / max_signals
            print(f"  🐘❌ ELEFANTE BEARISH: {elephant_signals_bear}/{max_signals} sinais")
            return {"pattern": "WHITE_ELEPHANT", "signal": "BEARISH", "confidence": confidence}
        else:
            return {"pattern": "BALANCED_ELEPHANT", "signal": "NEUTRAL", "confidence": 0.5}
    else:
        return {"pattern": "NO_PATTERN", "signal": "NEUTRAL", "confidence": 0}

# ✅✅✅ CORREÇÃO CRÍTICA: CICLO TAILANDÊS BASEADO EM TIMESTAMP REAL
def thai_cycle_analysis(closes, timestamp=None):
    """✅ CORREÇÃO: Thai market cycle analysis baseada em TIMESTAMP real"""
    if len(closes) < 12:
        return {"cycle_position": 0, 'cycle_phase': 'UNKNOWN', "signal": "NEUTRAL", "strength": 0.5}

    # ✅ Usar timestamp real ou hora atual (NÃO analysis_count)
    if timestamp is None:
        timestamp = time.time()
    
    # ✅ Ciclo baseado em minutos/horas reais, não contador arbitrário
    current_time = datetime.now()
    current_minute = current_time.minute
    current_cycle_pos = current_minute % THAI_CYCLE_PERIOD
    
    cycle_meaning = {
        0: ("START", "NEUTRAL", 0.5),
        1: ("ACCUMULATION", "BULLISH", 0.7),
        2: ("GROWTH", "BULLISH", 0.8),
        3: ("MATURITY", "NEUTRAL", 0.5),
        4: ("DISTRIBUTION", "BEARISH", 0.7),
        5: ("DECLINE", "BEARISH", 0.8)
    }
    
    phase, direction, strength = cycle_meaning.get(current_cycle_pos, ("UNKNOWN", "NEUTRAL", 0.5))
    
    print(f"  🇹🇭 Ciclo Tailandês: Posição {current_cycle_pos} ({phase}) - {direction} (Força: {strength})")
    
    return {
        "cycle_position": current_cycle_pos,
        "cycle_phase": phase,
        "signal": direction,
        "strength": strength
    }

def thai_nine_analysis(price):
    """Thai analysis based on number 9 cycles"""
    base_level = round(price / 9) * 9
    resistance = base_level + 9
    support = base_level - 9

    current_to_resistance = resistance - price
    current_to_support = price - support
    
    if current_to_resistance < current_to_support:
        signal = "BEARISH"
        strength = 1.0 - (current_to_resistance / 9)
    else:
        signal = "BULLISH"
        strength = 1.0 - (current_to_support / 9)
    
    return {
        "signal": signal,
        "strength": max(0.3, min(0.9, strength)),
        "resistance": resistance,
        "support": support
    }

# ✅✅✅ NOVA FUNÇÃO: UNIFICAR SINAL TAILANDÊS
def get_unified_thai_signal(thai_cycle, thai_elephant, thai_nine):
    """✅ CORREÇÃO: Unificar os 3 sinais tailandeses em um consenso"""
    
    signals = []
    strengths = []
    components = []
    
    # Ciclo
    if thai_cycle['signal'] != 'NEUTRAL':
        signals.append(thai_cycle['signal'])
        strengths.append(thai_cycle['strength'])
        components.append(f"Ciclo({thai_cycle['cycle_phase']})")
    
    # Elefante  
    if thai_elephant['signal'] != 'NEUTRAL' and thai_elephant['confidence'] > 0.4:
        signals.append(thai_elephant['signal'])
        strengths.append(thai_elephant['confidence'])
        components.append(f"Elefante({thai_elephant['pattern']})")
    
    # Nove
    if thai_nine['signal'] != 'NEUTRAL':
        signals.append(thai_nine['signal'])
        strengths.append(thai_nine['strength'])
        components.append("Ciclo9")
    
    if not signals:
        print("  🇹🇭 Sinal Tailandês: NEUTRAL (nenhum componente ativo)")
        return "NEUTRAL", 0.5
    
    # Contar bull vs bear
    bull_count = signals.count('BULLISH')
    bear_count = signals.count('BEARISH')
    
    if bull_count > bear_count:
        final_signal = 'BULLISH'
        avg_strength = sum(s for s, sig in zip(strengths, signals) if sig == 'BULLISH') / bull_count
        print(f"  🇹🇭 Sinal Tailandês: BULLISH ({bull_count}/{len(signals)} componentes: {', '.join(components)})")
    elif bear_count > bull_count:
        final_signal = 'BEARISH' 
        avg_strength = sum(s for s, sig in zip(strengths, signals) if sig == 'BEARISH') / bear_count
        print(f"  🇹🇭 Sinal Tailandês: BEARISH ({bear_count}/{len(signals)} componentes: {', '.join(components)})")
    else:
        final_signal = 'NEUTRAL'
        avg_strength = 0.5
        print(f"  🇹🇭 Sinal Tailandês: NEUTRAL (empate {bull_count}-{bear_count} componentes)")
    
    return final_signal, avg_strength

def detect_japanese_patterns(candles):
    """Detect Japanese candlestick patterns"""
    if len(candles) < 3:
        return []

    patterns = []
    
    for i in range(2, len(candles)):
        current = candles[i]
        prev = candles[i-1]
        prev2 = candles[i-2]
        
        # ✅ CORREÇÃO: Converter para float para evitar TypeError
        o = float(current['open'])
        h = float(current['high'])
        l = float(current['low'])
        c = float(current['close'])
        o1 = float(prev['open'])
        h1 = float(prev['high'])
        l1 = float(prev['low'])
        c1 = float(prev['close'])
        o2 = float(prev2['open'])
        h2 = float(prev2['high'])
        l2 = float(prev2['low'])
        c2 = float(prev2['close'])
        
        # Doji detection
        if (h - l) > 0 and abs(c - o) / (h - l) < 0.1:
            patterns.append({'index': i, 'pattern': 'Doji', 'side': 'neutral'})
        
        # Hammer/Shooting Star
        body = abs(c - o)
        lower_wick = min(o, c) - l
        upper_wick = h - max(o, c)
        
        if lower_wick >= 2 * body and upper_wick <= body:
            patterns.append({'index': i, 'pattern': 'Hammer', 'side': 'bull'})
        elif upper_wick >= 2 * body and lower_wick <= body:
            patterns.append({'index': i, 'pattern': 'Shooting Star', 'side': 'bear'})
    
    return patterns

def multicultural_analysis(features, timeframe_data, analysis_count):
    """Combinação de análises multicultural"""
    h1_data = timeframe_data.get('H1', {})
    if not h1_data:
        return {}

    closes = h1_data.get('closes', [])
    highs = h1_data.get('highs', [])
    lows = h1_data.get('lows', [])
    
    if not closes:
        return {}
    
    current_price = closes[-1]
    
    # Vedic Analysis
    vedic_number, vedic_planet, vedic_direction, vedic_strength = vedic_numerology_analysis(current_price)
    vedic_gann = vedic_gann_analysis(highs, lows, current_price)
    
    # ✅✅✅ CORREÇÃO: Análise Tailandesa ATUALIZADA
    thai_cycle = thai_cycle_analysis(closes)  # ✅ Agora sem analysis_count
    thai_elephant = thai_elephant_pattern(highs, lows, closes)
    thai_nine = thai_nine_analysis(current_price)
    
    # ✅✅✅ CORREÇÃO: Usar sinal tailandês unificado
    thai_final_signal, thai_final_strength = get_unified_thai_signal(thai_cycle, thai_elephant, thai_nine)
    
    # Japanese Candlestick Analysis
    japanese_analysis = {}
    for tf_name, data in timeframe_data.items():
        if 'candles' in data and len(data['candles']) > 5:
            patterns = detect_japanese_patterns(data['candles'])
            recent_patterns = [p for p in patterns if p['index'] >= len(data['candles'])-2]
            japanese_analysis[tf_name] = {
                'patterns_found': len(recent_patterns),
                'recent_patterns': recent_patterns
            }
    
    # ✅ Combinação multicultural ATUALIZADA
    vedic_score = vedic_strength if vedic_direction == "BULLISH" else 1 - vedic_strength
    
    # ✅ Usar sinal tailandês unificado
    thai_score = thai_final_strength if thai_final_signal == "BULLISH" else 1 - thai_final_strength
    
    # Japanese pattern scoring
    japanese_score = 0.5
    total_patterns = 0
    bull_patterns = 0
    
    for tf_data in japanese_analysis.values():
        for pattern in tf_data['recent_patterns']:
            total_patterns += 1
            if pattern['side'] == 'bull':
                bull_patterns += 1
            elif pattern['side'] == 'bear':
                bull_patterns -= 1
    
    if total_patterns > 0:
        japanese_score = 0.5 + (bull_patterns / total_patterns) * 0.5
    
    multicultural_prob = (vedic_score * 0.40 + thai_score * 0.35 + japanese_score * 0.25)
    
    if multicultural_prob >= PROB_BUY_THRESHOLD:
        multi_signal = "BUY"
    elif multicultural_prob <= PROB_SELL_THRESHOLD:
        multi_signal = "SELL"
    else:
        multi_signal = "HOLD"

    signals = {
        'VEDIC': {
            'number': vedic_number,
            'planet': vedic_planet,
            'direction': vedic_direction,
            'strength': vedic_strength,
            'gann_signal': vedic_gann['signal'],
            'current_angle': vedic_gann['current_angle']
        },
        'THAI': {
            'cycle_position': thai_cycle['cycle_position'],
            'cycle_phase': thai_cycle['cycle_phase'],
            'direction': thai_final_signal,  # ✅ Usar sinal unificado
            'strength': thai_final_strength,  # ✅ Usar força unificada
            'elephant_pattern': thai_elephant['pattern'],
            'elephant_signal': thai_elephant['signal'],
            'elephant_confidence': thai_elephant['confidence'],
            'nine_signal': thai_nine['signal'],
            'nine_strength': thai_nine['strength']
        },
        'JAPANESE': japanese_analysis,
        'MULTICULTURAL': {
            'probability': multicultural_prob,
            'signal': multi_signal,
            'confidence': abs(multicultural_prob - 0.5) * 2
        }
    }
    
    return signals

# ---------- FEATURE ENGINEERING ----------

def sigmoid(x):
    """Sigmoid function for probability"""
    try:
        return 1 / (1 + math.exp(-x))
    except:
        return 0.5

def adaptive_weights(market_regime):
    """Dynamic weights based on market regime"""
    if market_regime == "TRENDING":
        return {
            'intercept': -0.05,
            'above_sma20': 2.0,
            'rsi14': -0.005,
            'sma20_slope': 3.0,
            'dist_sma20_atr': 1.0,
            'vol_norm': -2.0,
            'ret1': 8.0
        }
    else:  # RANGING
        return {
            'intercept': 0.0,
            'above_sma20': 1.0,
            'rsi14': -0.01,
            'sma20_slope': 1.5,
            'dist_sma20_atr': 0.5,
            'vol_norm': -5.0,
            'ret1': 6.0
        }

def heuristic_probability(features, market_regime):
    """Advanced probability calculation"""
    weights = adaptive_weights(market_regime)

    rsi_norm = (features['rsi14'] - 50) / 50.0
    sma20_slope_norm = features['sma20_slope'] / max(1e-6, features['atr14'])
    vol_norm = features['vol_norm']
    dist_sma20_atr = features['dist_sma20_atr']
    ret1 = features['ret1']
    
    x = (weights['intercept'] +
         weights['above_sma20'] * features['above_sma20'] +
         weights['rsi14'] * rsi_norm +
         weights['sma20_slope'] * sma20_slope_norm +
         weights['dist_sma20_atr'] * dist_sma20_atr +
         weights['vol_norm'] * vol_norm +
         weights['ret1'] * ret1)
    
    return float(sigmoid(x))

def build_features_from_candles(candles):
    """✅✅✅ CORREÇÃO: Build features com dados suficientes"""
    if not candles or len(candles) < 30:  # ✅✅✅ Aumentado mínimo para 30
        print(f"❌ Dados insuficientes: {len(candles) if candles else 0} candles")
        return None

    closes = [float(c['close']) for c in candles]
    highs = [float(c['high']) for c in candles]
    lows = [float(c['low']) for c in candles]
    
    # ✅✅✅ IMPLEMENTAÇÃO: Coletar volumes
    volumes = []
    for c in candles:
        if 'volume' in c and c['volume'] is not None:
            volumes.append(float(c['volume']))
        else:
            # Volume sintético baseado no range do candle
            volume_synthetic = (float(c['high']) - float(c['low'])) * 1000
            volumes.append(volume_synthetic)
    
    current_price = closes[-1]
    
    # ✅ CORREÇÃO: Usar SMA para consistência
    sma_20 = simple_moving_average(closes, 20)
    rsi_14 = calculate_rsi(closes, 14)
    atr_14 = calculate_atr(highs, lows, closes, 14)
    adx_14 = calculate_adx(highs, lows, closes, 14)  # ✅✅✅ AGORA COM ADX CORRETO
    
    # ✅✅✅ NOVOS INDICADORES IMPLEMENTADOS
    macd_line, signal_line, histogram = calculate_macd(closes)
    bears_power, bulls_power = calculate_bears_bulls(highs, lows, closes, 13)
    volume_analysis = calculate_volume_analysis(volumes, closes, 20)
    
    # ✅✅✅ CORREÇÃO: Cálculo de slope mais robusto
    if len(closes) > 23:
        sma_20_prev = simple_moving_average(closes[:-3], 20)
        sma_slope = sma_20 - sma_20_prev
    else:
        sma_slope = 0
    
    dist_sma_atr = (current_price - sma_20) / atr_14 if atr_14 > 0 else 0
    above_sma = 1 if current_price > sma_20 else 0
    vol_norm = atr_14 / current_price if current_price > 0 else 0
    ret_1 = ((current_price - closes[-2]) / closes[-2]) if len(closes) > 1 else 0

    features = {
        'close': current_price,
        'sma20': sma_20,
        'rsi14': rsi_14,
        'atr14': atr_14,
        'adx': adx_14,  # ✅✅✅ AGORA VALOR REAL
        'macd_line': macd_line,  # ✅✅✅ NOVO: MACD
        'macd_signal': signal_line,  # ✅✅✅ NOVO: MACD Signal
        'macd_histogram': histogram,  # ✅✅✅ NOVO: MACD Histogram
        'bears_power': bears_power,  # ✅✅✅ NOVO: Bears Power
        'bulls_power': bulls_power,  # ✅✅✅ NOVO: Bulls Power
        'volume_analysis': volume_analysis,  # ✅✅✅ NOVO: Volume Analysis
        'sma20_slope': sma_slope,
        'dist_sma20_atr': dist_sma_atr,
        'above_sma20': above_sma,
        'vol_norm': vol_norm,
        'ret1': ret_1
    }
    
    return features, closes, highs, lows, volumes

# ---------- MARKET REGIME DETECTION ----------

def detect_market_regime(closes, current_adx, current_atr, avg_atr):
    """✅✅✅ CORREÇÃO: Market regime com ADX REAL"""
    if len(closes) < 28:
        return "RANGING"

    # ✅✅✅ AGORA com ADX realista (não mais 100.0)
    if current_adx > 25 and current_atr > avg_atr * 0.7:  # ✅ Limiar ajustado
        return "TRENDING"
    else:
        return "RANGING"

# ---------- FILTROS ----------

def volatility_filter(current_atr, avg_atr):
    """Volatility filter"""
    atr_ratio = current_atr / avg_atr if avg_atr > 0 else 1

    if atr_ratio > VOLATILITY_FILTER_MULTIPLIER:
        print(f"⚠️ Alta volatilidade: ATR ratio {atr_ratio:.2f}")
        return False
    return True

# ✅✅✅ CORREÇÃO #3: FILTRO RSI INTELIGENTE
def rsi_extreme_filter(rsi_value, signal, adx_value=25):
    """✅✅✅ CORREÇÃO: Filtro RSI inteligente que considera tendência"""
    
    # ✅ EM TENDÊNCIA FORTE: Relaxar filtros RSI
    if adx_value > 30:  # Tendência forte
        if signal == "BUY" and rsi_value > 75:  # ✅ Aumentado de 65 para 75
            return "HOLD", f"RSI sobrecomprado em tendência: {rsi_value:.1f}"
        elif signal == "SELL" and rsi_value < 25:  # ✅ Reduzido de 35 para 25
            return "HOLD", f"RSI sobrevendido em tendência: {rsi_value:.1f}"
    else:  # Mercado lateral
        if signal == "BUY" and rsi_value > 70:
            return "HOLD", f"RSI sobrecomprado: {rsi_value:.1f}"
        elif signal == "SELL" and rsi_value < 30:
            return "HOLD", f"RSI sobrevendido: {rsi_value:.1f}"
    
    # ✅ Manter lógica para extremos
    if rsi_value < RSI_EXTREME_OVERSOLD:
        if signal == "BUY":
            return "BUY", f"✅ RSI extremamente sobrevendido {rsi_value:.1f} CONFIRMA CALL"
        elif signal == "SELL":
            return "HOLD", f"🚫 RSI extremamente sobrevendido {rsi_value:.1f} - NÃO VENDER"
    
    elif rsi_value > RSI_EXTREME_OVERBOUGHT:
        if signal == "SELL":
            return "SELL", f"✅ RSI extremamente sobrecomprado {rsi_value:.1f} CONFIRMA PUT"
        elif signal == "BUY":
            return "HOLD", f"🚫 RSI extremamente sobrecomprado {rsi_value:.1f} - NÃO COMPRAR"
    
    return signal, None

# ---------- CSV LOGGING ----------

def log_signal_to_csv(signal_data):
    """Log signal to CSV file"""
    file_exists = os.path.isfile(LOG_CSV)

    signal_data_to_write = {}
    for k, v in signal_data.items():
        if k not in ['multicultural_analysis', 'timeframe_signals']:
            signal_data_to_write[k] = v
    
    with open(LOG_CSV, 'a', newline='') as csvfile:
        fieldnames = [
            'timestamp', 'symbol', 'signal', 'direction', 'probability', 'market_regime',
            'price', 'rsi', 'atr', 'adx', 'suggested_size', 'suggested_sl', 'suggested_tp',
            'rsi_filter', 'mtf_confirmation', 'final_signal', 'vedic_number', 'vedic_planet',
            'vedic_signal', 'thai_cycle', 'thai_elephant', 'multicultural_prob', 'japanese_patterns',
            'timeframe_consensus', 'block_reason', 'macd_line', 'macd_signal', 'macd_histogram',
            'bears_power', 'bulls_power', 'volume_ratio', 'volume_confirmation'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(signal_data_to_write)
    
    print(f"📝 Sinal salvo em: {LOG_CSV}")

# ---------- GERAÇÃO DE SINAL PARA TIMEFRAME ESPECÍFICO ----------

def generate_single_timeframe_signal(client, timeframe):
    """✅ NOVA FUNÇÃO: Gera sinal apenas para timeframe específico"""
    print(f"🎯 Gerando sinal para timeframe específico: {timeframe}")
    
    # Analisar apenas o timeframe específico
    timeframe_data = analyze_single_timeframe(client, timeframe)
    if not timeframe_data:
        return None
    
    # Obter dados do timeframe
    tf_signal_data = timeframe_data[timeframe]
    candles = candle_manager.get_candles(timeframe)
    
    if not candles or len(candles) < 30:
        return None
    
    # Construir features apenas com dados do timeframe específico
    result = build_features_from_candles(candles)
    if not result:
        return None
    
    features, closes, highs, lows, volumes = result
    
    # Calcular ATR médio
    if len(closes) > 14:
        avg_atr = sum([calculate_atr(highs[max(0,i-14):i+1], lows[max(0,i-14):i+1], closes[max(0,i-14):i+1], 14) for i in range(14, len(closes))]) / max(1, (len(closes) - 14))
    else:
        avg_atr = features['atr14']
    
    # Detectar regime de mercado
    market_regime = detect_market_regime(
        closes, features['adx'], features['atr14'], avg_atr
    )
    
    # Aplicar filtro de volatilidade
    if not volatility_filter(features['atr14'], avg_atr):
        return {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'symbol': SYMBOL,
            'signal': 'HOLD',
            'direction': 'NONE',
            'probability': 0.5,
            'market_regime': market_regime,
            'price': features['close'],
            'rsi': features['rsi14'],
            'atr': features['atr14'],
            'adx': features['adx'],
            'timeframe': timeframe,
            'timeframe_signal': tf_signal_data['signal'],
            'block_reason': "Alta volatilidade"
        }
    
    # Calcular probabilidade técnica
    tech_prob = heuristic_probability(features, market_regime)
    
    # ✅ SIMPLIFICADO: Usar apenas dados do timeframe específico
    combined_prob = tech_prob
    
    # Aplicar filtro Bull/Bear Strength
    combined_prob, force_decision = apply_bull_bear_strength_filter(
        candles, features, combined_prob, avg_atr
    )
    
    # ✅ LÓGICA SIMPLIFICADA PARA TIMEFRAME ÚNICO
    final_signal = "HOLD"
    direction = "NONE"

    if force_decision == 'BUY':
        if combined_prob >= PROB_BUY_THRESHOLD:
            final_signal = 'BUY'
            direction = 'CALL'
        else:
            print(f"🚫 BUY negado - Probabilidade insuficiente: {combined_prob:.1%}")
            
    elif force_decision == 'SELL':
        if combined_prob <= PROB_SELL_THRESHOLD:
            final_signal = 'SELL'
            direction = 'PUT'
        else:
            print(f"🚫 SELL negado - Probabilidade insuficiente: {combined_prob:.1%}")
            
    elif force_decision == 'HOLD':
        final_signal = 'HOLD'
        direction = 'NONE'
        
    else:
        # Lógica baseada no sinal do timeframe
        if tf_signal_data['signal'] == "BULLISH" and combined_prob >= PROB_BUY_THRESHOLD:
            final_signal = 'BUY'
            direction = 'CALL'
        elif tf_signal_data['signal'] == "BEARISH" and combined_prob <= PROB_SELL_THRESHOLD:
            final_signal = 'SELL' 
            direction = 'PUT'
        else:
            final_signal = 'HOLD'
            direction = 'NONE'
    
    # Aplicar filtro RSI
    initial_signal = final_signal
    final_signal, rsi_filter_reason = rsi_extreme_filter(features['rsi14'], final_signal, features['adx'])
    if final_signal != initial_signal:
        direction = 'NONE'
    
    # Cálculo de position sizing
    current_price = features['close']
    atr_value = features['atr14']
    
    if final_signal == 'BUY':
        sl = current_price - 1.5 * atr_value
        tp = current_price + 2.5 * atr_value
    elif final_signal == 'SELL':
        sl = current_price + 1.5 * atr_value  
        tp = current_price - 2.5 * atr_value
    else:
        sl = tp = 0
    
    risk_amount = 10000 * (RISK_PER_TRADE_PCT / 100)
    stop_distance = atr_value * 1.5
    stop_distance_pct = stop_distance / current_price if current_price > 0 else 0
    position_size = risk_amount / stop_distance_pct if stop_distance_pct > 0 else 0
    max_position = 10000 * (MAX_POSITION_SIZE_PCT / 100)
    suggested_size = min(position_size, max_position)
    
    signal_data = {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'symbol': SYMBOL,
        'signal': final_signal,
        'direction': direction,
        'probability': round(combined_prob, 3),
        'market_regime': market_regime,
        'price': round(current_price, 3),
        'rsi': round(features['rsi14'], 1),
        'atr': round(atr_value, 3),
        'adx': round(features['adx'], 1),
        'suggested_size': round(suggested_size, 2),
        'suggested_sl': round(sl, 3),
        'suggested_tp': round(tp, 3),
        'rsi_filter': rsi_filter_reason if final_signal == 'HOLD' and rsi_filter_reason else "OK",
        'final_signal': final_signal,
        'timeframe': timeframe,
        'timeframe_signal': tf_signal_data['signal'],
        'timeframe_strength': tf_signal_data['strength'],
        'block_reason': "Nenhum - Sinal gerado" if final_signal != 'HOLD' else "Probabilidade insuficiente",
        'macd_line': round(features.get('macd_line', 0), 4),
        'macd_signal': round(features.get('macd_signal', 0), 4),
        'macd_histogram': round(features.get('macd_histogram', 0), 4),
        'bears_power': round(features.get('bears_power', 0), 4),
        'bulls_power': round(features.get('bulls_power', 0), 4),
        'volume_ratio': features.get('volume_analysis', {}).get('volume_ratio', 0),
        'volume_confirmation': features.get('volume_analysis', {}).get('volume_confirmation', 'N/A')
    }
    
    log_signal_to_csv(signal_data)
    return signal_data

# ---------- SIGNAL GENERATION CORREÇÃO FINAL ----------

def generate_signal(client, analysis_count):
    """✅✅✅ ATUALIZADO: Geração de sinal com 6 TIMEFRAMES"""
    print("🔄 Gerando sinal com análise completa de 6 timeframes...")

    timeframe_signals, timeframe_data, rsi_values = analyze_multiple_timeframes(client)
    if not timeframe_data:
        print("❌ Falha ao obter dados multi-timeframe")
        return None, None
    
    # ✅✅✅ ATUALIZADO: APLICAR SISTEMA MARÉ/ONDAS PARA 6 TIMEFRAMES
    alignment_status, can_trade_alignment, alignment_msg = check_trend_alignment(timeframe_signals)
    quality_status, can_trade_quality, quality_msg = analyze_consensus_quality(timeframe_signals)
    
    print(f"🌊 SISTEMA 6 TIMEFRAMES: {alignment_msg}")
    print(f"🎯 QUALIDADE CONSENSO: {quality_msg}")
    
    # ✅✅✅ BLOQUEAR SE NÃO ATENDER CRITÉRIOS MARÉ/ONDAS
    if not can_trade_alignment:
        print("🚫 SINAL BLOQUEADO: Critérios de alinhamento Maré/Ondas não atendidos")
        signal_data = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'symbol': SYMBOL,
            'signal': 'HOLD',
            'direction': 'NONE',
            'probability': 0.5,
            'market_regime': 'RANGING',
            'price': 0,
            'rsi': 0,
            'atr': 0,
            'adx': 0,
            'suggested_size': 0,
            'suggested_sl': 0,
            'suggested_tp': 0,
            'rsi_filter': "N/A",
            'mtf_confirmation': False,
            'final_signal': 'HOLD',
            'vedic_number': 0,
            'vedic_planet': 'N/A',
            'vedic_signal': 'N/A',
            'thai_cycle': 'N/A',
            'thai_elephant': 'N/A',
            'multicultural_prob': 0.5,
            'japanese_patterns': 0,
            'timeframe_consensus': 'NEUTRAL',
            'block_reason': f"Alinhamento: {alignment_msg}",
            'macd_line': 0,
            'macd_signal': 0,
            'macd_histogram': 0,
            'bears_power': 0,
            'bulls_power': 0,
            'volume_ratio': 0,
            'volume_confirmation': 'N/A'
        }
        log_signal_to_csv(signal_data)
        return signal_data, rsi_values
    
    if not can_trade_quality:
        print("🚫 SINAL BLOQUEADO: Qualidade do consenso insuficiente")
        signal_data = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'symbol': SYMBOL,
            'signal': 'HOLD',
            'direction': 'NONE',
            'probability': 0.5,
            'market_regime': 'RANGING',
            'price': 0,
            'rsi': 0,
            'atr': 0,
            'adx': 0,
            'suggested_size': 0,
            'suggested_sl': 0,
            'suggested_tp': 0,
            'rsi_filter': "N/A",
            'mtf_confirmation': False,
            'final_signal': 'HOLD',
            'vedic_number': 0,
            'vedic_planet': 'N/A',
            'vedic_signal': 'N/A',
            'thai_cycle': 'N/A',
            'thai_elephant': 'N/A',
            'multicultural_prob': 0.5,
            'japanese_patterns': 0,
            'timeframe_consensus': 'NEUTRAL',
            'block_reason': f"Qualidade: {quality_msg}",
            'macd_line': 0,
            'macd_signal': 0,
            'macd_histogram': 0,
            'bears_power': 0,
            'bulls_power': 0,
            'volume_ratio': 0,
            'volume_confirmation': 'N/A'
        }
        log_signal_to_csv(signal_data)
        return signal_data, rsi_values
    
    # ✅✅✅ ATUALIZADO: Confirmação para CALL com 6 TIMEFRAMES
    call_confirmed, call_msg = verificar_confirmacao_call(timeframe_signals)
    
    # ✅ CONTINUAR ANÁLISE SE PASSOU NOS FILTROS MARÉ/ONDAS
    h1_data = timeframe_data.get('H1', {})
    if not h1_data:
        return None, None
    
    candles = h1_data.get('candles', [])
    print(f"📊 {len(candles)} candles H1 recebidos")
    
    result = build_features_from_candles(candles)
    if not result:
        print("❌ Dados insuficientes para análise técnica")
        return None, None
    
    features, closes, highs, lows, volumes = result
    
    # ✅ Obter consenso COM SISTEMA 6 TIMEFRAMES
    consensus, consensus_strength = get_timeframe_consensus(timeframe_signals, rsi_values)
    print(f"📊 Consenso 6 Timeframes: {consensus} (Força: {consensus_strength:.1%})")
    
    multicultural_data = multicultural_analysis(features, timeframe_data, analysis_count)
    multicultural_prob = multicultural_data.get('MULTICULTURAL', {}).get('probability', 0.5)
    
    # ✅ CORREÇÃO CRÍTICA: Calcular o score detalhado COM SISTEMA 6 TIMEFRAMES
    detailed_score = display_detailed_analysis(timeframe_signals, multicultural_prob, rsi_values)
    
    # ✅✅✅ CORREÇÃO: BLOQUEIO MENOS RESTRITIVO
    if detailed_score <= -900:
        print("🚫 SINAL BLOQUEADO: Score indica bloqueio por alinhamento/qualidade")
        signal_data = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'symbol': SYMBOL,
            'signal': 'HOLD',
            'direction': 'NONE',
            'probability': 0.5,
            'market_regime': 'RANGING',
            'price': features['close'],
            'rsi': features['rsi14'],
            'atr': features['atr14'],
            'adx': features['adx'],
            'suggested_size': 0,
            'suggested_sl': 0,
            'suggested_tp': 0,
            'rsi_filter': "N/A",
            'mtf_confirmation': False,
            'final_signal': 'HOLD',
            'vedic_number': multicultural_data.get('VEDIC', {}).get('number', 0),
            'vedic_planet': multicultural_data.get('VEDIC', {}).get('planet', 'N/A'),
            'vedic_signal': multicultural_data.get('VEDIC', {}).get('direction', 'N/A'),
            'thai_cycle': multicultural_data.get('THAI', {}).get('cycle_phase', 'N/A'),
            'thai_elephant': multicultural_data.get('THAI', {}).get('elephant_pattern', 'N/A'),
            'multicultural_prob': multicultural_prob,
            'japanese_patterns': sum(tf_data.get('patterns_found', 0) for tf_data in multicultural_data.get('JAPANESE', {}).values()),
            'timeframe_consensus': consensus,
            'block_reason': f"Score bloqueado: {detailed_score}",
            'macd_line': features.get('macd_line', 0),
            'macd_signal': features.get('macd_signal', 0),
            'macd_histogram': features.get('macd_histogram', 0),
            'bears_power': features.get('bears_power', 0),
            'bulls_power': features.get('bulls_power', 0),
            'volume_ratio': features.get('volume_analysis', {}).get('volume_ratio', 0),
            'volume_confirmation': features.get('volume_analysis', {}).get('volume_confirmation', 'N/A')
        }
        log_signal_to_csv(signal_data)
        return signal_data, rsi_values
    
    # Cálculo ATR médio
    if len(closes) > 14:
        avg_atr = sum([calculate_atr(highs[max(0,i-14):i+1], lows[max(0,i-14):i+1], closes[max(0,i-14):i+1], 14) for i in range(14, len(closes))]) / max(1, (len(closes) - 14))
    else:
        avg_atr = features['atr14']
    
    # ✅✅✅ AGORA com ADX REAL
    market_regime = detect_market_regime(
        closes, features['adx'], features['atr14'], avg_atr
    )
    
    if not volatility_filter(features['atr14'], avg_atr):
        signal_data = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'symbol': SYMBOL,
            'signal': 'HOLD',
            'direction': 'NONE',
            'probability': 0.5,
            'market_regime': market_regime,
            'price': features['close'],
            'rsi': features['rsi14'],
            'atr': features['atr14'],
            'adx': features['adx'],
            'suggested_size': 0,
            'suggested_sl': 0,
            'suggested_tp': 0,
            'rsi_filter': "Volatilidade alta",
            'mtf_confirmation': False,
            'final_signal': 'HOLD',
            'vedic_number': multicultural_data.get('VEDIC', {}).get('number', 0),
            'vedic_planet': multicultural_data.get('VEDIC', {}).get('planet', 'N/A'),
            'vedic_signal': multicultural_data.get('VEDIC', {}).get('direction', 'N/A'),
            'thai_cycle': multicultural_data.get('THAI', {}).get('cycle_phase', 'N/A'),
            'thai_elephant': multicultural_data.get('THAI', {}).get('elephant_pattern', 'N/A'),
            'multicultural_prob': multicultural_prob,
            'japanese_patterns': sum(tf_data.get('patterns_found', 0) for tf_data in multicultural_data.get('JAPANESE', {}).values()),
            'timeframe_consensus': consensus,
            'block_reason': "Alta volatilidade",
            'macd_line': features.get('macd_line', 0),
            'macd_signal': features.get('macd_signal', 0),
            'macd_histogram': features.get('macd_histogram', 0),
            'bears_power': features.get('bears_power', 0),
            'bulls_power': features.get('bulls_power', 0),
            'volume_ratio': features.get('volume_analysis', {}).get('volume_ratio', 0),
            'volume_confirmation': features.get('volume_analysis', {}).get('volume_confirmation', 'N/A')
        }
        log_signal_to_csv(signal_data)
        return signal_data, rsi_values
    
    tech_prob = heuristic_probability(features, market_regime)
    
    # ✅ CORREÇÃO: Pesos mais balanceados
    tech_weight = 0.35
    multi_weight = 0.30
    consensus_weight = 0.35
    
    combined_prob = (
        tech_prob * tech_weight +
        multicultural_prob * multi_weight +
        consensus_strength * consensus_weight
    )
    
    # ✅ CORREÇÃO: Ajuste baseado no consenso
    if consensus == "BULLISH":
        combined_prob = min(1.0, combined_prob * 1.1)
    elif consensus == "BEARISH":
        combined_prob = max(0.0, combined_prob * 0.9)
    
    combined_prob = max(0.0, min(1.0, combined_prob))
    
    # ✅✅✅ CORREÇÃO CRÍTICA: Aplicar filtro Bull/Bear Strength CORRIGIDO
    combined_prob, force_decision = apply_bull_bear_strength_filter(
        candles, features, combined_prob, avg_atr
    )
    
    # ✅✅✅ CORREÇÃO FINAL CRÍTICA: LÓGICA CORRIGIDA PARA SINAIS
    final_signal = "HOLD"
    direction = "NONE"

    if force_decision == 'BUY':
        if combined_prob >= PROB_BUY_THRESHOLD:
            final_signal = 'BUY'
            direction = 'CALL'
            print(f"✅ BUY confirmado por Bull/Bear Strength (Prob: {combined_prob:.1%})")
        else:
            print(f"🚫 BUY negado - Probabilidade insuficiente: {combined_prob:.1%} < {PROB_BUY_THRESHOLD:.1%}")
            
    elif force_decision == 'SELL':
        if combined_prob <= PROB_SELL_THRESHOLD:
            final_signal = 'SELL'
            direction = 'PUT'
            print(f"✅ SELL confirmado por Bull/Bear Strength (Prob: {combined_prob:.1%})")
        else:
            print(f"🚫 SELL negado - Probabilidade insuficiente: {combined_prob:.1%} > {PROB_SELL_THRESHOLD:.1%}")
            
    elif force_decision == 'HOLD':
        final_signal = 'HOLD'
        direction = 'NONE'
        print("✅ HOLD confirmado por Bull/Bear Strength")
        
    else:
        # ✅✅✅ CORREÇÃO CRÍTICA: LÓGICA SIMÉTRICA E INTELIGENTE COM CONFIRMAÇÃO CALL
        if detailed_score > 0.10 and combined_prob >= PROB_BUY_THRESHOLD and call_confirmed:
            final_signal = 'BUY'
            direction = 'CALL'
            print(f"✅ CALL CONFIRMADO: Score {detailed_score:.3f}, Prob {combined_prob:.1%}, {call_msg}")
                
        elif detailed_score < -0.10 and combined_prob <= PROB_SELL_THRESHOLD:
            final_signal = 'SELL'
            direction = 'PUT'
            print(f"✅ PUT CONFIRMADO: Score {detailed_score:.3f}, Prob {combined_prob:.1%}")
                
        else:
            final_signal = 'HOLD'
            direction = 'NONE'
            if not call_confirmed and detailed_score > 0:
                print(f"🟡 CALL BLOQUEADO: {call_msg}")
            else:
                print(f"🟡 HOLD - Score: {detailed_score:.3f}, Prob: {combined_prob:.1%} (fora dos limiares)")
    
    # ✅✅✅ CORREÇÃO #4: RSI filter INTELIGENTE com ADX
    initial_signal = final_signal
    final_signal, rsi_filter_reason = rsi_extreme_filter(features['rsi14'], final_signal, features['adx'])
    if final_signal != initial_signal:
        direction = 'NONE' if final_signal == 'HOLD' else direction
        print(f"🔧 Filtro RSI: {rsi_filter_reason}")
    
    # Cálculo de position sizing
    current_price = features['close']
    atr_value = features['atr14']
    
    if final_signal == 'BUY':
        sl = current_price - 1.5 * atr_value
        tp = current_price + 2.5 * atr_value
    elif final_signal == 'SELL':
        sl = current_price + 1.5 * atr_value  
        tp = current_price - 2.5 * atr_value
    else:
        sl = tp = 0
    
    risk_amount = 10000 * (RISK_PER_TRADE_PCT / 100)
    stop_distance = atr_value * 1.5
    stop_distance_pct = stop_distance / current_price if current_price > 0 else 0
    position_size = risk_amount / stop_distance_pct if stop_distance_pct > 0 else 0
    max_position = 10000 * (MAX_POSITION_SIZE_PCT / 100)
    suggested_size = min(position_size, max_position)
    
    # Contar padrões japoneses
    total_japanese_patterns = 0
    japanese_data = multicultural_data.get('JAPANESE', {})
    for tf_data in japanese_data.values():
        total_japanese_patterns += tf_data.get('patterns_found', 0)
    
    signal_data = {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'symbol': SYMBOL,
        'signal': final_signal,
        'direction': direction,
        'probability': round(combined_prob, 3),
        'market_regime': market_regime,
        'price': round(current_price, 3),
        'rsi': round(features['rsi14'], 1),
        'atr': round(atr_value, 3),
        'adx': round(features['adx'], 1),
        'suggested_size': round(suggested_size, 2),
        'suggested_sl': round(sl, 3),
        'suggested_tp': round(tp, 3),
        'rsi_filter': rsi_filter_reason if final_signal == 'HOLD' and rsi_filter_reason else "OK",
        'mtf_confirmation': True,
        'final_signal': final_signal,
        'vedic_number': multicultural_data.get('VEDIC', {}).get('number', 0),
        'vedic_planet': multicultural_data.get('VEDIC', {}).get('planet', 'N/A'),
        'vedic_signal': multicultural_data.get('VEDIC', {}).get('direction', 'N/A'),
        'thai_cycle': multicultural_data.get('THAI', {}).get('cycle_phase', 'N/A'),
        'thai_elephant': multicultural_data.get('THAI', {}).get('elephant_pattern', 'N/A'),
        'multicultural_prob': round(multicultural_prob, 3),
        'japanese_patterns': total_japanese_patterns,
        'timeframe_consensus': consensus,
        'block_reason': "Nenhum - Sinal gerado",
        'macd_line': round(features.get('macd_line', 0), 4),
        'macd_signal': round(features.get('macd_signal', 0), 4),
        'macd_histogram': round(features.get('macd_histogram', 0), 4),
        'bears_power': round(features.get('bears_power', 0), 4),
        'bulls_power': round(features.get('bulls_power', 0), 4),
        'volume_ratio': features.get('volume_analysis', {}).get('volume_ratio', 0),
        'volume_confirmation': features.get('volume_analysis', {}).get('volume_confirmation', 'N/A')
    }
    
    signal_data['multicultural_analysis'] = multicultural_data
    signal_data['timeframe_signals'] = timeframe_signals
    
    log_signal_to_csv(signal_data)
    return signal_data, rsi_values

# ---------- DISPLAY SIGNAL ATUALIZADO ----------

def display_signal(signal, rsi_values):
    """✅✅✅ ATUALIZADO: Display com 6 TIMEFRAMES"""
    if not signal:
        print("❌ Nenhum sinal gerado")
        return

    color = "🟢" if signal['signal'] == 'BUY' else "🔴" if signal['signal'] == 'SELL' else "🟡"
    prob_percent = signal['probability'] * 100
    
    # ✅ CORREÇÃO CRÍTICA: CLASSIFICAÇÃO CORRIGIDA
    if prob_percent >= 70:
        strength = "FORTE"
        strength_emoji = "🎯"
    elif prob_percent >= 60:
        strength = "MODERADO" 
        strength_emoji = "📊"
    elif prob_percent >= PROB_BUY_THRESHOLD * 100:
        strength = "FRACO"
        strength_emoji = "⚠️"
    elif prob_percent <= 30:
        strength = "FORTE"
        strength_emoji = "🎯"
    elif prob_percent <= 40:
        strength = "MODERADO"
        strength_emoji = "📊"
    elif prob_percent <= PROB_SELL_THRESHOLD * 100:
        strength = "FRACO" 
        strength_emoji = "⚠️"
    else:
        strength = "MUITO FRACO"
        strength_emoji = "🔻"
    
    multicultural_data = signal.get('multicultural_analysis', {})
    timeframe_signals = signal.get('timeframe_signals', {})
    
    print("\n" + "═" * 80)
    print("🎯 DERIV SIGNAL - SISTEMA 6 TIMEFRAMES COMPLETO")
    print("═" * 80)
    print(f"{color} SINAL: {signal['signal']} ({signal['direction']})")
    print(f"📊 Probabilidade Combinada: {prob_percent:.1f}% {strength_emoji} [{strength}]")
    print(f"🏛️ Regime: {signal['market_regime']}")
    print(f"📈 Consenso 6 Timeframes: {signal['timeframe_consensus']}")
    
    print("─" * 50)
    print("🌊 SISTEMA 6 TIMEFRAMES HIERÁRQUICOS:")
    
    # ✅ ANÁLISE DE ALINHAMENTO E QUALIDADE
    alignment_status, can_trade_alignment, alignment_msg = check_trend_alignment(timeframe_signals)
    quality_status, can_trade_quality, quality_msg = analyze_consensus_quality(timeframe_signals)
    
    print(f"  📊 Alinhamento Maré-Ondas: {alignment_status}")
    print(f"  🎯 Qualidade do Consenso: {quality_status}")
    
    for tf, data in timeframe_signals.items():
        tf_color = "🟢" if data['signal'] == 'BULLISH' else "🔴" if data['signal'] == 'BEARISH' else "🟡"
        dynamic_weights = get_dynamic_weights(rsi_values)
        weight = dynamic_weights.get(tf, 0.17) * 100
        
        # ✅✅✅ ATUALIZADO: Identificar cada timeframe no sistema hierárquico
        role = get_timeframe_role(tf)
        
        # ✅✅✅ NOVO: Mostrar indicadores adicionais
        volume_info = data.get('volume_analysis', {})
        macd_info = f"MACD:{data.get('macd_line', 0):.4f}"
        
        print(f"  {role} ({tf}): {tf_color} {data['signal']} (RSI: {data['rsi']:.1f}) [Peso: {weight:.0f}%]")
        print(f"     📊 {macd_info} | Bears:{data.get('bears_power', 0):.4f} | Bulls:{data.get('bulls_power', 0):.4f}")
        print(f"     📈 Volume: {volume_info.get('volume_confirmation', 'N/A')} (Ratio: {volume_info.get('volume_ratio', 0)})")
    
    print("─" * 50)
    print("🌏 ANÁLISE MULTICULTURAL:")
    
    if 'VEDIC' in multicultural_data:
        vedic = multicultural_data['VEDIC']
        print(f"📿 VÉDICO: Número {vedic['number']} ({vedic['planet']}) - {vedic['direction']}")
        print(f"  Ângulo Gann: {vedic['current_angle']} - Sinal: {vedic['gann_signal']}")
    
    if 'THAI' in multicultural_data:
        thai = multicultural_data['THAI']
        print(f"🐘 TAILANDÊS: Ciclo {thai['cycle_position']} ({thai['cycle_phase']})")
        print(f"  Padrão Elefante: {thai['elephant_pattern']} - Sinal: {thai['elephant_signal']}")
        if thai['elephant_pattern'] != 'NO_PATTERN':
            print(f"  Confiança Elefante: {thai.get('elephant_confidence', 0):.1%}")
        print(f"  Ciclo 9: {thai['nine_signal']} (Força: {thai['nine_strength']:.1%})")
    
    if 'JAPANESE' in multicultural_data:
        japanese = multicultural_data['JAPANESE']
        total_patterns = sum(tf_data['patterns_found'] for tf_data in japanese.values())
        print(f"🎌 JAPONÊS: {total_patterns} padrões encontrados")
    
    if 'MULTICULTURAL' in multicultural_data:
        multi = multicultural_data['MULTICULTURAL']
        print(f"🌐 MULTICULTURAL: {multi['signal']} ({multi['probability']:.1%})")
    
    if signal['rsi_filter'] != "OK":
        print(f"🔧 Filtro RSI: {signal['rsi_filter']}")
    
    print("─" * 50)
    print("📈 DADOS TÉCNICOS COMPLETOS:")
    print(f"💰 Preço Atual: {signal['price']}")
    print(f"📉 RSI 14: {signal['rsi']}")
    print(f"📏 ATR 14: {signal['atr']}")
    print(f"📈 ADX: {signal['adx']}")
    print(f"📊 MACD: {signal['macd_line']} | Signal: {signal['macd_signal']} | Histogram: {signal['macd_histogram']}")
    print(f"🐻 Bears Power: {signal['bears_power']} | 🐂 Bulls Power: {signal['bulls_power']}")
    print(f"📈 Volume: Ratio {signal['volume_ratio']} | Confirmação: {signal['volume_confirmation']}")
    
    print("─" * 50)
    if signal['signal'] != 'HOLD':
        print(f"💵 Position Size: ${signal['suggested_size']:.2f}")
        print(f"🛑 Stop Loss: {signal['suggested_sl']}")
        print(f"🎯 Take Profit: {signal['suggested_tp']}")
        risk_reward = abs(signal['suggested_tp'] - signal['price']) / abs(signal['suggested_sl'] - signal['price']) if signal['suggested_sl'] != signal['price'] else 0
        print(f"⚖️ Risk/Reward: 1:{risk_reward:.1f}")
        
        if strength in ["FORTE", "MODERADO"]:
            print("🎉 🎉 🎉 SINAL DE OPERAÇÃO! 🎉 🎉 🎉")
        else:
            print("⚠️  SINAL FRACO - CONSIDERE NÃO OPERAR  ⚠️")
    else:
        print("💤 Aguardando sinal ideal...")
        if 'block_reason' in signal and signal['block_reason'] != "Nenhum - Sinal gerado":
            print(f"🚫 Motivo do bloqueio: {signal['block_reason']}")
    
    print("─" * 50)
    print(f"⏰ {signal['timestamp']}")
    print("═" * 80)

# ========== SISTEMA RENDER + TELEGRAM ==========

class TradingBotManager:
    def __init__(self):
        self.client = None
        self.current_timeframe = "MULTI"  # Padrão: análise completa
        self.setup_deriv_client()
    
    def setup_deriv_client(self):
        """Configura o cliente Deriv em thread separada"""
        def connect_deriv():
            self.client = DerivClient(API_TOKEN)
            self.client.connect()
            time.sleep(2)
            if self.client.authorized:
                print("✅ Cliente Deriv configurado com sucesso!")
                # Iniciar atualização contínua de candles
                update_thread = threading.Thread(target=self.client.update_all_timeframes, daemon=True)
                update_thread.start()
                print("✅ Atualização contínua de candles iniciada!")
            else:
                print("❌ Falha na configuração do cliente Deriv")
        
        deriv_thread = threading.Thread(target=connect_deriv, daemon=True)
        deriv_thread.start()
    
    def change_timeframe(self, new_timeframe):
        """Alterar timeframe conforme solicitado via Telegram"""
        if new_timeframe.upper() == "MULTI":
            self.current_timeframe = "MULTI"
            return "✅ Modo alterado para: ANÁLISE COMPLETA 6 TIMEFRAMES"
        elif new_timeframe.upper() in TIMEFRAMES:
            self.current_timeframe = new_timeframe.upper()
            role = get_timeframe_role(self.current_timeframe)
            return f"✅ Timeframe alterado para: {self.current_timeframe} ({role})"
        return "❌ Timeframe inválido. Use: MULTI, M1, M5, M15, M30, H1, H4"
    
    def get_signal(self):
        """Gerar sinal baseado no modo atual (MULTI ou timeframe específico)"""
        if not self.client or not self.client.authorized:
            return "❌ Cliente Deriv não conectado. Aguarde alguns segundos..."
        
        try:
            if self.current_timeframe == "MULTI":
                print("🎯 Iniciando análise completa com 6 timeframes...")
                signal_data, rsi_values = generate_signal(self.client, 1)
            else:
                print(f"🎯 Iniciando análise para timeframe específico: {self.current_timeframe}")
                signal_data = generate_single_timeframe_signal(self.client, self.current_timeframe)
            
            if not signal_data:
                return "❌ Não foi possível gerar sinal. Problema na conexão com os dados."
            
            return self.format_signal_message(signal_data)
            
        except Exception as e:
            return f"❌ Erro ao gerar sinal: {str(e)}"
    
    def format_signal_message(self, signal_data):
        """✅✅✅ ATUALIZADO: Formatar mensagem no estilo solicitado COM VALIDADE DINÂMICA"""
        
        emoji = "🟢" if signal_data['direction'] == 'CALL' else "🔴" if signal_data['direction'] == 'PUT' else "⚪"
        
        # ✅✅✅ VALIDADE DINÂMICA baseada no timeframe
        timeframe_validity = {
            'M1': '1 minuto',
            'M5': '5 minutos', 
            'M15': '15 minutos',
            'M30': '30 minutos',
            'H1': '1 hora',
            'H4': '4 horas',
            'MULTI': '1 hora'  # Padrão para multi-timeframe
        }
        
        validity = timeframe_validity.get(self.current_timeframe, '1 hora')
        
        # ✅✅✅ NOVO ESTILO SOLICITADO
        message = f"""
{emoji} SINAL {signal_data['direction']} {emoji}

📊 Par: XAU/USD
⏰ Timeframe: {self.current_timeframe if self.current_timeframe != "MULTI" else "Multi-Timeframe"}
💰 Entrada: ${signal_data['price']:,.2f}
🎯 Alvo 1: ${signal_data['suggested_tp']:,.2f}
🛑 Stop: ${signal_data['suggested_sl']:,.2f}
⏳ Validade: {validity}
🎯 Confiança: {int(signal_data['probability'] * 100)}%
📉 Setup: Reversão de Tendência

🕒 Horário: {datetime.now().strftime("%H:%M:%S")}
🔔 ID: {random.randint(1000, 9999)}
"""
        
        return message

# Instância global do bot
trading_bot = TradingBotManager()

# ---------- COMANDOS TELEGRAM ----------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Bot de Trading - Sistema Timeframes Flexível* 🤖\n\n"
        "Comandos disponíveis:\n"
        "/sinal - Gerar sinal no modo atual\n"
        "/timeframe [MULTI|M1|M5|M15|M30|H1|H4] - Alterar modo\n"
        "/status - Status do sistema\n"
        "/info - Informações técnicas\n"
        "/ajuda - Ajuda",
        parse_mode='Markdown'
    )

async def sinal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if trading_bot.current_timeframe == "MULTI":
        await update.message.reply_text("🔄 Gerando sinal com análise completa de 6 timeframes...")
    else:
        role = get_timeframe_role(trading_bot.current_timeframe)
        await update.message.reply_text(f"🔄 Gerando sinal para {trading_bot.current_timeframe} ({role})...")
    
    signal_message = trading_bot.get_signal()
    await update.message.reply_text(signal_message, parse_mode='Markdown')

async def timeframe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        current_role = "ANÁLISE COMPLETA" if trading_bot.current_timeframe == "MULTI" else get_timeframe_role(trading_bot.current_timeframe)
        await update.message.reply_text(
            f"⏰ *Modo Atual:* {trading_bot.current_timeframe} ({current_role})\n\n"
            "Use: /timeframe [MULTI|M1|M5|M15|M30|H1|H4]\n"
            "• MULTI: Análise completa 6 timeframes\n"
            "• M1: 🌪️ Alerta Precoce\n"
            "• M5: 💨 Momentum\n"  
            "• M15: 💦 Ondas Curta\n"
            "• M30: 💧 Ondas Média\n"
            "• H1: 🌊 Ondas Longa\n"
            "• H4: 🌋 Maré Principal",
            parse_mode='Markdown'
        )
        return
    
    result = trading_bot.change_timeframe(context.args[0])
    await update.message.reply_text(result)

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    current_mode = "ANÁLISE COMPLETA 6 TIMEFRAMES" if trading_bot.current_timeframe == "MULTI" else f"{trading_bot.current_timeframe} ({get_timeframe_role(trading_bot.current_timeframe)})"
    
    status_msg = f"""
📊 *STATUS DO SISTEMA*

• ✅ Bot: Online
• 💱 Ativo: {SYMBOL}
• 🎯 Modo Trading: {TRADING_MODE}
• ⏰ Modo Atual: {current_mode}
• 🌐 Deriv: {'✅ Conectado' if trading_bot.client and trading_bot.client.authorized else '🔄 Conectando...'}
• 📊 Candles: {'✅ Armazenados' if candle_manager.candles_data else '🔄 Coletando...'}
• 🕒 Última Atualização: {datetime.now().strftime('%H:%M:%S')}

🔧 *Configurações:*
• CALL ≥ {PROB_BUY_THRESHOLD:.0%} | PUT ≤ {PROB_SELL_THRESHOLD:.0%}
• Confirmações: CALL {MIN_CALL_CONFIRMATIONS}+ | PUT {MIN_PUT_CONFIRMATIONS}+
• Timeframes Ativos: 6/6
• Limpeza: 23h Angola (22h UTC)
"""
    await update.message.reply_text(status_msg, parse_mode='Markdown')

async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    info_text = """
🎯 *SISTEMA TIMEFRAMES FLEXÍVEL*

📊 *MODOS DISPONÍVEIS:*
• 🌐 MULTI: Análise completa 6 timeframes
• 🌪️ M1: Alerta Precoce (12%)
• 💨 M5: Momentum (13%)
• 💦 M15: Ondas Curta (15%) 
• 💧 M30: Ondas Média (18%)
• 🌊 H1: Ondas Longa (20%)
• 🌋 H4: Maré Principal (22%)

📈 *ANÁLISE INCLUI:*
• RSI, MACD, Tendência Real
• Força Bulls/Bears  
• Confirmação Volume
• Filtros de Volatilidade
• Análise Multicultural

⚡ *ATUALIZAÇÃO CONTÍNUA*
🕒 *Limpeza automática às 23h Angola*

⚠️ *Isenção de Responsabilidade*
Este bot é para fins educacionais.
Opere por sua conta e risco.
"""
    await update.message.reply_text(info_text, parse_mode='Markdown')

async def ajuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)

# Configuração do Bot Telegram
def setup_telegram_bot():
    """Configura e executa o bot do Telegram em background"""
    async def run_bot():
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        
        # Handlers de comando
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("sinal", sinal))
        application.add_handler(CommandHandler("timeframe", timeframe))
        application.add_handler(CommandHandler("status", status))
        application.add_handler(CommandHandler("info", info))
        application.add_handler(CommandHandler("ajuda", ajuda))
        
        print("🤖 Bot do Telegram inicializado!")
        await application.run_polling()
    
    # Executa o bot em uma thread separada
    def run_async():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_bot())
    
    bot_thread = threading.Thread(target=run_async, daemon=True)
    bot_thread.start()

# ---------- LOOP INFINITO PARA MANTER O RENDER ATIVO ----------

def background_loop():
    """Loop infinito que mantém o app vivo"""
    while True:
        try:
            # Verificar limpeza de dados às 23h Angola
            candle_manager.cleanup_old_data()
            time.sleep(60)
        except Exception as e:
            print(f"Erro no loop: {e}")
            time.sleep(30)

# ---------- ROTAS RENDER ----------

@app.route('/')
def home():
    return {
        "status": "online", 
        "system": "6 Timeframes Flexível",
        "symbol": SYMBOL,
        "mode": TRADING_MODE,
        "current_timeframe": trading_bot.current_timeframe,
        "timestamp": datetime.now().isoformat()
    }

@app.route('/health')
def health():
    return {
        "status": "healthy", 
        "deriv_connected": trading_bot.client.authorized if trading_bot.client else False,
        "candles_stored": len(candle_manager.candles_data)
    }

@app.route('/sinal')
def sinal_web():
    signal_message = trading_bot.get_signal()
    return {"signal": signal_message}

# ---------- INICIALIZAÇÃO ----------

if __name__ == '__main__':
    print("🚀 Iniciando Sistema Timeframes Flexível no Render...")
    print(f"🎯 Modo de Trading: {TRADING_MODE}")
    print(f"💱 Símbolo: {SYMBOL}")
    print(f"🌐 Timeframes: {len(TIMEFRAMES)} configurados")
    print(f"⏰ Modo Inicial: {trading_bot.current_timeframe}")
    
    # Inicia o bot do Telegram
    if TELEGRAM_BOT_TOKEN:
        setup_telegram_bot()
        print("✅ Bot do Telegram configurado")
    else:
        print("❌ Token do Telegram não configurado")
    
    # Inicia o loop em background
    loop_thread = threading.Thread(target=background_loop, daemon=True)
    loop_thread.start()
    print("✅ Loop infinito iniciado")
    
    # Inicia o servidor Flask
    print(f"🌐 Servidor web iniciado na porta {PORT}")
    app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False)