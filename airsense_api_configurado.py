# =============================================================
#  AirSense — API Completa
#  Projeto Integrador | SENAI 134 | Ciência de Dados
# =============================================================
#  Funcionalidades:
#   1. Recebe dados da caixinha (sensor IoT) via MQTT
#   2. Salva medições no Supabase (tabela monitoramento_ambiental)
#   3. Busca dados externos de qualidade do ar (WAQI)
#   4. Salva dados externos no Supabase (tabela dados_externos)
#   5. Registra notificações (tabela historico_notificacoes)
# =============================================================

import os
import json
import threading
import requests
from datetime import datetime, timezone
from flask import Flask, jsonify, request
import paho.mqtt.client as mqtt
from supabase import create_client, Client

# =============================================================
# CONFIGURAÇÕES — altere aqui ou use variáveis de ambiente
# =============================================================

# Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://fbjfezoykihnqprdarwn.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_publishable_lIiumEWkDEx71ZL9C8Sjww_IguWieGm")

# MQTT (caixinha)
MQTT_BROKER   = "test.mosquitto.org"
MQTT_PORT     = 1883
MQTT_TOPIC    = "projeto_integrado/SENAI134/Cienciadedados/grupoX"

# WAQI (qualidade do ar externa)
WAQI_TOKEN    = os.environ.get("WAQI_TOKEN", "63b149c5e5226189d1ab137bf25a0ea82028126a")
WAQI_CIDADE   = "São Paulo"   # cidade consultada

# Ambiente padrão para medições da caixinha (id cadastrado no banco)
ID_AMBIENTE_PADRAO = 1

# Sensor padrão da caixinha (id cadastrado no banco)
ID_SENSOR_PADRAO = 1

# =============================================================
# INICIALIZAÇÃO
# =============================================================

app: Flask = Flask(__name__)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# =============================================================
# 1. MQTT — CAIXINHA (SENSOR IOT)
# =============================================================

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[MQTT] Conectado ao broker ({MQTT_BROKER})")
        client.subscribe(MQTT_TOPIC)
        print(f"[MQTT] Inscrito no tópico: {MQTT_TOPIC}")
    else:
        print(f"[MQTT] Falha na conexão. Código: {rc}")


def on_message(client, userdata, msg):
    """
    Recebe payload JSON da caixinha e salva no Supabase.
    Formato esperado:
    {
      "temperature": 24.5,
      "pressure": 1013.2,
      "altitude": 760.0,
      "humidity": 65.3,
      "CO2": 450,
      "particula1": 12.4,   <- PM2.5
      "particula2": 20.1,   <- PM10
      "timestamp": 1718000000
    }
    """
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        print(f"[MQTT] Mensagem recebida: {payload}")

        # Converte timestamp Unix → datetime com fuso UTC
        timestamp_unix = payload.get("timestamp")
        if timestamp_unix is None:
            print("[MQTT] Timestamp ausente no payload. Usando horário atual.")
            registro_tempo = datetime.now(tz=timezone.utc).isoformat()
        else:
            registro_tempo = datetime.fromtimestamp(int(timestamp_unix), tz=timezone.utc).isoformat()

        # Monta o registro para a tabela monitoramento_ambiental
        novo_registro = {
            "temperature":    payload.get("temperature"),
            "pressure":       payload.get("pressure"),
            "altitude":       payload.get("altitude"),
            "humidity":       payload.get("humidity"),
            "CO2":            payload.get("CO2"),
            "particula1":     payload.get("particula1"),
            "particula2":     payload.get("particula2"),
            "registro_tempo": registro_tempo,
            "id_ambiente":    ID_AMBIENTE_PADRAO,
            "id_sensor":      ID_SENSOR_PADRAO,
        }

        # Insere no Supabase
        resultado = supabase.table("monitoramento_ambiental").insert(novo_registro).execute()
        print(f"[MQTT] Dados salvos no Supabase: {resultado.data}")

        # Verifica regras de negócio e dispara notificações se necessário
        verificar_alertas(payload, resultado.data[0].get("id") if resultado.data else None)

    except json.JSONDecodeError as e:
        print(f"[MQTT] Erro ao decodificar JSON: {e}")
    except Exception as e:
        print(f"[MQTT] Erro ao salvar dados: {e}")


def iniciar_mqtt():
    """Inicia o cliente MQTT em thread separada (não bloqueia o Flask)."""
    cliente = mqtt.Client()
    cliente.on_connect = on_connect
    cliente.on_message = on_message
    cliente.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    cliente.loop_forever()   # mantém conexão viva


# =============================================================
# 2. API EXTERNA — WAQI (qualidade do ar)
# =============================================================

def buscar_dados_waqi(cidade: str = WAQI_CIDADE) -> dict | None:
    """
    Consulta a API WAQI e retorna os dados de qualidade do ar.
    Documentação: https://aqicn.org/json-api/doc/
    """
    url = f"https://api.waqi.info/feed/{cidade}/?token={WAQI_TOKEN}"
    try:
        resposta = requests.get(url, timeout=10)
        dados = resposta.json()

        if dados.get("status") != "ok":
            print(f"[WAQI] Resposta inesperada: {dados}")
            return None

        iaqi = dados["data"].get("iaqi", {})
        retorno = {
            "fonte":          "WAQI",
            "local_consulta": cidade,
            "pm25":           iaqi.get("pm25", {}).get("v"),
            "pm10":           iaqi.get("pm10", {}).get("v"),
            "CO2":            iaqi.get("co", {}).get("v"),   # monóxido de carbono (proxy)
            "temperature":    iaqi.get("t", {}).get("v"),
            "humidity":       iaqi.get("h", {}).get("v"),
            "data_medicao":   dados["data"].get("time", {}).get("iso"),
            "data_consulta":  datetime.now(tz=timezone.utc).isoformat(),
            "id_alerta":      0,  # sem alerta vinculado neste momento
        }
        print(f"[WAQI] Dados obtidos: {retorno}")
        return retorno

    except requests.RequestException as e:
        print(f"[WAQI] Erro na requisição: {e}")
        return None


def salvar_dados_externos(dados: dict) -> int | None:
    """Salva os dados externos no Supabase e retorna o id gerado."""
    try:
        resultado = supabase.table("dados_externos").insert(dados).execute()
        id_gerado = resultado.data[0].get("id_dado_externo") if resultado.data else None
        print(f"[WAQI] Dados salvos no Supabase. id={id_gerado}")
        return id_gerado
    except Exception as e:
        print(f"[WAQI] Erro ao salvar dados externos: {e}")
        return None


# =============================================================
# 3. REGRAS DE NEGÓCIO E NOTIFICAÇÕES
# =============================================================

LIMITES = {
    "CO2":        1000,   # ppm — acima disso: alerta
    "temperature_max": 35,   # °C — acima: calor extremo
    "temperature_min":  5,   # °C — abaixo: frio extremo
    "humidity_min":    30,   # % — abaixo: ar muito seco
    "particula1":      25,   # µg/m³ PM2.5 OMS
    "particula2":      50,   # µg/m³ PM10 OMS
}

def verificar_alertas(payload: dict, id_monitoramento: int | None):
    """Verifica limites e registra notificações no banco."""
    alertas = []

    co2 = payload.get("CO2")
    temp = payload.get("temperature")
    umid = payload.get("humidity")
    pm25 = payload.get("particula1")
    pm10 = payload.get("particula2")

    if co2 and co2 > LIMITES["CO2"]:
        alertas.append(f"⚠️ CO₂ elevado: {co2} ppm (limite: {LIMITES['CO2']} ppm)")

    if temp is not None:
        if temp > LIMITES["temperature_max"]:
            alertas.append(f"🌡️ Temperatura alta: {temp}°C")
        elif temp < LIMITES["temperature_min"]:
            alertas.append(f"🌡️ Temperatura baixa: {temp}°C")

    if umid and umid < LIMITES["humidity_min"]:
        alertas.append(f"💧 Ar muito seco: {umid}% umidade")

    if pm25 and pm25 > LIMITES["particula1"]:
        alertas.append(f"🏭 PM2.5 elevado: {pm25} µg/m³")

    if pm10 and pm10 > LIMITES["particula2"]:
        alertas.append(f"🏭 PM10 elevado: {pm10} µg/m³")

    for mensagem in alertas:
        registrar_notificacao(mensagem, id_monitoramento=id_monitoramento)


def registrar_notificacao(
    mensagem: str,
    canal: str = "sistema",
    id_monitoramento: int | None = None,
    id_dado_externo: int | None = None,
):
    """Registra uma notificação na tabela historico_notificacoes."""
    try:
        notificacao = {
            "id_alerta":        0,
            "mensagem":         mensagem,
            "canal":            canal,
            "status_envio":     "registrado",
            "enviado_em":       datetime.now(tz=timezone.utc).isoformat(),
            "id_monitoramento": id_monitoramento,
            "id_dado_externo":  id_dado_externo,
        }
        supabase.table("historico_notificacoes").insert(notificacao).execute()
        print(f"[ALERTA] {mensagem}")
    except Exception as e:
        print(f"[ALERTA] Erro ao registrar notificação: {e}")


# =============================================================
# 4. ROTAS DA API FLASK
# =============================================================

@app.route("/")
def index():
    return jsonify({"projeto": "AirSense", "status": "online"})


@app.route("/dados", methods=["GET"])
def listar_dados():
    """Retorna os últimos 50 registros do monitoramento ambiental."""
    try:
        resultado = (
            supabase.table("monitoramento_ambiental")
            .select("*")
            .order("registro_tempo", desc=True)
            .limit(50)
            .execute()
        )
        return jsonify(resultado.data)
    except Exception as e:
        return jsonify({"erro": str(e)}), 500


@app.route("/dados-externos", methods=["GET"])
def listar_dados_externos():
    """Retorna os últimos 20 registros de dados externos (WAQI)."""
    try:
        resultado = (
            supabase.table("dados_externos")
            .select("*")
            .order("data_consulta", desc=True)
            .limit(20)
            .execute()
        )
        return jsonify(resultado.data)
    except Exception as e:
        return jsonify({"erro": str(e)}), 500


@app.route("/buscar-ar-externo", methods=["POST"])
def buscar_ar_externo():
    """
    Consulta a WAQI agora e salva no banco.
    Chame via POST /buscar-ar-externo
    Body JSON opcional: { "cidade": "São Paulo" }
    """
    corpo = request.get_json(silent=True) or {}
    cidade = corpo.get("cidade", WAQI_CIDADE)

    dados = buscar_dados_waqi(cidade)
    if dados is None:
        return jsonify({"erro": "Falha ao consultar WAQI"}), 502

    id_externo = salvar_dados_externos(dados)
    return jsonify({"mensagem": "Dados externos salvos", "id": id_externo, "dados": dados})


@app.route("/notificacoes", methods=["GET"])
def listar_notificacoes():
    """Retorna as últimas 30 notificações registradas."""
    try:
        resultado = (
            supabase.table("historico_notificacoes")
            .select("*")
            .order("enviado_em", desc=True)
            .limit(30)
            .execute()
        )
        return jsonify(resultado.data)
    except Exception as e:
        return jsonify({"erro": str(e)}), 500


# =============================================================
# 5. INICIALIZAÇÃO
# =============================================================

if __name__ == "__main__":
    # Inicia MQTT em thread separada para não bloquear o Flask
    thread_mqtt = threading.Thread(target=iniciar_mqtt, daemon=True)
    thread_mqtt.start()
    print("[APP] Thread MQTT iniciada.")

    porta = int(os.environ.get("PORT", 8080))
    print(f"[APP] Flask rodando na porta {porta}")
    app.run(host="0.0.0.0", port=porta, debug=False)
