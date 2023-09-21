import paho.mqtt.client as mqtt
import os
import json

from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions
from dotenv import load_dotenv

# Load environment variables from .env file
dotenv_loaded = load_dotenv()

# Check if .env file was loaded successfully
if not dotenv_loaded:
    print("Failed to load .env file. Please make sure it exists in the project directory.")
    exit(1)

# Meta vars
user = os.getenv("USER")

# MQTT settings
mqtt_broker = os.getenv("MQTT_BROKER")
mqtt_topic = os.getenv("MQTT_TOPIC")
mqtt_username = os.getenv("MQTT_USERNAME")
mqtt_password = os.getenv("MQTT_PASSWORD")
mqtt_attribute_soc = os.getenv("MQTT_ATTRIBUTE_SOC")
mqtt_attribute_solar_voltage = os.getenv("MQTT_ATTRIBUTE_SOLAR_VOLTAGE")
mqtt_attribute_solar_current = os.getenv("MQTT_ATTRIBUTE_SOLAR_CURRENT")

# Supabase settings
supabase_url = os.getenv("SUPABASE_URL")
supabase_api_key = os.getenv("SUPABASE_API_KEY")
# supabase_table = os.getenv("SUPABASE_TABLE")

# Check for null values
if None in (
    user, 
    mqtt_broker, 
    mqtt_topic, 
    mqtt_username, 
    mqtt_password, 
    mqtt_attribute_soc, 
    mqtt_attribute_solar_voltage, 
    mqtt_attribute_solar_current, 
    supabase_url, 
    supabase_api_key
    ):
    print("One or more required environment variables are missing. Please check your .env file.")
    exit(1)

supabase: Client = create_client(
    supabase_url=supabase_url, 
    supabase_key=supabase_api_key, 
    options=ClientOptions(schema="public")
    )

# Callback when MQTT message is received
def on_message(client, userdata, message):
    payload = message.payload.decode("utf-8")
    json_payload = json.loads(payload)
    supabase.table(user).insert({
        "soc": int(json_payload[mqtt_attribute_soc]),
        "solar": float(json_payload[mqtt_attribute_solar_voltage]) * float(json_payload[mqtt_attribute_solar_current]),
    }).execute()

def subscribe():
    # Set up MQTT client
    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set(username=mqtt_username, password=mqtt_password)
    mqtt_client.on_message = on_message

    print(f"Subscribing to topic: {mqtt_topic}")
    # Connect to MQTT broker
    mqtt_client.connect(mqtt_broker)
    mqtt_client.subscribe(mqtt_topic)
    mqtt_client.loop_forever()

def main():
    subscribe()

main()