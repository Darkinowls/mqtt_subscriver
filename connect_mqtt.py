import paho.mqtt.client as mqtt

TOPIC = "denys-wokwi-weather12"

BROKER = "broker.hivemq.com"


def __on_connect(client: mqtt.Client, userdata, flags, rc: int):
    print("Connected with result code " + str(rc))
    client.subscribe(TOPIC)


def __on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
    global messages_in_bytes
    messages_in_bytes.append(msg.payload)
    print("MQTT:" + msg.topic + " " + str(msg.payload.decode()))


def __connect_mqtt(client: mqtt.Client):
    client.on_connect = __on_connect
    client.on_message = __on_message
    client.connect(BROKER)
    return client


messages_in_bytes: list[bytes] = []
mqtt_client = mqtt.Client()
__connect_mqtt(mqtt_client)

