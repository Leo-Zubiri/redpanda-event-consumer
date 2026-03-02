"""
Producer basico de Kafka usando confluent-kafka.

Este ejemplo muestra los conceptos fundamentales:
- Crear un producer
- Enviar mensajes con y sin key
- Manejar callbacks de confirmacion
- Usar flush() para asegurar entrega

Ejecutar:
 uv run python src/producers/basic_producer.py

Requisitos:
 - Redpanda corriendo en localhost:19092
 - docker compose -f docker/docker-compose.yml up -d
"""

from confluent_kafka import Producer


# =============================================================================
# CONFIGURACION
# =============================================================================

# Configuracion minima del producer
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:19092',  # Puerto externo de Redpanda
}

TOPIC = 'eventos-prueba'  # Topic donde enviaremos los mensajes


# =============================================================================
# CALLBACK DE ENTREGA
# =============================================================================

def delivery_callback(err, msg):
    """
    Callback que se ejecuta cuando Kafka confirma (o falla) la entrega.

    Este callback es fundamental para saber si los mensajes llegaron.
    Se ejecuta de forma asincrona cuando llamamos a poll() o flush().

    Args:
        err: KafkaError si hubo un problema, None si todo bien
        msg: El mensaje que se intento enviar (Message object)
    """
    if err is not None:
        print(f"[ERROR] Fallo al entregar mensaje: {err}")
    else:
        key = msg.key().decode('utf-8') if msg.key() else "(sin key)"
        value = msg.value().decode('utf-8') if msg.value() else ""

        print(
            f"[OK] partition={msg.partition()}, offset={msg.offset()}, "
            f"key={key}, value='{value}'"
        )


# =============================================================================
# FUNCIONES DE EJEMPLO
# =============================================================================

def enviar_mensaje_simple(producer: Producer, mensaje: str) -> None:
    """
    Envia un mensaje simple sin key.

    Sin key, Kafka usa round-robin para distribuir mensajes
    entre las particiones disponibles.
    """
    print(f"\nEnviando mensaje simple: '{mensaje}'")

    producer.produce(
        topic=TOPIC,
        value=mensaje.encode('utf-8'),
        callback=delivery_callback,
    )


def enviar_mensaje_con_key(producer: Producer, key: str, mensaje: str) -> None:
    """
    Envia un mensaje con key.

    La key determina a que particion va el mensaje.
    Mensajes con la misma key SIEMPRE van a la misma particion,
    lo que garantiza orden de procesamiento.
    """
    print(f"\nEnviando mensaje con key='{key}': '{mensaje}'")

    producer.produce(
        topic=TOPIC,
        key=key.encode('utf-8'),
        value=mensaje.encode('utf-8'),
        callback=delivery_callback,
    )


def enviar_multiples_mensajes(producer: Producer, cantidad: int) -> None:
    """
    Envia multiples mensajes demostrando el uso de poll().

    Cuando envias muchos mensajes, es buena practica llamar a poll()
    periodicamente para:
    1. Procesar callbacks pendientes
    2. Evitar que el buffer interno se llene
    """
    print(f"\nEnviando {cantidad} mensajes...")

    for i in range(cantidad):
        producer.produce(
            topic=TOPIC,
            key=f"batch-{i % 3}".encode('utf-8'),
            value=f"Mensaje numero {i}".encode('utf-8'),
            callback=delivery_callback,
        )

        if i % 10 == 0:
            producer.poll(0)

    producer.poll(0)


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Funcion principal que demuestra el uso del producer."""

    print("=" * 60)
    print("Producer Basico de Kafka")
    print("=" * 60)
    print(f"Broker: {KAFKA_CONFIG['bootstrap.servers']}")
    print(f"Topic: {TOPIC}")
    print("=" * 60)

    producer = Producer(KAFKA_CONFIG)

    enviar_mensaje_simple(producer, "Hola Redpanda!")

    enviar_mensaje_con_key(producer, "usuario-1", "Login exitoso")
    enviar_mensaje_con_key(producer, "usuario-1", "Compra realizada")
    enviar_mensaje_con_key(producer, "usuario-2", "Login exitoso")

    enviar_multiples_mensajes(producer, 5)

    print("\n" + "-" * 60)
    print("Esperando confirmacion de todos los mensajes (flush)...")
    print("-" * 60)

    mensajes_pendientes = producer.flush(timeout=10)

    if mensajes_pendientes > 0:
        print(f"[ADVERTENCIA] {mensajes_pendientes} mensajes no se pudieron enviar")
    else:
        print("\n[COMPLETADO] Todos los mensajes fueron entregados")


if __name__ == "__main__":
    main()
