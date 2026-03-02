"""
Consumer basico de Kafka usando confluent-kafka.

Ejecutar:
 uv run python src/consumers/basic_consumer.py

Requisitos:
 - Redpanda corriendo en localhost:19092
 - docker compose -f docker/docker-compose.yml up -d
"""

from confluent_kafka import Consumer, KafkaError, KafkaException


# =============================================================================
# CONFIGURACION
# =============================================================================

KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:19092',  # Puerto externo de Redpanda

    # group.id - OBLIGATORIO
    # Identifica el consumer group. Consumers con el mismo group.id
    # comparten el trabajo de consumir las particiones.
    'group.id': 'mi-primer-grupo',

    # client.id - Identificador unico para este consumer
    'client.id': 'basic-consumer-1',

    # auto.offset.reset - Que hacer si no hay offset guardado?
    # 'earliest': Leer desde el primer mensaje disponible
    # 'latest':   Leer solo mensajes nuevos (despues de suscribirse)
    'auto.offset.reset': 'earliest',

    # Commit manual para garantizar at-least-once delivery
    'enable.auto.commit': False,

    'session.timeout.ms': 45000,
    'heartbeat.interval.ms': 15000,
    'max.poll.interval.ms': 300000,
}

TOPIC = 'eventos-prueba'  # Mismo topic que usa el producer


# =============================================================================
# CALLBACKS DE REBALANCEO
# =============================================================================

def on_assign(_consumer, partitions) -> None:
    """Callback ejecutado cuando se asignan particiones a este consumer."""
    if partitions:
        partition_info = [f"{p.topic}[{p.partition}]" for p in partitions]
        print(f"[REBALANCE] Particiones asignadas: {', '.join(partition_info)}")


def on_revoke(consumer, partitions) -> None:
    """Callback ejecutado cuando se revocan particiones de este consumer."""
    if partitions:
        partition_info = [f"{p.topic}[{p.partition}]" for p in partitions]
        print(f"[REBALANCE] Particiones revocadas: {', '.join(partition_info)}")
        try:
            consumer.commit(asynchronous=False)
        except KafkaException:
            pass


# =============================================================================
# PROCESAMIENTO DE MENSAJES
# =============================================================================

def procesar_mensaje(msg) -> None:
    """
    Procesa un mensaje recibido de Kafka.

    Args:
        msg: Objeto Message de confluent-kafka
    """
    topic = msg.topic()
    partition = msg.partition()
    offset = msg.offset()
    timestamp = msg.timestamp()

    key = msg.key().decode('utf-8', errors='replace') if msg.key() else "(sin key)"
    value = msg.value().decode('utf-8', errors='replace') if msg.value() else ""

    print(f"[MENSAJE] topic={topic}, partition={partition}, offset={offset}")
    print(f"          key={key}")
    print(f"          value='{value}'")
    print(f"          timestamp={timestamp}")
    print()


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Funcion principal que demuestra el uso del consumer."""

    print("=" * 60)
    print("Consumer Basico de Kafka")
    print("=" * 60)
    print(f"Broker:   {KAFKA_CONFIG['bootstrap.servers']}")
    print(f"Topic:    {TOPIC}")
    print(f"Group ID: {KAFKA_CONFIG['group.id']}")
    print(f"Client:   {KAFKA_CONFIG['client.id']}")
    print("=" * 60)
    print("Esperando mensajes... (Ctrl+C para salir)")
    print()

    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    mensajes_procesados = 0
    mensajes_fallidos = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(
                        f"[INFO] Fin de particion {msg.partition()} "
                        f"alcanzado en offset {msg.offset()}"
                    )
                else:
                    print(f"[ERROR] {msg.error()}")
                continue

            try:
                procesar_mensaje(msg)
                mensajes_procesados += 1
                consumer.commit(asynchronous=False)

            except Exception as e:
                mensajes_fallidos += 1
                print(f"[ERROR] Fallo al procesar mensaje en offset {msg.offset()}: {e}")
                consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        print("\n" + "-" * 60)
        print("Interrupcion recibida, cerrando consumer...")

    finally:
        consumer.close()
        print(f"Mensajes procesados: {mensajes_procesados}")
        print(f"Mensajes fallidos:   {mensajes_fallidos}")
        print("Consumer cerrado correctamente")
        print("-" * 60)


if __name__ == "__main__":
    main()
