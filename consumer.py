import logging
import time
from typing import TYPE_CHECKING

from config import get_connection, configure_logging, MQ_EXCHANGE, MQ_ROUTING_KEY


if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingConnection
    from pika.spec import Basic, BasicProperties

log = logging.getLogger(__name__)


def process_new_message(ch: "BlockingConnection", method: "Basic.Deliver", properties: "BasicProperties", body: bytes):
    log.debug("Received message: %s", body)
    log.debug("Received message: %s", method)
    log.debug("Received message: %s", properties)
    log.debug("Received message: %s", ch)

    log.info("[ ] Start processing message %r", body)
    start = time.time()

    number = int(body[-2:])
    is_odd = number % 2
    time.sleep(1 + is_odd * 2)
    end = time.time()
    ch.basic_ack(delivery_tag=method.delivery_tag)
    log.info("[x] Finished processing message %s in %s seconds", body, end - start)

def consume_messages(channel: "BlockingConnection") -> None:
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue=MQ_ROUTING_KEY,
        on_message_callback=process_new_message,
        # auto_ack=True
    )
    log.warning("Waiting for messages...")
    channel.start_consuming()


def main():
    configure_logging(level=logging.INFO)
    with get_connection() as connection:
        log.info("Created connection: %s", connection)
        with connection.channel() as channel:
            log.info("Created channel: %s", channel)
            consume_messages(channel)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye!")
    
