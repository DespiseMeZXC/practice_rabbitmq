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
    time.sleep(0.05)
    end = time.time()
    log.info("Finished processing message %s ", body)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    log.info("[x] End processing message %r in %s seconds", body, end - start)

def consume_messages(channel: "BlockingConnection") -> None:
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
    
