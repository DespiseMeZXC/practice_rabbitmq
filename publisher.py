import logging
import time
from typing import TYPE_CHECKING

from config import get_connection, configure_logging, MQ_EXCHANGE, MQ_ROUTING_KEY


if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingConnection

log = logging.getLogger(__name__)

def produce_messages(channel: "BlockingConnection") -> None:
    queue = channel.queue_declare(queue=MQ_ROUTING_KEY)
    log.info("Queue declared: %r %s %s", MQ_ROUTING_KEY, queue, queue.method.message_count)
    message = f"Hello, World! {time.time()}"
    log.info("\n\nPublishing message: %s", message)
    channel.basic_publish(
        exchange=MQ_EXCHANGE,
        routing_key=MQ_ROUTING_KEY,
        body=message
    )
    log.warning("Published message: %s", message)


def main():
    configure_logging(level=logging.DEBUG)
    with get_connection() as connection:
        log.info("Created connection: %s", connection)
        with connection.channel() as channel:
            log.info("Created channel: %s", channel)
            produce_messages(channel)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Bye!")
    
