from loguru import logger
from typing import Callable
import concurrent.futures
import queue


class StreamingQ:
    def __init__(
        self,
        producer_func: Callable,
        producer_args: tuple,
        consumer_func: Callable,
        max_queue_size: int = 0,
    ):
        self.producer_func = producer_func
        self.producer_args = producer_args
        self.consumer_func = consumer_func

        # Use maxsize for backpressure control (0 = unlimited)
        self.q = queue.Queue(maxsize=max_queue_size)

    def execute(self):
        """
        Execute producer and consumer with true streaming using generators.
        Yields consumer results as they become available.
        """

        def producer():
            try:
                for item in self.producer_func(*self.producer_args):
                    self.q.put(item)
                    logger.debug(f"🌾 Produced {item}")
            except Exception as e:
                logger.error(f"Producer error: {e}")
                self.q.put(("ERROR", e))
            finally:
                # Signal end of production
                self.q.put(None)
                logger.debug("🌾 Producer finished")

        def consumer():
            while True:
                item = self.q.get()

                if item is None:
                    # End of stream signal
                    self.q.task_done()
                    break

                if isinstance(item, tuple) and item[0] == "ERROR":
                    # Propagate producer error
                    self.q.task_done()
                    raise item[1]

                try:
                    # Unpack item if it's a tuple, otherwise pass as single arg
                    if isinstance(item, tuple):
                        result = self.consumer_func(*item)
                    else:
                        result = self.consumer_func(item)

                    self.q.task_done()
                    logger.debug(f"🔥 Consumed {item} -> {result}")
                    yield result

                except Exception as e:
                    self.q.task_done()
                    logger.error(f"Consumer error processing {item}: {e}")
                    raise

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # Start producer in background
            future_producer = executor.submit(producer)

            try:
                # Yield results as they become available
                for result in consumer():
                    yield result

                # Wait for producer to complete
                future_producer.result()

            except Exception as e:
                # Cancel producer if consumer fails
                future_producer.cancel()
                raise


class ThreadingQ:
    def __init__(self) -> None:
        self.q = queue.Queue()

        self.producer_func = None
        self.producer_args = None
        self.consumer_func = None

    def add_producer(self, func, *args):
        self.producer_func = func
        self.producer_args = args or []
        return self

    def add_consumer(self, func):
        self.consumer_func = func
        # The consume args is based on producer output
        return self

    def execute(self):
        if not all(
            [
                self.producer_func is not None,
                self.producer_args is not None,
                self.consumer_func is not None,
            ]
        ):
            raise Exception("Producer and Consumer functions must be defined!")

        def producer():
            results = []

            for item in self.producer_func(*self.producer_args):
                self.q.put(item)
                results.append(item)
                logger.debug(f"🌾 Produced {item}")

            self.q.put(None)
            return results

        def consumer():
            results = []

            while True:
                item = self.q.get()
                if item is None:
                    break

                result = self.consumer_func(*item)
                results.append(result)

                self.q.task_done()
                logger.debug(f"🔥 Consumed {item}")

            return results

        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Schedule the producer and consumer
            self.future_producer = executor.submit(producer)
            self.future_consumer = executor.submit(consumer)

            producer_result = self.future_producer.result()
            logger.debug("✅ Producer done")
            consumer_result = self.future_consumer.result()
            logger.debug("✅ Consumer done")

        return producer_result, consumer_result
