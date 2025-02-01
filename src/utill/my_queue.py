import queue
import concurrent.futures

from loguru import logger


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
        if not all([self.producer_func is not None, self.producer_args is not None, self.consumer_func is not None]):
            raise Exception('Producer and Consumer functions must be defined!')

        def producer():
            results = []

            for item in self.producer_func(*self.producer_args):
                self.q.put(item)
                results.append(item)
                logger.debug(f'🌾 Produced {item}')

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
                logger.debug(f'🔥 Consumed {item}')

            return results

        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Schedule the producer and consumer
            self.future_producer = executor.submit(producer)
            self.future_consumer = executor.submit(consumer)

            producer_result = self.future_producer.result()
            logger.debug('✅ Producer done')
            consumer_result = self.future_consumer.result()
            logger.debug('✅ Consumer done')

        return producer_result, consumer_result
