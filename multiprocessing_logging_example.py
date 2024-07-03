import logging
import logging.handlers
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import sys
import traceback
from random import choice, random
import time

# Arrays used for random selections in this demo
LEVELS = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]
LOGGERS = ['process_log_a', 'process_log_b']
MESSAGES = [
    'Process message #1',
    'Process message #2',
    'Process message #3',
]

NUMBER_OF_WORKERS = 5
NUMBER_OF_POOL_WORKERS = 5
NUMBER_OF_MESSAGES = 5

LOG_SIZE = 1024  # 1 MB
BACKUP_COUNT = 10
LOG_FORMAT = '%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s'
LOG_NAME = 'mptest.log'


def configure_logging_format():
    """
    Configure the listener process logging.
    """
    root = logging.getLogger()
    h = logging.handlers.RotatingFileHandler(LOG_NAME, 'a', LOG_SIZE, BACKUP_COUNT)
    f = logging.Formatter(LOG_FORMAT)
    h.setLevel(logging.DEBUG)
    h.setFormatter(f)
    root.addHandler(h)


def main_process_listener(queue: multiprocessing.Queue):
    """
    This is the listener process top-level loop: wait for logging events
    (LogRecords)on the queue and handle them, quit when you get a None for a
    LogRecord.

    Parameters
    ----------
    queue: Queue
        Queue to get the log records from.
    """
    configure_logging_format()
    while True:
        try:
            record = queue.get()
            if record is None:  # sentinel to tell the listener to quit.
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)  # No level or filter logic applied - just do it!
        except Exception:
            traceback.print_exc(file=sys.stderr)


def broadcast_logs_from_pool_to_main_listener(pool_process_queue, main_process_queue):
    """
    This is the listener process top-level loop: wait for logging events from the pool process
    and broadcast them to the main listener process.

    pool_process_queue: Queue
        Pool process queue to get the log records from.
    main_process_queue: Queue
        Main process queue to put the log records to.
    """
    while True:
        try:
            record = pool_process_queue.get()
            if record is None:  # sentinel to tell the listener to quit.
                break
            # TODO: apply level of filtering
            main_process_queue.put(record)
        except Exception:
            traceback.print_exc(file=sys.stderr)


def configure_logging_for_multiprocessing(queue):
    """
    The worker configuration is done at the start of the worker process run.
    Note that on Windows you can't rely on fork semantics, so each process
    will run the logging configuration code when it starts.
    """
    h = logging.handlers.QueueHandler(queue)  # Handler needed to send records to queue
    root = logging.getLogger()
    root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(logging.DEBUG)


def pool_process(queue):
    configure_logging_for_multiprocessing(queue)

    name = multiprocessing.current_process().name
    print('Pool process started: %s' % name)
    for i in range(NUMBER_OF_MESSAGES):
        time.sleep(random())
        logging.getLogger('SUB'+choice(LOGGERS)).log(choice(LEVELS), 'SUB'+choice(MESSAGES))

    # Randomly wait for a while before closing the application
    if round(random()) == 1:
        time.sleep(4)
    print('Pool process finished: %s' % name)


def worker_process(queue):
    """
    Worker process that logs messages to the queue.

    Parameters
    ----------
    queue: Queue
        Queue to log the messages to.
    """
    # Set up the queue to store the log information
    configure_logging_for_multiprocessing(queue)

    process_logging_queue = multiprocessing.Manager().Queue(-1)
    configure_logging_for_multiprocessing(process_logging_queue)
    listener = multiprocessing.Process(target=broadcast_logs_from_pool_to_main_listener,
                                       args=(process_logging_queue, queue))
    listener.start()

    # Create ProcessPoolExecutor
    executor = ProcessPoolExecutor(max_workers=NUMBER_OF_POOL_WORKERS)
    for i in range(5):
        executor.submit(pool_process, process_logging_queue)

    # Send message
    name = multiprocessing.current_process().name
    print('Worker started: %s' % name)
    for i in range(NUMBER_OF_MESSAGES):
        time.sleep(random())
        logging.getLogger(choice(LOGGERS)).log(choice(LEVELS), choice(MESSAGES))

    # Randomly wait for a while before closing the application
    if round(random()) == 1:
        time.sleep(10)
    print('Worker finished: %s' % name)

    # Shutdown the executor and the listener
    executor.shutdown()
    process_logging_queue.put_nowait(None)


if __name__ == '__main__':
    main_logging_queue = multiprocessing.Manager().Queue(-1)
    configure_logging_for_multiprocessing(main_logging_queue)

    # Start the listener process
    logging_process = multiprocessing.Process(target=main_process_listener, args=(main_logging_queue,))
    logging_process.start()

    logging.getLogger('main_1').log(choice(LEVELS), 'main process 1')

    # Start the worker processes
    workers = []
    for i in range(NUMBER_OF_WORKERS):
        worker = multiprocessing.Process(target=worker_process, args=(main_logging_queue,))
        workers.append(worker)
        worker.start()

    # Log a message from the main process
    logging.getLogger('main_2').log(choice(LEVELS), 'main process 1')

    # Wait for all of the workers to finish
    for w in workers:
        w.join()

    main_logging_queue.put_nowait(None)
    logging_process.join()
