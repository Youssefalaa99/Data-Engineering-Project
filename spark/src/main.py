import logging
logger = logging.getLogger(__name__)
from jobs import kafka_to_mongo

def main():
    logging.basicConfig(
        # filename='spark/logs/app.log',
        format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
        level=logging.INFO
    )
    logging.info("=============== Begin Streaming Job ===============")
    kafka_to_mongo.start_job()
    logger.info("=============== Job Finished ===============")

if __name__ == '__main__':
    main()