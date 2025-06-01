import logging
logger = logging.getLogger(__name__)
# from jobs.kafka_to_mongo import 

def main():
    logging.basicConfig(
        filename='spark/logs/app.log',
        format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
        level=logging.INFO
    )
    logger.info("=============== Begin ETLs ===============")
    
    logger.info("=============== ETLs Finished ===============")

if __name__ == '__main__':
    main()