from cosmic.fengines import ant_remotefeng_map
from cosmic.hashpipe_aux import redis_obj
import time
import logging

logger = logging.getLogger('cosmicfeng_logger')
logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
fh = logging.FileHandler("/home/cosmic/logs/F_Engines.log")
fh.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter("[%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s] %(message)s")

# add formatter to ch
ch.setFormatter(formatter)
fh.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
logger.addHandler(fh)

class FEngineLogger:
    """
    Very simply, go through all antennas every sample period and collect all 
    desired status'
    """

    def __init__(self, redis_obj, polling_rate=30):
        self.redis_obj = redis_obj
        self.polling_rate = polling_rate
        self.ant_feng_map = ant_remotefeng_map.get_antennaFengineDict(
            self.redis_obj
        )
        
        logger.info("Starting FEngine logger...\n")

    def run(self):
        while True:
            for ant, feng in self.ant_feng_map.items():
                dts_status = feng.dts.get_status()
                delay_status = feng.delay.get_status()
                lo_status = feng.lo.get_status()
                logger.info(f"""******STATUS RECORDING FOR ANTENNA {ant}******:\nDTS STATUS:\n {dts_status}\nDELAY STATUS:\n {delay_status}\nLO STATUS:\n {lo_status}\n""")
            time.sleep(self.polling_rate)

if __name__ == "__main__":
    feng_logger = FEngineLogger(redis_obj)
    feng_logger.run()