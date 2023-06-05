from cosmic.redis_actions import redis_obj, redis_publish_service_pulse, redis_hget_keyvalues, redis_publish_dict_to_hash
from cosmic.fengines import ant_remotefeng_map
import time
import numpy as np
import math
import logging
from logging.handlers import RotatingFileHandler
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import os
import argparse


LOGFILENAME = "/home/cosmic/logs/F_Engines.log"
SERVICE_NAME = os.path.splitext(os.path.basename(__file__))[0]

logger = logging.getLogger('fengine_logger')
logger.setLevel(logging.INFO)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
fh = RotatingFileHandler(LOGFILENAME, mode = 'a', maxBytes = 512, backupCount = 0, encoding = None, delay = False)
fh.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter("[%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s] %(message)s")

# add formatter to ch
ch.setFormatter(formatter)
fh.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
logger.addHandler(fh)

FPGA_CLOCK_RATE_HZ = 256e6

def fetch_feng_status_dict(redis_obj, ant_feng_map):
    """
    Function collects from Redis hashes and over remoteobjects from the F-Engines a 
    status dictionary that it will return.

    Args:
        redis_obj object: an appropriate redis object that contains relevant hashes
        ant_feng_map dict: mapping of antenna names to cosmic_feng objects

    Returns:
        feng_dict dict: {ant:feng_dict} where feng_dict is a dictionary containing many fields detailing
                        the overall state of the F-Engine
        bad_ant_list: a list of F-Engines that are unreachable
    """
    ant_feng_status_dict = {}
    ant_prop = redis_hget_keyvalues(redis_obj, "META_antennaProperties")
    antdispmap = {}
    bad_ant_list=[]
    for ant, feng in ant_feng_map.items():
        try:
        #F-Engine times:
            delay_time_ms = (feng.delay.timer.get_fpga_time()*1000)/FPGA_CLOCK_RATE_HZ
            phaserotate_time_ms = (feng.phaserotate.timer.get_fpga_time()*1000)/FPGA_CLOCK_RATE_HZ
            lo_time_ms = (feng.lo.timer.get_fpga_time()*1000)/FPGA_CLOCK_RATE_HZ
            time_now = time.time()
            #check delay_time_ms, phaserotate_time_ms and lo_time_ms are within 0.1s of each other
            if abs(delay_time_ms - phaserotate_time_ms) > 100 or abs(delay_time_ms - lo_time_ms) > 100 or abs(phaserotate_time_ms - lo_time_ms) > 100:
                all_feng_time_match = False
            else:
                all_feng_time_match = True
            
            all_time_good = False
            #if all_feng_time_match, check delay_time_ms is within 0.1s of current time
            if all_feng_time_match:     
                if abs(delay_time_ms/1000 - time_now) <= 1:
                    all_time_good = True

            # try:
            #DTS status:
            parity_status = feng.dts.get_status_dict()
            parity_errs = parity_status['parity_errors']
            #Input bit stats:
            means, powers, rmss = feng.input.get_bit_stats()
            #Compile status dict
            ant_feng_status_dict[ant] = {
                "ant_path" : f"{ant_prop[ant]['server']}:{ant_prop[ant]['pcie_id']}_{ant_prop[ant]['pipeline_id']}",
                "ant_pad" : ant_prop[ant]["pad"],
                "dts_state_ok" : int(parity_status['ok']),
                "dts_state_gty_lock_ok" : int(parity_status['state_ok']['gty_lock_ok']),
                "dts_state_lock_ok" : int(parity_status['state_ok']['lock_ok']),
                "dts_state_sync_ok" : int(parity_status['state_ok']['sync_ok']),
                "feng_delay_time" : int(delay_time_ms),
                "feng_phaserotate_time" : int(phaserotate_time_ms),
                "feng_lo_time" : int(lo_time_ms),
                "feng_time_correct" : int(all_time_good)
            }
            try:
                ant_feng_status_dict[ant]["ant_displacement"] = math.sqrt(
                        ant_prop[ant]["X"] ** 2 + ant_prop[ant]["Y"] ** 2 + ant_prop[ant]["Z"] ** 2
                    )
            except:
                antdispmap[ant]["ant_displacement"] = -1.0
            ant_feng_status_dict[ant][f'dts_parity_errs'] = parity_errs
            ant_feng_status_dict[ant][f'inpt_means'] = means.tolist()
            ant_feng_status_dict[ant][f'inpt_powers'] = powers.tolist()
            ant_feng_status_dict[ant][f'inpt_rmss'] = rmss.tolist()
            eq_coeffs_identical = True
            eq_coeffs_mean = []
            for stream in range(len(means)):
                stream_eq_coeffs,bp = feng.eq.get_coeffs(stream)
                stream_eq_coeffs=np.array(stream_eq_coeffs)/bp
                eq_coeffs_identical &= np.all(stream_eq_coeffs)
                eq_coeffs_mean += [np.mean(stream_eq_coeffs)]
            ant_feng_status_dict[ant][f'eq_identical_coeffs'] = int(eq_coeffs_identical)
            ant_feng_status_dict[ant][f'eq_mean_coeffs'] = eq_coeffs_mean
        except:
            ant_feng_status_dict[ant] = f"Unable to reach {ant}. F-Engine may be unreachable."
            bad_ant_list += [ant]
            continue
    return ant_feng_status_dict, bad_ant_list

class FEngineLogger:
    """
    Very simply, go through all antennas every sample period and collect all 
    desired status'
    """

    def __init__(self, redis_obj, polling_rate,  influxdb_token):
        self.redis_obj = redis_obj
        self.polling_rate = polling_rate
        self.ant_feng_map = ant_remotefeng_map.get_antennaFengineDict(
            self.redis_obj
        )
        self.bucket = "fengine"
        token = influxdb_token
        self.client = InfluxDBClient(url='http://localhost:8086', token=token)
        self.org="seti"
        logger.info("Starting FEngine logger...\n")

    def send_fengdata_to_influx_db(self,feng_status_dict):
        """
        Given a feng status dictionary, collect from Redis hashes the antenna to F-Engine mapping and
        the antenna location displacement
        for loading to the InfluxDB database under bucket 'fengine'
        """
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        time_now = time.time_ns()
        for ant, state in feng_status_dict.items():
            if isinstance(state,dict):
                pt = Point("feng_stat").tag("ant",ant).field("ant_pad",state["ant_pad"]).time(time_now)
                write_api.write(self.bucket,self.org, pt)
                pt = Point("feng_stat").tag("ant",ant).field("dts_state_ok",state["dts_state_ok"]).time(time_now)
                write_api.write(self.bucket,self.org, pt)
                pt = Point("feng_stat").tag("ant",ant).field("dts_state_gty_lock_ok",state["dts_state_gty_lock_ok"]).time(time_now)
                write_api.write(self.bucket,self.org, pt)
                pt = Point("feng_stat").tag("ant",ant).field("dts_state_lock_ok",state["dts_state_lock_ok"]).time(time_now)
                write_api.write(self.bucket,self.org, pt)
                pt = Point("feng_stat").tag("ant",ant).field("dts_state_sync_ok",state["dts_state_sync_ok"]).time(time_now)
                write_api.write(self.bucket,self.org, pt)
                pt = Point("feng_stat").tag("ant",ant).field("eq_identical_coeffs",state["eq_identical_coeffs"]).time(time_now)
                write_api.write(self.bucket,self.org, pt)
                pt = Point("ant_stat").tag("ant",ant).field("displacement",state["ant_displacement"]).time(time_now)
                write_api.write(self.bucket,self.org, pt)
                pt = Point("ant_stat").tag("ant",ant).field("path",state["ant_path"]).time(time_now)
                write_api.write(self.bucket,self.org, pt)
                pt = Point("feng_stat").tag("ant",ant).field("feng_delay_time",state["feng_delay_time"]).time(time_now)
                write_api.write(self.bucket,self.org, pt)
                pt = Point("feng_stat").tag("ant",ant).field("feng_phaserotate_time",state["feng_phaserotate_time"]).time(time_now)
                write_api.write(self.bucket,self.org, pt)
                pt = Point("feng_stat").tag("ant",ant).field("feng_lo_time",state["feng_lo_time"]).time(time_now)
                write_api.write(self.bucket,self.org, pt)
                pt = Point("feng_stat").tag("ant",ant).field("feng_time_correct",state["feng_time_correct"]).time(time_now)
                write_api.write(self.bucket,self.org, pt)

                for err_ind in range(len(state['dts_parity_errs'])):
                    parity_err = state['dts_parity_errs'][err_ind]
                    pt = Point("feng_stat").tag("ant",ant).tag("dts_lane",err_ind).field("acc",parity_err["acc"]).time(time_now)
                    write_api.write(self.bucket,self.org, pt)
                    pt = Point("feng_stat").tag("ant",ant).tag("dts_lane",err_ind).field("count",parity_err["count"]).time(time_now)
                    write_api.write(self.bucket,self.org, pt)
                
                for stream in range(len(state["inpt_means"])):
                    pt = Point("feng_stat").tag("ant",ant).tag("stream",stream).field("inpt_mean",state['inpt_means'][stream]).time(time_now)
                    write_api.write(self.bucket,self.org, pt)
                    pt = Point("feng_stat").tag("ant",ant).tag("stream",stream).field("inpt_power",state['inpt_powers'][stream]).time(time_now)
                    write_api.write(self.bucket,self.org, pt)
                    pt = Point("feng_stat").tag("ant",ant).tag("stream",stream).field("inpt_rms",state['inpt_rmss'][stream]).time(time_now)
                    write_api.write(self.bucket,self.org, pt)
                    pt = Point("feng_stat").tag("ant",ant).tag("stream",stream).field("eq_mean_coeff",state['eq_mean_coeffs'][stream]).time(time_now)
                    write_api.write(self.bucket,self.org, pt)

    def run(self):
        """
        Every polling period, fetch from fetch_feng_status_dict() an antenna:fengdict mapping 
        to give an indication of the working state of the F-Engines. Publish this dictionary to
        Redis and log relevant fields to InfluxDB for dashboarding.
        """
        i = 0
        bad_ant_list=[]
        while True:
            if i > 20 and len(bad_ant_list) != 0:
                self.ant_feng_map = ant_remotefeng_map.get_antennaFengineDict(redis_obj)
                i = 0
            else:
                redis_publish_service_pulse(self.redis_obj, SERVICE_NAME)
                t = time.time()
                ant_feng_status_dict, bad_ant_list = fetch_feng_status_dict(self.redis_obj, self.ant_feng_map)
                print(ant_feng_status_dict)
                redis_publish_dict_to_hash(self.redis_obj, "FENG_state", ant_feng_status_dict)
                self.send_fengdata_to_influx_db(ant_feng_status_dict)
                duration = time.time() - t
                time.sleep(self.polling_rate - duration if duration < self.polling_rate else 0.0)
                i+=1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
    description=("Set up the FEngine logger.")
    )
    parser.add_argument(
    "-p","--polling_rate", type=int, help="FEngine polling rate in seconds.", default=30
    )
    parser.add_argument(
    "-c", "--clean", action="store_true",help="Delete the existing log file and start afresh.",
    )
    args = parser.parse_args()
    if os.path.exists(LOGFILENAME) and args.clean:
        print("Removing previous log file...")
        os.remove(LOGFILENAME)
    else:
        print("Nothing to clean, continuing...")
    
    if "INFLUXDB_TOKEN" in os.environ:
        influxdb_token = os.environ["INFLUXDB_TOKEN"]

    feng_logger = FEngineLogger(redis_obj, args.polling_rate, influxdb_token)
    feng_logger.run()
