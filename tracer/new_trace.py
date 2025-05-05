from argparse import ArgumentParser
from wrapper.nvconfigparser import ConfigParser
import os
from traces.mlsysops_trace import *

def parseConfig (filename: str) -> dict:
    """ Parses the configuration file using Python's configparser.

    Args:
        filename: name of the config file
    Returns:
        Dictionary of dictionaries with all the parsed parameters grouped according to the configuration sections
    """
   
    config = ConfigParser()
    config.read(filename)
    # Parse variables
    params = {}
    params["trace"] = {}
    params["trace"]["windowJobs"] = config['trace'].getint('windowJobs')
    params["trace"]["windows"] = config['trace'].getint('windows')
    params["trace"]["permute"] = config['trace'].getboolean('permute')
    params["trace"]["filename"] = config['trace']['filename']
    params["trace"]["dirname"] = config['trace']['dirname']
    params["job"] = {}
    params["job"]["jobSizeRange"] = config.getlistint('job','jobSizeRange')
    params["job"]["durationRange"] = config.getlistint('job','durationRange')
    
    return params



if __name__ == "__main__":
    aparser = ArgumentParser(prog="python new_trace.py", description="Trace generator for NVON systems simulator.")
    aparser.add_argument("-c", "--config", required=True, dest="config", help="path to configuration file(relative)",)
    args = aparser.parse_args()

    if  os.path.exists(args.config):
        params = parseConfig(args.config)
    else:
        print("The config file %s does not exist" %(args.config))
        exit()

    # Instansiate a Job object 
    mljob = mlJob(params["job"]["jobSizeRange"], params["job"]["durationRange"])
    # Setup trace generation
    mltracer = mlTracer(params["trace"]["windowJobs"], params["trace"]["windows"], params["trace"]["filename"], params["trace"]["dirname"],  mljob.job)
    # Curry on some initializations
    mltracer.resetEnv()

    # Generate workload, permutated or not
    if (params["trace"]["permute"]):
        mltracer.genPermWinWorkload()
    else:
        mltracer.genWinWorkload()

    #Write workload definition file
    mltracer.workloadDefinition()
   