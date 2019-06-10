#!/usr/bin/env python3

# Archive MQTT messages to plain files

import argparse
import json
import time
import datetime
import os
import os.path
import logging
import signal
import paho.mqtt.client as mqtt   # pip3 install paho-mqtt
import pprint
from archive_file_handling import compose_filename_for,set_filename_template,get_filename_template

PROG = 'mqtt-archive'
VERSION = '0.02'
SIGINT = False

def parse_args():
    parser = argparse.ArgumentParser(description='MQTT archiver')
    parser.add_argument('--conf', '-c', type=str,
                        help='name of configuration file (in JSON format)',
                        default='mqtt-archive.cfg.json')
    parser.add_argument('--version', '-v', action='version', version='%s %s'%(PROG, VERSION))
    return parser.parse_args()

def my_sigint_handler(signal, frame):
    """
    Called when program is interrupted with CTRL+C
    """
    global SIGINT
    logging.info("SIGINT detected")
    SIGINT = True
    return

def setup_logging():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')

def log_msg_to_archive(msg):
    """
    Append the given MQTT message to the appropriate archive file,
    creating new files as necessary.
    Currently, we use the most basic implementation: open the file
    every time we want to store a new message, and close it again afterwards.
    Optimisation options are legion:
     + buffer, only open file(s) occasionally
     + keep all files open at all times
     + ...
    """
    t = time.time()   # receive time - this is the message timestamp from now on
    fn = compose_filename_for(msg.topic, t)
    d = os.path.dirname(fn)
    os.makedirs(d, exist_ok=True)
    m = { 't':t, 'payload':msg.payload.decode("utf-8", "ignore") }
    # todo: this is likely not what we want. instead, try to intelligently...
    # ...decide if payload can be represented as...
    #    ...a number: 372893.232
    #    ...a string: 837 seconds (assume ASCII encoding?)
    #    ...fallback: a uuencoded byte array
    # Our goal should be to archive the payload as-is wherever possible,
    # potentially masking out \n by uuencoding.
    # Whoever retrieves the data can then put in some effort to convert
    # different types of payload to formats suitable for further processing.
    with open(fn,'a') as f:
        f.write(json.dumps(m) + '\n')
    
    
if __name__ == "__main__":
    setup_logging()
    logging.info("%s %s starting up at %s." % (
        PROG,
        VERSION,
        datetime.datetime.now(datetime.timezone.utc).isoformat()))

    #signal.signal(signal.SIGINT, my_sigint_handler)
    
    args = parse_args()
    #config = parse_config_file(args.conf)
    mycfg = {}
    if os.path.isfile(args.conf):
        logging.info('reading configuration file \'{}\'...'.format(args.conf))
        mycfg = json.load(open(args.conf))
        print(mycfg)
    else:
        logging.info('no configuration file \'{}\' found - using defaults...'.format(args.conf))

    template = mycfg.get('outtemplate',None)
    print(template)
    set_filename_template(template)
    logging.info('all data will be archived to \'{}\''.format(get_filename_template()))
    
    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(client, userdata, flags, rc):
        logging.info("Connected with result code "+str(rc))

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe("$SYS/#")
        client.subscribe("#")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        print(msg.topic+" --> "+str(msg.payload))
        log_msg_to_archive(msg)


    client = mqtt.Client()
    client.enable_logger()
    client.on_connect = on_connect
    client.on_message = on_message

    host = mycfg.get('mqtt-server', 'localhost')
    port = int(mycfg.get('mqtt-port', '1883'))
    logging.info('connecting to MQTT server \'{}\', port {}'.format(host, port))
    client.connect(host, port, 60)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_forever()
