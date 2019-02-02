#!/usr/bin/env python3

# MQTT Historian - retrieve historic MQTT payloads archived by mqtt-archiver.

# Next / TODO
# ===========
#
# - directly answer http requests
# - cache last X results?
# - list available topics in reply to http request
# - coalesce similar timestamps into one if data values don't overlap
#    --> check what can be gained. could be up to 50% for 2 time series, more for more!
# - no error if not a single data item availabe
# - general optimisation, e.g. maybe get rid of item() class altogether - sort will take
#   index argument
# - support for output formats other than dygraph
# - config file for common options, share with mqtt-archiver
# - share library for archive access with mqtt-archiver
# - support for different file granularities
# - support for aggregation data products
# - support for counters (Or should this be earlier in the chain? Probably.)
#
# - web page for multiple dygraph diagrams

import argparse
import json
import time
import datetime
import os
import os.path
import logging
import pprint
import heapq

PROG = 'mqtt-historian'
VERSION = '0.01'

def parse_args():
    parser = argparse.ArgumentParser(description='MQTT historian')
    parser.add_argument('--conf', '-c', type=str,
                        help='name of configuration file (in JSON format)',
                        default='mqtt-historian.cfg.json')
    parser.add_argument('--version', '-v', action='version', version='%s %s'%(PROG, VERSION))
    return parser.parse_args()

def setup_logging():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')

def compose_filename_for(topic, t):
    """
    Calculate the name of the archive file that should contain this message
    at the given time.
    Currently, we use a fixed one file per (UTC) day convention.
    """
    dt = datetime.datetime.utcfromtimestamp(t)
    f = 'archive' + os.sep + '%Y' + os.sep + '%m' + os.sep + '{}' + os.sep + '{}-%Y-%m-%dT000000Z.log'
    ds = dt.strftime(f)
    sanitised_topic = topic.replace('/','_')  # could improve on this basic version
    fn = ds.replace('{}', sanitised_topic)
    return fn
    
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
    fn = compose_filename_for(msg, t)
    d = os.path.dirname(fn)
    os.makedirs(d, exist_ok=True)
    m = { 't':t, 'payload':msg.payload.decode("utf-8", "ignore") }
    # todo: this is likely not what we want. instead, try to intelligently...
    # ...decide if payload can be represented as...
    #    ...a number: 372893.232
    #    ...a string: 837 seconds (assume ASCII encoding?)
    #    ...fallback: a uuencoded byte array
    with open(fn,'a') as f:
        f.write(json.dumps(m) + '\n')

def ai_parse(t):
    """
    Intelligently try to guess what format the timestamp t might be in,
    and return it as UNIX epoch value.
    """
    if isinstance(t, str):
        # assume ISO 8601 format
        logging.info('converting from ISO 8601')
        f = '%Y-%m-%dT%H%M%S%z'
        if t.endswith('Z'):
            t = t[:-1] + '+0000'
        if t[-5] != '+':
            t = t + '+0000'
        t = datetime.datetime.strptime(t, f)
        logging.info('result: %s' % t)
        
    if isinstance(t, datetime.datetime):
        t = t.timestamp()

    elif isinstance(t, float):
        pass  # assume this is a UNIX epoch value

    else:
        throw('don''t know how to parse time value %s' % t)
        
    return t
    
def retrieve_data_for(topics, starttime, endtime):
    """
    Read data for all the given topics.
    Return them as one big csv-style string.

    @param topics: list of one or more MQTT topic names. Wildcards not (yet) supported.
    @param starttime,endtime: Timestamps of desired first (including) and last (excluding) 
           data value. May be either
            * a time.time() "seconds of epoch" value
            * a String in ISO 8601 format, e.g. '2019-01-02T210000Z'
            * a python datetime object

    credits: multi-topic merging inspired by 
    http://neopythonic.blogspot.com/2008/10/sorting-million-32-bit-integers-in-2mb.html
    """
    # we might want to use heapq and generators

    # fix up timestamps
    starttime = ai_parse(starttime)
    endtime = ai_parse(endtime)

    # One generator for each topic
    # (todo: support wildcards in topics)
    iters = []
    for t in topics:
        iters.append(make_data_iterator_for_topic(t, starttime, endtime))

    # header
    retstr = 'time,'+','.join(topics)
    yield retstr

    # we build our own version of heapq.merge(): we also need to know which
    # topic the value came from, and also we want to combine similar timestamps
    # into one single output line
    elems = [next(i, item({'t':7e99})) for i in iters]   # prime elems

    while True:
        # always process the smallest item, and refill its slot
        val, idx = min((val, idx) for (idx, val) in enumerate(elems))
        retstr = str(elems[idx].myvals['t'])+','
        vals = []
        for (i,e) in enumerate(elems):
            if i==idx:
                vals.append(str(elems[idx].myvals['payload']))
            else:
                vals.append('')  # null value causes dygraph to ignore this val in this series
        retstr = retstr + ','.join(vals)
        yield retstr
        
        elems[idx] = next(iters[idx], item({'t':7e99}))
        if all([x.myvals['t']==7e99 for x in elems]):
            break
    
    #for x in heapq.merge(*iters):
    #    print(x)

class item(object):
    """
    An item is one data point, i.e. the archived MQTT payload and its 
    associated timestamp.
    items can compare themselves against each other - this enables
    their use in with heapq.
    """
    def __init__(self, vals):
        """
        Create a new object containing the values in vals.
        """
        self.myvals = vals

    def __str__(self):
        return 'item('+str(self.myvals)+')'
        
    def __lt__(self, other):
        return self.myvals['t'] < other.myvals['t']

    
def make_data_iterator_for_topic(topic, starttime, endtime):
    """
    Returns an iterator that iterates through all archived
    values between the given start and end times.
    @param starttime, endtime: time.time() values
    """
    assert(isinstance(starttime, float))
    assert(isinstance(endtime, float))
    if endtime <= starttime:
        return

    # approach:
    # 1. create first filename
    # 2. read data points, opening new files as needed
    # 3. as soon as first timestamp >= starttime is encountered, start returning data
    # 4. as soon as first timestamp >= endtime is encountered, stop returning data
    # (5. if no newer file is available, stop returning data)

    output_enable = False
    t = starttime
    while t < endtime:
        fn = compose_filename_for(topic, t)
        logging.info('attempting to open "%s"' % fn)
        if os.path.isfile(fn):
            with open(fn, 'r') as f:
                for line in f:
                    parsed = json.loads(line)
                    if parsed['t'] >= endtime:
                        logging.info('past endtime')
                        return
                    if parsed['t'] >= starttime:
                        #print(parsed)
                        yield item(parsed)

        # hard-coded knowledge that archive files are created one-per-day
        t = t + 60*60*24
    
if __name__ == "__main__":
    setup_logging()
    logging.info("%s %s starting up at %s." % (
        PROG,
        VERSION,
        datetime.datetime.now(datetime.timezone.utc).isoformat()))

    topics = ['sys/burner_hours_run', 'sys/temperatures/abgastemperatur', 'non_existant_topic']
        # todo: support wildcards like sys/temperatures/#
    d = retrieve_data_for(topics, '2019-01-24T20000', '2019-01-29T000000')
    for line in d:
        print(line)
    
