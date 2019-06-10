import datetime
import os
import glob

__outtemplate__ = 'archive' + os.sep + '%Y' + os.sep + '%m' + os.sep + '{}' + os.sep + '{}-%Y-%m-%dT000000Z.log'

def set_filename_template(s):
    """
    Define template for the directory structure that archived items will
    be written to.
    """
    global __outtemplate__
    if s is not None:
        __outtemplate__ = s

    
def get_filename_template():
    return __outtemplate__
    
def compose_filename_for(topic, t):
    """
    Calculate the name of the archive file that should contain this message
    at the given time.
    Currently, we use a fixed one file per (UTC) day convention.
    @param t: datetime.datetime
    """
    dt = datetime.datetime.utcfromtimestamp(t)
    f = __outtemplate__
    ds = dt.strftime(f)
    sanitised_topic = topic.replace('/','_')  # could improve on this basic version
    fn = ds.replace('{}', sanitised_topic)
    return fn

def scrape_list_of_topics():
    """
    Glob the archive to figure out what topics we have available
    """
    g = glob.glob('archive/*/*/*')
    g = [x.split('/')[3] for x in g]
    s = set(g)
    g = list(s)
    g.sort()
    return g
