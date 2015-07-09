import urllib2, urlparse, re, gzip, json
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

DEFAULT_PATH = "http://pancancer.info/gnos_metadata/latest/"

class AnalysisCheck(object):
    def __init__(self):
        self.load_donors()
        print self.find_json_gz()
    
    def find_json_gz(self):
        directory = urllib2.urlopen(DEFAULT_PATH)
        filename = re.search("donor_p_\d+\.jsonl\.gz", directory.read()).group()
        directory.close()
        gz_file = StringIO.StringIO(urllib2.urlopen(urlparse.urljoin(DEFAULT_PATH, filename)).read())
        json_data = map(json.loads, gzip.GzipFile(fileobj=gz_file).readlines())
        for i in json_data:
            print i["donor_unique_id"]
            print
        #print json_data

    def load_donors(self): pass

def main():
    AnalysisCheck()

if __name__ == "__main__":
    main()
