import urllib2, urlparse, re, gzip, json
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

DEFAULT_PATH = "http://pancancer.info/gnos_metadata/latest/"

class AnalysisCheck(object):
    def __init__(self):
        self.donors = self.load_donors()
        self.find_json_gz()
    
    def find_json_gz(self):
        directory = urllib2.urlopen(DEFAULT_PATH)
        filename = re.search("donor_p_\d+\.jsonl\.gz", directory.read()).group()
        directory.close()
        gz_file = StringIO.StringIO(urllib2.urlopen(urlparse.urljoin(DEFAULT_PATH, filename)).read())
        json_data = map(json.loads, gzip.GzipFile(fileobj=gz_file).readlines())
        done = []
        pending = []
        for donor in json_data:
            if donor["donor_unique_id"] in self.donors:
                if "sanger" in donor["flags"]["variant_calling_performed"]:
                    done.append(donor["donor_unique_id"])
                else:
                    pending.append(donor["donor_unique_id"])
        print "Done:\n", "\n".join(sorted(done))
        print "\n\n"
        print "Pending:\n", "\n".join(sorted(map(lambda x: x.replace("::", "\t"), pending)))

    def load_donors(self):
        donor_file = open("donors.lst")
        donors = map(lambda line: line[:-1].replace("\t", "::"), donor_file.readlines())
        donor_file.close()
        return donors

def main():
    AnalysisCheck()

if __name__ == "__main__":
    main()
