import urllib2, urlparse, re, gzip, json
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

workflow_id = "sanger_workflow"

DEFAULT_PATH = "http://pancancer.info/gnos_metadata/latest/"
GITHUB_URL = "https://github.com/"
REPO_URL = "ICGC-TCGA-PanCancer/pcawg-operations/tree/develop/variant_calling/%s/whitelists/sanger"%workflow_id
FILE_LOCATIONS = 'ICGC-TCGA-PanCancer/pcawg-operations/blob/develop/variant_calling/sanger_workflow/whitelists/sanger/.*?"'

class AnalysisCheck(object):
    def __init__(self):
        self.donors = self.load_donors()
        #json_data = self.find_json_gz()
        #self.load_json_data(json_data)
    
    def find_json_gz(self):
        directory = urllib2.urlopen(DEFAULT_PATH)
        filename = re.search("donor_p_\d+\.jsonl\.gz", directory.read()).group()
        directory.close()
        gz_file = StringIO.StringIO(urllib2.urlopen(urlparse.urljoin(DEFAULT_PATH, filename)).read())
        json_data = map(json.loads, gzip.GzipFile(fileobj=gz_file).readlines())
        return json_data

    def load_json_data(self, json_data):
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
        repo_dir = urllib2.urlopen(urlparse.urljoin(GITHUB_URL, REPO_URL))
        for file_match in re.findall(FILE_LOCATIONS, repo_dir.read()):
            self.load_donor_file(urlparse.urljoin(GITHUB_URL, file_match[:-1]))
        #donors = map(lambda line: line[:-1].replace("\t", "::"), donor_file.readlines())
        #return donors

    def load_donor_file(self, donor_name):
        print donor_name

def main():
    AnalysisCheck()

if __name__ == "__main__":
    main()
