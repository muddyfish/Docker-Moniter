import urllib2, re, gzip, json
import posixpath
try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

workflow_id = "sanger_workflow"

DEFAULT_PATH = "http://pancancer.info/gnos_metadata/latest/"
GITHUB_URL = "https://github.com/"
REPO_URL = "ICGC-TCGA-PanCancer/pcawg-operations/tree/develop/variant_calling/%s/whitelists/sanger"%workflow_id
SEARCH_FILES = 'ICGC-TCGA-PanCancer/pcawg-operations/blob/develop/variant_calling/sanger_workflow/whitelists/sanger/.*?"'
RAW_DIR = "https://raw.github.com/ICGC-TCGA-PanCancer/pcawg-operations/master/variant_calling/sanger_workflow/whitelists/sanger/"

opener = urllib2.build_opener()

class AnalysisCheck(object):
    def __init__(self):
        self.donors = self.load_donors()
        #json_data = self.find_json_gz()
        #self.load_json_data(json_data)
    
    def find_json_gz(self):
        directory = self.open_url(DEFAULT_PATH)
        filename = re.search("donor_p_\d+\.jsonl\.gz", directory.read()).group()
        directory.close()
        gz_file = StringIO.StringIO(opener.open(posixpath.join(DEFAULT_PATH, filename)).read())
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
        repo_dir = self.open_url(posixpath.join(GITHUB_URL, REPO_URL))
        for file_match in re.findall(SEARCH_FILES, repo_dir.read()):
            filename = file_match.split(posixpath.sep)[-1][:-1]
            self.load_donor_file(posixpath.join(RAW_DIR, filename))
            print "Returned"
        #donors = map(lambda line: line[:-1].replace("\t", "::"), donor_file.readlines())
        #return donors

    def load_donor_file(self, donor_name):
        print donor_name
        donor_file = self.open_url(donor_name, api = True)
        print "READING"
        print donor_file.read()
        donor_file.close()

    def open_url(self, url, api = False):
        print url
        headers = {}
        if api: headers = {'Accept': "text/html; charset=utf-8"}
        request = urllib2.Request(url, None, headers)
        return urllib2.urlopen(request)

def main():
    AnalysisCheck()

if __name__ == "__main__":
    main()
