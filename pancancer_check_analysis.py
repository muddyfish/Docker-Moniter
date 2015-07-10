#Python-2.7.9
import requests, re, gzip, json, os
import posixpath
from requests.adapters import HTTPAdapter
try:
    import cStringIO as StringIO
except ImportError:
    try:
        import StringIO
    except ImportError:
        import io as StringIO

workflow_id = "sanger_workflow"

DEFAULT_PATH = "http://pancancer.info/gnos_metadata/latest/"
GITHUB_URL = "http://github.com/"
REPO_URL = "ICGC-TCGA-PanCancer/pcawg-operations/tree/develop/variant_calling/%s/whitelists/sanger"%workflow_id
SEARCH_FILES = 'ICGC-TCGA-PanCancer/pcawg-operations/blob/develop/variant_calling/sanger_workflow/whitelists/sanger/.*?"'

RAW_DIR = "https://github.com/ICGC-TCGA-PanCancer/pcawg-operations/raw/develop/variant_calling/sanger_workflow/whitelists/sanger/"

proxies = {"http" : "http://wwwcache.sanger.ac.uk:3128",
           "https": "http://wwwcache.sanger.ac.uk:3128"}

class ProxyAdapter(HTTPAdapter):
    def proxy_headers(self, proxy):
        return {"User-agent": "Mozilla/5.0"}

session = requests.Session()
session.mount("https://", ProxyAdapter())

class AnalysisCheck(object):
    def __init__(self):
        self.donors = self.load_donors()
        json_data = self.find_json_gz()
        self.load_json_data(json_data)
    
    def find_json_gz(self):
        directory = self.open_url(DEFAULT_PATH)
        filename = re.search("donor_p_\d+\.jsonl\.gz", directory).group()
        gz_file = StringIO.StringIO(self.open_url(posixpath.join(DEFAULT_PATH, filename)))
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
        donors = []
        for file_match in re.findall(SEARCH_FILES, repo_dir):
            filename = file_match.split(posixpath.sep)[-1][:-1]
            donors.extend(self.load_donor_file(posixpath.join(RAW_DIR, filename)))
        #donors = map(lambda line: line[:-1].replace("\t", "::"), donor_file.readlines())
        return donors

    def load_donor_file(self, donor_name):
        donors = []
        donor_file = self.open_url(donor_name)
        for line in donor_file.split("\n"):
            if line != "":
                donors.append(line.replace("\t", "::"))
        return donors

    def open_url(self, url):
        r = session.get(url, proxies=proxies)
        return r.content


def main():
    AnalysisCheck()

if __name__ == "__main__":
    main()
