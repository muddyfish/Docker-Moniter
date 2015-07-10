#Python-2.7.9
import requests
from requests.adapters import HTTPAdapter

proxies = {"http" : "http://wwwcache.sanger.ac.uk:3128",
           "https": "http://wwwcache.sanger.ac.uk:3128"}

class ProxyAdapter(HTTPAdapter):
    def proxy_headers(self, proxy):
        return {"User-agent": "Mozilla/5.0"}

session = requests.Session()
session.mount("https://", ProxyAdapter())
r = session.get("https://raw.githubusercontent.com/ICGC-TCGA-PanCancer/pcawg-operations/develop/variant_calling/sanger_workflow/whitelists/sanger/sanger.150501-1028.from_cghub.txt", proxies=proxies)
print(r.content)
