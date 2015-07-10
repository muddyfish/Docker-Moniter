import requests

proxies = {"http" : "http://wwwcache.sanger.ac.uk:3128",
           "https": "http://wwwcache.sanger.ac.uk:3128"}

r = requests.get("https://raw.githubusercontent.com/ICGC-TCGA-PanCancer/pcawg-operations/develop/variant_calling/sanger_workflow/whitelists/sanger/sanger.150501-1028.from_cghub.txt", proxies=proxies)
print(r.content)
