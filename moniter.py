import subprocess

from readline_noblock import nonblocking_readlines

strings = ("docker", "WwDocker-")

class Moniter(object):
    def __init__(self):
        cluster_dict = self.find_docker()
        print self.parse_cluster(cluster_dict)

    def parse_cluster(self, cluster_dict):
        comp_data = {"idle": [],
                     "broken": [],
                     "headless": [],
                     "working": [],
                     "hidden": []}
        for comp in cluster_dict:
            data = cluster_dict[comp]
            if data == []:
                comp_data["hidden"].append(comp)
            if data == ["docker"]:
                comp_data["headless"].append(comp)
            if data == ["WwDocker-"]:
                comp_data["idle"].append(comp)
            if data == ["docker", "WwDocker-"]:
                comp_data["working"].append(comp)
            for i in data:
                print i[:5]
        return comp_data

    def find_docker(self):
        cmd = "dsh -Mg cgp5 'ps -fu cgppipe'"
        sub = subprocess.Popen(cmd, shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        stdout = nonblocking_readlines(sub.stdout)
        stderr = nonblocking_readlines(sub.stderr)
        out_dict = {"error": []}
        for i in range(1,4):
            for j in range(1,17):
                out_dict["cgp-5-%d-%02d"%(i,j)] = []
        while 1:
            try:
                k, v = self.process_line(stdout.next()[:-1], stderr.next()[:-1])
                if k is not None:
                    out_dict[k].append(v)
            except StopIteration: break
        return out_dict

    def process_line(self, stdout, stderr):
        if stderr:
            return stdout.split(":")[0], "error: " + stderr
        if stdout:
            for string in strings:
                if stdout.find(string) != -1:
                    return stdout.split(":")[0], string
        return None, None

def main():
    Moniter()

if __name__ == "__main__":
    main()


