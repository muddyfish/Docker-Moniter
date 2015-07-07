import subprocess, time, json

from readline_noblock import nonblocking_readlines

docker = "docker"
moniter = "WwDocker-"
strings = (docker, moniter)

class Moniter(object):
    def __init__(self):
        #Get the information
        cluster_dict = self.find_docker()
        #Parse it
        node_data = self.parse_cluster(cluster_dict)
        #Output
        print json.dumps(node_data, indent = 4)

    def parse_cluster(self, cluster):
        node_data = {"idle hosts": {},
                     "problem hosts": {},
                     "headless hosts": {},
                     "working": {},
                     "hidden": {}}
        for node_id, node in cluster.iteritems():
            if node == []: #None of the servives running
                node_data["hidden"][node_id] = ""
            elif node == [docker]: #Only the docker script is running
                node_data["headless hosts"][node_id] = ""
            elif node == [moniter]: #Only the moniter is running
                node_data["idle hosts"][node_id] = ""
            elif sorted(node) == [moniter, docker]: #Both are running
                node_data["working"][node_id] = ""
            else: #The only other possability is there's an error
                assert(node[0][:5] == "error")
                node_data["problem hosts"][node_id] = ":".join(node[0].split(":")[2:])[1:]
        return node_data

    def find_docker(self):
        cmd = "dsh -Mg cgp5 'ps -fu cgppipe'"
        sub = subprocess.Popen(cmd, shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        #Setup stdout and stderr
        #When read, if there is no data, will return None intead of blocking
        stdout = nonblocking_readlines(sub.stdout)
        stderr = nonblocking_readlines(sub.stderr)
        #Fill the output dict with every farm node
        nodes = {}
        for i in range(1,4):
            for j in range(1,17):
                nodes["cgp-5-%d-%02d"%(i,j)] = []
        #While the command isnt finished
        while 1:
            try:
                #stdout.next() doesnt strip the newline character
                k, v = self.process_line(stdout.next()[:-1], stderr.next()[:-1])
                #If there is data
                if k is not None:
                    nodes[k].append(v)
                else:
                    #Aim to reduce CPU time
                    time.sleep(0.5)
                #print k,v
            except StopIteration:
                pass#print "STOPITER"
        return nodes

    def process_line(self, stdout, stderr):
        #Stderr has higher priority (and if anything returns on it, stdout should be empty)
        if stderr:
            #Return the node and the error message
            return stderr.split(":")[0], "error: " + stderr
        if stdout:
            print stdout
            for string in strings:
                #If the given string is found in the line
                if stdout.find(string) != -1:
                    #Return the node and the string found
                    return stdout.split(":")[0], string
        return None, None

def main():
    Moniter()

if __name__ == "__main__":
    main()


