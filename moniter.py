import subprocess, json, tempfile, os

docker = "docker"
moniter = "WwDocker-"
strings = (docker, moniter)

dsh_path = "/nfs/users/nfs_c/cgppipe/.dsh/group/cgp5"

class Moniter(object):
    def __init__(self):
        #Get the information
        cluster_dict = self.find_docker()
        #Parse it
        node_data = self.parse_cluster(cluster_dict)
        #Continue parsing hidden nodes
        self.parse_hidden_nodes(node_data["hidden"])
        #Output
        print json.dumps(node_data, indent = 4, sort_keys = True)


    def parse_hidden_nodes(self, hidden_nodes):
        #Parse the nodes that aren't running either the docker or the monitering system
        #A temp file is needed for the dsh command with the names of every node it is to run the command on
        tmp_name = self.make_tmp_file("\n".join(hidden_nodes.keys()))
        cmd = "dsh -f %s -M 'grep -Fc \"UPLOADED FILE AFTER \" /cgp/datastore/oozie-*/generated-scripts/*vcfUpload_*.stdout'"%tmp_name
        #Run the command
        sub = subprocess.Popen(cmd, shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        stdout_buf, stderr_buf = sub.communicate()
        os.remove(tmp_name)
        #If the string 'UPLOADED FILE AFTER ' wasn't found, update the list
        for node in self.get_nodes(stderr_buf):
            hidden_nodes[node] = "Further analysis necessary"
        #For every other node, get the log data
        nodes = self.get_nodes(stdout_buf)
        tmp_name = self.make_tmp_file("\n".join(nodes))
        cmd = "dsh -f %s -M 'ls /cgp/datastore/*.ini'"%tmp_name
        sub = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        stdout_buf, _ = sub.communicate()
        for node_info in stdout_buf.rstrip().split("\n"):
            node, info = node_info.split(":")
            hidden_nodes[node] = info[1:]
        os.remove(tmp_name)
    
    def get_nodes(self, out):
        #Returns the names of nodes in a list of nodes
        return map(lambda node: node.split(":")[0], out.rstrip().split("\n"))

    def make_tmp_file(self, contents):
        #Create a temp file and write to it the contents given and return its name
        tmpfile = tempfile.NamedTemporaryFile(prefix = "docker_moniter_", delete=False)
        tmpfile.write(contents)
        tmp_name = tmpfile.name
        tmpfile.close()
        return tmp_name

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
        #Fill the output dict with every farm node
        nodes = {}
        node_names = open(dsh_path)
        for name in node_names:
            nodes[name.rstrip()] = []
        node_names.close()
        #Wait until cmd is finished
        stdout_buf, stderr_buf = sub.communicate()
        #Parse stdout
        for stdout in stdout_buf.split("\n"):
            node, prog = self.process_stdout(stdout)
            if node is not None:
                nodes[node].append(prog)
        #Parse stderr
        for stderr in stderr_buf.split("\n"):
            node, prog = self.process_stderr(stderr)
            if node is not None: 
                nodes[node].append(prog)
        return nodes

    def process_stdout(self, stdout):
        for string in strings:
            #If the given string is found in the line
            if stdout.find(string) != -1:
                #Return the node and the string found
                return stdout.split(":")[0], string
        return None, None

    def process_stderr(self, stderr):
        #Return the node and the error message
        if stderr == "": return None, None
        return stderr.split(":")[0], "error: " + stderr

def main():
    Moniter()

if __name__ == "__main__":
    main()


