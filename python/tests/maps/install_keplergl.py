import subprocess
install_cmd = subprocess.Popen(['pip', 'install', 'keplergl==0.3.2'])
install_output = str(install_cmd.communicate())