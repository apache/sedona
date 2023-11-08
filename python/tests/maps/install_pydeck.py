import subprocess
install_cmd = subprocess.Popen(['pip', 'install', 'pydeck==0.8.0'])
install_output = str(install_cmd.communicate())