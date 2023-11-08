from tests.maps import install_keplergl ##Make sure keplergl is installed before uninstalling it
import subprocess
uninstall_cmd = subprocess.Popen(['pip', 'uninstall', '-y', 'keplergl'])
uninstall_output = str(uninstall_cmd.communicate())