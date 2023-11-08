from tests.maps import install_pydeck ##Make sure pydeck is installed before uninstalling it
import subprocess
uninstall_cmd = subprocess.Popen(['pip', 'uninstall', '-y', 'pydeck'])
uninstall_output = str(uninstall_cmd.communicate())