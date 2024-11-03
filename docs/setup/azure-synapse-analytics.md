This tutorial will guide you through the process of installing Sedona on Azure Synapse Analytics when Data Exfiltration Protection (DEP) is enabled or when you have no internet connection from the Spark pools due to other networking constraints.

## Strong recommendations
1. Start with a clean Spark pool with no other packages installed to avoid package conflicts.
2. Apache Spark pool -> Apache Spark configuration: Use default configuration

## Sedona 1.6.1 on Spark 3.4 Python 3.10

### Step1: Download packages (9)
Caution: Precise versions are critical, latest is not always greatest here.

From Maven

- [sedona-spark-shaded-3.4_2.12-1.6.1.jar](https://mvnrepository.com/artifact/org.apache.sedona/sedona-spark-shaded-3.4_2.12/1.6.1)

- [geotools-wrapper-1.6.1-28.2.jar](https://mvnrepository.com/artifact/org.datasyslab/geotools-wrapper/1.6.1-28.2)

From PyPi

- [rasterio-1.4.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl](https://files.pythonhosted.org/packages/cd/ad/2d3a14e5a97ca827a38d4963b86071267a6cd09d45065cd753d7325699b6/rasterio-1.4.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl)

- [shapely-2.0.6-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl](https://files.pythonhosted.org/packages/2b/a6/302e0d9c210ccf4d1ffadf7ab941797d3255dcd5f93daa73aaf116a4db39/shapely-2.0.6-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl)

- [apache_sedona-1.6.1-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl](https://files.pythonhosted.org/packages/b6/71/09f7ca2b6697b2699c04d1649bb379182076d263a9849de81295d253220d/apache_sedona-1.6.1-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl)

- [click_plugins-1.1.1-py2.py3-none-any.whl](https://files.pythonhosted.org/packages/e9/da/824b92d9942f4e472702488857914bdd50f73021efea15b4cad9aca8ecef/click_plugins-1.1.1-py2.py3-none-any.whl)

- [cligj-0.7.2-py3-none-any.whl](https://files.pythonhosted.org/packages/73/86/43fa9f15c5b9fb6e82620428827cd3c284aa933431405d1bcf5231ae3d3e/cligj-0.7.2-py3-none-any.whl)

- [affine-2.4.0-py3-none-any.whl](https://files.pythonhosted.org/packages/0b/f7/85273299ab57117850cc0a936c64151171fac4da49bc6fba0dad984a7c5f/affine-2.4.0-py3-none-any.whl)

- [numpy-2.1.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl](https://files.pythonhosted.org/packages/fb/25/ba023652a39a2c127200e85aed975fc6119b421e2c348e5d0171e2046edb/numpy-2.1.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl)


### Step 2: Upload packages to Synapse Workspace 

https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-manage-workspace-packages

### Step 3: Add packages to Spark Pool
I used the second method on this page: **If you are updating from the Synapse Studio**

https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-manage-pool-packages#manage-packages-from-synapse-studio-or-azure-portal


### Step 4: Notebook
Start your notebook with:
```python
from sedona.spark import SedonaContext

config = SedonaContext.builder() \
    .config('spark.jars.packages',
            'org.apache.sedona:sedona-spark-shaded-3.4_2.12-1.6.1,'
            'org.datasyslab:geotools-wrapper-1.6.1-28.2') \
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
    .config("spark.sql.extensions", "org.apache.sedona.viz.sql.SedonaVizExtensions") \
    .getOrCreate()

sedona = SedonaContext.create(config)
```

Run a test

```python
sedona.sql("SELECT ST_GeomFromEWKT('SRID=4269;POINT(40.7128 -74.0060)')").show()
```

If you see the output of the point, then the installation is successful. Are you are all done with the setup.

## Background: How to identify packages for other/future versions of Spark and/or Sedona
Warning: this process is going to require some tenacious technical skills and troubleshooting.

Broad steps: build a linux VM from the same image as the deployed Spark Pool, configure for Synapse, install Sedona packages, idenitify required packages over and above baseline Synapse config.

This is the process for Sedona 1.6.1 on Spark 3.4 Python 3.10. (I previsouly also had success with 1.6.0)

### Step 1: Identify the Linux image of the Spark Pool by version
https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-34-runtime

### Step 2 : Download the ISO
https://github.com/microsoft/azurelinux/tree/2.0

### Step 3: build the VM
https://github.com/microsoft/azurelinux/blob/2.0/toolkit/docs/quick_start/quickstart.md#iso-image

Important settings if using Hyper-V
- Enable Secure Boot: Microsoft UEFI Certificate authority
- Cores 2
- Disable Dynamic Memory (fix at 8Gb), forgetting this setting causes havoc.


### Step 4: patch the VM
Connect the VM. Note: it will take longer to first boot than you'd expect
```sh
sudo dnf upgrade
```

### Step 5: optional but strongly recommended - install ssh-server (for best copy and paste experience)
```sh
sudo tdnf install -y openssh-server
```

Enable root and password auth
```sh
sudo vi /etc/ssh/sshd_config
-	PasswordAuthentication yes
-	PermitRootLogin yes
```

Start ssh-server
```bash
sudo systemctl enable --now sshd.service
```

Identify the ip of the VM (I'm using Hyper-V on windows 10 desktop)
```ps
Get-VMNetworkAdapter -VMName "Synapse Spark 3.4 Python 3.10 Sedona 1.6.1" | Select-Object -ExpandProperty IPAddresses
```


### Step 6: install Miniconda
```bash
cd /tmp
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x Miniconda3-latest-Linux-x86_64.sh
./Miniconda3-latest-Linux-x86_64.sh
```

### Step 7: install compilers
```sh
sudo tdnf -y install gcc g++
```

### Step 8: create baseline synapse virtual env

Download the virtual env spec
```bash
wget -O Synapse-Python310-CPU.yml https://raw.githubusercontent.com/microsoft/synapse-spark-runtime/refs/heads/main/Synapse/spark3.4/Synapse-Python310-CPU.yml source 
```

```bash
conda env create -f Synapse-Python310-CPU.yml -n synapse
```

if you get errors due to `fsspec_wrapper` then remove `fsspec_wrapper==0.1.13=py_3` from the yml and run again

if you get further but different errors from `pip` after making the above change, ignore them you can still proceed

### Step 9: install sedona python packages
```bash
conda activate synapse
echo "apache-sedona==1.6.1" > requirements.txt
pip install -r requirements.txt > pip-output.txt
```

### Step 10: identify Python packages to download
```bash
grep Downloading pip-output.txt
```

**This will be the list of packages you need to locate and download from PyPi**

Example output
```
Downloading apache_sedona-1.6.1-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl (177 kB)
Downloading shapely-2.0.6-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl (2.5 MB)
Downloading rasterio-1.4.2-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl (22.2 MB)
Downloading affine-2.4.0-py3-none-any.whl (15 kB)
Downloading cligj-0.7.2-py3-none-any.whl (7.1 kB)
Downloading click_plugins-1.1.1-py2.py3-none-any.whl (7.5 kB)
```

### Step 11: identify package conflicts in your deployed Azure Synapse Spark Pool (the real one, not the VM)
- upload packages to workspace
- add packages to your (clean!) Spark pool

Pay careful attention to errors reported back from Synpase and troubleshoot to resolve conflicts.

I did not have issues with Sedona 1.6.0 on Spark 3.4, but Sedona 1.6.1 and supporting packages had a conflict around `numpy` which requires us to download a specific version and add it to the packages list.

## Packages for Sedona 1.6.0 on Spark 3.4 Python 10
```
spark-xml_2.12-0.17.0.jar	
sedona-spark-shaded-3.4_2.12-1.6.0.jar	

click_plugins-1.1.1-py2.py3-none-any.whl	
affine-2.4.0-py3-none-any.whl	
apache_sedona-1.6.0-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl	
cligj-0.7.2-py3-none-any.whl	
rasterio-1.3.10-cp310-cp310-manylinux2014_x86_64.whl	
shapely-2.0.4-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl	
snuggs-1.4.7-py3-none-any.whl	
geotools-wrapper-1.6.0-28.2.jar
```

## Author notes:
If we're still using Synapse in the future (considering Databricks) and need to upgrade I will post an update in this document.

In the event of any problems with the above process, raise an issue on Sedona GitHub and tag me https://github.com/golfalot








