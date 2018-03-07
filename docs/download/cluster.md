# Set up your Apache Spark cluster

After downloading a Spark distribution from [Spark download page](http://spark.apache.org/downloads.html), you will find this shell in `./bin/` folder.

## Preliminary
1. Set up password-less SSH on your cluster. Each master-worker pair should have bi-directional password-less SSH.
2. Make sure you have installed JRE 1.8 or later.
3. Add the list of your workers' IP address in ./conf/slaves
4. Add the following lines in SPark configuration files, if necessary.


## Start your cluster
Go the root folder of the uncompressed Apache Spark folder. Start your spark cluster via a terminal

```
./sbin/start-all.sh
```
