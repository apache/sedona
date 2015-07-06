This is source code of benchmark application.

You first must have spatialHadoop installed in your cluster. Please check http://spatialhadoop.cs.umn.edu/ for detailed install instruction. If you can't use shadoop in your command line then these source code will failed.

To compile this source code. You can use the pom.xml and eclipse to compile this.

To execute these program, please first use maven to compile it into jar package. make sure you set up the correct main class.

Please check the following in the pom.xml:
<archive>
                        <manifest>
                            <mainClass>hadoop.map_reduce.CorelationWithIndex</mainClass>
                        </manifest>
                    </archive>

Use hadoop jar *.jar (arglist) to run it on the cluster.
