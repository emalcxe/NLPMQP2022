if [ ! -d "$HOME/jdk" ]; then
	mkdir ~/jdk
	cd ~/jdk
	wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
	tar -xvf openjdk*
	rm ./*.tar.gz*
fi
if [[ -z "${JAVA_HOME}" ]]; then
	export JAVA_HOME=~/jdk/jdk-11.0.2
	export PATH="$JAVA_HOME/bin:$PATH"
	export PATH

fi
if [ ! -d "$HOME/spark" ]; then
	mkdir ~/spark
	cd ~/spark
	wget https://archive.apache.org/dist/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz
	tar -xvf spark-3.2.2-bin-hadoop3.2.tgz
	rm ./*.tgz*
fi
export SPARK_HOME=~/spark/spark-3.2.2-bin-hadoop3.2
export PATH="$SPARK_HOME/bin:$PATH"
