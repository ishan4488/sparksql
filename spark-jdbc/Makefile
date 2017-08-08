
CLASSPATH=$(shell echo target/classes/ /home/luis/projects/Pentaho/design-tools/schema-workbench/lib/* | tr ' ' ':')

#java -cp target/hive-jdbc-1.2.2.jar org.apache.hive.jdbc.JDBCExperiments

run: build
	#javac src/java/org/apache/hive/jdbc/JDBCExperiments.java
	#java -cp target/hive-jdbc-1.2.2-standalone.jar org.apache.hive.jdbc.JDBCExperiments
	java -cp $(CLASSPATH) org.apache.hive.jdbc.JDBCExperiments

build: 
	mvn package

clean:
	-rm target/hive-jdbc-1.2.2-standalone.jar
	-rm target/hive-jdbc-1.2.2.jar
