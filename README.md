# import_advanti

Import the Advanti CSV data into Neo4j

# Instructions

1. Build it:

        mvn clean package

2. Copy target/importer-1.0-SNAPSHOT.jar to the plugins/ directory of your Neo4j server.

3. Download and copy additional jars to the plugins/ directory of your Neo4j server.
        
        wget http://central.maven.org/maven2/org/apache/commons/commons-csv/1.2/commons-csv-1.2.jar

4. Configure Neo4j by adding a line to conf/neo4j.conf:

        dbms.unmanaged_extension_classes=com.advantiload=/v1

5. Start Neo4j server.

6. pyLoader.py will connect to the server and load the data.

# Data
Data must be on the server.
