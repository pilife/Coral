#Coral

A Cross-Platform Unified Big Data Intelligent SQL Query System

Based On Apache Calcite

## Compile

make base shade jar (heavy)
> mvn clean install  -Dcheckstyle.skip -DskipTests

make coral jar (light)
> mvn clean install  -Dcheckstyle.skip -DskipTests -pl coral -P coral