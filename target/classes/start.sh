mvn -q -DskipTests package
java -jar target/vkexport-full-2.8.1-shaded.jar \
  --pfad t/ --prefix LU --schema mb --out export.sql --cp Cp850 \
  --batch-size 1000 --ddl-out ddl.sql
