To test

For MySQL

start MySQL in Docker

docker run -p 3306:3306  --name mysql -e MYSQL_ROOT_PASSWORD=secret mysql
create a schema in it called "springintegration"
run the schema creation script for mysql (mysql.sql)

post a submission to the rest endpoint:

curl --location --request POST 'localhost:8080' --header 'Content-Type: application/json' --data-raw '{"delay" : 1000,"submissionId" : "id","status" : "","description" : "description"}'

