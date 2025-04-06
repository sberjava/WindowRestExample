 Запуск postgres
 docker run --name my-postgres -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=mydb -d -p 5432:5432 postgres
 Запись дампа
 -XX:StartFlightRecording=filename=memory_recording.jfr,duration=60s,settings=profile