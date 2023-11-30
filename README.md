# SimpleUberWithKafka

## Inicializacion

- [ ] Inicializar los servicios de zookeper, kafka y el cluster flink
      ```
      sudo systemctl start zookeeper
      sudo systemctl start kafka
      ./start-cluster.sh
      ```

## Kafka
- [ ] Crear topicos
      ```
      sudo bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic solicitudViaje
    sudo bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic aceptarSolicitud
    sudo bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic solicitudConfirmada
      ```

## Flink
- [ ] Agregar tasks slots
      ```
      sudo nano flink-conf.yaml
      ```
      Para compilar y ejecutar el programa en Flink 
      ```
       mvn clean install && mvn clean package
      /opt/flink/bin/flink run -c com.tuempresa.KafkaFlinkConsumer target/proyecto-flink-kafka-1.0-SNAPSHOT.jar
      ```
- [ ] Ejemplo de solicitud
     ```
      sudo bin/kafka-console-producer.sh --broker-list localhost:9092 --topic solicitudViaje
      { "idViaje": "id", "idCliente": "456", "origen": "Ciudad Origen", "destino": "Ciudad Destino", "precio": "100.00" }
    ```
- [ ] Ejemplo de aceptacion
       ```
        sudo bin/kafka-console-producer.sh --broker-list localhost:9092 --topic aceptarSolicitud
        {"idViaje":"id","idConductor":"ConductorID"}
      ```
- [ ] Ejemplo de solicitud confirmada
     ```
      sudo bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic solicitudConfirmada
      {"idViaje":"id","idCliente":"456","origen":"Ciudad Origen","destino":"Ciudad Destino","precio":"100.00","idConductor":"ConductorID"}
     ```

