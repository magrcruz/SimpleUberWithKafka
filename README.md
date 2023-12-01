# SimpleUberWithKafka

## Inicialización

1. **Iniciar servicios de ZooKeeper, Kafka y el clúster Flink:**
   
  ```bash
  sudo systemctl start zookeeper
  sudo systemctl start kafka
  ./start-cluster.sh
  ```

## Kafka

2. **Crear tópicos:**
   
  ```bash
  sudo bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic solicitudViaje
  sudo bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic aceptarSolicitud
  sudo bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic solicitudConfirmada
  ```

## Flink

3. **Agregar ranuras de tareas (task slots):**
   
  ```bash
  sudo nano flink-conf.yaml
  ```
   
  Para compilar y ejecutar el programa en Flink:
   
  ```bash
  mvn clean install && mvn clean package
  /opt/flink/bin/flink run -c com.tuempresa.KafkaFlinkConsumer target/proyecto-flink-kafka-1.0-SNAPSHOT.jar
  ```

4. **Ejemplo de solicitud:**
   
  ```bash
  sudo bin/kafka-console-producer.sh --broker-list localhost:9092 --topic solicitudViaje
  { "idViaje": "id", "idCliente": "456", "origen": "Ciudad Origen", "destino": "Ciudad Destino", "precio": "100.00" }
  ```

5. **Ejemplo de aceptación:**
   
  ```bash
  sudo bin/kafka-console-producer.sh --broker-list localhost:9092 --topic aceptarSolicitud
  {"idViaje":"id","idConductor":"ConductorID"}
  ```

6. **Ejemplo de solicitud confirmada:**
   
  ```bash
  sudo bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic solicitudConfirmada
  ```

  {"idViaje":"id","idCliente":"456","origen":"Ciudad Origen","destino":"Ciudad Destino","precio":"100.00","idConductor":"ConductorID"}
