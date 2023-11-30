package com.tuempresa;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class KafkaFlinkConsumer {
    public static void main(String[] args) throws Exception {

        // Configuración del entorno de Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	    env.setParallelism(1);


        // Propiedades de Kafka
        Properties propiedad_solicitudes = new Properties();
        propiedad_solicitudes.setProperty("bootstrap.servers", "localhost:9092");
        propiedad_solicitudes.setProperty("group.id", "test");

        Properties propiedad_aceptar = new Properties();
        propiedad_aceptar.setProperty("bootstrap.servers", "localhost:9092");
        propiedad_aceptar.setProperty("group.id", "test");

        Properties propiedad_confirmar = new Properties();
        propiedad_confirmar.setProperty("bootstrap.servers", "localhost:9092");
        propiedad_confirmar.setProperty("group.id", "test");


        // Crear un stream de Flink para consumir datos de Kafka
        DataStream<Solicitud> solicitudes = env
                .addSource(new FlinkKafkaConsumer<>("solicitudViaje", new SolicitudDeserializationSchema(), propiedad_solicitudes));
        //solicitudes.print();

        DataStream<Aceptacion> aceptaciones = env
                .addSource(new FlinkKafkaConsumer<>("aceptarSolicitud", new AceptacionDeserializationSchema(), propiedad_aceptar));
        //aceptaciones.print();

        // Asigna clave a las solicitudes
        KeyedStream<Solicitud, String> solicitudesConClave = solicitudes.keyBy(solicitud -> solicitud.idViaje);
        KeyedStream<Aceptacion, String> aceptacionesConClave = aceptaciones.keyBy(aceptacion -> aceptacion.idViaje);

        // Conecta los flujos
        ConnectedStreams<Solicitud, Aceptacion> connectedStreams = solicitudesConClave
                .connect(aceptacionesConClave);

        DataStream<Resultado> resultado = connectedStreams.flatMap(new CoFlatMapFunction<Solicitud, Aceptacion, Resultado>() {
            Set<String> idsRegistrados = new HashSet<>();
            Map<String, Solicitud> solicitudesPendientes = new HashMap<>();
            @Override
            public void flatMap1(Solicitud solicitud, Collector<Resultado> collector) throws Exception {
                if (!idsRegistrados.contains(solicitud.idViaje)) {
                    idsRegistrados.add(solicitud.idViaje);
                    solicitudesPendientes.put(solicitud.idViaje, solicitud);
                }
            }

            @Override
            public void flatMap2(Aceptacion aceptacion, Collector<Resultado> collector) throws Exception {
                if (idsRegistrados.contains(aceptacion.idViaje)) {
                    idsRegistrados.remove(aceptacion.idViaje);

                    //Solicitud solicitudPendiente = solicitudesPendientes.get(aceptacion.idViaje);
                    Solicitud solicitudPendiente = solicitudesPendientes.remove(aceptacion.idViaje);

                    if (solicitudPendiente != null) {
                        // Utiliza la información de Solicitud y Aceptacion para construir Resultado
                        Resultado resultado = new Resultado(solicitudPendiente, aceptacion);

                        collector.collect(resultado);
                    } else {
                        System.out.println("Error no se encontro la solicitud");
                    }
                }
            }
        });
        //resultado.print();

        //Escribir resultados
        DataStream<String> jsonStream = resultado.map(r -> {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                return objectMapper.writeValueAsString(r);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                System.out.println("Hubo algun error");

                return "Hubo algun error";
            }
        });
        //jsonStream.print();

        jsonStream.addSink(new FlinkKafkaProducer<>("solicitudConfirmada", new SimpleStringSchema(), propiedad_confirmar));

        env.execute("Kafka Flink Consumer");
    }
}

// Clase para representar las solicitudes
class Solicitud {
    public String idViaje;
    public String idCliente;
    public String origen;
    public String destino;
    public String precio;
    public Solicitud(String idViaje, String idCliente, String origen, String destino, String precio) {
        this.idViaje = idViaje;
        this.idCliente = idCliente;
        this.origen = origen;
        this.destino = destino;
        this.precio = precio;
    }

    public Solicitud(String jsonString) {
        try {
            // Crear un ObjectMapper para convertir el JSON a JsonNode
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(jsonString);

            // Leer los valores del JsonNode y asignarlos a las variables de instancia
            this.idViaje = jsonNode.get("idViaje").asText();
            this.idCliente = jsonNode.get("idCliente").asText();
            this.origen = jsonNode.get("origen").asText();
            this.destino = jsonNode.get("destino").asText();
            this.precio = jsonNode.get("precio").asText();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Hubo un error al procesar el JSON");
        }
    }

    //{ "idViaje": "id", "idCliente": "456", "origen": "Ciudad Origen", "destino": "Ciudad Destino", "precio": "100.00" }
}

class SolicitudDeserializationSchema extends AbstractDeserializationSchema<Solicitud> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Solicitud deserialize(byte[] message) {
        try {
            // Convertir bytes a cadena (para la impresión de diagnóstico)
            String jsonString = new String(message, StandardCharsets.UTF_8);
            System.out.println("JSON recibido de Solicitud: " + jsonString);

            return new Solicitud(jsonString);
        } catch (Exception e) {
            // Manejar excepciones según tus necesidades
            e.printStackTrace();
            return null;
        }
    }
}

// Clase para representar las aceptaciones
class Aceptacion {
    public String idViaje;
    public String idConductor;

    public Aceptacion(String idViaje,String idConductor) {
        this.idViaje = idViaje;
        this.idConductor = idConductor;
    }
    public Aceptacion(String jsonString) {
        try {
            // JSON a JsonNode
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(jsonString);

            // Leer los valores del JsonNode y asignarlos a las variables de instancia
            this.idViaje = jsonNode.get("idViaje").asText();
            this.idConductor = jsonNode.get("idConductor").asText();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Hubo un error al procesar el JSON");
        }
    }
    //{"idViaje":"id","idConductor":"ConductorID"}
}

class AceptacionDeserializationSchema extends AbstractDeserializationSchema<Aceptacion> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Aceptacion deserialize(byte[] message) {
        try {
            String jsonString = new String(message, StandardCharsets.UTF_8);
            System.out.println("JSON recibido de aceptacion: " + jsonString);

            return new Aceptacion(jsonString);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}

class Resultado {
    public String idViaje;
    public String idCliente;
    public String origen;
    public String destino;
    public String precio;
    public String idConductor;

    public Resultado(Solicitud solicitud, Aceptacion aceptacion) {
        this.idViaje = solicitud.idViaje;
        this.idCliente = solicitud.idCliente;
        this.origen = solicitud.origen;
        this.destino = solicitud.destino;
        this.precio = solicitud.precio;

        this.idConductor = aceptacion.idConductor;
    }
}
