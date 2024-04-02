package org.example;

import br.com.felipemira.jsonpathobject.utils.Convert;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.example.kafka.producer.DataProducer;
import org.example.kafka.schema.DataSchema;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

@SpringBootTest
public class AppTest {
    @Autowired
    private DataProducer producer;


    @Test
    void createAvroData() throws IOException {
        DataSchema data = new DataSchema();

        data.createShemaFile();
    }

    @Test
    void testProducerComplex() {
        String json = """
                 {
                    "loja": {
                        "bicicleta": {
                            "cor": "vermelho",
                            "preco": 19.95,
                            "detalhes": []
                        },
                        "livro": [
                            {
                                "autor": "Felipe Mira",
                                "preco": 99.95,
                                "categoria": "reference",
                                "titulo": "Como sou top",
                                "detalhes": {
                                    "editora": "BookCo",
                                    "isbn": "123-4567890123",
                                    "edicao": "1st"
                                }
                            },
                            {
                                "autor": "Felipe Mira",
                                "preco": 12.99,
                                "categoria": "fiction",
                                "titulo": "o dia em que errei",
                                "detalhes": {
                                    "editora": "BookCo",
                                    "isbn": "456-7890123456",
                                    "edicao": "1st"
                                }
                            }
                        ],
                        "disco": [
                            {
                                "artista": "Felipe Mira",
                                "preco": 9.99,
                                "genero": "Pop",
                                "album": "My First Album",
                                "detalhes": {
                                    "gravadora": "MusicCo",
                                    "dataDeLancamento": "2022-01-01",
                                    "musicas": [
                                        {
                                            "titulo": "My First Song",
                                            "duracao": "4:00",
                                            "detalhes": []
                                        },
                                        {
                                            "titulo": "My Second Song",
                                            "duracao": "4:00",
                                            "detalhes": [
                                                {
                                                    "coParticipacao": "Mira Felipe",
                                                    "trecho": "3:25"
                                                },
                                                {
                                                    "coParticipacao": "Mira Felipe",
                                                    "trecho": "2:10"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            },
                            {
                                "artista": "Felipe Mira",
                                "preco": 14.99,
                                "genero": "Rock",
                                "album": "My Second Album",
                                "detalhes": {
                                    "gravadora": "MusicCo",
                                    "dataDeLancamento": "2022-06-01",
                                    "musicas": []
                                }
                            }
                        ]
                    }
                }
                """;

        Convert convert = new Convert();

        Map<CharSequence, Object> list = convert.toMapJsonPath(json);

        org.example.avro.model.DataAvro dataAvro = org.example.avro.model.DataAvro.newBuilder()
                .setData(list).build();

        producer.sendAsync(dataAvro);
    }

    @Test
    void testProducer() {
        String json = """
                {
                  "carros": [
                    {
                      "cor": "vermelho",
                      "quantidadeDePortas": 4,
                      "modelo": "Sedan",
                      "ano": 2020,
                      "quilometragem": 15000,
                      "consumoMedio": 15.5,
                      "potenciaMotor": 2.0,
                      "possuiArCondicionado": true,
                      "proprietarioAnterior": null
                    },
                    {
                      "cor": "azul",
                      "quantidadeDePortas": 2,
                      "modelo": "Esportivo",
                      "ano": 2021,
                      "quilometragem": 5000,
                      "consumoMedio": 12.3,
                      "potenciaMotor": 3.0,
                      "possuiArCondicionado": true,
                      "proprietarioAnterior": null
                    },
                    {
                      "cor": "preto",
                      "quantidadeDePortas": 4,
                      "modelo": "SUV",
                      "ano": 2019,
                      "quilometragem": 30000,
                      "consumoMedio": 10.0,
                      "potenciaMotor": 2.5,
                      "possuiArCondicionado": true,
                      "proprietarioAnterior": null
                    }
                  ]
                }
                """;

        Convert convert = new Convert();

        Map<CharSequence, Object> list = convert.toMapJsonPath(json);

        org.example.avro.model.DataAvro dataAvro = org.example.avro.model.DataAvro.newBuilder()
                .setData(list).build();

        producer.sendAsync(dataAvro);
    }

    @Test
    void testProducerEndereco() {
        String json = """
                {
                	"endereco": {
                		"rua": "Estrada do Pequiá",
                		"numero": 4,
                		"detalhe": [
                			{
                				"complemento": "casa 1",
                				"pontoDeReferencia": "Próximo a padaria"
                			}
                		]
                	}
                }
                """;

        Convert convert = new Convert();

        Map<CharSequence, Object> list = convert.toMapJsonPath(json);

        org.example.avro.model.DataAvro dataAvro = org.example.avro.model.DataAvro.newBuilder()
                .setData(list).build();

        producer.sendAsync(dataAvro);
    }

}
