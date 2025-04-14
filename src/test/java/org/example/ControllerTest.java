package org.example;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.client.WebClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
public class ControllerTest {

    @Autowired
    private WebTestClient webTestClient;
    @Autowired
    private DataSource dataSource;
    @LocalServerPort
    private int port;

    @Container
    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpassword");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @ParameterizedTest()
    @ValueSource(strings = {"/entities/jdbc", "/entities/batis"})
    void mvcTest(String uri) {
        var maxValue = 9_981;
        createTable(maxValue);
        webTestClient.get()
                .uri(uri)
                .exchange()
                .expectStatus()
                .isOk()
                .expectBodyList(Dto.class)
                .hasSize(maxValue);
    }

    @ParameterizedTest()
    @ValueSource(strings = {"/entities/jdbc", "/entities/batis"})
    void productionTest(String uri) {

        var runtime = Runtime.getRuntime();
        var initialMemory = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Initial memory usage: " + initialMemory + " bytes");

        createTable(1_000_006);
        var client = WebClient.builder()
                .baseUrl("http://localhost:" + port)
                .build();

        var windowedFlux = client.get()
                .uri(uri)
                .retrieve()
                .bodyToFlux(Dto.class)
                .subscribeOn(Schedulers.boundedElastic())
                .window(10);

        var dataStream = windowedFlux
                .flatMap(window -> window
                        .doOnSubscribe(subscription -> System.out.println("New window started"))
                        .doOnComplete(() -> System.out.println("Window completed"))
                        .collectList()
                        .flatMap(this::writeToDisk)
                )
                .then();

        StepVerifier.create(dataStream).verifyComplete();
        var finalMemory = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Final memory usage: " + finalMemory + " bytes");
        System.out.println("Memory used: " + (finalMemory - initialMemory) + " bytes");
    }

    private Mono<Void> writeToDisk(List<Dto> chunk) {
        return Flux.fromIterable(chunk)
                .doOnNext(System.out::println)
                .then();
    }

    private void createTable(int maxValue) {
        try (var connection = dataSource.getConnection()) {
            try (var statement = connection.prepareStatement("CREATE SCHEMA IF NOT EXISTS testschema")) {
                statement.execute();
            }

            try (var statement = connection.prepareStatement("CREATE TABLE IF NOT EXISTS testschema.entities (id SERIAL PRIMARY KEY, name VARCHAR(255), description VARCHAR(1024))")) {
                statement.execute();
            }

            try (var statement = connection.prepareStatement("TRUNCATE TABLE testschema.entities")) {
                statement.execute();
            }

            var sql = "INSERT INTO testschema.entities (name, description) VALUES (?, ?)";
            try (var statement = connection.prepareStatement(sql)) {
                for (var i = 0; i < maxValue; i++) {
                    statement.setString(1, "Entity " + i);
                    statement.setString(2, "Description for Entity " + i);
                    statement.addBatch();
                }
                statement.executeBatch();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
