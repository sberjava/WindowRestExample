package org.example;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Function;
import java.util.stream.StreamSupport;

@Service
@AllArgsConstructor
@Slf4j
public class ServiceExample {
    private final DataSource dataSource;
    private final SqlSessionFactory sqlSessionFactory;

    public Flux<Dto> findAllJdbc() {
        var iterator = new ResultSetIterator(getConnection(), "SELECT * FROM testschema.entities");
        var jdbcToStream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
                false
        ).onClose(() -> {
            try {
                iterator.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close iterator", e);
            }
        });
        return Flux.fromStream(jdbcToStream).map(x -> new Dto(x.id(), x.name(), x.description()));
    }

    public Flux<Dto> findAllBatis() {
        Function<SqlSession, Cursor<BatisEntity>> cursorFunction = sqlSession -> sqlSession
                .getMapper(EntityMapper.class).findAll();
        var producer = new MyBatisFluxResultProducer<BatisEntity>(sqlSessionFactory);
        return producer.execute(cursorFunction).map(x -> new Dto(x.getId(), x.getName(), x.getDescription()));
    }

    private Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static class ResultSetIterator implements Iterator<Entity>, AutoCloseable {

        private final Connection connection;
        private final Statement statement;
        private final ResultSet resultSet;
        private boolean hasNext;

        public ResultSetIterator(Connection connection, String query) {
            this.connection = connection;
            try {
                this.statement = connection.createStatement();
                this.resultSet = statement.executeQuery(query);
                this.hasNext = resultSet.next();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to initialize ResultSetIterator", e);
            }
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Entity next() {
            if (!hasNext) {
                throw new NoSuchElementException("No more elements in ResultSet");
            }
            try {
                var entity = new Entity(
                        resultSet.getLong("id"),
                        resultSet.getString("name"),
                        resultSet.getString("description")
                );
                hasNext = resultSet.next();
                return entity;
            } catch (SQLException e) {
                throw new RuntimeException("Failed to fetch next row from ResultSet", e);
            }
        }

        @Override
        public void close() {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    log.error(e.getMessage());
                }
            }

            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    log.error(e.getMessage());
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.error(e.getMessage());
                }
            }
            log.info("Connection closed!");
        }
    }


    public static class MyBatisFluxResultProducer<T> {

        private final SqlSessionFactory sqlSessionFactory;

        private Cursor<T> cursor;

        private SqlSession sqlSession;

        public MyBatisFluxResultProducer(SqlSessionFactory sqlSessionFactory) {
            this.sqlSessionFactory = sqlSessionFactory;
        }

        public Flux<T> execute(Function<SqlSession, Cursor<T>> cursorFunction) {
            Optional.ofNullable(sqlSession)
                    .ifPresent(e -> {
                        throw new RuntimeException("Cursor already open");
                    });

            sqlSession = sqlSessionFactory.openSession();
            cursor = cursorFunction.apply(sqlSession);
            return Flux.fromStream(StreamSupport.stream(cursor.spliterator(), false).onClose(this::close));
        }

        public void close() {
            if (cursor.isOpen()) {
                try {
                    cursor.close();
                } catch (IOException e) {
                    log.error(e.getMessage());
                }
            }
            try {
                sqlSession.close();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }
}
