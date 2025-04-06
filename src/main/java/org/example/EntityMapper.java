package org.example;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.cursor.Cursor;

@Mapper
public interface EntityMapper {

    @Select("SELECT * FROM testschema.entities")
    Cursor<BatisEntity> findAll();
}
