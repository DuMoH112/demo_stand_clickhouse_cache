CREATE TABLE file(
    id SERIAL PRIMARY KEY,
    title varchar(255),
    count_rows int
);

CREATE TABLE filter_column1(
    id SERIAL PRIMARY KEY,
    title varchar(255)
);

CREATE TABLE filter_column2(
    id SERIAL PRIMARY KEY,
    title varchar(255)
);

CREATE TABLE filter_column3(
    id SERIAL PRIMARY KEY,
    title varchar(255)
);

CREATE TABLE filter_column4(
    id SERIAL PRIMARY KEY,
    title varchar(255)
);

CREATE TABLE filter_column5(
    id SERIAL PRIMARY KEY,
    title varchar(255)
);

CREATE TABLE filter_column6(
    id SERIAL PRIMARY KEY,
    title varchar(255)
);

CREATE TABLE filter_column7(
    id SERIAL PRIMARY KEY,
    title varchar(255)
);

CREATE TABLE filter_column8(
    id SERIAL PRIMARY KEY,
    title varchar(255)
);

CREATE TABLE filter_column9(
    id SERIAL PRIMARY KEY,
    title varchar(255)
);

CREATE TABLE filter_column10(
    id SERIAL PRIMARY KEY,
    title varchar(255)
);

INSERT INTO filter_column1(title) VALUES('Фильтр 1. Строка 1');
INSERT INTO filter_column1(title) VALUES('Фильтр 1. Строка 2');
INSERT INTO filter_column1(title) VALUES('Фильтр 1. Строка 3');
INSERT INTO filter_column1(title) VALUES('Фильтр 1. Строка 4');

INSERT INTO filter_column2(title) VALUES('Фильтр 2. Строка 1');
INSERT INTO filter_column2(title) VALUES('Фильтр 2. Строка 2');
INSERT INTO filter_column2(title) VALUES('Фильтр 2. Строка 3');
INSERT INTO filter_column2(title) VALUES('Фильтр 2. Строка 4');

INSERT INTO filter_column3(title) VALUES('Фильтр 3. Строка 1');
INSERT INTO filter_column3(title) VALUES('Фильтр 3. Строка 2');
INSERT INTO filter_column3(title) VALUES('Фильтр 3. Строка 3');
INSERT INTO filter_column3(title) VALUES('Фильтр 3. Строка 4');

INSERT INTO filter_column4(title) VALUES('Фильтр 4. Строка 1');
INSERT INTO filter_column4(title) VALUES('Фильтр 4. Строка 2');
INSERT INTO filter_column4(title) VALUES('Фильтр 4. Строка 3');
INSERT INTO filter_column4(title) VALUES('Фильтр 4. Строка 4');

INSERT INTO filter_column5(title) VALUES('Фильтр 5. Строка 1');
INSERT INTO filter_column5(title) VALUES('Фильтр 5. Строка 2');
INSERT INTO filter_column5(title) VALUES('Фильтр 5. Строка 3');
INSERT INTO filter_column5(title) VALUES('Фильтр 5. Строка 4');

INSERT INTO filter_column6(title) VALUES('Фильтр 6. Строка 1');
INSERT INTO filter_column6(title) VALUES('Фильтр 6. Строка 2');
INSERT INTO filter_column6(title) VALUES('Фильтр 6. Строка 3');
INSERT INTO filter_column6(title) VALUES('Фильтр 6. Строка 4');

INSERT INTO filter_column7(title) VALUES('Фильтр 7. Строка 1');
INSERT INTO filter_column7(title) VALUES('Фильтр 7. Строка 2');
INSERT INTO filter_column7(title) VALUES('Фильтр 7. Строка 3');
INSERT INTO filter_column7(title) VALUES('Фильтр 7. Строка 4');

INSERT INTO filter_column8(title) VALUES('Фильтр 8. Строка 1');
INSERT INTO filter_column8(title) VALUES('Фильтр 8. Строка 2');
INSERT INTO filter_column8(title) VALUES('Фильтр 8. Строка 3');
INSERT INTO filter_column8(title) VALUES('Фильтр 8. Строка 4');

INSERT INTO filter_column9(title) VALUES('Фильтр 9. Строка 1');
INSERT INTO filter_column9(title) VALUES('Фильтр 9. Строка 2');
INSERT INTO filter_column9(title) VALUES('Фильтр 9. Строка 3');
INSERT INTO filter_column9(title) VALUES('Фильтр 9. Строка 4');

INSERT INTO filter_column10(title) VALUES('Фильтр 10. Строка 1');
INSERT INTO filter_column10(title) VALUES('Фильтр 10. Строка 2');
INSERT INTO filter_column10(title) VALUES('Фильтр 10. Строка 3');
INSERT INTO filter_column10(title) VALUES('Фильтр 10. Строка 4');

CREATE TABLE raw_data(
    id BIGSERIAL PRIMARY KEY,
    file_id int REFERENCES file(id) ON DELETE CASCADE,
    dt date,
    filter_column1_id int REFERENCES filter_column1(id),
    filter_column2_id int REFERENCES filter_column2(id),
    filter_column3_id int REFERENCES filter_column3(id),
    filter_column4_id int REFERENCES filter_column4(id),
    filter_column5_id int REFERENCES filter_column5(id),
    filter_column6_id int REFERENCES filter_column6(id),
    filter_column7_id int REFERENCES filter_column7(id),
    filter_column8_id int REFERENCES filter_column8(id),
    filter_column9_id int REFERENCES filter_column9(id),
    filter_column10_id int REFERENCES filter_column10(id),
    value bigint
);

CREATE INDEX raw_data_file_id_index ON raw_data USING btree (file_id);
CREATE INDEX raw_data_dt_index ON raw_data USING btree (dt);
CREATE INDEX raw_data_filter_column1_id_index ON raw_data USING btree (filter_column1_id);
CREATE INDEX raw_data_filter_column2_id_index ON raw_data USING btree (filter_column2_id);
CREATE INDEX raw_data_filter_column3_id_index ON raw_data USING btree (filter_column3_id);
CREATE INDEX raw_data_filter_column4_id_index ON raw_data USING btree (filter_column4_id);
CREATE INDEX raw_data_filter_column5_id_index ON raw_data USING btree (filter_column5_id);
CREATE INDEX raw_data_filter_column6_id_index ON raw_data USING btree (filter_column6_id);
CREATE INDEX raw_data_filter_column7_id_index ON raw_data USING btree (filter_column7_id);
CREATE INDEX raw_data_filter_column8_id_index ON raw_data USING btree (filter_column8_id);
CREATE INDEX raw_data_filter_column9_id_index ON raw_data USING btree (filter_column9_id);
CREATE INDEX raw_data_filter_column10_id_index ON raw_data USING btree (filter_column10_id);


CREATE OR REPLACE FUNCTION random_int_between(low int, high int) 
   RETURNS int AS
$$
BEGIN
   RETURN floor(random() * (high - low + 1) + low);
END;
$$ language 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION random_dt_between(low_dt timestamp, high_dt timestamp)
   RETURNS date AS
$$
BEGIN
    RETURN date(low_dt + random() * (high_dt - low_dt));
END;
$$ language 'plpgsql' STRICT;

-- Функция наполнения справочных таблиц
CREATE OR REPLACE FUNCTION filling_of_raw_data_tables(count_rows_ int)
RETURNS boolean AS
$$
    DECLARE
        file_id_ int;
    BEGIN
        INSERT INTO file(title, count_rows) VALUES('RAW_DATA', count_rows_) RETURNING id INTO file_id_;
        FOR i IN 1..count_rows_ LOOP
            INSERT INTO raw_data(
                file_id,
                dt,
                filter_column1_id,
                filter_column2_id,
                filter_column3_id,
                filter_column4_id,
                filter_column5_id,
                filter_column6_id,
                filter_column7_id,
                filter_column8_id,
                filter_column9_id,
                filter_column10_id,
                value
            )
            VALUES (
                file_id_,
                random_dt_between('2019-01-01 00:00:00', '2020-01-01 00:00:00'),
                random_between(1, 4),
                random_between(1, 4),
                random_between(1, 4),
                random_between(1, 4),
                random_between(1, 4),
                random_between(1, 4),
                random_between(1, 4),
                random_between(1, 4),
                random_between(1, 4),
                random_between(1, 4),
                random_between(1, 10000)
            );

        END LOOP;
        RETURN true;
    END
$$
    LANGUAGE plpgsql;
--
