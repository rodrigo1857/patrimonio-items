DROP SEQUENCE IF EXISTS bytsscom_bytsig.grupo_id_seq;
DROP SEQUENCE IF EXISTS bytsscom_bytsig.clase_id_seq;
DROP SEQUENCE IF EXISTS bytsscom_bytsig.familia_id_seq;


--- Creacion de secuencias
CREATE SEQUENCE IF NOT EXISTS bytsscom_bytsig.grupo_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bytsscom_bytsig.clase_id_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE IF NOT EXISTS bytsscom_bytsig.familia_id_seq START WITH 1 INCREMENT BY 1;

alter table bytsscom_bytsig.item
    add id_familia integer default 1;

--- Creacion de tablas
create table bytsscom_bytsig.item_grupo
(
    id_grupo        integer default nextval('bytsscom_bytsig.grupo_id_seq'::regclass)
        constraint id_item_grupo_pk
            primary key,
    codigo_grupo      varchar,
    descripcion_grupo varchar,
    tipo              varchar,
    activo             boolean DEFAULT TRUE
);


create table bytsscom_bytsig.item_clase
(
    id_clase     integer default nextval('bytsscom_bytsig.clase_id_seq'::regclass)
        constraint id_item_clase_pk
            primary key,
    codigo_clase varchar,
    nombre_clase varchar,
    id_grupo     integer
        constraint item_clase_grupo_fk
            references bytsscom_bytsig.item_grupo
            on update cascade on delete cascade,
    activo             boolean DEFAULT TRUE
);


create table bytsscom_bytsig.item_familia
(
    id_familia        integer default nextval('bytsscom_bytsig.familia_id_seq'::regclass) not null
        constraint id_item_familia_pk
            primary key,
    codigo_familia    varchar,
    nombre_familia    varchar,
    id_clase          integer
        constraint item_familia_clase__fk
            references bytsscom_bytsig.item_clase
            on update cascade on delete cascade,
    secuencia_familia varchar,
    activo             boolean DEFAULT TRUE
);

