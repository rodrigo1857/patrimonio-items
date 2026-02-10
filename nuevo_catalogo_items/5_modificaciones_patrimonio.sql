
-------------------------creacion de log de cambios en patrimonio_ambiente

CREATE OR REPLACE FUNCTION bytsscom_bytsig.log_ambiente_historial()
    RETURNS TRIGGER AS $$
BEGIN
    -- 1. CASO UPDATE: El usuario cambió algo (ej. el nombre)
    IF (TG_OP = 'UPDATE') THEN
        -- "Cerramos" la versión anterior para que ya no sea la 'actual'
        UPDATE bytsscom_bytsig.patrimonio_ambiente_historial
        SET fecha_final = NEW.fecha_modificacion
        WHERE id_patrimonio_ambiente = NEW.id_patrimonio_ambiente
          AND fecha_final IS NULL;
    END IF;

    -- 2. REGISTRO DE LA VERSIÓN VIGENTE
    -- Se ejecuta tanto en INSERT como en UPDATE
    INSERT INTO bytsscom_bytsig.patrimonio_ambiente_historial
    (
        id_patrimonio_ambiente, codigo, nombre, tipo, id_patrimonio_area,
        tipu_siga, ubic_siga, piso, centro_costo_id,
        etiqueta_nombre, id_patrimonio_tipo_ambiente, estado_patrimonio_ambiente,
        id_responsable_ambiente, id_persona_modificacion, id_patrimonio_local,
        fecha_inicio, fecha_final
    )
    VALUES
        (
            NEW.id_patrimonio_ambiente, NEW.codigo, NEW.nombre, NEW.tipo, NEW.id_patrimonio_area,
            NEW.tipu_siga, NEW.ubic_siga, NEW.piso, NEW.centro_costo_id,
            NEW.etiqueta_nombre, NEW.id_patrimonio_tipo_ambiente, NEW.estado_patrimonio_ambiente,
            NEW.id_responsable_ambiente, NEW.id_persona_modificacion, NEW.id_patrimonio_local,
            COALESCE(NEW.fecha_modificacion, now()),
            NULL
        );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_after_insert_ambiente ON bytsscom_bytsig.patrimonio_ambiente;

CREATE TRIGGER trg_log_patrimonio_ambiente
    AFTER INSERT OR UPDATE ON bytsscom_bytsig.patrimonio_ambiente
    FOR EACH ROW
EXECUTE PROCEDURE bytsscom_bytsig.log_ambiente_historial();





------------------------------cambio de id_item a id_familia en patrimonio_bien


alter table bytsscom_bytsig.patrimonio_bien
    add column id_familia integer default 1;


UPDATE bytsscom_bytsig.patrimonio_bien pb
SET id_familia = i.id_familia
FROM bytsscom_bytsig.item i
WHERE i.id_item = pb.id_item;

WITH conteo_items AS (
    SELECT id_item, COUNT(*) as frecuencia
    FROM bytsscom_bytsig.patrimonio_bien
    GROUP BY id_item
),
duplicados_priorizados AS (
    SELECT
        pb.id_patrimonio_bien,
        ROW_NUMBER() OVER (
            PARTITION BY pb.id_familia, pb.correlativo, pb.correlativo_ordinal
            ORDER BY ci.frecuencia DESC, pb.id_patrimonio_bien ASC
            ) as ranking
    FROM bytsscom_bytsig.patrimonio_bien pb
            JOIN conteo_items ci ON pb.id_item = ci.id_item
    WHERE pb.estado_patrimonio_bien::text = 'R'::text
)
DELETE FROM bytsscom_bytsig.patrimonio_bien
WHERE id_patrimonio_bien IN (
    SELECT id_patrimonio_bien
    FROM duplicados_priorizados
    WHERE ranking > 1
);


drop index bytsscom_bytsig.unique_patrimonio_bien_estado_registrado;
drop index bytsscom_bytsig.unique_patrimonio_bien_estado_anulado;

-- Creacion de indices
create unique index unique_patrimonio_bien_estado_registrado
    on bytsscom_bytsig.patrimonio_bien (id_familia, correlativo, correlativo_ordinal)
    where patrimonio_bien.estado_patrimonio_bien::text = 'R'::text;
create unique index unique_patrimonio_bien_estado_anulado
    on bytsscom_bytsig.patrimonio_bien (id_familia, correlativo, correlativo_ordinal)
    where patrimonio_bien.estado_patrimonio_bien::text = 'A'::text;