create table bytsscom_bytsig.items_grupos_auditoria_cambios
(
    id_auditoria       serial
        primary key,
    nombre_tabla       varchar not null,
    operacion          varchar not null,
    id_registro        integer not null,
    valores_anteriores jsonb,
    usuario_cambio     varchar,
    fecha_cambio       timestamp default CURRENT_TIMESTAMP,
    valores_nuevos     jsonb
);



CREATE OR REPLACE FUNCTION bytsscom_bytsig.fn_auditar_cambios() 
RETURNS TRIGGER AS $$
DECLARE
    v_user TEXT;
    v_id_afectado INTEGER;
    v_columna_id TEXT := TG_ARGV[0]; -- Nombre de la PK pasado como parámetro
BEGIN
    -- 1. Obtener el usuario
    v_user := current_setting('usuario.q20', true);
    IF v_user IS NULL OR v_user = '' THEN
        v_user := current_user;
    END IF;

    -- 2. Identificar el ID del registro de forma dinámica
    IF (TG_OP = 'DELETE') THEN
        v_id_afectado := (to_jsonb(OLD) ->> v_columna_id)::int;
    ELSE
        v_id_afectado := (to_jsonb(NEW) ->> v_columna_id)::int;
    END IF;

    -- 3. Registrar los cambios según la operación
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO bytsscom_bytsig.items_grupos_auditoria_cambios 
            (nombre_tabla, operacion, id_registro, valores_nuevos, usuario_cambio)
        VALUES (TG_TABLE_NAME, TG_OP, v_id_afectado, to_jsonb(NEW), v_user);

    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO bytsscom_bytsig.items_grupos_auditoria_cambios 
            (nombre_tabla, operacion, id_registro, valores_anteriores, valores_nuevos, usuario_cambio)
        VALUES (TG_TABLE_NAME, TG_OP, v_id_afectado, to_jsonb(OLD), to_jsonb(NEW), v_user);

    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO bytsscom_bytsig.items_grupos_auditoria_cambios 
            (nombre_tabla, operacion, id_registro, valores_anteriores, usuario_cambio)
        VALUES (TG_TABLE_NAME, TG_OP, v_id_afectado, to_jsonb(OLD), v_user);
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;



-- Para item_grupo
CREATE TRIGGER tr_auditoria_grupo
    AFTER INSERT OR UPDATE OR DELETE ON bytsscom_bytsig.item_grupo
    FOR EACH ROW EXECUTE PROCEDURE bytsscom_bytsig.fn_auditar_cambios('id_grupo');

-- Para item_clase
CREATE TRIGGER tr_auditoria_clase
    AFTER INSERT OR UPDATE OR DELETE ON bytsscom_bytsig.item_clase
    FOR EACH ROW EXECUTE PROCEDURE bytsscom_bytsig.fn_auditar_cambios('id_clase');

-- Para item_familia
CREATE TRIGGER tr_auditoria_familia
    AFTER INSERT OR UPDATE OR DELETE ON bytsscom_bytsig.item_familia
    FOR EACH ROW EXECUTE PROCEDURE bytsscom_bytsig.fn_auditar_cambios('id_familia');






CREATE OR REPLACE FUNCTION bytsscom_bytsig.fn_inactivar_en_cascada()
    RETURNS TRIGGER AS $$
BEGIN
    -- Verificamos que el estado esté cambiando de TRUE a FALSE
    IF (NEW.activo = FALSE AND OLD.activo IS DISTINCT FROM FALSE) THEN

        -- 1. De Grupo a Clase
        IF TG_TABLE_NAME = 'item_grupo' THEN
            UPDATE bytsscom_bytsig.item_clase
            SET activo = FALSE
            WHERE id_grupo = NEW.id_grupo;

            -- 2. De Clase a Familia
        ELSIF TG_TABLE_NAME = 'item_clase' THEN
            UPDATE bytsscom_bytsig.item_familia
            SET activo = FALSE
            WHERE id_clase = NEW.id_clase;

            -- 3. De Familia a Items (Aquí estaba el error de anidación)
        ELSIF TG_TABLE_NAME = 'item_familia' THEN
            UPDATE bytsscom_bytsig.item
            SET activo_item = 0,
                motivo_anulacion = 'ANULACION MASIVA POR ANULACION DE FAMILIA',
                sys_fech_anulcion = now(),
                id_per_anula = current_setting('usuario.q20', true)::integer
            WHERE id_familia = NEW.id_familia AND activo_item = 1;
        END IF;

    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- Trigger para Grupo
CREATE TRIGGER tr_inactivar_clases_por_grupo
    AFTER UPDATE OF activo ON bytsscom_bytsig.item_grupo
    FOR EACH ROW
    WHEN (NEW.activo = FALSE)
EXECUTE PROCEDURE bytsscom_bytsig.fn_inactivar_en_cascada();

-- Trigger para Clase
CREATE TRIGGER tr_inactivar_familias_por_clase
    AFTER UPDATE OF activo ON bytsscom_bytsig.item_clase
    FOR EACH ROW
    WHEN (NEW.activo = FALSE)
EXECUTE PROCEDURE bytsscom_bytsig.fn_inactivar_en_cascada();

-- Trigger para Familia
CREATE TRIGGER tr_inactivar_items_por_familia
    AFTER UPDATE OF activo ON bytsscom_bytsig.item_familia
    FOR EACH ROW
    WHEN (NEW.activo = FALSE)
EXECUTE PROCEDURE bytsscom_bytsig.fn_inactivar_en_cascada();







create or replace view bytsscom_bytsig.vw_item_historial
            (id_item, id_corr_historial, cod_item, nomb_item, tipo_item, proceso_item, tipo_bien_pat,
             id_item_clasificacion, activo_item, id_unidad_medida, id_per_registra, fts_item, sys_fech_registro,
             id_item_sup, sys_fech_anulcion, motivo_anulacion, id_per_anula, verificado, temporal)
as
SELECT tb.id_item,
       tb.id_corr_historial,
       tb.cod_item,
       tb.nomb_item,
       tb.tipo_item,
       tb.proceso_item,
       tb.tipo_bien_pat,
       tb.id_item_clasificacion,
       tb.activo_item,
       tb.id_unidad_medida,
       tb.id_per_registra,
       tb.fts_item,
       tb.sys_fech_registro,
       tb.id_item_sup,
       tb.sys_fech_anulcion,
       tb.motivo_anulacion,
       tb.id_per_anula,
       tb.verificado,
       tb.temporal,
       tb.id_familia
FROM (SELECT i.id_item,
             0 AS id_corr_historial,
             i.cod_item,
             i.nomb_item,
             i.tipo_item,
             i.proceso_item,
             i.tipo_bien_pat,
             i.id_item_clasificacion,
             i.activo_item,
             i.id_unidad_medida,
             i.id_per_registra,
             i.fts_item,
             i.sys_fech_registro,
             i.id_item_sup,
             i.sys_fech_anulcion,
             i.motivo_anulacion,
             i.id_per_anula,
             i.verificado,
             i.temporal,
             i.id_familia
      FROM bytsscom_bytsig.item i
      UNION ALL
      SELECT ih.id_item,
             ih.id_corr_historial,
             ih.cod_item,
             ih.nomb_item,
             i.tipo_item,
             i.proceso_item,
             COALESCE(ih.tipo_bien_pat, i.tipo_bien_pat) AS tipo_bien_pat,
             i.id_item_clasificacion,
             i.activo_item,
             i.id_unidad_medida,
             ih.id_per_modifica,
             i.fts_item,
             ih.sys_fech_registro,
             i.id_item_sup,
             i.sys_fech_anulcion,
             i.motivo_anulacion,
             i.id_per_anula,
             ih.verificado,
             i.temporal,
             i.id_familia
      FROM bytsscom_bytsig.item i
               JOIN bytsscom_bytsig.item_historial ih ON i.id_item = ih.id_item) tb;

