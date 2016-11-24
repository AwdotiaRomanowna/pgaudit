SET ROLE TO postgres;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE OR REPLACE FUNCTION audit._partitions_month_table_creator(
  i_schema_name  NAME,
  i_table_name   NAME,
  i_table_suffix TEXT,
  i_stamp_column TIMESTAMP WITH TIME ZONE)
  RETURNS VOID AS
  $BODY$
  BEGIN
    EXECUTE
    'CREATE TABLE IF NOT EXISTS audit.' || i_schema_name || '_' || i_table_name
    || ' (LIKE ' || i_schema_name || '.' || i_table_name || ') INHERITS (audit.abstract)';

    EXECUTE
    'CREATE TABLE IF NOT EXISTS audit.' || i_schema_name || '_' || i_table_name || i_table_suffix
    || ' (CHECK(event_time >= '
    || quote_literal(date_trunc('month', i_stamp_column))
    || ' AND event_time < '
    || quote_literal(date_trunc('month', i_stamp_column) + INTERVAL '1 month')
    || ')) INHERITS (audit.' || i_schema_name || '_' || i_table_name || ')';
  END;
  $BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
COST 100;

CREATE OR REPLACE FUNCTION audit.config_insert()
  RETURNS TRIGGER AS
  $BODY$
  DECLARE
    l_query_create TEXT;
  BEGIN
/* CREATE MAIN AUDIT TABLE */

    l_query_create := 'CREATE TABLE audit.' || quote_ident(NEW.schema_name || '_' || NEW.table_name)
                      || ' (LIKE ' || NEW.schema_name || '.' || NEW.table_name ||
                      ' ) INHERITS(audit.abstract);';

    EXECUTE l_query_create;

/* CREATE TRIGGERS */

    l_query_create := '
              CREATE TRIGGER trigger_' || NEW.table_name || '_audit_update
              BEFORE UPDATE
              ON ' || NEW.schema_name || '.' || NEW.table_name || '
              FOR EACH ROW
              EXECUTE PROCEDURE audit.update();

              CREATE TRIGGER trigger_' || NEW.table_name || '_audit_insert
              AFTER INSERT
              ON ' || NEW.schema_name || '.' || NEW.table_name || '
              FOR EACH ROW
              EXECUTE PROCEDURE audit.insert();

              CREATE TRIGGER trigger_' || NEW.table_name || '_audit_delete
              BEFORE DELETE
              ON ' || NEW.schema_name || '.' || NEW.table_name || '
              FOR EACH ROW
              EXECUTE PROCEDURE audit.delete();

              CREATE TRIGGER trigger_' || NEW.table_name || '_audit_truncate
              BEFORE TRUNCATE
              ON ' || NEW.schema_name || '.' || NEW.table_name || '
              FOR EACH STATEMENT
              EXECUTE PROCEDURE audit.truncate();
              ';

    EXECUTE l_query_create;
    RETURN NULL;

  END;
  $BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
COST 100;
ALTER FUNCTION audit.config_insert() SET search_path=audit, PUBLIC;

CREATE OR REPLACE FUNCTION audit.config_update()
  RETURNS TRIGGER AS
  $BODY$
  DECLARE
    l_query_create TEXT;
  BEGIN

/* ENABLE/DISABLE TRIGGERS */
    IF OLD.enabled = TRUE AND NEW.enabled = FALSE
    THEN

      l_query_create := 'ALTER TABLE ' || OLD.schema_name || '.' || OLD.table_name ||
                        ' DISABLE TRIGGER trigger_' || OLD.table_name || '_audit_update;

                      ALTER TABLE ' || OLD.schema_name || '.' || OLD.table_name ||
                        ' DISABLE TRIGGER trigger_' || OLD.table_name || '_audit_insert;

                      ALTER TABLE ' || OLD.schema_name || '.' || OLD.table_name ||
                        ' DISABLE TRIGGER trigger_' || OLD.table_name || '_audit_delete;

                      ALTER TABLE ' || OLD.schema_name || '.' || OLD.table_name ||
                        ' DISABLE TRIGGER trigger_' || OLD.table_name || '_audit_truncate;
                      ';

      EXECUTE l_query_create;
      RETURN NEW;

    ELSE
      l_query_create := 'ALTER TABLE ' || OLD.schema_name || '.' || OLD.table_name ||
                        ' ENABLE TRIGGER trigger_' || OLD.table_name || '_audit_update;

                      ALTER TABLE ' || OLD.schema_name || '.' || OLD.table_name ||
                        ' ENABLE TRIGGER trigger_' || OLD.table_name || '_audit_insert;

                      ALTER TABLE ' || OLD.schema_name || '.' || OLD.table_name ||
                        ' ENABLE TRIGGER trigger_' || OLD.table_name || '_audit_delete;

                      ALTER TABLE ' || OLD.schema_name || '.' || OLD.table_name ||
                        ' ENABLE TRIGGER trigger_' || OLD.table_name || '_audit_truncate;
                      ';

      EXECUTE l_query_create;
      RETURN NEW;

    END IF;

  END;
  $BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
COST 100;
ALTER FUNCTION audit.config_update() SET search_path=audit, PUBLIC;

CREATE OR REPLACE FUNCTION audit.config_delete()
  RETURNS TRIGGER AS
  $BODY$
  DECLARE
    l_query_create TEXT;
  BEGIN

/* DELETE TRIGGERS */

    l_query_create := '
            DROP TRIGGER trigger_' || OLD.table_name || '_audit_update
              ON ' || OLD.schema_name || '.' || OLD.table_name || ';

            DROP TRIGGER trigger_' || OLD.table_name || '_audit_insert
              ON ' || OLD.schema_name || '.' || OLD.table_name || ';

            DROP TRIGGER trigger_' || OLD.table_name || '_audit_delete
              ON ' || OLD.schema_name || '.' || OLD.table_name || ';

            DROP TRIGGER trigger_' || OLD.table_name || '_audit_truncate
              ON ' || OLD.schema_name || '.' || OLD.table_name;

    EXECUTE l_query_create;
    RETURN NULL;

  END;
  $BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
COST 100;
ALTER FUNCTION audit.config_delete() SET search_path=audit, PUBLIC;

CREATE OR REPLACE FUNCTION audit.insert()
  RETURNS TRIGGER AS
  $BODY$
  DECLARE
    l_table_suffix TEXT := to_char(NOW(), '_YYYY_MM');
    l_query        TEXT;
    l_table_name   TEXT := quote_ident(TG_TABLE_SCHEMA || '_' || TG_TABLE_NAME || l_table_suffix);

  BEGIN
    l_query:= format('INSERT INTO %I SELECT $1, $2, $3, $4.*', l_table_name);

    EXECUTE l_query
    USING CURRENT_TIMESTAMP, SESSION_USER, TG_OP, NEW;

    RETURN NEW;

    EXCEPTION WHEN UNDEFINED_TABLE
    THEN
      PERFORM audit._partitions_month_table_creator(i_schema_name := TG_TABLE_SCHEMA, i_table_name := TG_TABLE_NAME,
                                                      i_table_suffix := l_table_suffix, i_stamp_column := NOW());

      EXECUTE l_query
      USING CURRENT_TIMESTAMP, SESSION_USER, TG_OP, NEW;
      RETURN NEW;

    WHEN OTHERS
      THEN
        RAISE NOTICE 'ERROR: INTERNAL ERROR IN AUDIT FUNCTION audit.insert()';
        RETURN NEW;

  END;
  $BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
COST 100;
ALTER FUNCTION audit.insert() SET search_path= PUBLIC, audit;

CREATE OR REPLACE FUNCTION audit.update()
  RETURNS TRIGGER AS
  $BODY$
  DECLARE
    l_table_suffix TEXT := to_char(NOW(), '_YYYY_MM');
    l_query        TEXT;
    l_table_name   TEXT := quote_ident(TG_TABLE_SCHEMA || '_' || TG_TABLE_NAME || l_table_suffix);

  BEGIN
  /* ADD ROW ONLY IF SOMETHING CHANGED */

    IF md5(NEW :: TEXT) = md5(OLD :: TEXT)
    THEN
      RETURN NEW;
    END IF;

    l_query:= format('INSERT INTO %I SELECT $1, $2, $3, $4.*', l_table_name);

    EXECUTE l_query
    USING CURRENT_TIMESTAMP, SESSION_USER, TG_OP, OLD;

    RETURN NEW;

    EXCEPTION WHEN UNDEFINED_TABLE
    THEN
      PERFORM audit._partitions_month_table_creator(i_schema_name := TG_TABLE_SCHEMA, i_table_name := TG_TABLE_NAME,
                                                      i_table_suffix := l_table_suffix, i_stamp_column := NOW());

      EXECUTE l_query
      USING CURRENT_TIMESTAMP, SESSION_USER, TG_OP, OLD;
      RETURN NEW;

    WHEN OTHERS
      THEN
        RAISE NOTICE 'ERROR: INTERNAL ERROR IN AUDIT FUNCTION audit.update()';
        RETURN NEW;

        RETURN NEW;

  END;
  $BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
COST 100;
ALTER FUNCTION audit.update() SET search_path=audit, PUBLIC;

CREATE OR REPLACE FUNCTION audit.delete()
  RETURNS TRIGGER AS
  $BODY$
  DECLARE
    l_table_suffix TEXT := to_char(NOW(), '_YYYY_MM');
    l_query        TEXT;
    l_table_name   TEXT := quote_ident(TG_TABLE_SCHEMA || '_' || TG_TABLE_NAME || l_table_suffix);

  BEGIN
    l_query:= format('INSERT INTO %I SELECT $1, $2, $3, $4.*', l_table_name);

    EXECUTE l_query
    USING CURRENT_TIMESTAMP, SESSION_USER, TG_OP, OLD;

    RETURN OLD;

    EXCEPTION WHEN UNDEFINED_TABLE
    THEN
      PERFORM audit._partitions_month_table_creator(i_schema_name := TG_TABLE_SCHEMA, i_table_name := TG_TABLE_NAME,
                                                      i_table_suffix := l_table_suffix, i_stamp_column := NOW());

      EXECUTE l_query
      USING CURRENT_TIMESTAMP, SESSION_USER, TG_OP, OLD;
      RETURN OLD;

    WHEN OTHERS
      THEN
        RAISE NOTICE 'ERROR: INTERNAL ERROR IN AUDIT FUNCTION audit.delete()';
        RETURN OLD;

        RETURN OLD;

  END;
  $BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
COST 100;
ALTER FUNCTION audit.delete() SET search_path=audit, PUBLIC;

CREATE OR REPLACE FUNCTION audit.truncate()
  RETURNS trigger AS
$BODY$
  DECLARE
    l_table_suffix TEXT := to_char(NOW(), '_YYYY_MM');
    l_query        TEXT;
    l_table_name   TEXT := quote_ident(TG_TABLE_SCHEMA || '_' || TG_TABLE_NAME || l_table_suffix);
    l_table_name_original TEXT := TG_TABLE_NAME;
  BEGIN
    l_query:= format('INSERT INTO %I SELECT $1, $2, $3, * FROM %I', l_table_name, l_table_name_original);

    EXECUTE l_query
    USING CURRENT_TIMESTAMP, SESSION_USER, TG_OP;

    RETURN NULL;

    EXCEPTION WHEN UNDEFINED_TABLE
    THEN
      PERFORM audit._partitions_month_table_creator(i_schema_name := TG_TABLE_SCHEMA, i_table_name := TG_TABLE_NAME,
                                                      i_table_suffix := l_table_suffix, i_stamp_column := NOW());

      EXECUTE l_query
      USING CURRENT_TIMESTAMP, SESSION_USER, TG_OP;
      RETURN NULL;

    WHEN OTHERS
      THEN
        RAISE NOTICE 'ERROR: INTERNAL ERROR IN AUDIT FUNCTION audit.truncate()';
        RETURN NULL;

  END;
  $BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION audit.truncate() SET search_path=audit, public;

CREATE TABLE audit.abstract
(
  event_time  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  executed_by TEXT                     NOT NULL DEFAULT "session_user"(),
  operation   TEXT                     NOT NULL
)
WITH (
OIDS =FALSE
);

CREATE TABLE audit.config
(
  schema_name TEXT    NOT NULL DEFAULT 'public' :: TEXT,
  table_name  TEXT    NOT NULL,
  enabled     BOOLEAN NOT NULL DEFAULT TRUE,
  CONSTRAINT pk_config PRIMARY KEY (table_name, schema_name)
)
WITH (
OIDS =FALSE
);

CREATE TRIGGER trigger_audit_config_delete
AFTER DELETE
ON audit.config
FOR EACH ROW
EXECUTE PROCEDURE audit.config_delete();

CREATE TRIGGER trigger_audit_config_insert
AFTER INSERT
ON audit.config
FOR EACH ROW
EXECUTE PROCEDURE audit.config_insert();

CREATE TRIGGER trigger_audit_config_update
BEFORE UPDATE
ON audit.config
FOR EACH ROW
EXECUTE PROCEDURE audit.config_update();


