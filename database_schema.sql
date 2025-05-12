-- Cria o banco de dados (execute este bloco conectado a um banco como postgres)
CREATE DATABASE "sales-db"
    WITH
    OWNER = airflow
    ENCODING = 'UTF8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- Criação de Trigger para atualizar a coluna _updated
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW._updated = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Tabela de categorias
CREATE TABLE IF NOT EXISTS public.categories
(
    id bigint NOT NULL,
    category_name character varying NOT NULL,
    _created timestamp without time zone NOT NULL DEFAULT now(),
    _updated timestamp without time zone NOT NULL DEFAULT now() ON UPDATE now(),
    PRIMARY KEY (id)
)
TABLESPACE pg_default;

CREATE TRIGGER update_categories_modtime
BEFORE UPDATE ON public.categories
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

ALTER TABLE IF EXISTS public.categories
    OWNER to airflow;

-- Tabela de funcionários
CREATE TABLE IF NOT EXISTS public.employees
(
    id integer NOT NULL,
    name character varying NOT NULL,
    _created timestamp without time zone NOT NULL DEFAULT now(),
    _updated timestamp without time zone NOT NULL DEFAULT now() ON UPDATE now(),
    PRIMARY KEY (id)
)
TABLESPACE pg_default;

CREATE TRIGGER update_employees_modtime
BEFORE UPDATE ON public.employees
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

ALTER TABLE IF EXISTS public.employees
    OWNER to airflow;

-- Tabela de vendas
CREATE TABLE IF NOT EXISTS public.sales
(
    id integer NOT NULL,
    employee_id integer NOT NULL,
    category_id integer NOT NULL,
    sale_date date NOT NULL,
    sale_value numeric(20, 2) NOT NULL,
    _created timestamp without time zone NOT NULL DEFAULT now(),
    _updated timestamp without time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (id),
    CONSTRAINT fk_sales_categories FOREIGN KEY (category_id)
        REFERENCES public.categories (id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID,
    CONSTRAINT fk_sales_employees FOREIGN KEY (employee_id)
        REFERENCES public.employees (id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID
)
TABLESPACE pg_default;

CREATE TRIGGER update_sales_modtime
BEFORE UPDATE ON public.sales
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

ALTER TABLE IF EXISTS public.sales
    OWNER to airflow;
