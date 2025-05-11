-- Cria o banco de dados (execute este bloco conectado a um banco como postgres)
CREATE DATABASE "sales-db"
    WITH
    OWNER = airflow
    ENCODING = 'UTF8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- Agora conecte ao banco de dados recém-criado
-- Este comando funciona no psql, mas se você estiver usando uma ferramenta gráfica (como DBeaver ou pgAdmin),
-- apenas abra uma nova conexão com o banco `sales_db`
\c sales_db

-- Tabela de categorias
CREATE TABLE IF NOT EXISTS public.categories
(
    id bigint NOT NULL,
    category_name character varying(30)[] NOT NULL,
    PRIMARY KEY (id)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS public.categories
    OWNER to airflow;

-- Tabela de vendas
CREATE TABLE IF NOT EXISTS public.sales
(
    id integer NOT NULL,
    employee_id integer NOT NULL,
    category_id integer NOT NULL,
    sale_date date NOT NULL,
    sale_value numeric(20, 2) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_sale_employee FOREIGN KEY (employee_id)
        REFERENCES public.employee (id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID,
    CONSTRAINT fk_sale_categories FOREIGN KEY (category_id)
        REFERENCES public.categories (id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
        NOT VALID
);
ALTER TABLE IF EXISTS public.sales
    OWNER to airflow;

-- Tabela de funcionários
CREATE TABLE IF NOT EXISTS public.employee
(
    id integer NOT NULL,
    name character varying(60) NOT NULL,
    PRIMARY KEY (id)
)
TABLESPACE pg_default;
ALTER TABLE IF EXISTS public.employee
    OWNER to airflow;
