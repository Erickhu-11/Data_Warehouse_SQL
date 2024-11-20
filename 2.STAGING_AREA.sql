--Grupo #3
--Erick Hernández Ugalde    100%
--Caleb Leon Azofeifa       100%
--Daniel Madrigal Mendez    100%

--------------------------------------------------------------------------------
-- Creación de usuario para modelo Staging Area.
--------------------------------------------------------------------------------
-- alter session set "_ORACLE_SCRIPT" = TRUE;
-- DROP USER PROYECTOABD_SA CASCADE;

 CREATE USER PROYECTOABD_SA IDENTIFIED BY Oracle01 DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
 GRANT CONNECT, RESOURCE TO PROYECTOABD_SA;
--------------------------------------------------------------------------------

CREATE TABLE SA_TIPO_ENVIO (
   TPE_ID                       VARCHAR2(255),
   TPE_DESCRIPCION              VARCHAR2(255),
   TPE_ESTADO                   VARCHAR2(255),
   TPE_REQUIERE_CONFIRMACION    VARCHAR2(255)
);

CREATE TABLE SA_TIPO_PRODUCTO (
   TPD_ID           VARCHAR2(255),
   TPD_DESCRIPCION  VARCHAR2(255)
);

CREATE TABLE SA_CLIENTE (
   CTE_ID       VARCHAR2(255),
   CTE_NOMBRE   VARCHAR2(255)
);

CREATE TABLE SA_PRODUCTO (
   PRD_ID               VARCHAR2(255),
   PRD_TPD_ID           VARCHAR2(255),
   PRD_NOMBRE           VARCHAR2(255),
   PRD_CANTIDAD         VARCHAR2(255),
   PRD_COSTO_UNITARIO   VARCHAR2(255)
);

CREATE TABLE SA_ORDEN_COMPRA (
   OCP_ID       VARCHAR2(255),
   OCP_PRD_ID   VARCHAR2(255),
   OCP_CTE_ID   VARCHAR2(255),
   OCP_TPE_ID   VARCHAR2(255)
);