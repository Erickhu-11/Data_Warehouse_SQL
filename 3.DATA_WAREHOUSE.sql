

--------------------------------------------------------------------------------
-- Creaci n de usuario para modelo Data-Warehouse.
--------------------------------------------------------------------------------
-- alter session set "_ORACLE_SCRIPT" = TRUE;
-- DROP USER PROYECTOABD_DW CASCADE;

 CREATE USER PROYECTOABD_DW IDENTIFIED BY Oracle01 DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
 GRANT CONNECT, RESOURCE TO PROYECTOABD_DW;
--------------------------------------------------------------------------------

CREATE TABLE DIM_TIPO_ENVIO (
   TPE_ID          INTEGER NOT NULL,
   TPE_DESCRIPCION VARCHAR2(30) NOT NULL,
   CONSTRAINT PK_TPE_ID PRIMARY KEY(TPE_ID),
   CONSTRAINT CK_TPE_DESCRIPCION CHECK(LENGTH(TPE_DESCRIPCION) >= 5)
);

CREATE TABLE DIM_TIPO_PRODUCTO (
   TPD_ID          INTEGER NOT NULL,
   TPD_DESCRIPCION VARCHAR2(30) NOT NULL,
   CONSTRAINT PK_TPD_ID PRIMARY KEY(TPD_ID),
   CONSTRAINT CK_TPD_DESCRIPCION CHECK(LENGTH(TPD_DESCRIPCION) >= 3)
);

CREATE TABLE DIM_CLIENTE (
   CTE_ID          INTEGER NOT NULL,
   CTE_NOMBRE VARCHAR2(70) NOT NULL,
   CONSTRAINT PK_CTE_ID PRIMARY KEY(CTE_ID),
   CONSTRAINT CK_CTE_NOMBRE CHECK(LENGTH(CTE_NOMBRE) >= 1)
);

CREATE TABLE DIM_PRODUCTO (
   PRD_ID          INTEGER NOT NULL,
   PRD_NOMBRE VARCHAR2(50) NOT NULL,
   CONSTRAINT PK_PRD_ID PRIMARY KEY(PRD_ID),
   CONSTRAINT CK_PRD_NOMBRE CHECK(LENGTH(PRD_NOMBRE) >= 2)
);

CREATE TABLE FAC_TRANSACCION (
   TRN_TPE_ID INTEGER NOT NULL,
   TRN_TPD_ID INTEGER NOT NULL,
   TRN_CTE_ID INTEGER NOT NULL,
   TRN_PRD_ID INTEGER NOT NULL,
   TRN_PRD_CANTIDAD NUMBER(20,2) NOT NULL,
   TRN_PRD_COSTO_UNITARIO NUMBER(20,2) NOT NULL,
   TRN_TPE_ESTADO NUMBER(20,2) NOT NULL,
   TRN_TPE_REQUIERE_CONFIRMACION NUMBER(20,2) NOT NULL,
   CONSTRAINT PK_TRN PRIMARY KEY (TRN_TPE_ID, TRN_TPD_ID, TRN_CTE_ID, TRN_PRD_ID),
   CONSTRAINT FK_TRN_TPE FOREIGN KEY(TRN_TPE_ID) REFERENCES DIM_TIPO_ENVIO(TPE_ID),
   CONSTRAINT FK_TRN_TPD FOREIGN KEY(TRN_TPD_ID) REFERENCES DIM_TIPO_PRODUCTO(TPD_ID),
   CONSTRAINT FK_TRN_CTE FOREIGN KEY(TRN_CTE_ID) REFERENCES DIM_CLIENTE(CTE_ID),
   CONSTRAINT FK_TRN_PRD FOREIGN KEY(TRN_PRD_ID) REFERENCES DIM_PRODUCTO(PRD_ID),
   CONSTRAINT CK_TRN_PRD_CANTIDAD CHECK(TRN_PRD_CANTIDAD > 0),
   CONSTRAINT CK_TRN_PRD_COSTO_UNITARIO CHECK(TRN_PRD_COSTO_UNITARIO > 0),
   CONSTRAINT CK_TRN_TPE_ESTADO CHECK(TRN_TPE_ESTADO > 0),
   CONSTRAINT CK_TRN_TPE_REQUIERE_CONFIRMACION CHECK(TRN_TPE_REQUIERE_CONFIRMACION > 0)
);


--DROP DE LAS TABLAS(EN CASO DE ERROR)


DROP TABLE  DIM_TIPO_ENVIO;

DROP TABLE  DIM_TIPO_PRODUCTO;

DROP TABLE  DIM_CLIENTE;

DROP TABLE DIM_PRODUCTO;

DROP TABLE FAC_TRANSACCION;
