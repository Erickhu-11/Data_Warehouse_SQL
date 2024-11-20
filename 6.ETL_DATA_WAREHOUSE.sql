--------------------------------------------------------------------------------
-- Función para validar numeros enteros.
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION VALIDA_NUMERO_ENTERO(P_NUMERO VARCHAR2) RETURN CHAR AS
   V_NUMERO NUMBER;
BEGIN
   V_NUMERO := TO_NUMBER(P_NUMERO);
   IF V_NUMERO = ROUND(V_NUMERO) THEN
      RETURN 'S';
   ELSE
      RETURN 'N';
   END IF;
EXCEPTION
   WHEN OTHERS THEN
      RETURN 'N';
END;
/

CREATE OR REPLACE FUNCTION VALIDA_NUMERO_DECIMAL(P_NUMERO VARCHAR2) RETURN CHAR AS
   V_NUMERO NUMBER(20,2);
BEGIN
   V_NUMERO := TO_NUMBER(P_NUMERO);
   IF V_NUMERO <> ROUND(V_NUMERO,0) THEN
      RETURN 'S';
   ELSE
      RETURN 'N';
   END IF;
EXCEPTION
   WHEN OTHERS THEN
      RETURN 'N';
END;
/

--------------------------------------------------------------------------------
-- Se crea una tabla de errores por cada tabla del DW.
--------------------------------------------------------------------------------

CREATE TABLE PROYECTOABD_DW.ERROR_DIM_CLIENTE (
   CTE_ID                           VARCHAR2(255),
   CTE_NOMBRE                       VARCHAR2(255),
   CTE_ERROR                        VARCHAR2(4000)
);

CREATE TABLE PROYECTOABD_DW.ERROR_DIM_PRODUCTO (
   PRD_ID                           VARCHAR2(255),
   PRD_NOMBRE                       VARCHAR2(255),
   PRD_ERROR                        VARCHAR2(4000)
);

CREATE TABLE PROYECTOABD_DW.ERROR_DIM_TIPO_ENVIO (
   TPE_ID                           VARCHAR2(255),
   TPE_DESCRIPCION                  VARCHAR2(255),
   TPE_ERROR                        VARCHAR2(4000)
);

CREATE TABLE PROYECTOABD_DW.ERROR_DIM_TIPO_PRODUCTO(
    TPD_ID                          VARCHAR2(255),
    TPD_DESCRIPCION                 VARCHAR2(255),
    TPD_ERROR                       VARCHAR2(4000)
);

CREATE TABLE PROYECTOABD_DW.ERROR_FAC_TRANSACCION(
    TRN_TPE_ID                      VARCHAR2(255), 
    TRN_TPD_ID                      VARCHAR2(255), 
    TRN_CTE_ID                      VARCHAR2(255), 
    TRN_PRD_ID                      VARCHAR2(255),
    TRN_PRD_CANTIDAD                VARCHAR2(255),
    TRN_PRD_COSTO_UNITARIO          VARCHAR2(255),
    TRN_TPE_ESTADO                  VARCHAR2(255),
    TRN_TPE_REQUIERE_CONFIRMACION   VARCHAR2(255),
    TRN_ERROR                       VARCHAR2(4000)
);
    
--------------------------------------------------------------------------------
-- Especificacion del paquete_Cliente.
--------------------------------------------------------------------------------

CREATE OR REPLACE PACKAGE PROYECTOABD_DW.ETL_DW AS
   PROCEDURE MigrarDatos;
   PROCEDURE MigrarCliente;
   PROCEDURE MigrarProducto;
   PROCEDURE MigrarEnvio;
   PROCEDURE MigrarTipoProducto;
   PROCEDURE MigrarTransaccion;
END ETL_DW;
/
--------------------------------------------------------------------------------
-- Cuerpo del paquete.
--------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY PROYECTOABD_DW.ETL_DW AS

   -- Migración de Clientes.
   
   PROCEDURE MigrarCliente IS
      V_ERROR         INTEGER;
      V_NUMERO        INTEGER;
      V_ERROR_MENSAJE VARCHAR2(4000);
      CURSOR C_DATOS IS
         SELECT CTE.CTE_ID,
                CTE.CTE_NOMBRE
           FROM PROYECTOABD_SA.SA_CLIENTE CTE
          WHERE CTE.CTE_ID NOT IN (SELECT D.CTE_ID FROM PROYECTOABD_DW.DIM_CLIENTE D)
          ORDER BY CTE.CTE_ID;
   BEGIN
      FOR D_DATOS IN C_DATOS LOOP
         BEGIN
            V_ERROR := 0;
            V_ERROR_MENSAJE := '';
            
            -- Validaciones del código del cliente
            
            IF D_DATOS.CTE_ID IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Código nulo. ';
            ELSIF VALIDA_NUMERO_ENTERO(D_DATOS.CTE_ID) = 'N' THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Código no numérico. ';
            ELSE
               V_NUMERO := TO_NUMBER(D_DATOS.CTE_ID);
               IF V_NUMERO <= 0 THEN
                  V_ERROR := 1;
                  V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Código negativo o cero. ';
               END IF;
            END IF;
            
            -- Validaciones del nombre del cliente
            
            IF D_DATOS.CTE_NOMBRE IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre nulo. ';
            ELSIF LENGTH(D_DATOS.CTE_NOMBRE) > 40 THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre con longitud mayor a 40 caracteres. ';
            ELSIF LENGTH(D_DATOS.CTE_NOMBRE) < 5 THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre con longitud menor a 5 caracteres. ';
            ELSIF VALIDA_NUMERO_ENTERO(D_DATOS.CTE_NOMBRE) = 'S' THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre no debe incluir números. ';
            END IF;
            
            -- Inserción de datos en la tabla de cliente
            
            IF V_ERROR = 0 THEN
               INSERT INTO PROYECTOABD_DW.DIM_CLIENTE (CTE_ID, CTE_NOMBRE)
               VALUES (D_DATOS.CTE_ID, D_DATOS.CTE_NOMBRE);
            ELSE
               INSERT INTO PROYECTOABD_DW.ERROR_DIM_CLIENTE (CTE_ID, CTE_NOMBRE, CTE_ERROR)
               VALUES (D_DATOS.CTE_ID, D_DATOS.CTE_NOMBRE, V_ERROR_MENSAJE);            
            END IF;
         EXCEPTION
            WHEN OTHERS THEN
               INSERT INTO PROYECTOABD_DW.ERROR_DIM_CLIENTE (CTE_ID, CTE_NOMBRE, CTE_ERROR)
               VALUES (D_DATOS.CTE_ID, D_DATOS.CTE_NOMBRE, 'Error al insertar');
         END;
      END LOOP;
   END MigrarCliente;

   -- Migración de Productos.
   
   PROCEDURE MigrarProducto IS
      V_ERROR         INTEGER;
      V_NUMERO        INTEGER;
      V_ERROR_MENSAJE VARCHAR2(4000);
      CURSOR C_DATOS IS
         SELECT PRD.PRD_ID,
                PRD.PRD_NOMBRE
           FROM PROYECTOABD_SA.SA_PRODUCTO PRD
          WHERE PRD.PRD_ID NOT IN (SELECT D.PRD_ID FROM PROYECTOABD_DW.DIM_PRODUCTO D)
          ORDER BY PRD.PRD_ID;
   BEGIN
      FOR D_DATOS IN C_DATOS LOOP
         BEGIN
            V_ERROR := 0;
            V_ERROR_MENSAJE := '';
            
            -- Validaciones del código del producto
            
            IF D_DATOS.PRD_ID IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Código nulo. ';
            ELSIF VALIDA_NUMERO_ENTERO(D_DATOS.PRD_ID) = 'N' THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Código no numérico. ';
            ELSE
               V_NUMERO := TO_NUMBER(D_DATOS.PRD_ID);
               IF V_NUMERO <= 0 THEN
                  V_ERROR := 1;
                  V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Código negativo o cero. ';
               END IF;
            END IF;
            
            -- Validaciones del nombre del producto
            
            IF D_DATOS.PRD_NOMBRE IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre nulo. ';
            ELSIF LENGTH(D_DATOS.PRD_NOMBRE) > 40 THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre con longitud mayor a 40 caracteres. ';
            ELSIF LENGTH(D_DATOS.PRD_NOMBRE) < 5 THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre con longitud menor a 5 caracteres. ';
            ELSIF VALIDA_NUMERO_ENTERO(D_DATOS.PRD_NOMBRE) = 'S' THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Nombre no debe incluir números. ';
            END IF;
            
            -- Inserción de datos en la tabla de producto
            
            IF V_ERROR = 0 THEN
               INSERT INTO PROYECTOABD_DW.DIM_PRODUCTO (PRD_ID, PRD_NOMBRE)
               VALUES (D_DATOS.PRD_ID, D_DATOS.PRD_NOMBRE);
            ELSE
               INSERT INTO PROYECTOABD_DW.ERROR_DIM_PRODUCTO (PRD_ID, PRD_NOMBRE, PRD_ERROR)
               VALUES (D_DATOS.PRD_ID, D_DATOS.PRD_NOMBRE, V_ERROR_MENSAJE);            
            END IF;
         EXCEPTION
            WHEN OTHERS THEN
               INSERT INTO PROYECTOABD_DW.ERROR_DIM_PRODUCTO (PRD_ID, PRD_NOMBRE, PRD_ERROR)
               VALUES (D_DATOS.PRD_ID, D_DATOS.PRD_NOMBRE, 'Error al insertar');
         END;
      END LOOP;
   END MigrarProducto;

   -- Migración de Envíos.
   
   PROCEDURE MigrarEnvio IS
      V_ERROR         INTEGER;
      V_NUMERO        INTEGER;
      V_ERROR_MENSAJE VARCHAR2(4000);
      CURSOR E_DATOS IS
         SELECT TPE.TPE_ID,
                TPE.TPE_DESCRIPCION
           FROM PROYECTOABD_SA.SA_TIPO_ENVIO TPE
          WHERE TPE.TPE_ID NOT IN (SELECT E.TPE_ID FROM PROYECTOABD_DW.DIM_TIPO_ENVIO E)
          ORDER BY TPE.TPE_ID;
   BEGIN
      FOR D_DATOS IN E_DATOS LOOP
         BEGIN
            V_ERROR := 0;
            V_ERROR_MENSAJE := '';

            -- Validaciones del ID del envío
            
            IF D_DATOS.TPE_ID IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID nulo. ';
            ELSIF VALIDA_NUMERO_ENTERO(D_DATOS.TPE_ID) = 'N' THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID no numérico. ';
            ELSE
               V_NUMERO := TO_NUMBER(D_DATOS.TPE_ID);
               IF V_NUMERO <= 0 THEN
                  V_ERROR := 1;
                  V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID negativo o cero. ';
               END IF;
            END IF;

            -- Validaciones de la descripción del envío
            
            IF D_DATOS.TPE_DESCRIPCION IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripción nula. ';
            ELSIF LENGTH(D_DATOS.TPE_DESCRIPCION) > 255 THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripción mayor a 255 caracteres. ';
            ELSIF LENGTH(D_DATOS.TPE_DESCRIPCION) < 4 THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripción menor a 5 caracteres. ';
            END IF;

            -- Inserción de datos en la tabla de tipo de envío
            
            IF V_ERROR = 0 THEN
               INSERT INTO PROYECTOABD_DW.DIM_TIPO_ENVIO (TPE_ID, TPE_DESCRIPCION)
               VALUES (D_DATOS.TPE_ID, D_DATOS.TPE_DESCRIPCION);
            ELSE
               INSERT INTO PROYECTOABD_DW.ERROR_DIM_TIPO_ENVIO (TPE_ID, TPE_DESCRIPCION, TPE_ERROR)
               VALUES (D_DATOS.TPE_ID, D_DATOS.TPE_DESCRIPCION, V_ERROR_MENSAJE);            
            END IF;
         EXCEPTION
            WHEN OTHERS THEN
               INSERT INTO PROYECTOABD_DW.ERROR_DIM_TIPO_ENVIO (TPE_ID, TPE_DESCRIPCION, TPE_ERROR)
               VALUES (D_DATOS.TPE_ID, D_DATOS.TPE_DESCRIPCION, 'Error al insertar');
         END;
      END LOOP;
   END MigrarEnvio;

   -- Migración de Tipo de Productos.
   
   PROCEDURE MigrarTipoProducto IS
      V_ERROR         INTEGER;
      V_NUMERO        INTEGER;
      V_ERROR_MENSAJE VARCHAR2(4000);
      CURSOR C_DATOS IS
         SELECT TPD.TPD_ID,
                TPD.TPD_DESCRIPCION
           FROM PROYECTOABD_SA.SA_TIPO_PRODUCTO TPD
          WHERE TPD.TPD_ID NOT IN (SELECT D.TPD_ID FROM PROYECTOABD_DW.DIM_TIPO_PRODUCTO D)
          ORDER BY TPD.TPD_ID;
   BEGIN
      FOR D_DATOS IN C_DATOS LOOP
         BEGIN
            V_ERROR := 0;
            V_ERROR_MENSAJE := '';
            
            -- Validaciones del código del tipo de producto
            
            IF D_DATOS.TPD_ID IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Código nulo. ';
            ELSIF VALIDA_NUMERO_ENTERO(D_DATOS.TPD_ID) = 'N' THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Código no numérico. ';
            ELSE
               V_NUMERO := TO_NUMBER(D_DATOS.TPD_ID);
               IF V_NUMERO <= 0 THEN
                  V_ERROR := 1;
                  V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Código negativo o cero. ';
               END IF;
            END IF;
            
            -- Validaciones de la descripción del tipo de producto
            
            IF D_DATOS.TPD_DESCRIPCION IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripción nula. ';
            ELSIF LENGTH(D_DATOS.TPD_DESCRIPCION) > 50 THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripción mayor a 50 caracteres. ';
            ELSIF LENGTH(D_DATOS.TPD_DESCRIPCION) < 5 THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Descripción menor a 5 caracteres. ';
            END IF;
            
            -- Inserción de datos en la tabla de tipo de producto
            
            IF V_ERROR = 0 THEN
               INSERT INTO PROYECTOABD_DW.DIM_TIPO_PRODUCTO (TPD_ID, TPD_DESCRIPCION)
               VALUES (D_DATOS.TPD_ID, D_DATOS.TPD_DESCRIPCION);
            ELSE
               INSERT INTO PROYECTOABD_DW.ERROR_DIM_TIPO_PRODUCTO (TPD_ID, TPD_DESCRIPCION, TPD_ERROR)
               VALUES (D_DATOS.TPD_ID, D_DATOS.TPD_DESCRIPCION, V_ERROR_MENSAJE);            
            END IF;
         EXCEPTION
            WHEN OTHERS THEN
               INSERT INTO PROYECTOABD_DW.ERROR_DIM_TIPO_PRODUCTO (TPD_ID, TPD_DESCRIPCION, TPD_ERROR)
               VALUES (D_DATOS.TPD_ID, D_DATOS.TPD_DESCRIPCION, 'Error al insertar');
         END;
      END LOOP;
   END MigrarTipoProducto;

--Migración de transacciones

  PROCEDURE MigrarTransaccion IS
   V_ERROR INTEGER;
   V_NUMERO INTEGER;
   V_ERROR_MENSAJE VARCHAR2(4000);
   CURSOR C_DATOS IS
   SELECT OCP.OCP_TPE_ID,
          OCP.OCP_PRD_ID,
          OCP.OCP_CTE_ID,
          TPD.TPD_ID,
          PRD.PRD_CANTIDAD,
          PRD.PRD_COSTO_UNITARIO,
          TPE.TPE_ESTADO,
          TPE.TPE_REQUIERE_CONFIRMACION
     FROM PROYECTOABD_SA.SA_ORDEN_COMPRA OCP
     JOIN PROYECTOABD_SA.SA_PRODUCTO PRD ON OCP.OCP_PRD_ID = PRD.PRD_ID
     JOIN PROYECTOABD_SA.SA_TIPO_PRODUCTO TPD ON PRD.PRD_TPD_ID = TPD.TPD_ID
     JOIN PROYECTOABD_SA.SA_TIPO_ENVIO TPE ON OCP.OCP_TPE_ID = TPE.TPE_ID
    WHERE (OCP.OCP_TPE_ID, OCP.OCP_PRD_ID, OCP.OCP_CTE_ID) NOT IN 
          (SELECT TRN_TPE_ID, TRN_PRD_ID, TRN_CTE_ID FROM PROYECTOABD_DW.FAC_TRANSACCION)
    ORDER BY OCP.OCP_TPE_ID, OCP.OCP_PRD_ID, OCP.OCP_CTE_ID;
BEGIN
   FOR D_DATOS IN C_DATOS LOOP
      BEGIN
         V_ERROR := 0;
         V_ERROR_MENSAJE := '';
         
         -- Validaciones del ID Tipo Envio
         
         IF D_DATOS.OCP_TPE_ID IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID nulo. ';
            ELSIF VALIDA_NUMERO_ENTERO(D_DATOS.OCP_TPE_ID) = 'N' THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID no numérico. ';
            ELSE
               V_NUMERO := TO_NUMBER(D_DATOS.OCP_TPE_ID);
               IF V_NUMERO <= 0 THEN
                  V_ERROR := 1;
                  V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID negativo o cero. ';
               END IF;
            END IF;
         
         -- Validaciones del ID Producto
         
         IF D_DATOS.OCP_PRD_ID IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID nulo. ';
            ELSIF VALIDA_NUMERO_ENTERO(D_DATOS.OCP_PRD_ID) = 'N' THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID no numérico. ';
            ELSE
               V_NUMERO := TO_NUMBER(D_DATOS.OCP_PRD_ID);
               IF V_NUMERO <= 0 THEN
                  V_ERROR := 1;
                  V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID negativo o cero. ';
               END IF;
            END IF;
            
         -- Validaciones del ID Cliente
         
         IF D_DATOS.OCP_CTE_ID IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID nulo. ';
            ELSIF VALIDA_NUMERO_ENTERO(D_DATOS.OCP_CTE_ID) = 'N' THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID no numérico. ';
            ELSE
               V_NUMERO := TO_NUMBER(D_DATOS.OCP_CTE_ID);
               IF V_NUMERO <= 0 THEN
                  V_ERROR := 1;
                  V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID negativo o cero. ';
               END IF;
            END IF;
            
            
        -- Validaciones del ID Tipo Producto
         
         IF D_DATOS.TPD_ID IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID nulo. ';
            ELSIF VALIDA_NUMERO_ENTERO(D_DATOS.TPD_ID) = 'N' THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID no numérico. ';
            ELSE
               V_NUMERO := TO_NUMBER(D_DATOS.TPD_ID);
               IF V_NUMERO <= 0 THEN
                  V_ERROR := 1;
                  V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'ID negativo o cero. ';
               END IF;
            END IF;

         -- Validaciones de la cantidad de la transacción
         
         IF D_DATOS.PRD_CANTIDAD IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Cantidad de producto no válida. ';
            ELSIF VALIDA_NUMERO_ENTERO(D_DATOS.PRD_CANTIDAD) = 'N' THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Cantidad no numérica. ';
            ELSE
               V_NUMERO := TO_NUMBER(D_DATOS.PRD_CANTIDAD);
               IF V_NUMERO <= 0 THEN
                  V_ERROR := 1;
                  V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Cantidad negativa o cero. ';
               END IF;
            END IF;
            
        -- Validaciones del costo unitario de la transacción
        
         IF D_DATOS.PRD_COSTO_UNITARIO IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Costo unitario no válido. ';
            ELSIF VALIDA_NUMERO_ENTERO(D_DATOS.PRD_COSTO_UNITARIO) = 'N' THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Costo unitario no numérico. ';
            ELSE
               V_NUMERO := TO_NUMBER(D_DATOS.PRD_COSTO_UNITARIO);
               IF V_NUMERO <= 0 THEN
                  V_ERROR := 1;
                  V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Cantidad negativa o cero. ';
               END IF;
            END IF;
         
        -- Validaciones del estado de la transacción
        
        IF D_DATOS.TPE_ESTADO IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Estado nulo. ';
            ELSIF VALIDA_NUMERO_ENTERO(D_DATOS.TPE_ESTADO) = 'N' THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Estado no numérico. ';
            ELSE
               V_NUMERO := TO_NUMBER(D_DATOS.TPE_ESTADO);
               IF V_NUMERO < 0 THEN
                  V_ERROR := 1;
                  V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'Estado negativo. ';
               END IF;
            END IF;

         -- Validaciones de la confirmación de la transacción
         
        IF D_DATOS.TPE_REQUIERE_CONFIRMACION IS NULL THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'RC nulo. ';
            ELSIF VALIDA_NUMERO_ENTERO(D_DATOS.TPE_REQUIERE_CONFIRMACION) = 'N' THEN
               V_ERROR := 1;
               V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'RC no numérico. ';
            ELSE
               V_NUMERO := TO_NUMBER(D_DATOS.TPE_REQUIERE_CONFIRMACION);
               IF V_NUMERO < 0 THEN
                  V_ERROR := 1;
                  V_ERROR_MENSAJE := V_ERROR_MENSAJE || 'RC negativo. ';
               END IF;
            END IF;

         -- Inserción en la tabla de hechos o en la tabla de errores
         IF V_ERROR = 0 THEN
            INSERT INTO PROYECTOABD_DW.FAC_TRANSACCION (TRN_TPE_ID, TRN_PRD_ID, TRN_CTE_ID, TRN_TPD_ID, 
                                                        TRN_PRD_CANTIDAD, TRN_PRD_COSTO_UNITARIO, 
                                                        TRN_TPE_ESTADO, TRN_TPE_REQUIERE_CONFIRMACION)
            VALUES (D_DATOS.OCP_TPE_ID, D_DATOS.OCP_PRD_ID, D_DATOS.OCP_CTE_ID, D_DATOS.TPD_ID, 
                    D_DATOS.PRD_CANTIDAD, D_DATOS.PRD_COSTO_UNITARIO, 
                    D_DATOS.TPE_ESTADO, D_DATOS.TPE_REQUIERE_CONFIRMACION);
         ELSE
            INSERT INTO PROYECTOABD_DW.ERROR_FAC_TRANSACCION (TRN_TPE_ID, TRN_PRD_ID, TRN_CTE_ID, TRN_TPD_ID, 
                                                        TRN_PRD_CANTIDAD, TRN_PRD_COSTO_UNITARIO, 
                                                        TRN_TPE_ESTADO, TRN_TPE_REQUIERE_CONFIRMACION, TRN_ERROR)
            VALUES (D_DATOS.OCP_TPE_ID, D_DATOS.OCP_PRD_ID, D_DATOS.OCP_CTE_ID, D_DATOS.TPD_ID, 
                    D_DATOS.PRD_CANTIDAD, D_DATOS.PRD_COSTO_UNITARIO, 
                    D_DATOS.TPE_ESTADO, D_DATOS.TPE_REQUIERE_CONFIRMACION, V_ERROR_MENSAJE);
         END IF;

      EXCEPTION
         WHEN OTHERS THEN
            INSERT INTO PROYECTOABD_DW.ERROR_FAC_TRANSACCION (TRN_TPE_ID, TRN_PRD_ID, TRN_CTE_ID, TRN_TPD_ID, 
                                                        TRN_PRD_CANTIDAD, TRN_PRD_COSTO_UNITARIO, 
                                                        TRN_TPE_ESTADO, TRN_TPE_REQUIERE_CONFIRMACION, TRN_ERROR)
            VALUES (D_DATOS.OCP_TPE_ID, D_DATOS.OCP_PRD_ID, D_DATOS.OCP_CTE_ID, D_DATOS.TPD_ID, 
                    D_DATOS.PRD_CANTIDAD, D_DATOS.PRD_COSTO_UNITARIO, 
                    D_DATOS.TPE_ESTADO, D_DATOS.TPE_REQUIERE_CONFIRMACION, 'Error al insertar');
      END;
   END LOOP;
END MigrarTransaccion;

PROCEDURE MigrarDatos IS
      BEGIN
         MigrarCliente;
         MigrarProducto;
         MigrarEnvio;
         MigrarTipoProducto;
         MigrarTransaccion;
      END;
END ETL_DW;
/

EXECUTE ETL_DW.MigrarDatos;

COMMIT;

SELECT *FROM PROYECTOABD_DW.DIM_TIPO_PRODUCTO ORDER BY TPD_ID;
SELECT *FROM PROYECTOABD_DW.DIM_TIPO_ENVIO ORDER BY TPE_ID;
SELECT *FROM PROYECTOABD_DW.DIM_CLIENTE ORDER BY CTE_ID;
SELECT *FROM PROYECTOABD_DW.DIM_PRODUCTO ORDER BY PRD_ID;
SELECT *FROM PROYECTOABD_DW.FAC_TRANSACCION ORDER BY TRN_TPE_ID;

SELECT * FROM PROYECTOABD_DW.ERROR_DIM_TIPO_PRODUCTO ORDER BY TPD_ID;
SELECT * FROM PROYECTOABD_DW.ERROR_DIM_TIPO_ENVIO ORDER BY TPE_ID;
SELECT * FROM PROYECTOABD_DW.ERROR_DIM_CLIENTE ORDER BY CTE_ID;
SELECT * FROM PROYECTOABD_DW.ERROR_DIM_PRODUCTO ORDER BY PRD_ID;
SELECT *FROM PROYECTOABD_DW.ERROR_FAC_TRANSACCION ORDER BY TPD_ID;