--Grupo #3
--Erick Hernández Ugalde    100%
--Caleb Leon Azofeifa       100%
--Daniel Madrigal Mendez    100%

SELECT * FROM SA_PRODUCTO;

--******COMPILAR CON PROYECTOABD_DW***************!!!!!!!!!!!!!!!!!!!------------

CREATE OR REPLACE PACKAGE PROYECTOABD_DW.ETL_SA AS
   PROCEDURE CARGA_CLIENTE;
   PROCEDURE CARGA_ORDEN_COMPRA;
   PROCEDURE CARGA_PRODUCTO;
   PROCEDURE CARGA_TIPO_ENVIO;
   PROCEDURE CARGA_TIPO_PRODUCTO;
   PROCEDURE INICIA_PROCESO;
END ETL_SA;
/

CREATE OR REPLACE PACKAGE BODY ETL_SA AS

   PROCEDURE CARGA_CLIENTE AS
      CURSOR C_CLIENTE IS
             SELECT CTE.CTE_ID,
                    CTE.CTE_NOMBRE
               FROM PROYECTOABD_ER.CLIENTE CTE
              WHERE NOT EXISTS (SELECT 'X' FROM PROYECTOABD_SA.SA_CLIENTE CSA 
              WHERE CSA.CTE_ID = CTE.CTE_ID)
              ORDER BY CTE.CTE_ID;
   BEGIN
      FOR D_CLIENTE IN C_CLIENTE LOOP
          INSERT
            INTO PROYECTOABD_SA.SA_CLIENTE(CTE_ID, CTE_NOMBRE)
            VALUES(D_CLIENTE.CTE_ID, D_CLIENTE.CTE_NOMBRE);
      END LOOP;
      COMMIT;
   END;
   
--------------------------------------------------------------------------------

   PROCEDURE CARGA_ORDEN_COMPRA AS
      CURSOR C_ORDEN_COMPRA IS
             SELECT OCP.OCP_ID,
                    OCP.OCP_PRD_ID,
                    OCP.OCP_CTE_ID,
                    OCP.OCP_TPE_ID
               FROM PROYECTOABD_ER.ORDEN_COMPRA OCP
              WHERE NOT EXISTS (SELECT 'X' FROM PROYECTOABD_SA.SA_ORDEN_COMPRA 
              OSA WHERE OSA.OCP_ID = OCP.OCP_ID)
              ORDER BY OCP.OCP_ID;
   BEGIN
      FOR D_ORDEN_COMPRA IN C_ORDEN_COMPRA LOOP
          INSERT
            INTO PROYECTOABD_SA.SA_ORDEN_COMPRA(OCP_ID, OCP_PRD_ID, 
            OCP_CTE_ID, OCP_TPE_ID)
            VALUES(D_ORDEN_COMPRA.OCP_ID, D_ORDEN_COMPRA.OCP_PRD_ID, 
            D_ORDEN_COMPRA.OCP_CTE_ID, D_ORDEN_COMPRA.OCP_TPE_ID);
      END LOOP;
      COMMIT;
   END;
   
--------------------------------------------------------------------------------

    PROCEDURE CARGA_PRODUCTO AS
      CURSOR C_PRODUCTO IS
             SELECT PRD.PRD_ID,
                    PRD.PRD_TPD_ID,
                    PRD.PRD_NOMBRE,
                    PRD.PRD_CANTIDAD,
                    PRD.PRD_COSTO_UNITARIO
               FROM PROYECTOABD_ER.PRODUCTO PRD
              WHERE NOT EXISTS (SELECT 'X' FROM PROYECTOABD_SA.SA_PRODUCTO PSA 
              WHERE PSA.PRD_ID = PRD.PRD_ID)
              ORDER BY PRD.PRD_ID;
   BEGIN
      FOR D_PRODUCTO IN C_PRODUCTO LOOP
          INSERT
            INTO PROYECTOABD_SA.SA_PRODUCTO(PRD_ID, PRD_TPD_ID, PRD_NOMBRE, 
            PRD_CANTIDAD, PRD_COSTO_UNITARIO)
            VALUES(D_PRODUCTO.PRD_ID, D_PRODUCTO.PRD_TPD_ID, 
            D_PRODUCTO.PRD_NOMBRE, D_PRODUCTO.PRD_CANTIDAD, 
            D_PRODUCTO.PRD_COSTO_UNITARIO);
      END LOOP;
      COMMIT;
   END;
   
--------------------------------------------------------------------------------

    PROCEDURE CARGA_TIPO_ENVIO AS
      CURSOR C_TENVIO IS
             SELECT TPE.TPE_ID,
                    TPE.TPE_DESCRIPCION,
                    TPE.TPE_ESTADO,
                    TPE.TPE_REQUIERE_CONFIRMACION
               FROM PROYECTOABD_ER.TIPO_ENVIO TPE
              WHERE NOT EXISTS (SELECT 'X' FROM PROYECTOABD_SA.SA_TIPO_ENVIO TSA 
              WHERE TSA.TPE_ID = TPE.TPE_ID)
              ORDER BY TPE.TPE_ID;
   BEGIN
      FOR D_TENVIO IN C_TENVIO LOOP
          INSERT
            INTO PROYECTOABD_SA.SA_TIPO_ENVIO(TPE_ID, TPE_DESCRIPCION, 
            TPE_ESTADO, TPE_REQUIERE_CONFIRMACION)
            VALUES(D_TENVIO.TPE_ID, D_TENVIO.TPE_DESCRIPCION, 
            D_TENVIO.TPE_ESTADO, D_TENVIO.TPE_REQUIERE_CONFIRMACION);
      END LOOP;
      COMMIT;
   END;
   
--------------------------------------------------------------------------------

    PROCEDURE CARGA_TIPO_PRODUCTO AS
      CURSOR C_TPRODUCTO IS
             SELECT TPD.TPD_ID,
                    TPD.TPD_DESCRIPCION
               FROM PROYECTOABD_ER.TIPO_PRODUCTO TPD
              WHERE NOT EXISTS (SELECT 'X' FROM PROYECTOABD_SA.SA_TIPO_PRODUCTO 
              DSA 
              WHERE DSA.TPD_ID = TPD.TPD_ID)
              ORDER BY TPD.TPD_ID;
   BEGIN
      FOR D_TPRODUCTO IN C_TPRODUCTO LOOP
          INSERT
            INTO PROYECTOABD_SA.SA_TIPO_PRODUCTO(TPD_ID, TPD_DESCRIPCION)
            VALUES(D_TPRODUCTO.TPD_ID, D_TPRODUCTO.TPD_DESCRIPCION);
      END LOOP;
      COMMIT;
   END;
   
--------------------------------------------------------------------------------   

   PROCEDURE INICIA_PROCESO AS
   BEGIN
      CARGA_CLIENTE;
      CARGA_ORDEN_COMPRA;
      CARGA_PRODUCTO;
      CARGA_TIPO_ENVIO;
      CARGA_TIPO_PRODUCTO;
   END;

END;
/

EXECUTE ETL_SA.INICIA_PROCESO;