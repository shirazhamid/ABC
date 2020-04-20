--------------------------------------------------------
--  File created - Monday-April-20-2020   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure ETL_LOAD_CORE_MGR_ACJH
--------------------------------------------------------
set define off;

  CREATE OR REPLACE PROCEDURE "XGENMGR"."ETL_LOAD_CORE_MGR_ACJH" ( para_UserID INTEGER)AS

  var_ErrorLogID        INTEGER DEFAULT NULL;
  var_ProcessStatusID   INTEGER DEFAULT NULL;
  var_RowsToBeProcessed INTEGER :=0;
  var_Rowsprocessed     INTEGER :=0;
  var_ErrorCode         INTEGER DEFAULT NULL;
  var_ErrorMessage      VARCHAR(500) DEFAULT NULL;
  
BEGIN
/* -------------------------------------------------------------------------*
Procedure       : ETL_LOAD_CORE_MGR_ACJH
Author          : Harsha
Date Created    : 30/06/2017
Date Last Mod.  :
Description     : Load  ACJH infomation from CORE system to LOLC MGR.
Level           : Sub ETL

=============================================================================*/

/* Set Error log And process status Log ID from 
Squences */
/* Get Total number of Rows to be processed. Simply get count from 
-COUNT(*) CORE- tbl_lomc_lese_info */

SELECT  COUNT(*) INTO var_RowsToBeProcessed
FROM ACJH_STG;

/* write Process Status Log */

INSERT INTO TBL_CORE_PROCESS_STATUS_LOG
(
LOGID,
PROCESSDESC,
PHASE,
STARTTIME,
ROWSTOBEPROCESSED
)
SELECT 
SEQ_CORE_PROCESS_STATUS_LOG.NEXTVAL,
'Load TBL_CORE_ACJH Data from CORE system: (PROC - ETL_LOAD_CORE_MGR_ACJH)',
'TBL_CORE_ACJH',
SYSDATE,
var_RowsToBeProcessed 
FROM DUAL;

/* Get Current Value From Sequence INTO variable var_ProcessStatusID
Leter Reuse variable var_ProcessStatusID to update information written
in ProcessStatus_Log */

SELECT SEQ_CORE_PROCESS_STATUS_LOG.CURRVAL INTO var_ProcessStatusID
FROM DUAL;

-- -----------------------------------------------------------------*
-- INSERT LESE Information from CORE to Staging Table
-- -----------------------------------------------------------------*
--SET TRANSACTION READ ONLY;
EXECUTE IMMEDIATE 'TRUNCATE TABLE TBL_CORE_ACJH';

INSERT INTO TBL_CORE_ACJH
(
    ACJH_CLT_TYPES,
    ACJH_NO,
    ACJH_TYPE,
    ACJH_CONT_NO,
    ACJH_APP_DATE,
    ACJH_AMT,
    ACJH_DRCR,
    ACJH_DATE,
    ACJH_USR,
    ACJH_RCPT_DATE,
    DATE_CREATED
)
SELECT
  ACJH_CLT_TYPE,
    ACJH_NO,
    ACJH_TYPE,
    ACJH_CONT_NO,
    ACJH_APP_DATE,
    ACJH_AMT,
    ACJH_DRCR,
    ACJH_DATE,
    ACJH_USR,
    ACJH_RCPT_DATE,
    SYSDATE
FROM 
ACJH_STG;

--COMMIT;  -- end read-only transaction

/* Update Process Status Log- Reporting Successful Completion */
UPDATE  TBL_CORE_PROCESS_STATUS_LOG
SET     ROWSPROCESSE=var_RowsToBeProcessed,
        ENDTIM=SYSDATE,
        COMMENTS='ETL- Completed Successfully !'
WHERE   LOGID= var_ProcessStatusID;
COMMIT;


EXCEPTION -- EXCEPTION Occured -----------------------------------------------------------------------*
   -- GET Error Information --------------------------------------------------------------------------*
   WHEN OTHERS THEN
      var_ErrorCode := SQLCODE;
      var_ErrorMessage := SUBSTR(SQLERRM, 1, 500);
      
      ROLLBACK; 
  -- ------------------------------------------------------------------------------------------------*
      
      /* Add row to Process Status Log- Indcating Error occured */
    UPDATE      TBL_PROCESS_STATUS_LOG
          SET     ROWSPROCESSE=0,
                  ENDTIM=CURRENT_TIMESTAMP,
                  COMMENTS='#ETL- Completed With Errors, Please refer TBL_ERROR_LOG_DB for more details !'
          WHERE   LOGID= var_ProcessStatusID; 
          
      /* Log error Information in TBL_ERROR_LOG_DB */
      
                    INSERT INTO TBL_ERROR_LOG_DB
                    (
                    LOGID,
                    ITEMNAME,
                    ERROR_MESSAGE,
                    POSTDATETIME,
                    CREATEDBY
               
                    )
                   SELECT
                   SEQ_ERROR_LOG.NEXTVAL,
                   'Load Data From CORE to MGR- BANK Data',
                   var_ErrorMessage,
                   SYSDATE,
                    para_UserID 
                    FROM DUAL;
      
         COMMIT;
               
               RAISE; 
       
END ETL_LOAD_CORE_MGR_ACJH;
 

/
