-- Stored Procedure
CREATE OR REPLACE PROCEDURE KSFPA.ONLINE_UGAM_PVT.AGGREGATION("START_DT" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '


var START_DT = START_DT;



var v_TBL_NAME = ''AGGREGATION'';

//-------------------------------------------- AGGREGATE_BASE_TABLE------------------
var inst_AGGREGATE_BASE_TABLE =`
 MERGE INTO KSFPA.ONLINE_UGAM_PVT.AGGREGATE_BASE_TABLE TEMP USING
( 
SELECT DISTINCT EXTERNALORDERID,
                CUSTOMERORDERTYPE,
                COUNT(DISTINCT ORDERID) AS ORDERCOUNT
FROM
(SELECT 
cus.ORDDATE,
cus.ORDERDATE,
cus.EXTERNALORDERID, 
cus.CUSTOMERORDERTYPE,
cus.ORDERTOTAL,
cus.BILLSTATE,
st.ORDERID,
st.STR_ID,
st.SHIPMENTTYPE,
so.SKU,
so.ORIGINALQUANTITY
FROM KSFPA.OMS.CUSTOMERORDER_REALTIME cus
INNER JOIN KSFPA.OMS.STOREORDER_REALTIME st
ON (st.EXTERNALORDERID = cus.EXTERNALORDERID)
INNER JOIN KSFPA.OMS.STOREORDERITEMS_REALTIME so
ON (so.ORDERID = st.ORDERID)
WHERE TO_DATE(cus.ORDERDATE) BETWEEN Last_day(date(''`+START_DT+`'') -14,''week'')+1 AND Last_day(date(''`+START_DT+`'') -7,''week'')
AND st.STR_ID NOT IN (''9998'',''9999'',''9996'',''9997''))
WHERE LTRIM(RTRIM(SHIPMENTTYPE)) = ''Original''
GROUP BY 1,2) MERGETEMP ON 
MERGETEMP.EXTERNALORDERID=TEMP.EXTERNALORDERID
AND MERGETEMP.CUSTOMERORDERTYPE=TEMP.CUSTOMERORDERTYPE

WHEN NOT MATCHED THEN
INSERT
    (
        EXTERNALORDERID,
        CUSTOMERORDERTYPE,
        ORDERCOUNT
        
     )
VALUES
    (
        MERGETEMP.EXTERNALORDERID,
        MERGETEMP.CUSTOMERORDERTYPE,
        MERGETEMP.ORDERCOUNT
);
`;


//-------------------------------------------- AGGREGATED_AOV_ORDERS------------------
var inst_AGGREGATED_AOV_ORDERS =`
MERGE INTO KSFPA.ONLINE_UGAM_PVT.AGGREGATED_AOV_ORDERS TEMP USING
( 
SELECT ACCOUNTING_YEAR,
       ACCOUNTING_WEEK_NUMBER_IN_YEAR,
       FY_PW,
       CUSTOMERORDERTYPE,
       COUNT(DISTINCT(CASE WHEN ORDERCOUNT >1 THEN EXTERNALORDERID END)) AS AGGREGATED_ORDERS,
       SUM(CASE WHEN ORDERCOUNT >1 THEN SALES END)/COUNT(DISTINCT(CASE WHEN ORDERCOUNT >1 
       THEN EXTERNALORDERID END)) AS AGGREGATED_AOV,
       SUM(CASE WHEN ORDERCOUNT =1 THEN SALES END)/COUNT(DISTINCT(CASE WHEN ORDERCOUNT =1
       THEN EXTERNALORDERID END)) AS NON_AGGREGATED_AOV
FROM 
(SELECT DISTINCT ACCOUNTING_YEAR,
        ACCOUNTING_WEEK_NUMBER_IN_YEAR,
     CONCAT(''FY'',ACCOUNTING_YEAR,''P'',RIGHT(ACCOUNTING_PERIOD_NUMBER,2),''W'',ACCOUNTING_WEEK_NUMBER)                AS FY_PW,
     so.EXTERNALORDERID,
     cus.CUSTOMERORDERTYPE,
     so.STOREORDERTOTAL AS SALES, --incl GST
     -- CASE WHEN SHIPCOUNTRY = 'AU' THEN ((soi.ORIGINALQUANTITY * soi.UNITPRICE)/1.1) 
     --      WHEN SHIPCOUNTRY = 'NZ' THEN ((soi.ORIGINALQUANTITY * soi.UNITPRICE)/1.15) END AS SALES --Excl GST
     AT.ORDERCOUNT
FROM KSFPA.OMS.CUSTOMERORDER_REALTIME cus 
INNER JOIN KSFPA.OMS.STOREORDER_REALTIME so
ON (so.EXTERNALORDERID = cus.EXTERNALORDERID)
INNER JOIN KSF_SOPHIA_DATA_INTELLIGENCE_HUB_PROD.COMMON_DIMENSIONS.DIM_DATE DD
ON DD.DATE = cus.orddate 
INNER JOIN KSFPA.ONLINE_UGAM_PVT.AGGREGATE_BASE_TABLE AT
ON AT.EXTERNALORDERID = cus.EXTERNALORDERID
AND LTRIM(RTRIM(SHIPMENTTYPE)) = ''Original''
-- AND so.EXTERNALORDERID = 354984850
AND TO_DATE(cus.ORDERDATE) BETWEEN Last_day(date(''`+START_DT+`'') -14,''week'')+1 AND Last_day(date(''`+START_DT+`'') -7,''week'')
AND so.STR_ID NOT IN (''9998'',''9999'',''9996'',''9997'')
--GROUP BY 1,2,3,4,5
ORDER BY 1,2)
GROUP BY 1,2,3,4) MERGETEMP ON 
MERGETEMP.ACCOUNTING_YEAR=TEMP.ACCOUNTING_YEAR
AND MERGETEMP.ACCOUNTING_WEEK_NUMBER_IN_YEAR=TEMP.ACCOUNTING_WEEK_NUMBER_IN_YEAR
AND MERGETEMP.FY_PW=TEMP.FY_PW
AND MERGETEMP.CUSTOMERORDERTYPE=TEMP.CUSTOMERORDERTYPE

WHEN NOT MATCHED THEN
INSERT
    (
        ACCOUNTING_YEAR,
        ACCOUNTING_WEEK_NUMBER_IN_YEAR,
        FY_PW,
        CUSTOMERORDERTYPE,
        AGGREGATED_ORDERS,
        AGGREGATED_AOV,
        NON_AGGREGATED_AOV
     )
VALUES
    (
        MERGETEMP.ACCOUNTING_YEAR,
        MERGETEMP.ACCOUNTING_WEEK_NUMBER_IN_YEAR,
        MERGETEMP.FY_PW,
        MERGETEMP.CUSTOMERORDERTYPE,
        MERGETEMP.AGGREGATED_ORDERS,
        MERGETEMP.AGGREGATED_AOV,
        MERGETEMP.NON_AGGREGATED_AOV
);
`;

//---------------------------------------------------------------------------------------------------------------------------------------
      
var inst_rows_count = `SELECT "number of rows inserted" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))`;
var inst_rows_count_stmt = snowflake.createStatement({sqlText:inst_rows_count});
var upd_rows_count = `SELECT "number of rows updated" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))`;
var upd_rows_count_stmt = snowflake.createStatement({sqlText:upd_rows_count});
var trans_rows_count_val=0;

//---------------------------------------------------------------------------------------------------------------------------------------

try{

  var v_qry_result = "";
      
  var insert_log_query = `
  call KSFPA.ONLINE_UGAM_PVT.AGGREGATION_RUN_LOG_SP(''NULL'',''`+ v_TBL_NAME+`'',''INSERT'',''NULL'');
  `;
  var insert_log_query_stmt = snowflake.createStatement({sqlText:insert_log_query});
  insert_log_query_stmt.execute();

//---------------------------------------------------------------------------------------------------------------------------------------

  var update_process_log_query = `
  call KSFPA.ONLINE_UGAM_PVT.AGGREGATION_PROCESS_LOG_SP(''`+ v_TBL_NAME+`'',''0'',''0'',''Procedure Started'');
  `;
  var update_process_log_query_stmt = snowflake.createStatement({sqlText:update_process_log_query});
  update_process_log_query_stmt.execute();

//--------------------------------------------------------------------------------------------


 var update_process_log_query = `
  call KSFPA.ONLINE_UGAM_PVT.AGGREGATION_PROCESS_LOG_SP(''AGGREGATE_BASE_TABLE'',''0'',''0'',''AGGREGATE_BASE_TABLE - Started'');
  `;
  var update_process_log_query_stmt = snowflake.createStatement({sqlText:update_process_log_query});
  update_process_log_query_stmt.execute();
    
    var result_AGGREGATE_BASE_TABLE = snowflake.createStatement({sqlText: inst_AGGREGATE_BASE_TABLE}).execute();
//    return inst_AGGREGATE_BASE_TABLE
    aft_rows_count = inst_rows_count_stmt.execute();
    aft_rows_count.next();
    aft_rows_count_val=aft_rows_count.getColumnValue(1);
//     return aft_rows_count_val
    

    var update_process_log_query = `
  call KSFPA.ONLINE_UGAM_PVT.AGGREGATION_PROCESS_LOG_SP(''AGGREGATE_BASE_TABLE'',''`+aft_rows_count_val+`'',''`+trans_rows_count_val+`'',''AGGREGATE_BASE_TABLE - Completed'');
  `;
  var update_process_log_query_stmt = snowflake.createStatement({sqlText:update_process_log_query});
  update_process_log_query_stmt.execute();
      

//--------------------------------------------------------------------------------------------


 var update_process_log_query = `
  call KSFPA.ONLINE_UGAM_PVT.AGGREGATION_PROCESS_LOG_SP(''AGGREGATED_AOV_ORDERS'',''0'',''0'',''AGGREGATED_AOV_ORDERS - Started'');
  `;
  var update_process_log_query_stmt = snowflake.createStatement({sqlText:update_process_log_query});
  update_process_log_query_stmt.execute();
    
    var result_AGGREGATED_AOV_ORDERS = snowflake.createStatement({sqlText: inst_AGGREGATED_AOV_ORDERS}).execute();
//    return inst_AGGREGATED_AOV_ORDERS
    aft_rows_count = inst_rows_count_stmt.execute();
    aft_rows_count.next();
    aft_rows_count_val=aft_rows_count.getColumnValue(1);
//     return aft_rows_count_val
    

    var update_process_log_query = `
  call KSFPA.ONLINE_UGAM_PVT.AGGREGATION_PROCESS_LOG_SP(''AGGREGATED_AOV_ORDERS'',''`+aft_rows_count_val+`'',''`+trans_rows_count_val+`'',''AGGREGATED_AOV_ORDERS - Completed'');
  `;
  var update_process_log_query_stmt = snowflake.createStatement({sqlText:update_process_log_query});
  update_process_log_query_stmt.execute();
      

//--------------------------------------------------------------------------------------------
      
  var update_process_log_query = `
  call KSFPA.ONLINE_UGAM_PVT.AGGREGATION_PROCESS_LOG_SP(''`+ v_TBL_NAME+`'',''0'',''0'',''Procedure Completed'');
  `;
  var update_process_log_query_stmt = snowflake.createStatement({sqlText:update_process_log_query});
  update_process_log_query_stmt.execute();

//--------------------------------------------------------------------------------------------------------------------------------------

    commit_query_log = `commit;`;
    var update_log_query = `
    call KSFPA.ONLINE_UGAM_PVT.AGGREGATION_RUN_LOG_SP(''`+START_DT+`'',''`+ v_TBL_NAME+`'',''SUCCESS'',''SUCCESS'');
    `;
    var update_log_query_stmt = snowflake.createStatement({sqlText:update_log_query});
    update_log_query_stmt.execute();
      
      snowflake.createStatement({sqlText:`commit;`}).execute();

    // v_qry_result = "Inserted records count:"+aft_rows_count_val+" || Merge records updated:"+trans_rows_count_val+"\\n";
    v_qry_result = `COMPLETED`;
}

//---------------------------------------------------------------------------------------------------------------------------------------

catch(err){

    result =  "Failed: Code: " + err.code + " | State: " + err.state;
    result += "\\n  Message: " + err.message;
    result += "\\nStack Trace:\\n" + err.stackTraceTxt;

//---------------------Capturing the log to track the OverAll Procedure Execution--------------------------------------------------------------------

    var update_log_query = `
    call KSFPA.ONLINE_UGAM_PVT.AGGREGATION_RUN_LOG_SP(''`+START_DT+`'',''`+ v_TBL_NAME+`'',''FAILURE'',''`+result+`'');
    `;
    var update_log_query_stmt = snowflake.createStatement({sqlText:update_log_query});
    update_log_query_stmt.execute();


    snowflake.createStatement({sqlText:`commit;`}).execute();
    v_qry_result = err;
}
return v_qry_result;
';


-- View Query
create or replace view KSFPA.ONLINE_UGAM_PVT.AGGREGATED_VALUE(
	FY_PW,
	CUSTOMERORDERTYPE,
	YOY_AGGREGATED_VALUE
) as
SELECT FY_PW,
       CUSTOMERORDERTYPE,
       TY_AGGREGATED_VALUE - LY_AGGREGATED_VALUE AS YOY_AGGREGATED_VALUE
FROM
(SELECT AG1.FY_PW,
        DENSE_RANK() OVER(ORDER BY AG1.FY_PW DESC) AS FY_PW_RANK,
        AG1.CUSTOMERORDERTYPE,
        (AG1.AGGREGATED_AOV - AG1.NON_AGGREGATED_AOV)*AG1.AGGREGATED_ORDERS AS 
        TY_AGGREGATED_VALUE,
        (AG2.AGGREGATED_AOV - AG2.NON_AGGREGATED_AOV)*AG2.AGGREGATED_ORDERS AS                        LY_AGGREGATED_VALUE
FROM AGGREGATED_AOV_ORDERS AG1
INNER JOIN AGGREGATED_AOV_ORDERS AG2
ON AG1.ACCOUNTING_YEAR-1= AG2.ACCOUNTING_YEAR
AND AG1.ACCOUNTING_WEEK_NUMBER_IN_YEAR = AG2.ACCOUNTING_WEEK_NUMBER_IN_YEAR
AND AG1.CUSTOMERORDERTYPE = AG2.CUSTOMERORDERTYPE)
WHERE FY_PW_RANK=1 ;
