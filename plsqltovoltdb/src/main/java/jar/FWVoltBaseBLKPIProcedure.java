package com.openet.bl.procedures;

import org.voltdb.*;
import org.voltdb.types.TimestampType;
import java.util.Locale;
import java.sql.Timestamp;
import java.util.Arrays;
import java.math.BigDecimal;


public class FWVoltBaseBLKPIProcedure extends FWVoltBaseProcedure {

    // Payment type constant values.
    public final String C_PT_BOTH     = "BOTH";
    public final String C_PT_PR       = "PR";
    public final String C_PT_SL       = "SL";
    public final String C_PT_CPMSL    = "CPMSL";
    public final String C_PT_CPMPR    = "CPMPR";
    public final String C_PT_CRISMS   = "CRISMS";
    public final String C_PT_CRIMMS   = "CRIMMS";
    public final String C_PT_CRICPMIP = "CRICPMIP";
    String[] paymentTypes = { C_PT_BOTH, C_PT_PR, C_PT_SL, C_PT_CPMSL, C_PT_CPMPR, C_PT_CRISMS, C_PT_CRIMMS, C_PT_CRICPMIP };

    // Insert BL_KPI_DATA.
    public final SQLStmt insertBlKpiDataStmt = new SQLStmt(
        "INSERT INTO BL_KPI_DATA(KPI_TIME, KPI, SUB_CATEGORY1, SUB_CATEGORY2, VALUE) VALUES (?, ?, ?, ?, ?)"
    );

    // Prepaid.
    public final SQLStmt fetchPRCCNResultCodes = new SQLStmt(
          "SELECT CCN_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_PR_SMS_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +  "GROUP BY CCN_RESULT_CODE"
    );

    public final SQLStmt fetchPRGBGErrors = new SQLStmt(
          "SELECT GBG_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_PR_SMS_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND GBG_RESULT_CODE NOT IN (2001,4241,4242,4243,4244,4010,4012) "
        +  "GROUP BY GBG_RESULT_CODE"
    );

    public final SQLStmt fetchPRRequestsPerOMG = new SQLStmt(
          "SELECT ORIGIN_HOST, COUNT(*) COUNTER "
        +   "FROM BL_PR_SMS_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +  "GROUP BY ORIGIN_HOST"
    );

    public final SQLStmt fetchPRCCNRequestsPerTM = new SQLStmt(
          "SELECT TM_NAME, COUNT(*) COUNTER "
        +   "FROM BL_PR_SMS_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND CCN_LATENCY > 0.01 "
        +  "GROUP BY TM_NAME"
    );

    public final SQLStmt fetchPRSuccFailRefunds = new SQLStmt(
          "SELECT CCN_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_PR_SMS_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND REQ_TYPE = 'RR' "
        +  "GROUP BY CCN_RESULT_CODE"
    );

    public final SQLStmt fetchPRLatencies = new SQLStmt(
          "SELECT CAST(CASE WHEN a.VALUE1  IS NULL THEN 0 ELSE a.VALUE1  END AS DECIMAL) VALUE1, "
        +        "CAST(CASE WHEN a.VALUE2  IS NULL THEN 0 ELSE a.VALUE2  END AS DECIMAL) VALUE2, "
        +        "CAST(CASE WHEN a.VALUE3  IS NULL THEN 0 ELSE a.VALUE3  END AS DECIMAL) VALUE3, "
        +        "CAST(CASE WHEN a.VALUE4  IS NULL THEN 0 ELSE a.VALUE4  END AS DECIMAL) VALUE4, "
        +        "CAST(CASE WHEN a.VALUE5  IS NULL THEN 0 ELSE a.VALUE5  END AS DECIMAL) VALUE5, "
        +        "CAST(CASE WHEN a.VALUE6  IS NULL THEN 0 ELSE a.VALUE6  END AS DECIMAL) VALUE6, "
        +        "CAST(CASE WHEN a.VALUE7  IS NULL THEN 0 ELSE a.VALUE7  END AS DECIMAL) VALUE7, "
        +        "CAST(CASE WHEN a.VALUE8  IS NULL THEN 0 ELSE a.VALUE8  END AS DECIMAL) VALUE8, "
        +        "CAST(CASE WHEN a.VALUE9  IS NULL THEN 0 ELSE a.VALUE9  END AS DECIMAL) VALUE9, "
        +        "CAST(CASE WHEN a.VALUE10 IS NULL THEN 0 ELSE a.VALUE10 END AS DECIMAL) VALUE10, "
        +        "CAST(CASE WHEN a.VALUE11 IS NULL THEN 0 ELSE a.VALUE11 END AS DECIMAL) VALUE11, "
        +        "CAST(CASE WHEN a.VALUE12 IS NULL THEN 0 ELSE a.VALUE12 END AS DECIMAL) VALUE12 "
        +   "FROM ( "
        +         "SELECT MAX(AC_LATENCY) VALUE1, MAX(TOTAL_LATENCY) VALUE2, MAX(CCN_LATENCY) VALUE3, "
        +                "AVG(AC_LATENCY) VALUE4, AVG(TOTAL_LATENCY) VALUE5, AVG(CCN_LATENCY) VALUE6, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 20  THEN AC_LATENCY ELSE 20  END, CASE WHEN AC_LATENCY < 9999      THEN AC_LATENCY ELSE 9999      END, 1, 0) END) VALUE7, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 6   THEN AC_LATENCY ELSE 6   END, CASE WHEN AC_LATENCY < 19.999999 THEN AC_LATENCY ELSE 19.999999 END, 1, 0) END) VALUE8, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 4   THEN AC_LATENCY ELSE 4   END, CASE WHEN AC_LATENCY < 5.999999  THEN AC_LATENCY ELSE 5.999999  END, 1, 0) END) VALUE9, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 2   THEN AC_LATENCY ELSE 2   END, CASE WHEN AC_LATENCY < 3.999999  THEN AC_LATENCY ELSE 3.999999  END, 1, 0) END) VALUE10, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 0.1 THEN AC_LATENCY ELSE 0.1 END, CASE WHEN AC_LATENCY < 1.999999  THEN AC_LATENCY ELSE 1.999999  END, 1, 0) END) VALUE11, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 0   THEN AC_LATENCY ELSE 0   END, CASE WHEN AC_LATENCY < 0.099999  THEN AC_LATENCY ELSE 0.099999  END, 1, 0) END) VALUE12 "
        +           "FROM BL_PR_SMS_TRAFFIC "
        +          "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +            "AND INSERT_DATE < TO_TIMESTAMP(SECOND, ?) "
        +        ") a"
    );

    public final SQLStmt fetchPRLateBilled = new SQLStmt(
          "SELECT CCN_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_PR_SMS_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND TIME_MODE = 'OFFLINE' "
        +  "GROUP BY CCN_RESULT_CODE"
    );

    // SmartLimits.
    public final SQLStmt fetchSLCCNResultCodes = new SQLStmt(
          "SELECT CCN_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_SL_SMS_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +  "GROUP BY CCN_RESULT_CODE"
    );

    public final SQLStmt fetchSLGBGErrors = new SQLStmt(
          "SELECT GBG_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_SL_SMS_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND GBG_RESULT_CODE NOT IN (2001,4241,4242,4243,4244,4010,4012) "
        +  "GROUP BY GBG_RESULT_CODE"
    );

    public final SQLStmt fetchSLRequestsPerOMG = new SQLStmt(
          "SELECT ORIGIN_HOST, COUNT(*) COUNTER "
        +   "FROM BL_SL_SMS_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +  "GROUP BY ORIGIN_HOST"
    );

    public final SQLStmt fetchSLCCNRequestsPerTM = new SQLStmt(
          "SELECT TM_NAME, COUNT(*) COUNTER "
        +   "FROM BL_SL_SMS_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND CCN_LATENCY > 0.01 "
        +  "GROUP BY TM_NAME"
    );

    public final SQLStmt fetchSLSuccFailRefunds = new SQLStmt(
          "SELECT CCN_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_SL_SMS_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND REQ_TYPE = 'RR' "
        +  "GROUP BY CCN_RESULT_CODE"
    );

    public final SQLStmt fetchSLLatencies = new SQLStmt(
          "SELECT CAST(CASE WHEN a.VALUE1  IS NULL THEN 0 ELSE a.VALUE1  END AS DECIMAL) VALUE1, "
        +        "CAST(CASE WHEN a.VALUE2  IS NULL THEN 0 ELSE a.VALUE2  END AS DECIMAL) VALUE2, "
        +        "CAST(CASE WHEN a.VALUE3  IS NULL THEN 0 ELSE a.VALUE3  END AS DECIMAL) VALUE3, "
        +        "CAST(CASE WHEN a.VALUE4  IS NULL THEN 0 ELSE a.VALUE4  END AS DECIMAL) VALUE4, "
        +        "CAST(CASE WHEN a.VALUE5  IS NULL THEN 0 ELSE a.VALUE5  END AS DECIMAL) VALUE5, "
        +        "CAST(CASE WHEN a.VALUE6  IS NULL THEN 0 ELSE a.VALUE6  END AS DECIMAL) VALUE6, "
        +        "CAST(CASE WHEN a.VALUE7  IS NULL THEN 0 ELSE a.VALUE7  END AS DECIMAL) VALUE7, "
        +        "CAST(CASE WHEN a.VALUE8  IS NULL THEN 0 ELSE a.VALUE8  END AS DECIMAL) VALUE8, "
        +        "CAST(CASE WHEN a.VALUE9  IS NULL THEN 0 ELSE a.VALUE9  END AS DECIMAL) VALUE9, "
        +        "CAST(CASE WHEN a.VALUE10 IS NULL THEN 0 ELSE a.VALUE10 END AS DECIMAL) VALUE10, "
        +        "CAST(CASE WHEN a.VALUE11 IS NULL THEN 0 ELSE a.VALUE11 END AS DECIMAL) VALUE11, "
        +        "CAST(CASE WHEN a.VALUE12 IS NULL THEN 0 ELSE a.VALUE12 END AS DECIMAL) VALUE12 "
        +   "FROM ( "
        +         "SELECT MAX(AC_LATENCY) VALUE1, MAX(TOTAL_LATENCY) VALUE2, MAX(CCN_LATENCY) VALUE3, "
        +                "AVG(AC_LATENCY) VALUE4, AVG(TOTAL_LATENCY) VALUE5, AVG(CCN_LATENCY) VALUE6, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 20  THEN AC_LATENCY ELSE 20  END, CASE WHEN AC_LATENCY < 9999      THEN AC_LATENCY ELSE 9999      END, 1, 0) END) VALUE7, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 6   THEN AC_LATENCY ELSE 6   END, CASE WHEN AC_LATENCY < 19.999999 THEN AC_LATENCY ELSE 19.999999 END, 1, 0) END) VALUE8, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 4   THEN AC_LATENCY ELSE 4   END, CASE WHEN AC_LATENCY < 5.999999  THEN AC_LATENCY ELSE 5.999999  END, 1, 0) END) VALUE9, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 2   THEN AC_LATENCY ELSE 2   END, CASE WHEN AC_LATENCY < 3.999999  THEN AC_LATENCY ELSE 3.999999  END, 1, 0) END) VALUE10, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 0.1 THEN AC_LATENCY ELSE 0.1 END, CASE WHEN AC_LATENCY < 1.999999  THEN AC_LATENCY ELSE 1.999999  END, 1, 0) END) VALUE11, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 0   THEN AC_LATENCY ELSE 0   END, CASE WHEN AC_LATENCY < 0.099999  THEN AC_LATENCY ELSE 0.099999  END, 1, 0) END) VALUE12 "
        +           "FROM BL_SL_SMS_TRAFFIC "
        +          "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +            "AND INSERT_DATE < TO_TIMESTAMP(SECOND, ?) "
        +        ") a"
    );

    public final SQLStmt fetchSLLateBilled = new SQLStmt(
          "SELECT CCN_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_SL_SMS_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND TIME_MODE = 'OFFLINE' "
        +  "GROUP BY CCN_RESULT_CODE"
    );

    // Prepaid CPM.
    public final SQLStmt fetchPRCPMCCNResultCodes = new SQLStmt(
          "SELECT CCN_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_PR_CPM_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +  "GROUP BY CCN_RESULT_CODE"
    );

    public final SQLStmt fetchPRCPMErrors = new SQLStmt(
          "SELECT CPM_RESULT_CODE, COUNT(*) "
        +   "FROM BL_PR_CPM_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND CPM_RESULT_CODE NOT IN (2001,4241,4242,4243,4244,4010,4012) "
        +  "GROUP BY CPM_RESULT_CODE"
    );

    public final SQLStmt fetchPRCPMRequestsPerCPM = new SQLStmt(
          "SELECT ORIGIN_HOST, RETRANSMIT_FLAG, COUNT(*) COUNTER "
        +   "FROM BL_PR_CPM_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +  "GROUP BY ORIGIN_HOST, RETRANSMIT_FLAG"
    );

    public final SQLStmt fetchPRCPMCCNRequestsPerTM = new SQLStmt(
          "SELECT TM_NAME, COUNT(*) COUNTER "
        +   "FROM BL_PR_CPM_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND CCN_LATENCY > 0.005 "
        +  "GROUP BY TM_NAME"
    );

    public final SQLStmt fetchPRCPMSuccFailRefunds = new SQLStmt(
          "SELECT CCN_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_PR_CPM_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND REQ_TYPE = 'RR' "
        +  "GROUP BY CCN_RESULT_CODE"
    );

    public final SQLStmt fetchPRCPMLatencies = new SQLStmt(
          "SELECT CAST(CASE WHEN a.VALUE1  IS NULL THEN 0 ELSE a.VALUE1  END AS DECIMAL) VALUE1, "
        +        "CAST(CASE WHEN a.VALUE2  IS NULL THEN 0 ELSE a.VALUE2  END AS DECIMAL) VALUE2, "
        +        "CAST(CASE WHEN a.VALUE3  IS NULL THEN 0 ELSE a.VALUE3  END AS DECIMAL) VALUE3, "
        +        "CAST(CASE WHEN a.VALUE4  IS NULL THEN 0 ELSE a.VALUE4  END AS DECIMAL) VALUE4, "
        +        "CAST(CASE WHEN a.VALUE5  IS NULL THEN 0 ELSE a.VALUE5  END AS DECIMAL) VALUE5, "
        +        "CAST(CASE WHEN a.VALUE6  IS NULL THEN 0 ELSE a.VALUE6  END AS DECIMAL) VALUE6, "
        +        "CAST(CASE WHEN a.VALUE7  IS NULL THEN 0 ELSE a.VALUE7  END AS DECIMAL) VALUE7, "
        +        "CAST(CASE WHEN a.VALUE8  IS NULL THEN 0 ELSE a.VALUE8  END AS DECIMAL) VALUE8, "
        +        "CAST(CASE WHEN a.VALUE9  IS NULL THEN 0 ELSE a.VALUE9  END AS DECIMAL) VALUE9, "
        +        "CAST(CASE WHEN a.VALUE10 IS NULL THEN 0 ELSE a.VALUE10 END AS DECIMAL) VALUE10, "
        +        "CAST(CASE WHEN a.VALUE11 IS NULL THEN 0 ELSE a.VALUE11 END AS DECIMAL) VALUE11, "
        +        "CAST(CASE WHEN a.VALUE12 IS NULL THEN 0 ELSE a.VALUE12 END AS DECIMAL) VALUE12 "
        +   "FROM ( "
        +         "SELECT MAX(AC_LATENCY) VALUE1, MAX(TOTAL_LATENCY) VALUE2, MAX(CCN_LATENCY) VALUE3, "
        +                "AVG(AC_LATENCY) VALUE4, AVG(TOTAL_LATENCY) VALUE5, AVG(CCN_LATENCY) VALUE6, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 20  THEN AC_LATENCY ELSE 20  END, CASE WHEN AC_LATENCY < 9999      THEN AC_LATENCY ELSE 9999      END, 1, 0) END) VALUE7, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 6   THEN AC_LATENCY ELSE 6   END, CASE WHEN AC_LATENCY < 19.999999 THEN AC_LATENCY ELSE 19.999999 END, 1, 0) END) VALUE8, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 4   THEN AC_LATENCY ELSE 4   END, CASE WHEN AC_LATENCY < 5.999999  THEN AC_LATENCY ELSE 5.999999  END, 1, 0) END) VALUE9, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 2   THEN AC_LATENCY ELSE 2   END, CASE WHEN AC_LATENCY < 3.999999  THEN AC_LATENCY ELSE 3.999999  END, 1, 0) END) VALUE10, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 0.1 THEN AC_LATENCY ELSE 0.1 END, CASE WHEN AC_LATENCY < 1.999999  THEN AC_LATENCY ELSE 1.999999  END, 1, 0) END) VALUE11, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 0   THEN AC_LATENCY ELSE 0   END, CASE WHEN AC_LATENCY < 0.099999  THEN AC_LATENCY ELSE 0.099999  END, 1, 0) END) VALUE12 "
        +           "FROM BL_PR_CPM_TRAFFIC "
        +          "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +            "AND INSERT_DATE < TO_TIMESTAMP(SECOND, ?) "
        +        ") a"
    );

    // SmartLimits CPM.
    public final SQLStmt fetchSLCPMCCNResultCodes = new SQLStmt(
          "SELECT CCN_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_SL_CPM_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +  "GROUP BY CCN_RESULT_CODE"
    );

    public final SQLStmt fetchSLCPMErrors = new SQLStmt(
          "SELECT CPM_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_SL_CPM_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND CPM_RESULT_CODE NOT IN (2001,4241,4242,4243,4244,4010,4012) "
        +  "GROUP BY CPM_RESULT_CODE"
    );

    public final SQLStmt fetchSLCPMRequestsPerCPM = new SQLStmt(
          "SELECT ORIGIN_HOST, RETRANSMIT_FLAG, COUNT(*) COUNTER "
        +   "FROM BL_SL_CPM_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +  "GROUP BY ORIGIN_HOST, RETRANSMIT_FLAG"
    );

    public final SQLStmt fetchSLCPMCCNRequestsPerTM = new SQLStmt(
          "SELECT TM_NAME, COUNT(*) COUNTER "
        +   "FROM BL_SL_CPM_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND CCN_LATENCY > 0.005 "
        +  "GROUP BY TM_NAME"
    );

    public final SQLStmt fetchSLCPMLatencies = new SQLStmt(
          "SELECT CAST(CASE WHEN a.VALUE1  IS NULL THEN 0 ELSE a.VALUE1  END AS DECIMAL) VALUE1, "
        +        "CAST(CASE WHEN a.VALUE2  IS NULL THEN 0 ELSE a.VALUE2  END AS DECIMAL) VALUE2, "
        +        "CAST(CASE WHEN a.VALUE3  IS NULL THEN 0 ELSE a.VALUE3  END AS DECIMAL) VALUE3, "
        +        "CAST(CASE WHEN a.VALUE4  IS NULL THEN 0 ELSE a.VALUE4  END AS DECIMAL) VALUE4, "
        +        "CAST(CASE WHEN a.VALUE5  IS NULL THEN 0 ELSE a.VALUE5  END AS DECIMAL) VALUE5, "
        +        "CAST(CASE WHEN a.VALUE6  IS NULL THEN 0 ELSE a.VALUE6  END AS DECIMAL) VALUE6, "
        +        "CAST(CASE WHEN a.VALUE7  IS NULL THEN 0 ELSE a.VALUE7  END AS DECIMAL) VALUE7, "
        +        "CAST(CASE WHEN a.VALUE8  IS NULL THEN 0 ELSE a.VALUE8  END AS DECIMAL) VALUE8, "
        +        "CAST(CASE WHEN a.VALUE9  IS NULL THEN 0 ELSE a.VALUE9  END AS DECIMAL) VALUE9, "
        +        "CAST(CASE WHEN a.VALUE10 IS NULL THEN 0 ELSE a.VALUE10 END AS DECIMAL) VALUE10, "
        +        "CAST(CASE WHEN a.VALUE11 IS NULL THEN 0 ELSE a.VALUE11 END AS DECIMAL) VALUE11, "
        +        "CAST(CASE WHEN a.VALUE12 IS NULL THEN 0 ELSE a.VALUE12 END AS DECIMAL) VALUE12 "
        +   "FROM ( "
        +         "SELECT MAX(AC_LATENCY) VALUE1, MAX(TOTAL_LATENCY) VALUE2, MAX(CCN_LATENCY) VALUE3, "
        +                "AVG(AC_LATENCY) VALUE4, AVG(TOTAL_LATENCY) VALUE5, AVG(CCN_LATENCY) VALUE6, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 20  THEN AC_LATENCY ELSE 20  END, CASE WHEN AC_LATENCY < 9999      THEN AC_LATENCY ELSE 9999      END, 1, 0) END) VALUE7, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 6   THEN AC_LATENCY ELSE 6   END, CASE WHEN AC_LATENCY < 19.999999 THEN AC_LATENCY ELSE 19.999999 END, 1, 0) END) VALUE8, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 4   THEN AC_LATENCY ELSE 4   END, CASE WHEN AC_LATENCY < 5.999999  THEN AC_LATENCY ELSE 5.999999  END, 1, 0) END) VALUE9, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 2   THEN AC_LATENCY ELSE 2   END, CASE WHEN AC_LATENCY < 3.999999  THEN AC_LATENCY ELSE 3.999999  END, 1, 0) END) VALUE10, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 0.1 THEN AC_LATENCY ELSE 0.1 END, CASE WHEN AC_LATENCY < 1.999999  THEN AC_LATENCY ELSE 1.999999  END, 1, 0) END) VALUE11, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 0   THEN AC_LATENCY ELSE 0   END, CASE WHEN AC_LATENCY < 0.099999  THEN AC_LATENCY ELSE 0.099999  END, 1, 0) END) VALUE12 "
        +           "FROM BL_SL_CPM_TRAFFIC "
        +          "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +            "AND INSERT_DATE < TO_TIMESTAMP(SECOND, ?) "
        +        ") a"
    );

    // Prepaid Cricket SMS.
    public final SQLStmt fetchPRCRISMSOCSResultCodes = new SQLStmt(
          "SELECT OCS_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_PR_CRICKET_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND TM_NAME LIKE '%FDA%' "
        +  "GROUP BY OCS_RESULT_CODE"
    );

    public final SQLStmt fetchPRCRISMSUMRErrors = new SQLStmt(
          "SELECT CRICKET_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_PR_CRICKET_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND CRICKET_RESULT_CODE NOT IN (2001, 4241, 4245) "
        +    "AND TM_NAME LIKE '%FDA%' "
        +  "GROUP BY CRICKET_RESULT_CODE"
    );

    public final SQLStmt fetchPRCRISMSRequestsPerUMR = new SQLStmt(
          "SELECT ORIGIN_HOST, RETRANSMIT_FLAG, COUNT(*) COUNTER "
        +   "FROM BL_PR_CRICKET_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND TM_NAME LIKE '%FDA%' "
        +  "GROUP BY ORIGIN_HOST, RETRANSMIT_FLAG"
    );

    public final SQLStmt fetchPRCRISMSOCSRequestsPerTM = new SQLStmt(
          "SELECT TM_NAME, COUNT(*) COUNTER "
        +   "FROM BL_PR_CRICKET_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND OCS_LATENCY > 0 "
        +    "AND TM_NAME LIKE '%FDA%' "
        +  "GROUP BY TM_NAME"
    );

    public final SQLStmt fetchPRCRISMSLatencies = new SQLStmt(
          "SELECT CAST(CASE WHEN a.VALUE1  IS NULL THEN 0 ELSE a.VALUE1  END AS DECIMAL) VALUE1, "
        +        "CAST(CASE WHEN a.VALUE2  IS NULL THEN 0 ELSE a.VALUE2  END AS DECIMAL) VALUE2, "
        +        "CAST(CASE WHEN a.VALUE3  IS NULL THEN 0 ELSE a.VALUE3  END AS DECIMAL) VALUE3, "
        +        "CAST(CASE WHEN a.VALUE4  IS NULL THEN 0 ELSE a.VALUE4  END AS DECIMAL) VALUE4, "
        +        "CAST(CASE WHEN a.VALUE5  IS NULL THEN 0 ELSE a.VALUE5  END AS DECIMAL) VALUE5, "
        +        "CAST(CASE WHEN a.VALUE6  IS NULL THEN 0 ELSE a.VALUE6  END AS DECIMAL) VALUE6, "
        +        "CAST(CASE WHEN a.VALUE7  IS NULL THEN 0 ELSE a.VALUE7  END AS DECIMAL) VALUE7, "
        +        "CAST(CASE WHEN a.VALUE8  IS NULL THEN 0 ELSE a.VALUE8  END AS DECIMAL) VALUE8, "
        +        "CAST(CASE WHEN a.VALUE9  IS NULL THEN 0 ELSE a.VALUE9  END AS DECIMAL) VALUE9, "
        +        "CAST(CASE WHEN a.VALUE10 IS NULL THEN 0 ELSE a.VALUE10 END AS DECIMAL) VALUE10, "
        +        "CAST(CASE WHEN a.VALUE11 IS NULL THEN 0 ELSE a.VALUE11 END AS DECIMAL) VALUE11, "
        +        "CAST(CASE WHEN a.VALUE12 IS NULL THEN 0 ELSE a.VALUE12 END AS DECIMAL) VALUE12 "
        +   "FROM ( "
        +         "SELECT MAX(AC_LATENCY) VALUE1, MAX(TOTAL_LATENCY) VALUE2, MAX(OCS_LATENCY) VALUE3, "
        +                "AVG(AC_LATENCY) VALUE4, AVG(TOTAL_LATENCY) VALUE5, AVG(OCS_LATENCY) VALUE6, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 20  THEN AC_LATENCY ELSE 20  END, CASE WHEN AC_LATENCY < 9999      THEN AC_LATENCY ELSE 9999      END, 1, 0) END) VALUE7, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 6   THEN AC_LATENCY ELSE 6   END, CASE WHEN AC_LATENCY < 19.999999 THEN AC_LATENCY ELSE 19.999999 END, 1, 0) END) VALUE8, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 4   THEN AC_LATENCY ELSE 4   END, CASE WHEN AC_LATENCY < 5.999999  THEN AC_LATENCY ELSE 5.999999  END, 1, 0) END) VALUE9, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 2   THEN AC_LATENCY ELSE 2   END, CASE WHEN AC_LATENCY < 3.999999  THEN AC_LATENCY ELSE 3.999999  END, 1, 0) END) VALUE10, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 0.1 THEN AC_LATENCY ELSE 0.1 END, CASE WHEN AC_LATENCY < 1.999999  THEN AC_LATENCY ELSE 1.999999  END, 1, 0) END) VALUE11, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 0   THEN AC_LATENCY ELSE 0   END, CASE WHEN AC_LATENCY < 0.099999  THEN AC_LATENCY ELSE 0.099999  END, 1, 0) END) VALUE12 "
        +           "FROM BL_PR_CRICKET_TRAFFIC "
        +          "WHERE TM_NAME LIKE '%FDA%' "
        +            "AND INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +            "AND INSERT_DATE < TO_TIMESTAMP(SECOND, ?) "
        +        ") a"
    );

    // Prepaid Cricket MMS.
    public final SQLStmt fetchPRCRIMMSOCSResultCodes = new SQLStmt(
          "SELECT OCS_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_PR_CRICKET_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND TM_NAME LIKE '%MMS%' "
        +  "GROUP BY OCS_RESULT_CODE"
    );

    public final SQLStmt fetchPRCRIMMSMMSCErrors = new SQLStmt(
          "SELECT CRICKET_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_PR_CRICKET_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND CRICKET_RESULT_CODE NOT IN (2001, 4241, 4245) "
        +    "AND TM_NAME LIKE '%MMS%' "
        +  "GROUP BY CRICKET_RESULT_CODE"
    );

    public final SQLStmt fetchPRCRIMMSRequestsPerMMSC = new SQLStmt(
          "SELECT ORIGIN_HOST, RETRANSMIT_FLAG, COUNT(*) COUNTER "
        +   "FROM BL_PR_CRICKET_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND TM_NAME LIKE '%MMS%' "
        +  "GROUP BY ORIGIN_HOST, RETRANSMIT_FLAG"
    );

    public final SQLStmt fetchPRCRIMMSOCSRequestsPerTM = new SQLStmt(
          "SELECT TM_NAME, COUNT(*) COUNTER "
        +   "FROM BL_PR_CRICKET_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND OCS_LATENCY > 0 "
        +    "AND TM_NAME LIKE '%MMS%' "
        +  "GROUP BY TM_NAME"
    );

    public final SQLStmt fetchPRCRIMMSLatencies = new SQLStmt(
          "SELECT CAST(CASE WHEN a.VALUE1  IS NULL THEN 0 ELSE a.VALUE1  END AS DECIMAL) VALUE1, "
        +        "CAST(CASE WHEN a.VALUE2  IS NULL THEN 0 ELSE a.VALUE2  END AS DECIMAL) VALUE2, "
        +        "CAST(CASE WHEN a.VALUE3  IS NULL THEN 0 ELSE a.VALUE3  END AS DECIMAL) VALUE3, "
        +        "CAST(CASE WHEN a.VALUE4  IS NULL THEN 0 ELSE a.VALUE4  END AS DECIMAL) VALUE4, "
        +        "CAST(CASE WHEN a.VALUE5  IS NULL THEN 0 ELSE a.VALUE5  END AS DECIMAL) VALUE5, "
        +        "CAST(CASE WHEN a.VALUE6  IS NULL THEN 0 ELSE a.VALUE6  END AS DECIMAL) VALUE6, "
        +        "CAST(CASE WHEN a.VALUE7  IS NULL THEN 0 ELSE a.VALUE7  END AS DECIMAL) VALUE7, "
        +        "CAST(CASE WHEN a.VALUE8  IS NULL THEN 0 ELSE a.VALUE8  END AS DECIMAL) VALUE8, "
        +        "CAST(CASE WHEN a.VALUE9  IS NULL THEN 0 ELSE a.VALUE9  END AS DECIMAL) VALUE9, "
        +        "CAST(CASE WHEN a.VALUE10 IS NULL THEN 0 ELSE a.VALUE10 END AS DECIMAL) VALUE10, "
        +        "CAST(CASE WHEN a.VALUE11 IS NULL THEN 0 ELSE a.VALUE11 END AS DECIMAL) VALUE11, "
        +        "CAST(CASE WHEN a.VALUE12 IS NULL THEN 0 ELSE a.VALUE12 END AS DECIMAL) VALUE12 "
        +   "FROM ( "
        +         "SELECT MAX(AC_LATENCY) VALUE1, MAX(TOTAL_LATENCY) VALUE2, MAX(OCS_LATENCY) VALUE3, "
        +                "AVG(AC_LATENCY) VALUE4, AVG(TOTAL_LATENCY) VALUE5, AVG(OCS_LATENCY) VALUE6, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 20  THEN AC_LATENCY ELSE 20  END, CASE WHEN AC_LATENCY < 9999      THEN AC_LATENCY ELSE 9999      END, 1, 0) END) VALUE7, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 6   THEN AC_LATENCY ELSE 6   END, CASE WHEN AC_LATENCY < 19.999999 THEN AC_LATENCY ELSE 19.999999 END, 1, 0) END) VALUE8, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 4   THEN AC_LATENCY ELSE 4   END, CASE WHEN AC_LATENCY < 5.999999  THEN AC_LATENCY ELSE 5.999999  END, 1, 0) END) VALUE9, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 2   THEN AC_LATENCY ELSE 2   END, CASE WHEN AC_LATENCY < 3.999999  THEN AC_LATENCY ELSE 3.999999  END, 1, 0) END) VALUE10, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 0.1 THEN AC_LATENCY ELSE 0.1 END, CASE WHEN AC_LATENCY < 1.999999  THEN AC_LATENCY ELSE 1.999999  END, 1, 0) END) VALUE11, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 0   THEN AC_LATENCY ELSE 0   END, CASE WHEN AC_LATENCY < 0.099999  THEN AC_LATENCY ELSE 0.099999  END, 1, 0) END) VALUE12 "
        +           "FROM BL_PR_CRICKET_TRAFFIC "
        +          "WHERE TM_NAME LIKE '%MMS%' "
        +            "AND INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +            "AND INSERT_DATE < TO_TIMESTAMP(SECOND, ?) "
        +        ") a"
    );

    // Prepaid Cricket CPMIP.
    public final SQLStmt fetchPRCRICPMIPOCSResultCodes = new SQLStmt(
          "SELECT OCS_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_PR_CPM_CRI_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND TRAFFIC_TYPE = 1 "
        +  "GROUP BY OCS_RESULT_CODE"
    );

    public final SQLStmt fetchPRCRICPMIPErrors = new SQLStmt(
          "SELECT CRICKET_RESULT_CODE, COUNT(*) COUNTER "
        +   "FROM BL_PR_CPM_CRI_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND CRICKET_RESULT_CODE NOT IN (2001,4241,4245) "
        +    "AND TRAFFIC_TYPE = 1 "
        +  "GROUP BY CRICKET_RESULT_CODE"
    );

    public final SQLStmt fetchPRCRICPMIPRequestsPerCPM = new SQLStmt(
          "SELECT ORIGIN_HOST, RETRANSMIT_FLAG, COUNT(*) COUNTER "
        +   "FROM BL_PR_CPM_CRI_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND TRAFFIC_TYPE = 1 "
        +  "GROUP BY ORIGIN_HOST, RETRANSMIT_FLAG"
    );

    public final SQLStmt fetchPRCRICPMIPOCSRequestsPerTM = new SQLStmt(
          "SELECT TM_NAME, COUNT(*) COUNTER "
        +   "FROM BL_PR_CPM_CRI_TRAFFIC "
        +  "WHERE INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +    "AND INSERT_DATE <  TO_TIMESTAMP(SECOND, ?) "
        +    "AND OCS_LATENCY > 0 "
        +    "AND TRAFFIC_TYPE = 1 "
        +  "GROUP BY TM_NAME"
    );

    public final SQLStmt fetchPRCRICPMIPLatencies = new SQLStmt(
          "SELECT CAST(CASE WHEN a.VALUE1  IS NULL THEN 0 ELSE a.VALUE1  END AS DECIMAL) VALUE1, "
        +        "CAST(CASE WHEN a.VALUE2  IS NULL THEN 0 ELSE a.VALUE2  END AS DECIMAL) VALUE2, "
        +        "CAST(CASE WHEN a.VALUE3  IS NULL THEN 0 ELSE a.VALUE3  END AS DECIMAL) VALUE3, "
        +        "CAST(CASE WHEN a.VALUE4  IS NULL THEN 0 ELSE a.VALUE4  END AS DECIMAL) VALUE4, "
        +        "CAST(CASE WHEN a.VALUE5  IS NULL THEN 0 ELSE a.VALUE5  END AS DECIMAL) VALUE5, "
        +        "CAST(CASE WHEN a.VALUE6  IS NULL THEN 0 ELSE a.VALUE6  END AS DECIMAL) VALUE6, "
        +        "CAST(CASE WHEN a.VALUE7  IS NULL THEN 0 ELSE a.VALUE7  END AS DECIMAL) VALUE7, "
        +        "CAST(CASE WHEN a.VALUE8  IS NULL THEN 0 ELSE a.VALUE8  END AS DECIMAL) VALUE8, "
        +        "CAST(CASE WHEN a.VALUE9  IS NULL THEN 0 ELSE a.VALUE9  END AS DECIMAL) VALUE9, "
        +        "CAST(CASE WHEN a.VALUE10 IS NULL THEN 0 ELSE a.VALUE10 END AS DECIMAL) VALUE10, "
        +        "CAST(CASE WHEN a.VALUE11 IS NULL THEN 0 ELSE a.VALUE11 END AS DECIMAL) VALUE11, "
        +        "CAST(CASE WHEN a.VALUE12 IS NULL THEN 0 ELSE a.VALUE12 END AS DECIMAL) VALUE12 "
        +   "FROM ( "
        +         "SELECT MAX(AC_LATENCY) VALUE1, MAX(TOTAL_LATENCY) VALUE2, MAX(OCS_LATENCY) VALUE3, "
        +                "AVG(AC_LATENCY) VALUE4, AVG(TOTAL_LATENCY) VALUE5, AVG(OCS_LATENCY) VALUE6, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 20  THEN AC_LATENCY ELSE 20  END, CASE WHEN AC_LATENCY < 9999      THEN AC_LATENCY ELSE 9999      END, 1, 0) END) VALUE7, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 6   THEN AC_LATENCY ELSE 6   END, CASE WHEN AC_LATENCY < 19.999999 THEN AC_LATENCY ELSE 19.999999 END, 1, 0) END) VALUE8, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 4   THEN AC_LATENCY ELSE 4   END, CASE WHEN AC_LATENCY < 5.999999  THEN AC_LATENCY ELSE 5.999999  END, 1, 0) END) VALUE9, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 2   THEN AC_LATENCY ELSE 2   END, CASE WHEN AC_LATENCY < 3.999999  THEN AC_LATENCY ELSE 3.999999  END, 1, 0) END) VALUE10, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 0.1 THEN AC_LATENCY ELSE 0.1 END, CASE WHEN AC_LATENCY < 1.999999  THEN AC_LATENCY ELSE 1.999999  END, 1, 0) END) VALUE11, "
        +                "SUM(CASE WHEN AC_LATENCY IS NULL THEN 0 ELSE DECODE(CASE WHEN AC_LATENCY > 0   THEN AC_LATENCY ELSE 0   END, CASE WHEN AC_LATENCY < 0.099999  THEN AC_LATENCY ELSE 0.099999  END, 1, 0) END) VALUE12 "
        +           "FROM BL_PR_CPM_CRI_TRAFFIC "
        +          "WHERE TRAFFIC_TYPE = 1 "
        +            "AND INSERT_DATE >= TO_TIMESTAMP(SECOND, ?) "
        +            "AND INSERT_DATE < TO_TIMESTAMP(SECOND, ?) "
        +        ") a"
    );


    // Convert the time in seconds to date time format "YYYYMMDDHH24MI" without rounding.
    public String convertSecondsToYYYYMMDDHH24MI(long  pl_seconds)
        throws VoltAbortException {

        String  ls_date_time = null;
        try {
            String         ls_microseconds = Long.toString(pl_seconds) + "000000";
            TimestampType  lt_date_time    = new TimestampType(Long.parseLong(ls_microseconds));
            //  Note that the string format returned from a toString call of TimestampType type value is "YYYY-MM-DD HH24:MI:SS.ssssss".
            String  ls_temp_date_time = lt_date_time.toString();
            // Now we need to return it based on the date time format "YYYYMMDDHH24MI".
            ls_date_time = ls_temp_date_time.substring(0, 4) + ls_temp_date_time.substring(5, 7) + ls_temp_date_time.substring(8, 10) + ls_temp_date_time.substring(11, 13) + ls_temp_date_time.substring(14, 16);
        } catch (Exception e) {
            //System.out.println("Unable to convert " + pl_seconds + " seconds to YYYYMMDDHH24MI format with ERROR: " + e.getMessage());
            //TLOG("PopulateBL_KPI_DATA", null, "Unable to convert " + pl_seconds + " seconds to YYYYMMDDHH24MI format with ERROR: " + e.getMessage(), C_ERROR, "N", 0, C_DEFAULT_TZ, "N");
            throw new VoltAbortException("Unable to convert " + pl_seconds + " seconds to YYYYMMDDHH24MI format with ERROR: " + e.getMessage());
        }
        if (ls_date_time != null){
            //TLOG("PopulateBL_KPI_DATA", null, "Converted date time in 'YYYYMMDDHH24MI' format is " + ls_date_time, C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
            return ls_date_time;
        } else {
            //System.out.println("Unable to convert " + pl_seconds + " seconds to YYYYMMDDHH24MI format");
            //TLOG("PopulateBL_KPI_DATA", null, "Unable to convert " + pl_seconds + " seconds to YYYYMMDDHH24MI format", C_ERROR, "N", 0, C_DEFAULT_TZ, "N");
            throw new VoltAbortException("Unable to convert " + pl_seconds + " seconds to YYYYMMDDHH24MI format");
        }
    }

    // Helper function for InsertBL_KPI_DATA.
    public VoltTable[] blKpiData(String  ps_kpi_time,
                                 String  ps_kpi,
                                 String  ps_sub_category1,
                                 String  ps_sub_category2,
                                 String  ps_value)
        throws VoltAbortException {

        if (ps_value != null){
            // Convert the value frm string to BigDecimal first.
            BigDecimal lbd_value;
            try {
                lbd_value = new BigDecimal(ps_value);
            } catch (Exception e) {
                throw new VoltAbortException("Unable to convert the retrieved value string " + ps_value + " to BigDecimal type");
            }

            //TLOG("PopulateBL_KPI_DATA", null, "Inserting values into BL_KPI_DATA", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
            voltQueueSQL(insertBlKpiDataStmt, ps_kpi_time, ps_kpi, ps_sub_category1, ps_sub_category2, lbd_value);
            return voltExecuteSQL();
        } else {
            //System.out.println("Please enter valid value for ps_value");
            //TLOG("PopulateBL_KPI_DATA", null, "Please enter valid value for ps_value in blKpiData", C_ERROR, "N", 0, C_DEFAULT_TZ, "N");
            throw new VoltAbortException("Need to enter valid value for ps_value in blKpiData");
        }
    }

    // Helper function for GrabPrepaidTrafficData.
    public long GrabPrepaidTrafficDataHelper(long  pl_start_time,
                                             long  pl_end_time)
        throws VoltAbortException {

        // Convert the time in seconds to date time format "YYYYMMDDHH24MI".
        String  ls_previous_hour = convertSecondsToYYYYMMDDHH24MI(pl_start_time);

        TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Starting Prepaid data collection.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");

        voltQueueSQL(fetchPRCCNResultCodes, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRGBGErrors, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRRequestsPerOMG, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCCNRequestsPerTM, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRSuccFailRefunds, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRLatencies, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRLateBilled, pl_start_time, pl_end_time);

        VoltTable[] results = voltExecuteSQL();
        VoltTable   result;
        if (results.length == 7) {
            result = results[0];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, result.getRowCount() + " record(s) present in table BL_PR_SMS_TRAFFIC for Prepaid PR_CCN_ResultCodes.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Processing record " + r + " from table BL_PR_SMS_TRAFFIC for Prepaid PR_CCN_ResultCodes.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with CCN_RESULT_CODE " + Long.toString(rowValue.getLong("CCN_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_CCN_ResultCodes", Long.toString(rowValue.getLong("CCN_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "No records present in table BL_PR_SMS_TRAFFIC for Prepaid PR_CCN_ResultCodes.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[1];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, result.getRowCount() + " record(s) present in table BL_PR_SMS_TRAFFIC for Prepaid PR_GBG_Errors.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Processing record " + r + " from table BL_PR_SMS_TRAFFIC for Prepaid PR_GBG_Errors.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with GBG_RESULT_CODE " + Long.toString(rowValue.getLong("GBG_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_GBG_Errors", Long.toString(rowValue.getLong("GBG_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "No records present in table BL_PR_SMS_TRAFFIC for Prepaid PR_GBG_Errors.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[2];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, result.getRowCount() + " record(s) present in table BL_PR_SMS_TRAFFIC for Prepaid Total_PR_Requests_per_OMG.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Processing record " + r + " from table BL_PR_SMS_TRAFFIC for Prepaid Total_PR_Requests_per_OMG.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with ORIGIN_HOST " + rowValue.getString("ORIGIN_HOST") + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_PR_Requests_per_OMG", rowValue.getString("ORIGIN_HOST"), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "No records present in table BL_PR_SMS_TRAFFIC for Prepaid Total_PR_Requests_per_OMG.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[3];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, result.getRowCount() + " record(s) present in table BL_PR_SMS_TRAFFIC for Prepaid Total_PR_CCN_Requests_per_TM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Processing record " + r + " from table BL_PR_SMS_TRAFFIC for Prepaid Total_PR_CCN_Requests_per_TM.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with TM_NAME " + rowValue.getString("TM_NAME") + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_PR_CCN_Requests_per_TM", rowValue.getString("TM_NAME"), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "No records present in table BL_PR_SMS_TRAFFIC for Prepaid Total_PR_CCN_Requests_per_TM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[4];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, result.getRowCount() + " record(s) present in table BL_PR_SMS_TRAFFIC for Prepaid Total_PR_SuccFailed_Refunds.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Processing record " + r + " from table BL_PR_SMS_TRAFFIC for Prepaid Total_PR_SuccFailed_Refunds.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with CCN_RESULT_CODE " + Long.toString(rowValue.getLong("CCN_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_PR_SuccFailed_Refunds", Long.toString(rowValue.getLong("CCN_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "No records present in table BL_PR_SMS_TRAFFIC for Prepaid Total_PR_SuccFailed_Refunds.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[5];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, result.getRowCount() + " record(s) present in table BL_PR_SMS_TRAFFIC for Prepaid latency.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Processing record " + r + " from table BL_PR_SMS_TRAFFIC for Prepaid latency.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Retrieved VALUE1 " + rowValue.getDecimalAsBigDecimal("VALUE1").toString() + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_Internal_latency", "max", null, rowValue.getDecimalAsBigDecimal("VALUE1").toString());
                    blKpiData(ls_previous_hour, "PR_Overall_latency",  "max", null, rowValue.getDecimalAsBigDecimal("VALUE2").toString());
                    blKpiData(ls_previous_hour, "PR_External_latency", "max", null, rowValue.getDecimalAsBigDecimal("VALUE3").toString());
                    blKpiData(ls_previous_hour, "PR_Internal_latency", "avg", null, rowValue.getDecimalAsBigDecimal("VALUE4").toString());
                    blKpiData(ls_previous_hour, "PR_Overall_latency",  "avg", null, rowValue.getDecimalAsBigDecimal("VALUE5").toString());
                    blKpiData(ls_previous_hour, "PR_External_latency", "avg", null, rowValue.getDecimalAsBigDecimal("VALUE6").toString());
                    blKpiData(ls_previous_hour, "PR_Bucketized_Internal_latency", "20 to 9999", null, rowValue.getDecimalAsBigDecimal("VALUE7").toString());
                    blKpiData(ls_previous_hour, "PR_Bucketized_Internal_latency", "6 to 19.999999", null, rowValue.getDecimalAsBigDecimal("VALUE8").toString());
                    blKpiData(ls_previous_hour, "PR_Bucketized_Internal_latency", "4 to 5.999999", null, rowValue.getDecimalAsBigDecimal("VALUE9").toString());
                    blKpiData(ls_previous_hour, "PR_Bucketized_Internal_latency", "2 to 3.999999", null, rowValue.getDecimalAsBigDecimal("VALUE10").toString());
                    blKpiData(ls_previous_hour, "PR_Bucketized_Internal_latency", "0.1 to 1.999999", null, rowValue.getDecimalAsBigDecimal("VALUE11").toString());
                    blKpiData(ls_previous_hour, "PR_Bucketized_Internal_latency", "0 to 0.099999", null, rowValue.getDecimalAsBigDecimal("VALUE12").toString());
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "No records present in table BL_PR_SMS_TRAFFIC for Prepaid latency.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[6];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, result.getRowCount() + " record(s) present in table BL_PR_SMS_TRAFFIC for Prepaid Total_PR_Late_Billed.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Processing record " + r + " from table BL_PR_SMS_TRAFFIC for Prepaid Total_PR_Late_Billed.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with CCN_RESULT_CODE " + Long.toString(rowValue.getLong("CCN_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_PR_Late_Billed", Long.toString(rowValue.getLong("CCN_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "No records present in table BL_PR_SMS_TRAFFIC for Prepaid Total_PR_Late_Billed.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }
        } else {
            throw new VoltAbortException("Wrong number of SQL executed, " + results.length + " for GrabPrepaidTrafficData, expected 7");
        }

        TLOG("PopulateBL_KPI_DATA(" + C_PT_PR + ")", null, "Prepaid data collection done.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");

        return 1;
    }

    // Helper function for GrabSmartLimitsTrafficData.
    public long GrabSmartLimitsTrafficDataHelper(long  pl_start_time,
                                                 long  pl_end_time)
        throws VoltAbortException {

        // Convert the time in seconds to date time format "YYYYMMDDHH24MI".
        String  ls_previous_hour = convertSecondsToYYYYMMDDHH24MI(pl_start_time);

        TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Starting SmartLimits data collection.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");

        voltQueueSQL(fetchSLCCNResultCodes, pl_start_time, pl_end_time);
        voltQueueSQL(fetchSLGBGErrors, pl_start_time, pl_end_time);
        voltQueueSQL(fetchSLRequestsPerOMG, pl_start_time, pl_end_time);
        voltQueueSQL(fetchSLCCNRequestsPerTM, pl_start_time, pl_end_time);
        voltQueueSQL(fetchSLSuccFailRefunds, pl_start_time, pl_end_time);
        voltQueueSQL(fetchSLLatencies, pl_start_time, pl_end_time);
        voltQueueSQL(fetchSLLateBilled, pl_start_time, pl_end_time);

        VoltTable[] results = voltExecuteSQL();
        VoltTable   result;
        if (results.length == 7) {
            result = results[0];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, result.getRowCount() + " record(s) present in table BL_SL_SMS_TRAFFIC for SmartLimits SL_CCN_ResultCodes.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Processing record " + r + " from table BL_SL_SMS_TRAFFIC for SmartLimits SL_CCN_ResultCodes.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with CCN_RESULT_CODE " + Long.toString(rowValue.getLong("CCN_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "SL_CCN_ResultCodes", Long.toString(rowValue.getLong("CCN_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "No record present in table BL_SL_SMS_TRAFFIC for SmartLimits SL_CCN_ResultCodes.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[1];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, result.getRowCount() + " record(s) present in table BL_SL_SMS_TRAFFIC for SmartLimits SL_GBG_Errors.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Processing record " + r + " from table BL_SL_SMS_TRAFFIC for SmartLimits SL_GBG_Errors.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with GBG_RESULT_CODE " + Long.toString(rowValue.getLong("GBG_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "SL_GBG_Errors", Long.toString(rowValue.getLong("GBG_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "No record present in table BL_SL_SMS_TRAFFIC for SmartLimits SL_GBG_Errors.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[2];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, result.getRowCount() + " record(s) present in table BL_SL_SMS_TRAFFIC for SmartLimits Total_SL_Requests_per_OMG.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Processing record " + r + " from table BL_SL_SMS_TRAFFIC for SmartLimits Total_SL_Requests_per_OMG.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with ORIGIN_HOST " + rowValue.getString("ORIGIN_HOST") + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_SL_Requests_per_OMG", rowValue.getString("ORIGIN_HOST"), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "No record present in table BL_SL_SMS_TRAFFIC for SmartLimits Total_SL_Requests_per_OMG.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[3];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, result.getRowCount() + " record(s) present in table BL_SL_SMS_TRAFFIC for SmartLimits Total_SL_CCN_Requests_per_TM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Processing record " + r + " from table BL_SL_SMS_TRAFFIC for SmartLimits Total_SL_CCN_Requests_per_TM.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with TM_NAME " + rowValue.getString("TM_NAME") + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_SL_CCN_Requests_per_TM", rowValue.getString("TM_NAME"), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "No record present in table BL_SL_SMS_TRAFFIC for SmartLimits Total_SL_CCN_Requests_per_TM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[4];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, result.getRowCount() + " record(s) present in table BL_SL_SMS_TRAFFIC for SmartLimits Total_SL_SuccFailed_Refunds.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Processing record " + r + " from table BL_SL_SMS_TRAFFIC for SmartLimits Total_SL_SuccFailed_Refunds.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with CCN_RESULT_CODE " + Long.toString(rowValue.getLong("CCN_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_SL_SuccFailed_Refunds", Long.toString(rowValue.getLong("CCN_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "No record present in table BL_SL_SMS_TRAFFIC for SmartLimits Total_SL_SuccFailed_Refunds.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[5];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, result.getRowCount() + " record(s) present in table BL_SL_SMS_TRAFFIC for SmartLimits latency.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Processing record " + r + " from table BL_SL_SMS_TRAFFIC for SmartLimits latency.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Retrieved VALUE1 " + rowValue.getDecimalAsBigDecimal("VALUE1").toString() + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "SL_Internal_latency", "max", null, rowValue.getDecimalAsBigDecimal("VALUE1").toString());
                    blKpiData(ls_previous_hour, "SL_Overall_latency",  "max", null, rowValue.getDecimalAsBigDecimal("VALUE2").toString());
                    blKpiData(ls_previous_hour, "SL_External_latency", "max", null, rowValue.getDecimalAsBigDecimal("VALUE3").toString());
                    blKpiData(ls_previous_hour, "SL_Internal_latency", "avg", null, rowValue.getDecimalAsBigDecimal("VALUE4").toString());
                    blKpiData(ls_previous_hour, "SL_Overall_latency",  "avg", null, rowValue.getDecimalAsBigDecimal("VALUE5").toString());
                    blKpiData(ls_previous_hour, "SL_External_latency", "avg", null, rowValue.getDecimalAsBigDecimal("VALUE6").toString());
                    blKpiData(ls_previous_hour, "SL_Bucketized_Internal_latency", "20 to 9999", null, rowValue.getDecimalAsBigDecimal("VALUE7").toString());
                    blKpiData(ls_previous_hour, "SL_Bucketized_Internal_latency", "6 to 19.999999", null, rowValue.getDecimalAsBigDecimal("VALUE8").toString());
                    blKpiData(ls_previous_hour, "SL_Bucketized_Internal_latency", "4 to 5.999999", null, rowValue.getDecimalAsBigDecimal("VALUE9").toString());
                    blKpiData(ls_previous_hour, "SL_Bucketized_Internal_latency", "2 to 3.999999", null, rowValue.getDecimalAsBigDecimal("VALUE10").toString());
                    blKpiData(ls_previous_hour, "SL_Bucketized_Internal_latency", "0.1 to 1.999999", null, rowValue.getDecimalAsBigDecimal("VALUE11").toString());
                    blKpiData(ls_previous_hour, "SL_Bucketized_Internal_latency", "0 to 0.099999", null, rowValue.getDecimalAsBigDecimal("VALUE12").toString());
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "No record present in table BL_SL_SMS_TRAFFIC for SmartLimits latency.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[6];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, result.getRowCount() + " record(s) present in table BL_SL_SMS_TRAFFIC for SmartLimits Total_SL_Late_Billed.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Processing record " + r + " from table BL_SL_SMS_TRAFFIC for SmartLimits Total_SL_Late_Billed.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with CCN_RESULT_CODE " + Long.toString(rowValue.getLong("CCN_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_SL_Late_Billed", Long.toString(rowValue.getLong("CCN_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "No record present in table BL_SL_SMS_TRAFFIC for SmartLimits Total_SL_Late_Billed.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }
        } else {
            throw new VoltAbortException("Wrong number of SQL executed, " + results.length + " for GrabSmartLimitsTrafficData, expected 7");
        }

        TLOG("PopulateBL_KPI_DATA(" + C_PT_SL + ")", null, "SmartLimits data collection done.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");

        return 1;
    }

    // Helper function for GrabPrepaidCPMTrafficData.
    public long GrabPrepaidCPMTrafficDataHelper(long  pl_start_time,
                                                long  pl_end_time)
        throws VoltAbortException {

        // Convert the time in seconds to date time format "YYYYMMDDHH24MI".
        String  ls_previous_hour = convertSecondsToYYYYMMDDHH24MI(pl_start_time);

        TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "Starting Prepaid CPM data collection.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");

        voltQueueSQL(fetchPRCPMCCNResultCodes, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCPMErrors, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCPMRequestsPerCPM, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCPMCCNRequestsPerTM, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCPMSuccFailRefunds, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCPMLatencies, pl_start_time, pl_end_time);

        VoltTable[] results = voltExecuteSQL();
        VoltTable   result;
        if (results.length == 6) {
            result = results[0];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CPM_TRAFFIC for Prepaid CPM PR_CPM_CCN_ResultCodes.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "Processing record " + r + " from table BL_PR_CPM_TRAFFIC for Prepaid CPM PR_CPM_CCN_ResultCodes.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with CCN_RESULT_CODE " + Long.toString(rowValue.getLong("CCN_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_CPM_CCN_ResultCodes", Long.toString(rowValue.getLong("CCN_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "No record present in table BL_PR_CPM_TRAFFIC for Prepaid CPM PR_CPM_CCN_ResultCodes.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[1];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CPM_TRAFFIC for Prepaid CPM PR_CPM_CPM_Errors.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "Processing record " + r + " from table BL_PR_CPM_TRAFFIC for Prepaid CPM PR_CPM_CPM_Errors.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with CPM_RESULT_CODE " + Long.toString(rowValue.getLong("CPM_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_CPM_CPM_Errors", Long.toString(rowValue.getLong("CPM_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "No record present in table BL_PR_CPM_TRAFFIC for Prepaid CPM PR_CPM_CPM_Errors.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[2];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CPM_TRAFFIC for Prepaid CPM Total_PR_CPM_Requests_per_CPM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "Processing record " + r + " from table BL_PR_CPM_TRAFFIC for Prepaid CPM Total_PR_CPM_Requests_per_CPM.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with ORIGIN_HOST " + rowValue.getString("ORIGIN_HOST") + " and RETRANSMIT_FLAG " + Long.toString(rowValue.getLong("RETRANSMIT_FLAG")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_PR_CPM_Requests_per_CPM", rowValue.getString("ORIGIN_HOST"), Long.toString(rowValue.getLong("RETRANSMIT_FLAG")), Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "No record present in table BL_PR_CPM_TRAFFIC for Prepaid CPM Total_PR_CPM_Requests_per_CPM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[3];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CPM_TRAFFIC for Prepaid CPM Total_PR_CPM_CCN_Requests_per_TM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "Processing record " + r + " from table BL_PR_CPM_TRAFFIC for Prepaid CPM Total_PR_CPM_CCN_Requests_per_TM.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with TM_NAME " + rowValue.getString("TM_NAME") + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_PR_CPM_CCN_Requests_per_TM", rowValue.getString("TM_NAME"), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "No record present in table BL_PR_CPM_TRAFFIC for Prepaid CPM Total_PR_CPM_CCN_Requests_per_TM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[4];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CPM_TRAFFIC for Prepaid CPM Total_PR_CPM_SuccFailed_Refunds.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "Processing record " + r + " from table BL_PR_CPM_TRAFFIC for Prepaid CPM Total_PR_CPM_SuccFailed_Refunds.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with CCN_RESULT_CODE " + Long.toString(rowValue.getLong("CCN_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_PR_CPM_SuccFailed_Refunds", Long.toString(rowValue.getLong("CCN_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "No record present in table BL_PR_CPM_TRAFFIC for Prepaid CPM Total_PR_CPM_SuccFailed_Refunds.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[5];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CPM_TRAFFIC for Prepaid CPM latency.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "Processing record " + r + " from table BL_PR_CPM_TRAFFIC for Prepaid CPM latency.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "Retrieved VALUE1 " + rowValue.getDecimalAsBigDecimal("VALUE1").toString() + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_CPM_Internal_latency", "max", null, rowValue.getDecimalAsBigDecimal("VALUE1").toString());
                    blKpiData(ls_previous_hour, "PR_CPM_Overall_latency",  "max", null, rowValue.getDecimalAsBigDecimal("VALUE2").toString());
                    blKpiData(ls_previous_hour, "PR_CPM_External_latency", "max", null, rowValue.getDecimalAsBigDecimal("VALUE3").toString());
                    blKpiData(ls_previous_hour, "PR_CPM_Internal_latency", "avg", null, rowValue.getDecimalAsBigDecimal("VALUE4").toString());
                    blKpiData(ls_previous_hour, "PR_CPM_Overall_latency",  "avg", null, rowValue.getDecimalAsBigDecimal("VALUE5").toString());
                    blKpiData(ls_previous_hour, "PR_CPM_External_latency", "avg", null, rowValue.getDecimalAsBigDecimal("VALUE6").toString());
                    blKpiData(ls_previous_hour, "PR_CPM_Bucketized_Internal_latency", "20 to 9999", null, rowValue.getDecimalAsBigDecimal("VALUE7").toString());
                    blKpiData(ls_previous_hour, "PR_CPM_Bucketized_Internal_latency", "6 to 19.999999", null, rowValue.getDecimalAsBigDecimal("VALUE8").toString());
                    blKpiData(ls_previous_hour, "PR_CPM_Bucketized_Internal_latency", "4 to 5.999999", null, rowValue.getDecimalAsBigDecimal("VALUE9").toString());
                    blKpiData(ls_previous_hour, "PR_CPM_Bucketized_Internal_latency", "2 to 3.999999", null, rowValue.getDecimalAsBigDecimal("VALUE10").toString());
                    blKpiData(ls_previous_hour, "PR_CPM_Bucketized_Internal_latency", "0.1 to 1.999999", null, rowValue.getDecimalAsBigDecimal("VALUE11").toString());
                    blKpiData(ls_previous_hour, "PR_CPM_Bucketized_Internal_latency", "0 to 0.099999", null, rowValue.getDecimalAsBigDecimal("VALUE12").toString());
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "No record present in table BL_PR_CPM_TRAFFIC for Prepaid CPM latency.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }
        } else {
            throw new VoltAbortException("Wrong number of SQL executed, " + results.length + " for GrabPrepaidCPMTrafficData, expected 6");
        }

        TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMPR + ")", null, "Prepaid CPM data collection done.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");

        return 1;
    }

    // Helper function for GrabSmartLimitsCPMTrafficData.
    public long GrabSmartLimitsCPMTrafficDataHelper(long  pl_start_time,
                                                    long  pl_end_time)
        throws VoltAbortException {

        // Convert the time in seconds to date time format "YYYYMMDDHH24MI".
        String  ls_previous_hour = convertSecondsToYYYYMMDDHH24MI(pl_start_time);

        TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "Starting SmartLimits CPM data collection.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");

        voltQueueSQL(fetchSLCPMCCNResultCodes, pl_start_time, pl_end_time);
        voltQueueSQL(fetchSLCPMErrors, pl_start_time, pl_end_time);
        voltQueueSQL(fetchSLCPMRequestsPerCPM, pl_start_time, pl_end_time);
        voltQueueSQL(fetchSLCPMCCNRequestsPerTM, pl_start_time, pl_end_time);
        voltQueueSQL(fetchSLCPMLatencies, pl_start_time, pl_end_time);

        VoltTable[] results = voltExecuteSQL();
        VoltTable   result;
        if (results.length == 5) {
            result = results[0];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, result.getRowCount() + " record(s) present in table BL_SL_CPM_TRAFFIC for SmartLimits CPM SL_CPM_CCN_ResultCodes.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "Processing record " + r + " from table BL_SL_CPM_TRAFFIC for SmartLimits CPM SL_CPM_CCN_ResultCodes.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with CCN_RESULT_CODE " + Long.toString(rowValue.getLong("CCN_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "SL_CPM_CCN_ResultCodes", Long.toString(rowValue.getLong("CCN_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "No record present in table BL_SL_CPM_TRAFFIC for SmartLimits CPM SL_CPM_CCN_ResultCodes.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[1];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, result.getRowCount() + " record(s) present in table BL_SL_CPM_TRAFFIC for SmartLimits CPM SL_CPM_CPM_Errors.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "Processing record " + r + " from table BL_SL_CPM_TRAFFIC for SmartLimits CPM SL_CPM_CPM_Errors.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with CPM_RESULT_CODE " + Long.toString(rowValue.getLong("CPM_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "SL_CPM_CPM_Errors", Long.toString(rowValue.getLong("CPM_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "No record present in table BL_SL_CPM_TRAFFIC for SmartLimits CPM SL_CPM_CPM_Errors.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[2];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, result.getRowCount() + " record(s) present in table BL_SL_CPM_TRAFFIC for SmartLimits CPM Total_SL_CPM_Requests_per_CPM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "Processing record " + r + " from table BL_SL_CPM_TRAFFIC for SmartLimits CPM Total_SL_CPM_Requests_per_CPM.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with ORIGIN_HOST " + rowValue.getString("ORIGIN_HOST") + " and RETRANSMIT_FLAG " + Long.toString(rowValue.getLong("RETRANSMIT_FLAG")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_SL_CPM_Requests_per_CPM", rowValue.getString("ORIGIN_HOST"), Long.toString(rowValue.getLong("RETRANSMIT_FLAG")), Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "No record present in table BL_SL_CPM_TRAFFIC for SmartLimits CPM Total_SL_CPM_Requests_per_CPM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[3];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, result.getRowCount() + " record(s) present in table BL_SL_CPM_TRAFFIC for SmartLimits CPM Total_SL_CPM_CCN_Requests_per_TM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "Processing record " + r + " from table BL_SL_CPM_TRAFFIC for SmartLimits CPM Total_SL_CPM_CCN_Requests_per_TM.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with TM_NAME " + rowValue.getString("TM_NAME") + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_SL_CPM_CCN_Requests_per_TM", rowValue.getString("TM_NAME"), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "No record present in table BL_SL_CPM_TRAFFIC for SmartLimits CPM Total_SL_CPM_CCN_Requests_per_TM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[4];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, result.getRowCount() + " record(s) present in table BL_SL_CPM_TRAFFIC for SmartLimits CPM latency.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "Processing record " + r + " from table BL_SL_CPM_TRAFFIC for SmartLimits CPM latency.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "Retrieved VALUE1 " + rowValue.getDecimalAsBigDecimal("VALUE1").toString() + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "SL_CPM_Internal_latency", "max", null, rowValue.getDecimalAsBigDecimal("VALUE1").toString());
                    blKpiData(ls_previous_hour, "SL_CPM_Overall_latency",  "max", null, rowValue.getDecimalAsBigDecimal("VALUE2").toString());
                    blKpiData(ls_previous_hour, "SL_CPM_External_latency", "max", null, rowValue.getDecimalAsBigDecimal("VALUE3").toString());
                    blKpiData(ls_previous_hour, "SL_CPM_Internal_latency", "avg", null, rowValue.getDecimalAsBigDecimal("VALUE4").toString());
                    blKpiData(ls_previous_hour, "SL_CPM_Overall_latency",  "avg", null, rowValue.getDecimalAsBigDecimal("VALUE5").toString());
                    blKpiData(ls_previous_hour, "SL_CPM_External_latency", "avg", null, rowValue.getDecimalAsBigDecimal("VALUE6").toString());
                    blKpiData(ls_previous_hour, "SL_CPM_Bucketized_Internal_latency", "20 to 9999", null, rowValue.getDecimalAsBigDecimal("VALUE7").toString());
                    blKpiData(ls_previous_hour, "SL_CPM_Bucketized_Internal_latency", "6 to 19.999999", null, rowValue.getDecimalAsBigDecimal("VALUE8").toString());
                    blKpiData(ls_previous_hour, "SL_CPM_Bucketized_Internal_latency", "4 to 5.999999", null, rowValue.getDecimalAsBigDecimal("VALUE9").toString());
                    blKpiData(ls_previous_hour, "SL_CPM_Bucketized_Internal_latency", "2 to 3.999999", null, rowValue.getDecimalAsBigDecimal("VALUE10").toString());
                    blKpiData(ls_previous_hour, "SL_CPM_Bucketized_Internal_latency", "0.1 to 1.999999", null, rowValue.getDecimalAsBigDecimal("VALUE11").toString());
                    blKpiData(ls_previous_hour, "SL_CPM_Bucketized_Internal_latency", "0 to 0.099999", null, rowValue.getDecimalAsBigDecimal("VALUE12").toString());
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "No record present in table BL_SL_CPM_TRAFFIC for SmartLimits CPM latency.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }
        } else {
            throw new VoltAbortException("Wrong number of SQL executed, " + results.length + " for GrabSmartLimitsCPMTrafficData, expected 5");
        }

        TLOG("PopulateBL_KPI_DATA(" + C_PT_CPMSL + ")", null, "SmartLimits CPM data collection done.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");

        return 1;
    }

    // Helper function for GrabPRCRISMSTrafficData.
    public long GrabPRCRISMSTrafficDataHelper(long  pl_start_time,
                                              long  pl_end_time)
        throws VoltAbortException {

        // Convert the time in seconds to date time format "YYYYMMDDHH24MI".
        String  ls_previous_hour = convertSecondsToYYYYMMDDHH24MI(pl_start_time);

        TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "Starting Prepaid Cricket SMS data collection.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");

        voltQueueSQL(fetchPRCRISMSOCSResultCodes, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCRISMSUMRErrors, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCRISMSRequestsPerUMR, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCRISMSOCSRequestsPerTM, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCRISMSLatencies, pl_start_time, pl_end_time);

        VoltTable[] results = voltExecuteSQL();
        VoltTable   result;
        if (results.length == 5) {
            result = results[0];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS PR_Cricket_SMS_OCS_ResultCodes.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "Processing record " + r + " from table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS PR_Cricket_SMS_OCS_ResultCodes.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with OCS_RESULT_CODE " + Long.toString(rowValue.getLong("OCS_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_Cricket_SMS_OCS_ResultCodes", Long.toString(rowValue.getLong("OCS_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "No record present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS PR_Cricket_SMS_OCS_ResultCodes.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[1];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS PR_CRICKET_SMS_UMR_Errors.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "Processing record " + r + " from table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS PR_CRICKET_SMS_UMR_Errors.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with CRICKET_RESULT_CODE " + Long.toString(rowValue.getLong("CRICKET_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_CRICKET_SMS_UMR_Errors", Long.toString(rowValue.getLong("CRICKET_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "No record present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS PR_CRICKET_SMS_UMR_Errors.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[2];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS Total_PR_CRICKET_SMS_Requests_per_UMR.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "Processing record " + r + " from table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS Total_PR_CRICKET_SMS_Requests_per_UMR.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with ORIGIN_HOST " + rowValue.getString("ORIGIN_HOST") + " and RETRANSMIT_FLAG " + Long.toString(rowValue.getLong("RETRANSMIT_FLAG")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_PR_CRICKET_SMS_Requests_per_UMR", rowValue.getString("ORIGIN_HOST"), Long.toString(rowValue.getLong("RETRANSMIT_FLAG")), Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "No record present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS Total_PR_CRICKET_SMS_Requests_per_UMR.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[3];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS Total_PR_CRICKET_SMS_OCS_Requests_per_TM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "Processing record " + r + " from table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS Total_PR_CRICKET_SMS_OCS_Requests_per_TM.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with TM_NAME " + rowValue.getString("TM_NAME") + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_PR_CRICKET_SMS_OCS_Requests_per_TM", rowValue.getString("TM_NAME"), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "No record present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS Total_PR_CRICKET_SMS_OCS_Requests_per_TM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[4];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS latency.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "Processing record " + r + " from table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS latency.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "Retrieved VALUE1 " + rowValue.getDecimalAsBigDecimal("VALUE1").toString() + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_CRICKET_SMS_Internal_latency", "max", null, rowValue.getDecimalAsBigDecimal("VALUE1").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_SMS_Overall_latency",  "max", null, rowValue.getDecimalAsBigDecimal("VALUE2").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_SMS_External_latency", "max", null, rowValue.getDecimalAsBigDecimal("VALUE3").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_SMS_Internal_latency", "avg", null, rowValue.getDecimalAsBigDecimal("VALUE4").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_SMS_Overall_latency",  "avg", null, rowValue.getDecimalAsBigDecimal("VALUE5").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_SMS_External_latency", "avg", null, rowValue.getDecimalAsBigDecimal("VALUE6").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_SMS_Bucketized_Internal_latency", "20 to 9999", null, rowValue.getDecimalAsBigDecimal("VALUE7").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_SMS_Bucketized_Internal_latency", "6 to 19.999999", null, rowValue.getDecimalAsBigDecimal("VALUE8").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_SMS_Bucketized_Internal_latency", "4 to 5.999999", null, rowValue.getDecimalAsBigDecimal("VALUE9").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_SMS_Bucketized_Internal_latency", "2 to 3.999999", null, rowValue.getDecimalAsBigDecimal("VALUE10").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_SMS_Bucketized_Internal_latency", "0.1 to 1.999999", null, rowValue.getDecimalAsBigDecimal("VALUE11").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_SMS_Bucketized_Internal_latency", "0 to 0.099999", null, rowValue.getDecimalAsBigDecimal("VALUE12").toString());
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "No record present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket SMS latency.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }
        } else {
            throw new VoltAbortException("Wrong number of SQL executed, " + results.length + " for GrabPRCRISMSTrafficData, expected 5");
        }

        TLOG("PopulateBL_KPI_DATA(" + C_PT_CRISMS + ")", null, "Prepaid Cricket SMS data collection done.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");

        return 1;
    }

    // Helper function for GrabPRCRIMMSTrafficData.
    public long GrabPRCRIMMSTrafficDataHelper(long  pl_start_time,
                                              long  pl_end_time)
        throws VoltAbortException {

        // Convert the time in seconds to date time format "YYYYMMDDHH24MI".
        String  ls_previous_hour = convertSecondsToYYYYMMDDHH24MI(pl_start_time);

        TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "Starting Prepaid Cricket MMS data collection.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");

        voltQueueSQL(fetchPRCRIMMSOCSResultCodes, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCRIMMSMMSCErrors, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCRIMMSRequestsPerMMSC, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCRIMMSOCSRequestsPerTM, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCRIMMSLatencies, pl_start_time, pl_end_time);

        VoltTable[] results = voltExecuteSQL();
        VoltTable   result;
        if (results.length == 5) {
            result = results[0];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS PR_Cricket_MMS_OCS_ResultCodes.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "Processing record " + r + " from table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS PR_Cricket_MMS_OCS_ResultCodes.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with OCS_RESULT_CODE " + Long.toString(rowValue.getLong("OCS_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_Cricket_MMS_OCS_ResultCodes", Long.toString(rowValue.getLong("OCS_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "No record present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS PR_Cricket_MMS_OCS_ResultCodes.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[1];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS PR_CRICKET_MMS_MMSC_Errors.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "Processing record " + r + " from table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS PR_CRICKET_MMS_MMSC_Errors.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with CRICKET_RESULT_CODE " + Long.toString(rowValue.getLong("CRICKET_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_CRICKET_MMS_MMSC_Errors", Long.toString(rowValue.getLong("CRICKET_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "No record present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS PR_CRICKET_MMS_MMSC_Errors.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[2];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS Total_PR_CRICKET_MMS_Requests_per_MMSC.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "Processing record " + r + " from table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS Total_PR_CRICKET_MMS_Requests_per_MMSC.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with ORIGIN_HOST " + rowValue.getString("ORIGIN_HOST") + " and RETRANSMIT_FLAG " + Long.toString(rowValue.getLong("RETRANSMIT_FLAG")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_PR_CRICKET_MMS_Requests_per_MMSC", rowValue.getString("ORIGIN_HOST"), Long.toString(rowValue.getLong("RETRANSMIT_FLAG")), Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "No record present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS Total_PR_CRICKET_MMS_Requests_per_MMSC.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[3];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS Total_PR_CRICKET_MMS_OCS_Requests_per_TM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "Processing record " + r + " from table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS Total_PR_CRICKET_MMS_OCS_Requests_per_TM.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with TM_NAME " + rowValue.getString("TM_NAME") + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_PR_CRICKET_MMS_OCS_Requests_per_TM", rowValue.getString("TM_NAME"), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "No record present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS Total_PR_CRICKET_MMS_OCS_Requests_per_TM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[4];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS latency.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "Processing record " + r + " from table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS latency.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "Retrieved VALUE1 " + rowValue.getDecimalAsBigDecimal("VALUE1").toString() + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_CRICKET_MMS_Internal_latency", "max", null, rowValue.getDecimalAsBigDecimal("VALUE1").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_MMS_Overall_latency",  "max", null, rowValue.getDecimalAsBigDecimal("VALUE2").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_MMS_External_latency", "max", null, rowValue.getDecimalAsBigDecimal("VALUE3").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_MMS_Internal_latency", "avg", null, rowValue.getDecimalAsBigDecimal("VALUE4").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_MMS_Overall_latency",  "avg", null, rowValue.getDecimalAsBigDecimal("VALUE5").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_MMS_External_latency", "avg", null, rowValue.getDecimalAsBigDecimal("VALUE6").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_MMS_Bucketized_Internal_latency", "20 to 9999", null, rowValue.getDecimalAsBigDecimal("VALUE7").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_MMS_Bucketized_Internal_latency", "6 to 19.999999", null, rowValue.getDecimalAsBigDecimal("VALUE8").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_MMS_Bucketized_Internal_latency", "4 to 5.999999", null, rowValue.getDecimalAsBigDecimal("VALUE9").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_MMS_Bucketized_Internal_latency", "2 to 3.999999", null, rowValue.getDecimalAsBigDecimal("VALUE10").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_MMS_Bucketized_Internal_latency", "0.1 to 1.999999", null, rowValue.getDecimalAsBigDecimal("VALUE11").toString());
                    blKpiData(ls_previous_hour, "PR_CRICKET_MMS_Bucketized_Internal_latency", "0 to 0.099999", null, rowValue.getDecimalAsBigDecimal("VALUE12").toString());
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "No record present in table BL_PR_CRICKET_TRAFFIC for Prepaid Cricket MMS latency.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }
        } else {
            throw new VoltAbortException("Wrong number of SQL executed, " + results.length + " for GrabPRCRIMMSTrafficData, expected 5");
        }

        TLOG("PopulateBL_KPI_DATA(" + C_PT_CRIMMS + ")", null, "Prepaid Cricket MMS data collection done.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");

        return 1;
    }

    // Helper function for GrabPRCRICPMIPTrafficData.
    public long GrabPRCRICPMIPTrafficDataHelper(long  pl_start_time,
                                                long  pl_end_time)
        throws VoltAbortException {

        // Convert the time in seconds to date time format "YYYYMMDDHH24MI".
        String  ls_previous_hour = convertSecondsToYYYYMMDDHH24MI(pl_start_time);

        TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "Starting Prepaid Cricket CPMIP data collection.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");

        voltQueueSQL(fetchPRCRICPMIPOCSResultCodes, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCRICPMIPErrors, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCRICPMIPRequestsPerCPM, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCRICPMIPOCSRequestsPerTM, pl_start_time, pl_end_time);
        voltQueueSQL(fetchPRCRICPMIPLatencies, pl_start_time, pl_end_time);

        VoltTable[] results = voltExecuteSQL();
        VoltTable   result;
        if (results.length == 5) {
            result = results[0];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP PR_CRI_CPMIP_OCS_ResultCodes.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "Processing record " + r + " from table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP PR_CRI_CPMIP_OCS_ResultCodes.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with OCS_RESULT_CODE " + Long.toString(rowValue.getLong("OCS_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_CRI_CPMIP_OCS_ResultCodes", Long.toString(rowValue.getLong("OCS_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "No record present in table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP PR_CRI_CPMIP_OCS_ResultCodes.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[1];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP PR_CRI_CPMIP_CPM_Errors.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "Processing record " + r + " from table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP PR_CRI_CPMIP_CPM_Errors.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with CRICKET_RESULT_CODE " + Long.toString(rowValue.getLong("CRICKET_RESULT_CODE")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_CRI_CPMIP_CPM_Errors", Long.toString(rowValue.getLong("CRICKET_RESULT_CODE")), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "No record present in table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP PR_CRI_CPMIP_CPM_Errors.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[2];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP Total_PR_CRI_CPMIP_Requests_per_CPM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "Processing record " + r + " from table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP Total_PR_CRI_CPMIP_Requests_per_CPM.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with ORIGIN_HOST " + rowValue.getString("ORIGIN_HOST") + " and RETRANSMIT_FLAG " + Long.toString(rowValue.getLong("RETRANSMIT_FLAG")) + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_PR_CRI_CPMIP_Requests_per_CPM", rowValue.getString("ORIGIN_HOST"), Long.toString(rowValue.getLong("RETRANSMIT_FLAG")), Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "No record present in table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP Total_PR_CRI_CPMIP_Requests_per_CPM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[3];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP Total_PR_CRI_CPMIP_OCS_Requests_per_TM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "Processing record " + r + " from table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP Total_PR_CRI_CPMIP_OCS_Requests_per_TM.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "Found " + Long.toString(rowValue.getLong("COUNTER")) + " entries with TM_NAME " + rowValue.getString("TM_NAME") + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "Total_PR_CRI_CPMIP_OCS_Requests_per_TM", rowValue.getString("TM_NAME"), null, Long.toString(rowValue.getLong("COUNTER")));
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "No record present in table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP Total_PR_CRI_CPMIP_OCS_Requests_per_TM.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }

            result = results[4];
            if (result.getRowCount() > 0) {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, result.getRowCount() + " record(s) present in table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP latency.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
                for (int r = 0; r < result.getRowCount(); r++) {
                    TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "Processing record " + r + " from table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP latency.", C_DEBUG2, "N", 0, C_DEFAULT_TZ, "N");
                    VoltTableRow rowValue = result.fetchRow(r);
                    //TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "Retrieved VALUE1 " + rowValue.getDecimalAsBigDecimal("VALUE1").toString() + ".", C_DEBUG3, "N", 0, C_DEFAULT_TZ, "N");
                    blKpiData(ls_previous_hour, "PR_CRI_CPMIP_Internal_latency", "max", null, rowValue.getDecimalAsBigDecimal("VALUE1").toString());
                    blKpiData(ls_previous_hour, "PR_CRI_CPMIP_Overall_latency",  "max", null, rowValue.getDecimalAsBigDecimal("VALUE2").toString());
                    blKpiData(ls_previous_hour, "PR_CRI_CPMIP_External_latency", "max", null, rowValue.getDecimalAsBigDecimal("VALUE3").toString());
                    blKpiData(ls_previous_hour, "PR_CRI_CPMIP_Internal_latency", "avg", null, rowValue.getDecimalAsBigDecimal("VALUE4").toString());
                    blKpiData(ls_previous_hour, "PR_CRI_CPMIP_Overall_latency",  "avg", null, rowValue.getDecimalAsBigDecimal("VALUE5").toString());
                    blKpiData(ls_previous_hour, "PR_CRI_CPMIP_External_latency", "avg", null, rowValue.getDecimalAsBigDecimal("VALUE6").toString());
                    blKpiData(ls_previous_hour, "PR_CRI_CPMIP_Bucketized_Internal_latency", "20 to 9999", null, rowValue.getDecimalAsBigDecimal("VALUE7").toString());
                    blKpiData(ls_previous_hour, "PR_CRI_CPMIP_Bucketized_Internal_latency", "6 to 19.999999", null, rowValue.getDecimalAsBigDecimal("VALUE8").toString());
                    blKpiData(ls_previous_hour, "PR_CRI_CPMIP_Bucketized_Internal_latency", "4 to 5.999999", null, rowValue.getDecimalAsBigDecimal("VALUE9").toString());
                    blKpiData(ls_previous_hour, "PR_CRI_CPMIP_Bucketized_Internal_latency", "2 to 3.999999", null, rowValue.getDecimalAsBigDecimal("VALUE10").toString());
                    blKpiData(ls_previous_hour, "PR_CRI_CPMIP_Bucketized_Internal_latency", "0.1 to 1.999999", null, rowValue.getDecimalAsBigDecimal("VALUE11").toString());
                    blKpiData(ls_previous_hour, "PR_CRI_CPMIP_Bucketized_Internal_latency", "0 to 0.099999", null, rowValue.getDecimalAsBigDecimal("VALUE12").toString());
                }
            } else {
                TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "No record present in table BL_PR_CPM_CRI_TRAFFIC for Prepaid Cricket CPMIP latency.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");
            }
        } else {
            throw new VoltAbortException("Wrong number of SQL executed, " + results.length + " for GrabPRCRICPMIPTrafficData, expected 5");
        }

        TLOG("PopulateBL_KPI_DATA(" + C_PT_CRICPMIP + ")", null, "Prepaid Cricket CPMIP data collection done.", C_DEBUG1, "N", 0, C_DEFAULT_TZ, "N");

        return 1;
    }
}


