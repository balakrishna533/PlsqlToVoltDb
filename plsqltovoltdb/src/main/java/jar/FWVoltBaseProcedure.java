package com.openet.bl.procedures;

import org.voltdb.*;
import java.util.Locale;
import java.sql.Timestamp;
import java.util.Arrays;


public class FWVoltBaseProcedure extends VoltProcedure {

    // Internal log level constant values.
    public final String C_SYSERROR = "0";
    public final String C_ERROR    = "1";
    public final String C_WARN     = "2";
    public final String C_INFO     = "3";
    public final String C_DEBUG1   = "4";
    public final String C_DEBUG2   = "5";
    public final String C_DEBUG3   = "6";
    String[] logLevels = { C_SYSERROR, C_ERROR, C_WARN, C_INFO, C_DEBUG1, C_DEBUG2, C_DEBUG3 };

    // Business logic log level constant values.
    public final String C_BL_SYSERROR = "SYSERROR";
    public final String C_BL_ERROR    = "ERROR";
    public final String C_BL_WARN     = "WARN";
    public final String C_BL_INFO     = "INFO";

    // Default installation ID.
    public final String C_INSTALLATION_ID = "1";

    // Default time zone used.
    // Note that VoltDB only kept time values in UTC/GMT. For other time zones, you will need to externally calculate based on that time zone offset value.
    public final String C_DEFAULT_TZ = "GMT";

    // Utilizing SEQUENCE_TABLE table to select CURRENT_TIMESTAMP from VoltDB, similar to select from DUAL table in Oracle.
    public final SQLStmt selectSysdate = new SQLStmt(
          "SELECT CURRENT_TIMESTAMP                           SYSDATE, "
        +        "SINCE_EPOCH(SECOND, CURRENT_TIMESTAMP)      SYSDATE_SECONDS, "
        +        "SINCE_EPOCH(MILLISECOND, CURRENT_TIMESTAMP) SYSDATE_MILLISECONDS, "
        +        "SINCE_EPOCH(MICROSECOND, CURRENT_TIMESTAMP) SYSDATE_MICROSECONDS "
        +   "FROM SEQUENCE_TABLE LIMIT 1"
    );

    public final SQLStmt selectSequence = new SQLStmt(
        "SELECT CURRENT_VALUE, MAX_VALUE, PREFIX_VALUE, START_VALUE, LEADING_ZERO_FLAG FROM SEQUENCE_TABLE WHERE SEQUENCE_NAME = ? AND MAX_VALUE > 0 AND START_VALUE >= 0 AND START_VALUE <= MAX_VALUE"
    );

    public final SQLStmt updateSequence = new SQLStmt(
        "UPDATE SEQUENCE_TABLE SET CURRENT_VALUE = ?, LAST_MODIFIED_TIME = CURRENT_TIMESTAMP WHERE SEQUENCE_NAME = ?"
    );

    public final SQLStmt insertLog = new SQLStmt(
        "INSERT INTO FW_LOG_MESSAGE (SEQUENCE_NUMBER, COMPONENT_NAME, THREAD_ID, LOG_TIMESTAMP, MESSAGE_ID, LOG_LEVEL_TYPE, MESSAGE_TEXT, TRANSACTION_ID, LOG_TIMEZONE, IS_PRIORITY_MESSAGE) VALUES (?, ?, 0, CURRENT_TIMESTAMP, '0', ?, ?, ?, ?, ?)"
    );

    public final SQLStmt insertLogV11 = new SQLStmt(
        "INSERT INTO FW_LOG_ENTRY_1 (SEQUENCE_NUMBER, INSTALLATION_ID, COMPONENT_NAME, LOGICAL_COMPONENT_NAME, THREAD_ID, LOG_TIMESTAMP, MESSAGE_ID, LOG_LEVEL_TYPE, MESSAGE_TEXT, TRANSACTION_ID, LOG_TIMEZONE, IS_PRIORITY_MESSAGE) VALUES (?, ?, ?, ?, 0, CURRENT_TIMESTAMP, '0', ?, ?, ?, ?, ?)"
    );


    public long  getSysdateInSeconds()
        throws VoltAbortException {

        // Retrieved and returned the current VoltDB system date.
        voltQueueSQL(selectSysdate, EXPECT_ONE_ROW);
        VoltTable[]  selectResultSet = voltExecuteSQL();
        VoltTable    selectResult    = selectResultSet[0];
        VoltTableRow rowValue        = selectResult.fetchRow(0);

        // Verify that the value are the same.
        //if (rowValue.getTimestampAsLong(0) != rowValue.getLong(3)) {
        //    throw new VoltAbortException("CURRENT_TIMESTAMP calls are giving a different value when selected in the same row");
        //}

        return rowValue.getLong(1);
    }

    public long  getSysdateInMilliSeconds()
        throws VoltAbortException {

        // Retrieved and returned the current VoltDB system date.
        voltQueueSQL(selectSysdate, EXPECT_ONE_ROW);
        VoltTable[]  selectResultSet = voltExecuteSQL();
        VoltTable    selectResult    = selectResultSet[0];
        VoltTableRow rowValue        = selectResult.fetchRow(0);

        // Verify that the value are the same.
        //if (rowValue.getTimestampAsLong(0) != rowValue.getLong(3)) {
        //    throw new VoltAbortException("CURRENT_TIMESTAMP calls are giving a different value when selected in the same row");
        //}

        return rowValue.getLong(2);
    }

    public long  getSysdateInMicroSeconds()
        throws VoltAbortException {

        // Retrieved and returned the current VoltDB system date.
        voltQueueSQL(selectSysdate, EXPECT_ONE_ROW);
        VoltTable[]  selectResultSet = voltExecuteSQL();
        VoltTable    selectResult    = selectResultSet[0];
        VoltTableRow rowValue        = selectResult.fetchRow(0);

        // Verify that the value are the same.
        //if (rowValue.getTimestampAsLong(0) != rowValue.getLong(3)) {
        //    throw new VoltAbortException("CURRENT_TIMESTAMP calls are giving a different value when selected in the same row");
        //}

        return rowValue.getTimestampAsLong(0);
    }

    public VoltTable  getSysdate()
        throws VoltAbortException {

        // Retrieved the current VoltDB system date.
        voltQueueSQL(selectSysdate, EXPECT_ONE_ROW);
        VoltTable[]  selectResultSet = voltExecuteSQL();
        VoltTable    selectResult    = selectResultSet[0];
        VoltTableRow rowValue        = selectResult.fetchRow(0);

        // Verify that the value are the same.
        //if (rowValue.getTimestampAsLong(0) != rowValue.getLong(3)) {
        //    throw new VoltAbortException("CURRENT_TIMESTAMP calls are giving a different value when selected in the same row");
        //}

        // Return the current VoltDB system date in all the possible format.
        VoltTable newTable = new VoltTable(
            new VoltTable.ColumnInfo("SYSDATE", VoltType.TIMESTAMP),
            new VoltTable.ColumnInfo("SYSDATE_SECONDS", VoltType.BIGINT),
            new VoltTable.ColumnInfo("SYSDATE_MILLISECONDS", VoltType.BIGINT),
            new VoltTable.ColumnInfo("SYSDATE_MICROSECONDS", VoltType.BIGINT),
            new VoltTable.ColumnInfo("SYSDATE_STRING", VoltType.STRING),
            new VoltTable.ColumnInfo("CURRENT_DAY_HOUR", VoltType.STRING));
        String  currentDayHour = new java.text.SimpleDateFormat("EEE HHmm yyyyMMdd").format(new java.util.Date(rowValue.getLong(2))).toUpperCase();
        newTable.addRow(rowValue.getTimestampAsTimestamp(0), rowValue.getLong(1), rowValue.getLong(2), rowValue.getLong(3), rowValue.getTimestampAsTimestamp(0).toString(), currentDayHour);

        return newTable;
    }

    public VoltTable  getSequence(String  sequenceName,
                                  int     increaseFlag)
        throws VoltAbortException {

        // First retrieved the current sequence value to be used.
        voltQueueSQL(selectSequence, EXPECT_ONE_ROW, sequenceName);
        VoltTable[]  selectResultSet = voltExecuteSQL();
        VoltTable    selectResult    = selectResultSet[0];
        VoltTableRow rowValue        = selectResult.fetchRow(0);
        long   currentValue    = rowValue.getLong("CURRENT_VALUE");
        long   maxValue        = rowValue.getLong("MAX_VALUE");
        long   startValue      = rowValue.getLong("START_VALUE");
        long   leadingZeroFlag = rowValue.getLong("LEADING_ZERO_FLAG");
        String prefixValue     = "";
        if (rowValue.getString("PREFIX_VALUE") != null) {
            prefixValue = rowValue.getString("PREFIX_VALUE");
        }
        String precisionFormat = "%d";
        if (leadingZeroFlag == 1) {
            int precisionValue = Long.toString(maxValue).length();
            // Making sure this is copying the number using Western Arabic/European numerals.
            precisionFormat = String.format(Locale.US, "%%0%dd", precisionValue);
        }
        // Making sure this is copying the number using Western Arabic/European numerals.
        String returnValue = prefixValue + String.format(Locale.US, precisionFormat, currentValue);

        // Increased the sequence number for the next usage.
        if (increaseFlag == 1) {
            long newValue = currentValue + 1;
            if (newValue > maxValue) {
                newValue = startValue;
            }
            voltQueueSQL(updateSequence, newValue, sequenceName);
            voltExecuteSQL();
        }

        // Return the current value for the sequence combined with unique_prefix
        VoltTable newTable = new VoltTable(
            new VoltTable.ColumnInfo("SEQUENCE_NAME", VoltType.STRING),
            new VoltTable.ColumnInfo("CURRENT_VALUE", VoltType.STRING));
        newTable.addRow(sequenceName, returnValue);

        return newTable;
    }

    public VoltTable[]  TLOG(String  p_component_name,
                             String  p_trans_id,
                             String  p_msg_text,
                             String  p_msg_type,
                             String  p_alertable,
                             int     p_thread_id,
                             String  p_tz,
                             String  p_commit_asyn)
        throws VoltAbortException {

        String  l_ispri    = null;
        String  l_msg_type = p_msg_type;
        String  l_msg_text = null;
        String  l_tz       = C_DEFAULT_TZ;

        if (p_alertable != null) {
            if (p_alertable.equals("Y")) {
                l_ispri = "Y";
            }
        }
        if (p_msg_type == null) {
            l_msg_type = C_DEBUG1;
        }
        if (!(Arrays.asList(logLevels).contains(l_msg_type))) {
            l_msg_type = C_SYSERROR;
            l_msg_text = "Unknown message type of " + p_msg_type + " requested";
        }
        if (p_tz != null) {
            l_tz = p_tz;
        }

        VoltTable     l_result = getSequence("FW_MESSAGE_SEQ", 1);
        VoltTableRow  l_row = l_result.fetchRow(0);
        String        l_curr_seq_val_str = l_row.getString("CURRENT_VALUE");
        long          l_curr_seq_val;
        try {
            l_curr_seq_val = Long.parseLong(l_curr_seq_val_str);
        } catch (NumberFormatException e) {
            throw new VoltAbortException("Unable to convert the retrieved FW_MESSAGE_SEQ.CURRENT_VALUE for current sequence");
        }
        if (l_msg_text != null) {
            // Need to make sure that the next sequence is retrieved first before queing the log message inserts.
            l_result = getSequence("FW_MESSAGE_SEQ", 1);
            l_row = l_result.fetchRow(0);
            String  l_next_seq_val_str = l_row.getString("CURRENT_VALUE");
            long    l_next_seq_val;
            try {
                l_next_seq_val = Long.parseLong(l_next_seq_val_str);
            } catch (NumberFormatException e) {
                throw new VoltAbortException("Unable to convert the retrieved FW_MESSAGE_SEQ.CURRENT_VALUE for next sequence");
            }
            voltQueueSQL(insertLog, l_curr_seq_val, p_component_name, l_msg_type, l_msg_text, p_trans_id, l_tz, l_ispri);
            l_curr_seq_val = l_next_seq_val;
        }
        voltQueueSQL(insertLog, l_curr_seq_val, p_component_name, l_msg_type, p_msg_text, p_trans_id, l_tz, l_ispri);
        return voltExecuteSQL();
    }

    public VoltTable[]  ALOG(String  p_component_name,
                             String  p_trans_id,
                             String  p_msg_text,
                             String  p_msg_type,
                             String  p_alertable,
                             int     p_thread_id,
                             String  p_tz)
        throws VoltAbortException {

        return TLOG(p_component_name, p_trans_id, p_msg_text, p_msg_type, p_alertable, p_thread_id, p_tz, "Y");
    }

    public VoltTable[]  TLOG_V11(String  p_component_name,
                                 String  p_logical_component_name,
                                 String  p_trans_id,
                                 String  p_msg_text,
                                 String  p_msg_type,
                                 String  p_alertable,
                                 int     p_thread_id,
                                 String  p_tz,
                                 String  p_commit_asyn,
                                 String  p_installation_id)
        throws VoltAbortException {

        String  l_ispri    = null;
        String  l_msg_type = p_msg_type;
        String  l_msg_text = null;
        String  l_tz       = C_DEFAULT_TZ;

        if (p_alertable != null) {
            if (p_alertable.equals("Y")) {
                l_ispri = "Y";
            }
        }
        if (p_msg_type == null) {
            l_msg_type = C_DEBUG1;
        }
        if (!(Arrays.asList(logLevels).contains(l_msg_type))) {
            l_msg_type = C_SYSERROR;
            l_msg_text = "Unknown message type of " + p_msg_type + " requested";
        }
        if (p_tz != null) {
            l_tz = p_tz;
        }

        VoltTable     l_result = getSequence("FW_MESSAGE_SEQ", 1);
        VoltTableRow  l_row = l_result.fetchRow(0);
        String        l_curr_seq_val_str = l_row.getString("CURRENT_VALUE");
        long          l_curr_seq_val;
        try {
            l_curr_seq_val = Long.parseLong(l_curr_seq_val_str);
        } catch (NumberFormatException e) {
            throw new VoltAbortException("Unable to convert the retrieved FW_MESSAGE_SEQ.CURRENT_VALUE for current sequence");
        }
        if (l_msg_text != null) {
            // Need to make sure that the next sequence is retrieved first before queing the log message inserts.
            l_result = getSequence("FW_MESSAGE_SEQ", 1);
            l_row = l_result.fetchRow(0);
            String  l_next_seq_val_str = l_row.getString("CURRENT_VALUE");
            long    l_next_seq_val;
            try {
                l_next_seq_val = Long.parseLong(l_next_seq_val_str);
            } catch (NumberFormatException e) {
                throw new VoltAbortException("Unable to convert the retrieved FW_MESSAGE_SEQ.CURRENT_VALUE for next sequence");
            }
            voltQueueSQL(insertLogV11, l_curr_seq_val, p_installation_id, p_component_name, p_logical_component_name, l_msg_type, l_msg_text, p_trans_id, l_tz, l_ispri);
            l_curr_seq_val = l_next_seq_val;
        }
        voltQueueSQL(insertLogV11, l_curr_seq_val, p_installation_id, p_component_name, p_logical_component_name, l_msg_type, p_msg_text, p_trans_id, l_tz, l_ispri);
        return voltExecuteSQL();
    }

    public VoltTable[]  ALOG_V11(String  p_component_name,
                                 String  p_logical_component_name,
                                 String  p_trans_id,
                                 String  p_msg_text,
                                 String  p_msg_type,
                                 String  p_alertable,
                                 int     p_thread_id,
                                 String  p_tz,
                                 String  p_installation_id)
        throws VoltAbortException {

        return TLOG_V11(p_component_name, p_logical_component_name, p_trans_id, p_msg_text, p_msg_type, p_alertable, p_thread_id, p_tz, "Y", p_installation_id);
    }
}


