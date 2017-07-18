package com.openet.bl.procedures;

import org.voltdb.*;
import java.util.Locale;
import java.sql.Timestamp;
import java.util.Arrays;


public class FW_LOG_V11_LOG extends FWVoltBaseProcedure {

    public VoltTable[]  run(long    p_job_id,
                            String  p_msg_text,
                            String  p_msg_type,
                            String  p_alertable)
        throws VoltAbortException {

        String  l_msg_type = C_ERROR;

        if (p_msg_type == C_BL_SYSERROR) {
            l_msg_type = C_SYSERROR;
        } else if (p_msg_type == C_BL_ERROR) {
            l_msg_type = C_ERROR;
        } else if (p_msg_type == C_BL_INFO) {
            l_msg_type = C_INFO;
        } else if (p_msg_type == C_BL_WARN) {
            l_msg_type = C_WARN;
        }

        return ALOG_V11("BL", "BL", Long.toString(p_job_id), p_msg_text, l_msg_type, p_alertable, 0, null, C_INSTALLATION_ID);
    }
}


