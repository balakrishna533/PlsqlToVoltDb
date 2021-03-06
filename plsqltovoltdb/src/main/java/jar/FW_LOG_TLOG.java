package com.openet.bl.procedures;

import org.voltdb.*;
import java.util.Locale;
import java.sql.Timestamp;
import java.util.Arrays;


public class FW_LOG_TLOG extends FWVoltBaseProcedure {

    public VoltTable[]  run(String  p_component_name,
                            String  p_trans_id,
                            String  p_msg_text,
                            String  p_msg_type,
                            String  p_alertable,
                            int     p_thread_id,
                            String  p_tz,
                            String  p_commit_asyn)
        throws VoltAbortException {

        return TLOG(p_component_name, p_trans_id, p_msg_text, p_msg_type, p_alertable, p_thread_id, p_tz, p_commit_asyn);
    }
}


