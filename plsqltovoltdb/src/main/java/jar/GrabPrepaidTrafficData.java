package com.openet.bl.procedures;

import org.voltdb.*;
import java.util.Locale;
import java.sql.Timestamp;
import java.util.Arrays;


public class GrabPrepaidTrafficData extends FWVoltBaseBLKPIProcedure {

    public long run(long  pl_start_time,
                    long  pl_end_time)
        throws VoltAbortException {

        return GrabPrepaidTrafficDataHelper(pl_start_time, pl_end_time);
    }
}