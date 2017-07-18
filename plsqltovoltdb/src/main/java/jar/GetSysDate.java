package com.openet.bl.procedures;

import org.voltdb.*;
import java.util.Locale;
import java.sql.Timestamp;
import java.util.Arrays;


public class GetSysDate extends FWVoltBaseProcedure {

    public VoltTable  run()
        throws VoltAbortException {

        return getSysdate();
    }
}


