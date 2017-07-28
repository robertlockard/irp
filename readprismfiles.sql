--------------------------------------------------------
--  DDL for Package IRP_READPRISMFILES
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IRP"."IRP_READPRISMFILES" IS
--
PROCEDURE IRP_CENSUSFILE( sDirName IN VARCHAR2, sFileName IN VARCHAR2 );
--
PROCEDURE IRP_TARGETFILE( sDirName IN VARCHAR2, sFileName IN VARCHAR2 );
--
PROCEDURE IRP_SCHEDULEJOB( sFMCSAfiletype IN VARCHAR2, sDownloadStatus IN VARCHAR2 );
--
PROCEDURE IRP_PROCESS_TARGET;
--
FUNCTION irp_is_targeted(sUSDOTNO IN VARCHAR2 DEFAULT NULL, sVIN IN VARCHAR2 DEFAULT NULL) return boolean;
--
FUNCTION  WHAT_VERSION RETURN VARCHAR2;
--
	-- define the cursors we need.
	CURSOR tgt_cur IS
	select c.irp_tcar_irpnbr, c.irp_tcar_id, v.IRP_FLV_VEHICLEID, v.IRP_VEH_TITLENBR, count(*) knt
	from 	irp.IRP_FLEETVEHICLE_VW v,
			irp.irp_targetedcarriers c,
			irp.irp_prismmcsip p
	where v.irp_fle_irpnbr  = c.irp_tcar_irpnbr			-- irp number is the key for carriers (applicants)
		and irp_mcs_step      = v.IRP_PLC_MCSIPSTEP		-- the mcsipstep code tells us the type of targeting
		and irp_tcar_status   = 'T'				-- is the vehicle currently targeted.
		and IRP_MCS_TGTROAD   = 'Y'				-- is the vehicle to be targeted on the road?
    and trunc(IRP_TCAR_ACTIVITYDATE) = trunc(sysdate)
	group by c.irp_tcar_irpnbr, c.irp_tcar_id, v.IRP_FLV_VEHICLEID, v.IRP_VEH_TITLENBR;
END;

/
--------------------------------------------------------
--  DDL for Package Body IRP_READPRISMFILES
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IRP"."IRP_READPRISMFILES" IS
--
--   Change History:
--   20170217.1 - added clear_mvaflags function to clean up mva flags after the flag
--              - is cleared from db2.
--   20170113.1 - fixed clear_db2_flag. the userdata field was incorrct, so was not 
--                clearing the flag. Fixed to append 'F' as sup_source
--   20161207.1 - fixed function set_db2flag. The del code was picking up the wrong
--              - column.
--   20160831.1 - modified firp_add_veh_target to limit only those where the SID code
--                is for highway stop per. Neil. modified firp_add_fv_target to limit
--                only those where the SID code is for highway stop.
--   20160810.1 - Added information to the e-mail message indicating the Oracle
--                database version, instance name, and the Server Name.
--   20160611.1 - Added irp_process_target to run after irp_targetfile.
--   20160601.1 - Added ipr_process_target. This will move through the targeted tables
--                and mark carriers and vehicles targeted and remove any carriers and
--                vehicles that have been removed from the targeted file.
--   20160504.1 - Refinements to the Baseline file handling logic were added.  We try to
--                get the file data information from the file name, but if this doesn't
--                work, we'll leave this field blank.  Since the table is truncated, these
--                values can be added in later by using an SQL statement similar to:
--                     update IRP_PRSMLCLCENSUS
--                        SET IRP_PLC_FILEDATE = TO_DATE( '05/04/2016','mm/dd/yyyy')
--                      WHERE IRP_PLC_FILEDATE IS NULL;
--                or a similar statement, where the number of updated rows should match
--                the number of rows inserted after the table was TRUNCATED as:
--                      2765660 rows updated.
--                Then, a COMMIT statement would be required.
--   20160502.1 - Handling of PRISM Baseline file produced once a month.  Allow the start
--                of the FMCSA file update jobs to run by type and note whether execution
--                was successful or not which dictates whether to proceed.
--   20160323.1 - Add code to set the valued of the File Date column which is derived
--                from the Filename, which should be formatted as YYYYMMDD.CEN or
--                YYYYMMDD.TGT.  This is important, partcularly for the Target tables
--                because we need to determine if a record is to be removed.
--   20160205.1 - Allow for completely NULL parameter input for the current files
--   20160204.1 - Add similar functions to IRP_TARGETFILE such as E-mail, and correct
--                issues found in testing.
--   20160203.1 - Add DIRECTORY name parameter to inputs, E-mail handling, etc.
--   20160129.1 - Add a duplicate record count to make sure that the totals balance correctly
--   20160128.1 - Allow NULL filenames to be entered which will default to a filename
--                based on SYSDATE, as <YYYYMMDD>.CEN, etc.
--   20151027.1 - Initial version of this package.
--
--  This package reads the PRISM files.
--
--
PKG_VERSION_TXT  VARCHAR2(30) := '20170216.1';

FUNCTION clear_mvaflags (sTitleNbr IN VARCHAR2) RETURN NUMBER IS
	iVehId NUMBER;			-- vehicle id that we are going to get from title number.
	iResults NUMBER := 0;	-- the number of rows that have been deleted from irp.irp_mvaflags
BEGIN
	-- first things first, get the vehicle id for the title number that we will use to 
	-- update irp.irp_mvaflags.
	BEGIN
		SELECT irp_veh_id 
		INTO iVehId
		FROM irp.irp_vehicles
		WHERE irp_veh_titlenbr = sTitleNbr;
	EXCEPTION WHEN NO_DATA_FOUND THEN
		iResults := -1;
		RETURN iResults;
	END;
	
	-- remove the vehicle from irp.irp_mvaflags.
	BEGIN
		IF iResults = 0 THEN
			DELETE FROM irp.irp_mvaflags
			WHERE IRP_MFG_VEHICLEID = iVehId
			AND IRP_MFG_UNITCODE = '0086';
			iResults := SQL%ROWCOUNT;
			COMMIT;
			RETURN iResults;
		END IF;
	EXCEPTION WHEN OTHERS THEN
		RETURN -2;
	END;
	
END clear_mvaflags;

FUNCTION clear_db2_flags RETURN NUMBER IS

    CURSOR carrier_cur IS
/* this is a collection of all carriers, fleetvehicles and vehicles that
    are have been targeted, and been removed from irp_prsmlcltgtcar. */
    select distinct irp_app_irpnbr, 
			v.irp_veh_titlenbr,
			IRP_TCAR_SUSPENSIONDATE
    from irp.irp_applicants a,
        irp.irp_targetedcarriers tc,
        irp.irp_fleets f,
        irp.irp_fleetvehicles fv,
        irp.irp_vehicles v,
        irp.IRP_REGISTRATIONYEARINSTANCES ryi
    where irp_app_irpnbr  = irp_tcar_irpnbr
      and irp_app_irpnbr  = irp_fle_irpnbr
      and irp_fle_id      = irp_flv_fleetid
      and irp_fle_id      = irp_ryi_fleetid
      and irp_veh_id      = irp_flv_vehicleid
      and irp_ryi_nbr     = IRP_FLV_REGYEARINSTNBR
      and irp_ryi_regyear = irp_flv_regyear
      and irp_ryi_fleetid = irp_flv_fleetid
      and irp_veh_id      = irp_flv_vehicleid
      and irp_ryi_expiredate > trunc(sysdate)
      and ((irp_app_target = 'Y' and irp_app_status = 'T') or irp_veh_target = 'Y' or irp_flv_target = 'Y')
      and irp_tcar_status = 'T'
      and not exists
        (select 'y'
         from IRP.IRP_PRSMLCLTGTCAR PC
         WHERE PC.IRP_PTC_USDOTNO = a.irp_app_usdotno);

        CURSOR vehicle_cur IS
        select distinct v.irp_veh_titlenbr, tc.IRP_TCAR_SUSPENSIONDATE
        from irp.irp_targetedcarriers tc,
            irp.irp_fleets f,
            irp.irp_fleetvehicles fv,
            irp.irp_vehicles v,
            irp.IRP_REGISTRATIONYEARINSTANCES ryi
        where irp_fle_id      = irp_flv_fleetid
          and irp_fle_id      = irp_ryi_fleetid
          and irp_veh_id      = irp_flv_vehicleid
          and irp_ryi_nbr     = IRP_FLV_REGYEARINSTNBR
          and irp_ryi_regyear = irp_flv_regyear
          and irp_ryi_fleetid = irp_flv_fleetid
          and irp_veh_id      = irp_flv_vehicleid
          and irp_ryi_expiredate > trunc(sysdate)
          and (irp_veh_target = 'Y' or irp_flv_target = 'Y')
          and irp_tcar_status = 'T'
          and not exists
            (select 'y'
             from IRP.IRP_PRSMLCLTGTVEH PC
             WHERE PC.IRP_PTV_VIN = v.irp_veh_vin);

  sUnitCode IRP.IRP_MVAFLAGS.IRP_MFG_USERDATA%TYPE;
  sUserData IRP.IRP_DELINQUENTCODE.IRP_DCO_UNITCODE%TYPE;
  sIrpNbr	IRP.IRP_APPLICANTS.IRP_APP_IRPNBR%TYPE;
  susp_src	varchar2(1) := 'F'; -- default to 'F'.

  iErrCode  NUMBER;
  sErrMsg   VARCHAR2(255);
  sRetMsg   VARCHAR2(2000);
  iID       NUMBER;         -- hold the log id

BEGIN

    -- get sUnitCode
    BEGIN
        SELECT IRP_DCO_UNITCODE
        INTO sUnitCode
        FROM IRP.IRP_DELINQUENTCODE
        WHERE IRP_DCO_DELINQUENTNAME = 'UNSAFE';

    EXCEPTION WHEN NO_DATA_FOUND THEN
        iErrCode := 1;
        sErrMsg  := '* ERROR * - Delinquency Code (' || 'UNSAFE' ||
                          ') not found in IRP_DELINQUENTCODE';
        RETURN( '-1' );
    WHEN OTHERS THEN
        iErrCode := SQLCODE;
        sErrMsg  := SUBSTR( SQLERRM, 1, 254 );
        RETURN( '-1' );
    END;

    FOR carrier_rec IN carrier_cur
    LOOP
		iID := utility.log_stack.create_entry(pUnit => $$PLSQL_UNIT,
											pLine => $$PLSQL_LINE,
											pStime => current_timestamp,
											pParms => 'carrier remove flag cursor for loop vehicle_id ' || carrier_rec.irp_veh_titlenbr);
		sys.dbms_output.put_line('carrier remove flag cursor for loop vehicle_id ' || carrier_rec.irp_veh_titlenbr);
		-- get the sUnitCode
		BEGIN
		select distinct IRP_MFG_UNITCODE
		INTO sUnitCode
		from irp.IRP_MVAFLAGS f,
			irp_vehicles v
		where f.IRP_MFG_VEHICLEID = v.irp_veh_id
		  and v.irp_veh_titlenbr = carrier_rec.irp_veh_titlenbr;
		EXCEPTION WHEN OTHERS THEN
		utility.log_stack.end_entry(pLogId => iID,
									pResults => 'raised error ' || sqlerrm);
		END;
	  
		-- get sUserData
		BEGIN
			susp_src := 'F';
			sUserData   := LPAD( TO_CHAR( carrier_rec.irp_app_irpnbr ), 6, '0' ) ||
						 ' ' || SUBSTR( LTRIM( RTRIM( NVL( susp_src, '?' ) ) ), 1, 1 );
		EXCEPTION WHEN OTHERS THEN
			iErrCode := SQLCODE;
			sErrMsg  := SUBSTR( SQLERRM, 1, 254 );
			RETURN '-1';
		END;
			BEGIN
				-- clear the flag.
				IF NOT IRP_MVADB2.IRP_TLEFLAG_DELETE( carrier_rec.irp_veh_titlenbr, carrier_rec.IRP_TCAR_SUSPENSIONDATE, sUnitCode,
													sUserData, sRetMsg, 'IRP', 'VA40573' ) THEN
					sys.dbms_output.put_line('results for ipr_tleflag_delete false');
				ELSE
					sys.dbms_output.put_line('results for ipr_tleflag_delete true');
				END IF;
			EXCEPTION WHEN OTHERS THEN
				SYS.DBMS_OUTPUT.PUT_LINE(SQLERRM);
			END;
    END LOOP;

    FOR vehicle_rec IN vehicle_cur
    LOOP
      iID := utility.log_stack.create_entry(pUnit => $$PLSQL_UNIT,
                                            pLine => $$PLSQL_LINE,
                                            pStime => current_timestamp,
                                            pParms => 'carrier remove flag cursor for loop vehicle_id ' || vehicle_rec.irp_veh_titlenbr);

        sys.dbms_output.put_line('carrier remove flag cursor for loop vehicle_id ' || vehicle_rec.irp_veh_titlenbr);
        -- get the sUnitCode
        BEGIN
          select IRP_MFG_UNITCODE
          into sUnitCode
          from irp.IRP_MVAFLAGS f,
              irp_vehicles v
          where f.IRP_MFG_VEHICLEID = v.irp_veh_id
            and v.irp_veh_titlenbr = vehicle_rec.irp_veh_titlenbr;
        EXCEPTION WHEN OTHERS THEN
          utility.log_stack.end_entry(pLogId => iID,
                                      pResults => 'raised error ' || sqlerrm);
        END;
		
		-- get sUserData
		BEGIN
			
			select DISTINCT IRP_FLE_IRPNBR
			into sIrpNbr
			from irp.irp_fleetvehicle_vw
			where IRP_VEH_TITLENBR = vehicle_rec.irp_veh_titlenbr;
			
			susp_src := 'F';

			sUserData   := LPAD( TO_CHAR( sIrpNbr ), 6, '0' ) ||
						 ' ' || SUBSTR( LTRIM( RTRIM( NVL( susp_src, '?' ) ) ), 1, 1 );
		--	
		EXCEPTION WHEN OTHERS THEN
			SYS.DBMS_OUTPUT.PUT_LINE(SQLERRM);
		END;
		--
		BEGIN
			IF NOT IRP_MVADB2.IRP_TLEFLAG_DELETE( vehicle_rec.irp_veh_titlenbr,vehicle_rec.IRP_TCAR_SUSPENSIONDATE, sUnitCode,
												sUserData, sRetMsg, 'IRP', 'VA40573' ) THEN
				sys.dbms_output.put_line('results for ipr_tleflag_delete false');
			ELSE
				sys.dbms_output.put_line('results for ipr_tleflag_delete true');
			END IF;
		EXCEPTION WHEN OTHERS THEN
			SYS.DBMS_OUTPUT.PUT_LINE(SQLERRM);
		END;
    --
	END LOOP;
	--
    RETURN 0;
EXCEPTION 
    WHEN OTHERS THEN    
      RAISE_APPLICATION_ERROR(-20017,'clear_db2_flags raised ' || sqlerrm);
      RETURN -1;
END;

--
FUNCTION WHAT_VERSION RETURN VARCHAR2 IS
    BEGIN
        RETURN( PKG_VERSION_TXT );
    END;
--
--
--
PROCEDURE IRP_CENSUSFILE( sDirName IN VARCHAR2, sFileName IN VARCHAR2 ) IS
--
    TYPE census_rectyp IS RECORD (
        PLC_FILTYP         IRP_PRSMLCLCENSUS.IRP_PLC_FILTYP%TYPE,
        PLC_MCSIPFLG       IRP_PRSMLCLCENSUS.IRP_PLC_MCSIPFLG%TYPE,
        PLC_USDOTNO        IRP_PRSMLCLCENSUS.IRP_PLC_USDOTNO%TYPE,
        PLC_ICCNO          IRP_PRSMLCLCENSUS.IRP_PLC_ICCNO%TYPE,
        PLC_TINTYP         IRP_PRSMLCLCENSUS.IRP_PLC_TINTYP%TYPE,
        PLC_TIN            IRP_PRSMLCLCENSUS.IRP_PLC_TIN%TYPE,
        PLC_COMPANY        IRP_PRSMLCLCENSUS.IRP_PLC_COMPANY%TYPE,
        PLC_DBA            IRP_PRSMLCLCENSUS.IRP_PLC_DBA%TYPE,
        PLC_ADDSTREET      IRP_PRSMLCLCENSUS.IRP_PLC_ADDSTREET%TYPE,
        PLC_ADDCITY        IRP_PRSMLCLCENSUS.IRP_PLC_ADDCITY%TYPE,
        PLC_ADDSTATE       IRP_PRSMLCLCENSUS.IRP_PLC_ADDSTATE%TYPE,
        PLC_ADDZIP         IRP_PRSMLCLCENSUS.IRP_PLC_ADDZIP%TYPE,
        PLC_ADDZIPEXT      IRP_PRSMLCLCENSUS.IRP_PLC_ADDZIPEXT%TYPE,
        PLC_ADDCOUNTY      IRP_PRSMLCLCENSUS.IRP_PLC_ADDCOUNTY%TYPE,
        PLC_CONPHONE       IRP_PRSMLCLCENSUS.IRP_PLC_CONPHONE%TYPE,
        PLC_MAILSTREET     IRP_PRSMLCLCENSUS.IRP_PLC_MAILSTREET%TYPE,
        PLC_MAILCITY       IRP_PRSMLCLCENSUS.IRP_PLC_MAILCITY%TYPE,
        PLC_MAILSTATE      IRP_PRSMLCLCENSUS.IRP_PLC_MAILSTATE%TYPE,
        PLC_MAILZIP        IRP_PRSMLCLCENSUS.IRP_PLC_MAILZIP%TYPE,
        PLC_MAILZIPEXT     IRP_PRSMLCLCENSUS.IRP_PLC_MAILZIPEXT%TYPE,
        PLC_MAILCOUNTY     IRP_PRSMLCLCENSUS.IRP_PLC_MAILCOUNTY%TYPE,
        PLC_CARRIERSTAT    IRP_PRSMLCLCENSUS.IRP_PLC_CARRIERSTAT%TYPE,
        PLC_MCMISCREDAT    IRP_PRSMLCLCENSUS.IRP_PLC_MCMISCREDAT%TYPE,
        PLC_DATEADDED      IRP_PRSMLCLCENSUS.IRP_PLC_DATEADDED%TYPE,
        PLC_MCMISLSTUPD    IRP_PRSMLCLCENSUS.IRP_PLC_MCMISLSTUPD%TYPE,
        PLC_MCMISLSTUSER   IRP_PRSMLCLCENSUS.IRP_PLC_MCMISLSTUSER%TYPE,
        PLC_ENTITYTYPE     IRP_PRSMLCLCENSUS.IRP_PLC_ENTITYTYPE%TYPE,
        PLC_INTERSTOPIND   IRP_PRSMLCLCENSUS.IRP_PLC_INTERSTOPIND%TYPE,
        PLC_INTRASTNOHAZ   IRP_PRSMLCLCENSUS.IRP_PLC_INTRASTNOHAZ%TYPE,
        PLC_INTRASTHAZMAT  IRP_PRSMLCLCENSUS.IRP_PLC_INTRASTHAZMAT%TYPE,
        PLC_MCSIPSTEP      IRP_PRSMLCLCENSUS.IRP_PLC_MCSIPSTEP%TYPE,
        PLC_MCSIPDATE      IRP_PRSMLCLCENSUS.IRP_PLC_MCSIPDATE%TYPE,
        PLC_MCS150LSTUPD   IRP_PRSMLCLCENSUS.IRP_PLC_MCS150LSTUPD%TYPE,
        PLC_FILEDATE       IRP_PRSMLCLCENSUS.IRP_PLC_FILEDATE%TYPE );               -- 20160323.1
--
    p_census_rec   census_rectyp;
    p_comp_rec     census_rectyp;
--
    F_PRISM_IN     UTL_FILE.FILE_TYPE;
--
--
    PRISMLine      VARCHAR2(500);
    iErrCode       NUMBER;                                                        -- 20160129.1
    sErrTxt        VARCHAR2(200);                                                 -- 20160129.1
--
    sRunFileNm           VARCHAR2(100);                                           -- 20160128.1
    sRunDirNm            VARCHAR2(100);                                           -- 20160205.1
--
    iRecCnt              PLS_INTEGER;
    iInsCnt              PLS_INTEGER;
    iUpdCnt              PLS_INTEGER;
    iDupCnt              PLS_INTEGER;                                             -- 20160129.1
    iRejCnt              PLS_INTEGER;                                             -- 20160203.1
    iBlkCnt              PLS_INTEGER;
    iCol                 PLS_INTEGER;                                             -- 20160203.1
    iLen                 PLS_INTEGER;                                             -- 20160203.1
    iEnd                 PLS_INTEGER;                                             -- 20160502.1
    bUpdFlag             BOOLEAN;
    s_inst_name          VARCHAR2(20);                                            -- 20160810.1
    s_host_name          VARCHAR2(70);                                            -- 20160810.1
    s_version            VARCHAR2(20);                                            -- 20160810.1
    sEmailMsg            VARCHAR2( 32000 );                                       -- 20160202.1
    iEmailMax  CONSTANT  PLS_INTEGER := 31500;                                    -- 20160202.1
    sCRLF      CONSTANT  VARCHAR2(5) := '<br>';                                   -- 20160203.1
    sPrismBase CONSTANT  VARCHAR2(20) := 'PRISM_BASELINE_';                       -- 20160502.1
    sBaselineDate        VARCHAR2(10);                                            -- 20160502.1
    bBaseline            BOOLEAN;                                                 -- 20160502.1
--
--
BEGIN
--
    bBaseLine := FALSE;                   -- BASELINE file processing flag        -- 20160502.1
    IF sDirName IS NULL THEN              --                                      -- 20160205.1
        sRunDirNm  := 'PRISM_DATA_IN';      -- Default directory if NONE is given   -- 20160205.1
    ELSE                                  --                                      -- 20160205.1
        sRunDirNm  := sDirName;             --                                      -- 20160205.1
    END IF;                               --                                      -- 20160205.1
--
    IF sFileName IS NULL THEN             -- Default to SYSDATE if no filename    -- 20160128.1
        sRunFileNm := TO_CHAR( SYSDATE, 'YYYYMMDD' ) || '.CEN';                     -- 20160128.1
        p_census_rec.PLC_FILEDATE := TRUNC( SYSDATE );                              -- 20160323.1
    ELSE                                  -- Is this a daily file or BASELINE     -- 20160128.1
        iCol := INSTR( UPPER( sFileName ), sPrismBase ); -- Is this a BASELINE?     -- 20160502.1
        IF iCol > 0 THEN                    -- Yes, then attempt to process it      -- 20160502.1
            sRunFileNm := sFileName;                                                  -- 20160413.1
            iEnd := INSTR( UPPER( sFileName ), '.', -1 );  -- find file extension     -- 20160502.1
            IF iEnd > 0 THEN                  -- We NEED to find the end!             -- 20160502.1
                iLen := LENGTH( sPrismBase );   -- Beg length of "header"               -- 20160504.1
                sBaseLineDate := SUBSTR( sFileName, iCol + iLen, iEnd - ( iCol + iLen ) ); -- 20160504.1
                BEGIN                                                                   -- 20160413.1
                    p_census_rec.PLC_FILEDATE := TO_DATE( sBaseLineDate, 'YYYYMMDD' );    -- 20160413.1
                EXCEPTION                                                               -- 20160413.1
                    WHEN OTHERS THEN                                                      -- 20160504.1
                        p_census_rec.PLC_FILEDATE := NULL;                                  -- 20160413.1
                END;                                                                    -- 20160413.1
            ELSE                                                                      -- 20160502.1
                p_census_rec.PLC_FILEDATE := NULL;                                      -- 20160413.1
            END IF;                                                                   -- 20160413.1
            EXECUTE IMMEDIATE( 'TRUNCATE TABLE IRP_PRSMLCLCENSUS' );                  -- 20160504.1
        ELSE                                                                        -- 20160413.1
            sRunFileNm := sFileName;                                                  -- 20160128.1
            iCol := INSTR( sFileName, '.' );                                          -- 20160323.1
            IF iCol = 0 THEN                                                          -- 20160323.1
                p_census_rec.PLC_FILEDATE := NULL;                                      -- 20160323.1
            ELSE                                                                      -- 20160323.1
                BEGIN                                                                   -- 20160323.1
                    p_census_rec.PLC_FILEDATE := TO_DATE( SUBSTR( sFileName, 1, ( iCol - 1 ) ), 'YYYYMMDD' );   -- 20160323.1
                EXCEPTION                                                               -- 20160323.1
                    WHEN VALUE_ERROR THEN                                                 -- 20160323.1
                        p_census_rec.PLC_FILEDATE := NULL;                                  -- 20160323.1
                END;                                                                    -- 20160323.1
            END IF;                                                                   -- 20160323.1
        END IF;                                                                     -- 20160413.1
    END IF;                                                                       -- 20160128.1
--
    IRP_SYSUTILITY.IRP_INSTINFO( s_inst_name, s_host_name, s_version  );          -- 20160810.1
    sEmailMsg  := 'Start IRP_READPRISMFILES,IRP_CENSUSFILE( ' || sRunDirNm || ', ' || sRunFileNm || ' )  Run on ' ||
                                TO_CHAR( SYSDATE, 'MM/DD/YYYY' ) || ' at ' || TO_CHAR( SYSDATE, 'HH24:MI:SS' ) || sCRLF || sCRLF;

    IF s_inst_name IS NOT NULL THEN
        sEmailMsg := sEmailMsg || '* Oracle ' || s_version || ' Database ( ' || s_inst_name || ' ) running on ' || s_host_name || sCRLF || sCRLF;
    END IF;
--
    F_PRISM_IN := UTL_FILE.FOPEN( sRunDirNm, sRunFileNm, 'R' );                   -- 20160203.1
--
    iRecCnt     := 0;
    iInsCnt     := 0;
    iUpdCnt     := 0;
    iDupCnt     := 0;                                                             -- 20160129.1
    iRejCnt     := 0;                                                             -- 20160203.1
    iBlkCnt     := 0;
--
    LOOP
        BEGIN
            UTL_FILE.GET_LINE( F_PRISM_IN, PRISMLine );
            IF LTRIM( RTRIM( SUBSTR( PRISMLine, 1, 100 ) ) ) IS NULL THEN
                iBlkCnt := iBlkCnt + 1;
            ELSE
                iRecCnt := iRecCnt + 1;
--
                iCol := 1;
                iLen := 1;
        p_census_rec.PLC_FILTYP           := SUBSTR( PRISMLine, iCol, iLen );
                iCol := iCol + iLen;
--
--        iLen := 1;                      -- PLACEHOLDER
        p_census_rec.PLC_MCSIPFLG         := SUBSTR( PRISMLine, iCol, iLen );
                iCol := iCol + iLen;
--
                iLen := 7;
                p_census_rec.PLC_USDOTNO          := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 18;
                p_census_rec.PLC_ICCNO            := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 1;
                p_census_rec.PLC_TINTYP           := SUBSTR( PRISMLine, iCol, iLen );
                iCol := iCol + iLen;
--
                iLen := 9;
                p_census_rec.PLC_TIN              := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 55;
                p_census_rec.PLC_COMPANY          := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
--        iLen := 55;
                p_census_rec.PLC_DBA              := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 30;
                p_census_rec.PLC_ADDSTREET        := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 25;
                p_census_rec.PLC_ADDCITY          := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 2;
                p_census_rec.PLC_ADDSTATE         := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 5;
                p_census_rec.PLC_ADDZIP           := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
--        iLen := 5;
                p_census_rec.PLC_ADDZIPEXT        := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 3;
                p_census_rec.PLC_ADDCOUNTY        := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 10;
                p_census_rec.PLC_CONPHONE         := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 30;
                p_census_rec.PLC_MAILSTREET       := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 25;
                p_census_rec.PLC_MAILCITY         := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 2;
                p_census_rec.PLC_MAILSTATE        := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 5;
                p_census_rec.PLC_MAILZIP          := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
--        iLen := 5;                      PLACEHOLDER
                p_census_rec.PLC_MAILZIPEXT       := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 3;
                p_census_rec.PLC_MAILCOUNTY       := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 1;
                p_census_rec.PLC_CARRIERSTAT      := SUBSTR( PRISMLine, iCol, iLen );
                iCol := iCol + iLen;
--
                iLen := 8;
                IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL OR
                     LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) = '00000000' THEN      -- 20160129.1
                    p_census_rec.PLC_MCMISCREDAT := NULL;
                ELSE
                    p_census_rec.PLC_MCMISCREDAT      := TO_DATE( SUBSTR( PRISMLine, iCol, iLen ), 'YYYYMMDD' );         -- 20151027
                END IF;
                iCol := iCol + iLen;
--
--        iLen := 8;                       PLACEHOLDER
                IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL OR
                     LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) = '00000000' THEN      -- 20160129.1
                    p_census_rec.PLC_DATEADDED := NULL;
                ELSE
                    p_census_rec.PLC_DATEADDED := TO_DATE( SUBSTR( PRISMLine, iCol, iLen ), 'YYYYMMDD' );
                END IF;
                iCol := iCol + iLen;
--
--        iLen := 8;                       PLACEHOLDER
                IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL OR
                     LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) = '00000000'  THEN
                    p_census_rec.PLC_MCMISLSTUPD := NULL;
                ELSE
                    p_census_rec.PLC_MCMISLSTUPD := TO_DATE( SUBSTR( PRISMLine, iCol, iLen ), 'YYYYMMDD' );
                END IF;
                iCol := iCol + iLen;
--
--        iLen := 8;                       PLACEHOLDER
                p_census_rec.PLC_MCMISLSTUSER     := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                iCol := iCol + iLen;
--
                iLen := 1;
                p_census_rec.PLC_ENTITYTYPE       := SUBSTR( PRISMLine, iCol, iLen );
                iCol := iCol + iLen;
--
--        iLen := 1;                       PLACEHOLDER
                p_census_rec.PLC_INTERSTOPIND     := SUBSTR( PRISMLine, iCol, iLen );
                iCol := iCol + iLen;
--
                iCol := iCol + 1;                  -- SKIP over 1 column (FILLER)
--        iLen := 1;                       PLACEHOLDER
                p_census_rec.PLC_INTRASTNOHAZ     := SUBSTR( PRISMLine, iCol, iLen );
                iCol := iCol + iLen;
--
--        iLen := 1;                       PLACEHOLDER
                p_census_rec.PLC_INTRASTHAZMAT    := SUBSTR( PRISMLine, iCol, iLen );
                iCol := iCol + iLen;
--
                iLen := 2;
                IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL THEN
                    p_census_rec.PLC_MCSIPSTEP := NULL;
                ELSE
                    p_census_rec.PLC_MCSIPSTEP := TO_NUMBER( SUBSTR( PRISMLine, iCol, iLen ) );
                END IF;
                iCol := iCol + iLen;
--
                iLen := 8;
                IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL OR
                     LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) = '00000000' THEN
                    p_census_rec.PLC_MCSIPDATE := NULL;
                ELSE
                    p_census_rec.PLC_MCSIPDATE := TO_DATE( SUBSTR( PRISMLine, iCol, iLen ), 'YYYYMMDD' );
                END IF;
                iCol := iCol + iLen;
--
                iCol := iCol + 28;                 -- Skip over 28 columns (FILLER)
--        iLen := 8;                       PLACEHOLDER
                IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL OR
                     LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) = '00000000' THEN
                    p_census_rec.PLC_MCS150LSTUPD := NULL;
                ELSE
                    p_census_rec.PLC_MCS150LSTUPD := TO_DATE( SUBSTR( PRISMLine, iCol, iLen ), 'YYYYMMDD' );
                END IF;
--
                BEGIN
                    INSERT INTO IRP_PRSMLCLCENSUS
                        ( IRP_PLC_FILTYP, IRP_PLC_MCSIPFLG, IRP_PLC_USDOTNO,
                            IRP_PLC_ICCNO, IRP_PLC_TINTYP, IRP_PLC_TIN,
                            IRP_PLC_COMPANY, IRP_PLC_DBA, IRP_PLC_ADDSTREET,
                            IRP_PLC_ADDCITY, IRP_PLC_ADDSTATE, IRP_PLC_ADDZIP,
                            IRP_PLC_ADDZIPEXT, IRP_PLC_ADDCOUNTY, IRP_PLC_CONPHONE,
                            IRP_PLC_MAILSTREET, IRP_PLC_MAILCITY, IRP_PLC_MAILSTATE,
                            IRP_PLC_MAILZIP, IRP_PLC_MAILZIPEXT, IRP_PLC_MAILCOUNTY,
                            IRP_PLC_CARRIERSTAT, IRP_PLC_MCMISCREDAT, IRP_PLC_DATEADDED,
                            IRP_PLC_MCMISLSTUPD, IRP_PLC_MCMISLSTUSER, IRP_PLC_ENTITYTYPE,
                            IRP_PLC_INTERSTOPIND, IRP_PLC_INTRASTNOHAZ, IRP_PLC_INTRASTHAZMAT,
                            IRP_PLC_MCSIPSTEP, IRP_PLC_MCSIPDATE, IRP_PLC_MCS150LSTUPD,
                            IRP_PLC_FILEDATE )                                                -- 20160323.1
                    VALUES
                        ( p_census_rec.PLC_FILTYP, p_census_rec.PLC_MCSIPFLG, p_census_rec.PLC_USDOTNO,
                            p_census_rec.PLC_ICCNO, p_census_rec.PLC_TINTYP, p_census_rec.PLC_TIN,
                            p_census_rec.PLC_COMPANY, p_census_rec.PLC_DBA, p_census_rec.PLC_ADDSTREET,
                            p_census_rec.PLC_ADDCITY, p_census_rec.PLC_ADDSTATE, p_census_rec.PLC_ADDZIP,
                            p_census_rec.PLC_ADDZIPEXT, p_census_rec.PLC_ADDCOUNTY, p_census_rec.PLC_CONPHONE,
                            p_census_rec.PLC_MAILSTREET, p_census_rec.PLC_MAILCITY, p_census_rec.PLC_MAILSTATE,
                            p_census_rec.PLC_MAILZIP, p_census_rec.PLC_MAILZIPEXT, p_census_rec.PLC_MAILCOUNTY,
                            p_census_rec.PLC_CARRIERSTAT, p_census_rec.PLC_MCMISCREDAT, p_census_rec.PLC_DATEADDED,
                            p_census_rec.PLC_MCMISLSTUPD, p_census_rec.PLC_MCMISLSTUSER, p_census_rec.PLC_ENTITYTYPE,
                            p_census_rec.PLC_INTERSTOPIND, p_census_rec.PLC_INTRASTNOHAZ, p_census_rec.PLC_INTRASTHAZMAT,
                            p_census_rec.PLC_MCSIPSTEP, p_census_rec.PLC_MCSIPDATE, p_census_rec.PLC_MCS150LSTUPD,
                            p_census_rec.PLC_FILEDATE );                                      -- 20160323.1
--
                    iInsCnt := iInsCnt + 1;
                EXCEPTION
                    WHEN DUP_VAL_ON_INDEX THEN
                        BEGIN
                            bUpdFlag := FALSE;
                            SELECT IRP_PLC_FILTYP, IRP_PLC_MCSIPFLG,
                                         IRP_PLC_ICCNO, IRP_PLC_TINTYP, IRP_PLC_TIN,
                                         IRP_PLC_COMPANY, IRP_PLC_DBA, IRP_PLC_ADDSTREET,
                                         IRP_PLC_ADDCITY, IRP_PLC_ADDSTATE, IRP_PLC_ADDZIP,
                                         IRP_PLC_ADDZIPEXT, IRP_PLC_ADDCOUNTY, IRP_PLC_CONPHONE,
                                         IRP_PLC_MAILSTREET, IRP_PLC_MAILCITY, IRP_PLC_MAILSTATE,
                                         IRP_PLC_MAILZIP, IRP_PLC_MAILZIPEXT, IRP_PLC_MAILCOUNTY,
                                         IRP_PLC_CARRIERSTAT, IRP_PLC_MCMISCREDAT, IRP_PLC_DATEADDED,
                                         IRP_PLC_MCMISLSTUPD, IRP_PLC_MCMISLSTUSER, IRP_PLC_ENTITYTYPE,
                                         IRP_PLC_INTERSTOPIND, IRP_PLC_INTRASTNOHAZ, IRP_PLC_INTRASTHAZMAT,
                                         IRP_PLC_MCSIPSTEP, IRP_PLC_MCSIPDATE, IRP_PLC_MCS150LSTUPD,
                                         IRP_PLC_FILEDATE                                                                       -- 20160323.1
                                INTO p_comp_rec.PLC_FILTYP, p_comp_rec.PLC_MCSIPFLG,
                                         p_comp_rec.PLC_ICCNO, p_comp_rec.PLC_TINTYP, p_comp_rec.PLC_TIN,
                                         p_comp_rec.PLC_COMPANY, p_comp_rec.PLC_DBA, p_comp_rec.PLC_ADDSTREET,
                                         p_comp_rec.PLC_ADDCITY, p_comp_rec.PLC_ADDSTATE, p_comp_rec.PLC_ADDZIP,
                                         p_comp_rec.PLC_ADDZIPEXT, p_comp_rec.PLC_ADDCOUNTY, p_comp_rec.PLC_CONPHONE,
                                         p_comp_rec.PLC_MAILSTREET, p_comp_rec.PLC_MAILCITY, p_comp_rec.PLC_MAILSTATE,
                                         p_comp_rec.PLC_MAILZIP, p_comp_rec.PLC_MAILZIPEXT, p_comp_rec.PLC_MAILCOUNTY,
                                         p_comp_rec.PLC_CARRIERSTAT, p_comp_rec.PLC_MCMISCREDAT, p_comp_rec.PLC_DATEADDED,
                                         p_comp_rec.PLC_MCMISLSTUPD, p_comp_rec.PLC_MCMISLSTUSER, p_comp_rec.PLC_ENTITYTYPE,
                                         p_comp_rec.PLC_INTERSTOPIND, p_comp_rec.PLC_INTRASTNOHAZ, p_comp_rec.PLC_INTRASTHAZMAT,
                                         p_comp_rec.PLC_MCSIPSTEP, p_comp_rec.PLC_MCSIPDATE, p_comp_rec.PLC_MCS150LSTUPD,
                                         p_comp_rec.PLC_FILEDATE                                                                -- 20160323.1
                                FROM IRP_PRSMLCLCENSUS
                             WHERE IRP_PLC_USDOTNO = p_census_rec.PLC_USDOTNO;
--
                            p_comp_rec.PLC_USDOTNO := p_census_rec.PLC_USDOTNO;
                            IF p_census_rec.PLC_FILTYP = p_comp_rec.PLC_FILTYP AND
                                 p_census_rec.PLC_MCSIPFLG = p_comp_rec.PLC_MCSIPFLG AND
                                 p_census_rec.PLC_ICCNO = p_comp_rec.PLC_ICCNO AND
                                 p_census_rec.PLC_TINTYP = p_comp_rec.PLC_TINTYP AND
                                 p_census_rec.PLC_TIN = p_comp_rec.PLC_TIN AND
                                 p_census_rec.PLC_COMPANY = p_comp_rec.PLC_COMPANY AND
                                 p_census_rec.PLC_DBA = p_comp_rec.PLC_DBA AND
                                 p_census_rec.PLC_ADDSTREET = p_comp_rec.PLC_ADDSTREET AND
                                 p_census_rec.PLC_ADDCITY = p_comp_rec.PLC_ADDCITY AND
                                 p_census_rec.PLC_ADDSTATE = p_comp_rec.PLC_ADDSTATE AND
                                 p_census_rec.PLC_ADDZIP = p_comp_rec.PLC_ADDZIP AND
                                 p_census_rec.PLC_ADDZIPEXT = p_comp_rec.PLC_ADDZIPEXT AND
                                 p_census_rec.PLC_ADDCOUNTY = p_comp_rec.PLC_ADDCOUNTY AND
                                 p_census_rec.PLC_CONPHONE = p_comp_rec.PLC_CONPHONE AND
                                 p_census_rec.PLC_MAILSTREET = p_comp_rec.PLC_MAILSTREET AND
                                 p_census_rec.PLC_MAILCITY = p_comp_rec.PLC_MAILCITY AND
                                 p_census_rec.PLC_MAILSTATE = p_comp_rec.PLC_MAILSTATE AND
                                 p_census_rec.PLC_MAILZIP = p_comp_rec.PLC_MAILZIP AND
                                 p_census_rec.PLC_MAILZIPEXT = p_comp_rec.PLC_MAILZIPEXT AND
                                 p_census_rec.PLC_MAILCOUNTY = p_comp_rec.PLC_MAILCOUNTY AND
                                 p_census_rec.PLC_CARRIERSTAT = p_comp_rec.PLC_CARRIERSTAT AND
                                 p_census_rec.PLC_MCMISCREDAT = p_comp_rec.PLC_MCMISCREDAT AND
                                 p_census_rec.PLC_DATEADDED = p_comp_rec.PLC_DATEADDED AND
                                 p_census_rec.PLC_MCMISLSTUPD = p_comp_rec.PLC_MCMISLSTUPD AND
                                 p_census_rec.PLC_MCMISLSTUSER = p_comp_rec.PLC_MCMISLSTUSER AND
                                 p_census_rec.PLC_ENTITYTYPE = p_comp_rec.PLC_ENTITYTYPE AND
                                 p_census_rec.PLC_INTERSTOPIND = p_comp_rec.PLC_INTERSTOPIND AND
                                 p_census_rec.PLC_INTRASTNOHAZ = p_comp_rec.PLC_INTRASTNOHAZ AND
                                 p_census_rec.PLC_INTRASTHAZMAT = p_comp_rec.PLC_INTRASTHAZMAT AND
                                 p_census_rec.PLC_MCSIPSTEP = p_comp_rec.PLC_MCSIPSTEP AND
                                 p_census_rec.PLC_MCSIPDATE = p_comp_rec.PLC_MCSIPDATE AND
                                 p_census_rec.PLC_MCS150LSTUPD = p_comp_rec.PLC_MCS150LSTUPD AND
                                 p_census_rec.PLC_FILEDATE = p_comp_rec.PLC_FILEDATE THEN       -- 20160323.1
--                   p_census_rec = p_comp_rec THEN
                                iDupCnt := iDupCnt + 1;                                         -- 20160129.1
                            ELSE
                                UPDATE IRP_PRSMLCLCENSUS
                                     SET IRP_PLC_FILTYP =  p_census_rec.PLC_FILTYP,
                                             IRP_PLC_MCSIPFLG = p_census_rec.PLC_MCSIPFLG,
                                             IRP_PLC_ICCNO = p_census_rec.PLC_ICCNO,
                                             IRP_PLC_TINTYP = p_census_rec.PLC_TINTYP,
                                             IRP_PLC_TIN = p_census_rec.PLC_TIN,
                                             IRP_PLC_COMPANY = p_census_rec.PLC_COMPANY,
                                             IRP_PLC_DBA = p_census_rec.PLC_DBA,
                                             IRP_PLC_ADDSTREET = p_census_rec.PLC_ADDSTREET,
                                             IRP_PLC_ADDCITY = p_census_rec.PLC_ADDCITY,
                                             IRP_PLC_ADDSTATE = p_census_rec.PLC_ADDSTATE,
                                             IRP_PLC_ADDZIP = p_census_rec.PLC_ADDZIP,
                                             IRP_PLC_ADDZIPEXT = p_census_rec.PLC_ADDZIPEXT,
                                             IRP_PLC_ADDCOUNTY = p_census_rec.PLC_ADDCOUNTY,
                                             IRP_PLC_CONPHONE = p_census_rec.PLC_CONPHONE,
                                             IRP_PLC_MAILSTREET = p_census_rec.PLC_MAILSTREET,
                                             IRP_PLC_MAILCITY = p_census_rec.PLC_MAILCITY,
                                             IRP_PLC_MAILSTATE = p_census_rec.PLC_MAILSTATE,
                                             IRP_PLC_MAILZIP = p_census_rec.PLC_MAILZIP,
                                             IRP_PLC_MAILZIPEXT = p_census_rec.PLC_MAILZIPEXT,
                                             IRP_PLC_MAILCOUNTY = p_census_rec.PLC_MAILCOUNTY,
                                             IRP_PLC_CARRIERSTAT = p_census_rec.PLC_CARRIERSTAT,
                                             IRP_PLC_MCMISCREDAT = p_census_rec.PLC_MCMISCREDAT,
                                             IRP_PLC_DATEADDED = p_census_rec.PLC_DATEADDED,
                                             IRP_PLC_MCMISLSTUPD = p_census_rec.PLC_MCMISLSTUPD,
                                             IRP_PLC_MCMISLSTUSER = p_census_rec.PLC_MCMISLSTUSER,
                                             IRP_PLC_ENTITYTYPE = p_census_rec.PLC_ENTITYTYPE,
                                             IRP_PLC_INTERSTOPIND = p_census_rec.PLC_INTERSTOPIND,
                                             IRP_PLC_INTRASTNOHAZ = p_census_rec.PLC_INTRASTNOHAZ,
                                             IRP_PLC_INTRASTHAZMAT = p_census_rec.PLC_INTRASTHAZMAT,
                                             IRP_PLC_MCSIPSTEP = p_census_rec.PLC_MCSIPSTEP,
                                             IRP_PLC_MCSIPDATE = p_census_rec.PLC_MCSIPDATE,
                                             IRP_PLC_MCS150LSTUPD = p_census_rec.PLC_MCS150LSTUPD,
                                             IRP_PLC_FILEDATE = p_census_rec.PLC_FILEDATE             -- 20160323.1
                                 WHERE IRP_PLC_USDOTNO = p_census_rec.PLC_USDOTNO;
                                iUpdCnt := iUpdCnt + 1;
                            END IF;
                        END;
                END;
                END IF;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
--          UTL_FILE.PUT_LINE( FILE_DEBUG, 'End-of-File on Input file: ' || sFileName || ', Records = ' || TO_CHAR( iRecCnt ) );
                    GOTO CLOSE_IN;
                WHEN OTHERS THEN
                    iErrCode := SQLCODE;
                    sErrTxt  := SUBSTR( SQLERRM, 1, 200 );
                    iRejCnt  := iRejCnt + 1;
                    SYS.DBMS_OUTPUT.PUT_LINE( '*ERROR* on Line: ' || TO_CHAR( iRecCnt + iBlkCnt ) || ', Column: ' || TO_CHAR( iCol ) || ', Field Length: ' || TO_CHAR( iLen ) );
                    SYS.DBMS_OUTPUT.PUT_LINE( 'USDOT #: ' || p_census_rec.PLC_USDOTNO || ', Errant Data: ' || SUBSTR( PRISMLine, iCol, iLen ) );
                    SYS.DBMS_OUTPUT.PUT_LINE( SUBSTR( PRISMLine, 1, 200 ) );
                    SYS.DBMS_OUTPUT.PUT_LINE( TO_CHAR( iErrCode ) );
                    SYS.DBMS_OUTPUT.PUT_LINE( sErrTxt );
--
                    IF LENGTH( sEmailMsg ) < iEmailMax THEN                               -- 20160203.1
                        sEmailMsg := sEmailMsg || '*ERROR* ' || TO_CHAR( iErrCode ) || ' on Line: ' || TO_CHAR( iRecCnt + iBlkCnt ) || ', Column: ' ||
                                                 TO_CHAR( iCol ) || ', Field Length: ' || TO_CHAR( iLen ) || sCRLF ||
                                                 'USDOT #: ' || p_census_rec.PLC_USDOTNO || ', Errant Data: ' || SUBSTR( PRISMLine, iCol, iLen ) || sCRLF ||
                                                 '[' || PRISMLine || ']' || sCRLF || sErrTxt || sCRLF || sCRLF;
                    ELSE                                        --                                            -- 20160203.1
                        SYS.DBMS_OUTPUT.PUT_LINE( '** Overflowed E-mail buffer' );                               -- 20160203.1
                    END IF;                                 --                                                -- 20160203.1
            END;
        END LOOP;
--
<<CLOSE_IN>>
        UTL_FILE.FCLOSE( F_PRISM_IN );
        COMMIT;
        SYS.DBMS_OUTPUT.PUT_LINE( 'Records Read in : ' || TO_CHAR( iRecCnt ) );
        SYS.DBMS_OUTPUT.PUT_LINE( 'Records Inserted: ' || TO_CHAR( iInsCnt ) );
        SYS.DBMS_OUTPUT.PUT_LINE( 'Records Updated : ' || TO_CHAR( iUpdCnt ) );
        SYS.DBMS_OUTPUT.PUT_LINE( 'Duplicate Recs:   ' || TO_CHAR( iDupCnt ) );
        SYS.DBMS_OUTPUT.PUT_LINE( 'Rejected Recs:    ' || TO_CHAR( iRejCnt ) );
        SYS.DBMS_OUTPUT.PUT_LINE( 'Blank Lines Read: ' || TO_CHAR( iBlkCnt ) );
--
        IF LENGTH( sEmailMsg ) < iEmailMax THEN                               -- 20160203.1
            sEmailMsg := sEmailMsg || 'Records Read in : ' || TO_CHAR( iRecCnt ) || sCRLF ||
                                                                'Records Inserted: ' || TO_CHAR( iInsCnt ) || sCRLF ||
                                                                'Records Updated : ' || TO_CHAR( iUpdCnt ) || sCRLF ||
                                                                'Duplicate Recs:   ' || TO_CHAR( iDupCnt ) || sCRLF ||
                                                                'REJECTED Reccs:   ' || TO_CHAR( iRejCnt ) || sCRLF ||
                                                                'Blank Lines Read: ' || TO_CHAR( iBlkCnt ) || sCRLF || sCRLF ||
                                     '*End* IRP_READPRISMFILES,IRP_CENSUSFILE( ' || sRunDirNm || ', ' || sRunFileNm || ' )  Stop on ' ||
                                     TO_CHAR( SYSDATE, 'MM/DD/YYYY' ) || ' at ' || TO_CHAR( SYSDATE, 'HH24:MI:SS' ) || sCRLF;
        ELSE                                        --                                            -- 20160203.1
            DBMS_OUTPUT.PUT_LINE( '** Overflowed E-mail buffer' );                               -- 20160203.1
        END IF;                                 --                                                -- 20160203.1
--
--
        MAIL_TOOLS.sendmail( smtp_server => IRP_PRINT.GET_SMTP_SERVER,          -- 20150817.1
                                                 smtp_server_port => 25,                            -- 20150817.1
                                                 from_name => 'irp@marylandmva.com',                -- 20150817.1
                                                 to_name => 'sschneider1@mdot.state.md.us',      -- 20160202.1
                                                 cc_name => 'rlockard@mdot.state.md.us',                                   -- 20150817.1
                                                 bcc_name => NULL,                                  -- 20150817.1
                                                 subject => 'ReadPRISMFiles job status',              -- 20150817.1
                                                 MESSAGE => TO_CLOB( sEmailMsg ),  -- 20150817.1
                                                 priority => NULL,                                  -- 20150817.1
                                                 filename => NULL,                                  -- 20150817.1
                                                 binaryfile => EMPTY_BLOB ( ),                      -- 20150817.1
                                                 DEBUG      => 0 );                                 -- 20150817.1
--
END IRP_CENSUSFILE;
--
-- =======================================================================================
--
--
PROCEDURE IRP_TARGETFILE( sDirName IN VARCHAR2, sFileName IN VARCHAR2 ) IS
--
    TYPE carrier_rectyp IS RECORD (
        PTC_FILTYP         IRP_PRSMLCLTGTCAR.IRP_PTC_FILTYP%TYPE,
        PTC_RECTYP         IRP_PRSMLCLTGTCAR.IRP_PTC_RECTYP%TYPE,
        PTC_USDOTNO        IRP_PRSMLCLTGTCAR.IRP_PTC_USDOTNO%TYPE,
        PTC_COMPANY        IRP_PRSMLCLTGTCAR.IRP_PTC_COMPANY%TYPE,
        PTC_DBA            IRP_PRSMLCLTGTCAR.IRP_PTC_DBA%TYPE,
        PTC_ADDSTREET      IRP_PRSMLCLTGTCAR.IRP_PTC_ADDSTREET%TYPE,
        PTC_ADDCITY        IRP_PRSMLCLTGTCAR.IRP_PTC_ADDCITY%TYPE,
        PTC_ADDSTATE       IRP_PRSMLCLTGTCAR.IRP_PTC_ADDSTATE%TYPE,
        PTC_ADDZIP         IRP_PRSMLCLTGTCAR.IRP_PTC_ADDZIP%TYPE,
        PTC_ADDZIPEXT      IRP_PRSMLCLTGTCAR.IRP_PTC_ADDZIPEXT%TYPE,
        PTC_ADDCOUNTY      IRP_PRSMLCLTGTCAR.IRP_PTC_ADDCOUNTY%TYPE,
        PTC_MCSIPSTEP      IRP_PRSMLCLTGTCAR.IRP_PTC_MCSIPSTEP%TYPE,
        PTC_MCSIPDATE      IRP_PRSMLCLTGTCAR.IRP_PTC_MCSIPDATE%TYPE,
        PTC_PRSMCRFCREDAT  IRP_PRSMLCLTGTCAR.IRP_PTC_PRSMCRFCREDAT%TYPE,
        PTC_MCMISLSTUPD    IRP_PRSMLCLTGTCAR.IRP_PTC_MCMISLSTUPD%TYPE,
        PTC_MCMISLSTUSER   IRP_PRSMLCLTGTCAR.IRP_PTC_MCMISLSTUSER%TYPE,
        PTC_CARTGTIND      IRP_PRSMLCLTGTCAR.IRP_PTC_CARTGTIND%TYPE,
        PTC_CARTGTDATE     IRP_PRSMLCLTGTCAR.IRP_PTC_CARTGTDATE%TYPE,
        PTC_FILEDATE       IRP_PRSMLCLTGTCAR.IRP_PTC_FILEDATE%TYPE );               -- 20160323.1
--
    TYPE vehicle_rectyp IS RECORD (
        PTV_FILTYP         IRP_PRSMLCLTGTVEH.IRP_PTV_FILTYP%TYPE,
        PTV_RECTYP         IRP_PRSMLCLTGTVEH.IRP_PTV_RECTYP%TYPE,
        PTV_USDOTNO        IRP_PRSMLCLTGTVEH.IRP_PTV_USDOTNO%TYPE,
        PTV_VIN            IRP_PRSMLCLTGTVEH.IRP_PTV_VIN%TYPE,
        PTV_PLATENBR       IRP_PRSMLCLTGTVEH.IRP_PTV_PLATENBR%TYPE,
        PTV_JURISCODE      IRP_PRSMLCLTGTVEH.IRP_PTV_JURISCODE%TYPE,
        PTV_EFFECTIVEDATE  IRP_PRSMLCLTGTVEH.IRP_PTV_EFFECTIVEDATE%TYPE,
        PTV_EXPIRATION     IRP_PRSMLCLTGTVEH.IRP_PTV_EXPIRATION%TYPE,
        PTV_VEHMAKE        IRP_PRSMLCLTGTVEH.IRP_PTV_VEHMAKE%TYPE,
        PTV_VEHYEAR        IRP_PRSMLCLTGTVEH.IRP_PTV_VEHYEAR%TYPE,
        PTV_PRSMCRFCREDAT  IRP_PRSMLCLTGTVEH.IRP_PTV_PRSMCRFCREDAT%TYPE,
        PTV_PRSMVHFCREDAT  IRP_PRSMLCLTGTVEH.IRP_PTV_PRSMVHFCREDAT%TYPE,
        PTV_VEHTGTIND      IRP_PRSMLCLTGTVEH.IRP_PTV_VEHTGTIND%TYPE,
        PTV_VEHTGTDATE     IRP_PRSMLCLTGTVEH.IRP_PTV_VEHTGTDATE%TYPE,
        PTV_FILEDATE       IRP_PRSMLCLTGTVEH.IRP_PTV_FILEDATE%TYPE );               -- 20160323.1
--
    p_carrier_rec        carrier_rectyp;
    p_compcar_rec        carrier_rectyp;
--
    p_vehicle_rec        vehicle_rectyp;
    p_compveh_rec        vehicle_rectyp;
--
    F_PRISM_IN     UTL_FILE.FILE_TYPE;
    iErrCode       NUMBER;                                                        -- 20160129.1
    sErrTxt        VARCHAR2(200);                                                 -- 20160129.1
--
    PRISMLine      VARCHAR2(500);
--
    sRunFileNm           VARCHAR2(100);                                           -- 20160128.1
    sRunDirNm            VARCHAR2(100);                                           -- 20160205.1
--
    iRecCnt              PLS_INTEGER;
    iCarRecCnt           PLS_INTEGER;                                             -- 20160129.1
    iInsCarCnt           PLS_INTEGER;
    iUpdCarCnt           PLS_INTEGER;
    iDupCarCnt           PLS_INTEGER;                                             -- 20160129.1
    iRejCarCnt           PLS_INTEGER;                                             -- 20160204.1
    iDelCarCnt           PLS_INTEGER;                                             -- 20160323.1
    iVehRecCnt           PLS_INTEGER;                                             -- 20160129.1
    iInsVehCnt           PLS_INTEGER;
    iUpdVehCnt           PLS_INTEGER;
    iDupVehCnt           PLS_INTEGER;                                             -- 20160129.1
    iRejVehCnt           PLS_INTEGER;                                             -- 20160204.1
    iDelVehCnt           PLS_INTEGER;                                             -- 20160323.1
    iBlkCnt              PLS_INTEGER;
    iUnkCnt              PLS_INTEGER;
    bUpdFlag             BOOLEAN;
    dFileDate            DATE;                                                    -- 20160323.1
--
    iCol                 PLS_INTEGER;                                             -- 20160204.1
    iLen                 PLS_INTEGER;                                             -- 20160204.1
    sEmailMsg            VARCHAR2( 32000 );                                       -- 20160202.1
    iEmailMax  CONSTANT  PLS_INTEGER := 31500;                                    -- 20160202.1
    sCRLF      CONSTANT  VARCHAR2(5) := '<br>';                                   -- 20160203.1
    sUSDOTno             VARCHAR2(20);                                            -- 20160204.1
--
--
BEGIN
--
--
    IF sDirName IS NULL THEN              --                                      -- 20160205.1
        sRunDirNm  := 'PRISM_DATA_IN';      -- Default directory if NONE is given   -- 20160205.1
    ELSE                                  --                                      -- 20160205.1
        sRunDirNm  := sDirName;             --                                      -- 20160205.1
    END IF;                               --                                      -- 20160205.1
--
    IF sFileName IS NULL THEN                                                     -- 20160128.1
        sRunFileNm := TO_CHAR( SYSDATE, 'YYYYMMDD' ) || '.TGT';                     -- 20160128.1
        p_carrier_rec.PTC_FILEDATE := TRUNC( SYSDATE );                             -- 20160323.1
        p_vehicle_rec.PTV_FILEDATE := TRUNC( SYSDATE );                             -- 20160323.1
        dFileDate  := TRUNC( SYSDATE );                                             -- 20160323.1
    ELSE                                                                          -- 20160128.1
        sRunFileNm := sFileName;                                                    -- 20160128.1
        iCol := INSTR( sFileName, '.' );                                            -- 20160323.1
        IF iCol = 0 THEN                                                            -- 20160323.1
            p_carrier_rec.PTC_FILEDATE := NULL;                                       -- 20160323.1
            p_vehicle_rec.PTV_FILEDATE := NULL;                                       -- 20160323.1
            dFileDate := NULL;                                                        -- 20160323.1
        ELSE                                                                        -- 20160323.1
            BEGIN                                                                     -- 20160323.1
                p_carrier_rec.PTC_FILEDATE := TO_DATE( SUBSTR( sFileName, 1, ( iCol - 1 ) ), 'YYYYMMDD' );   -- 20160323.1
                p_vehicle_rec.PTV_FILEDATE := TO_DATE( SUBSTR( sFileName, 1, ( iCol - 1 ) ), 'YYYYMMDD' );   -- 20160323.1
                dFileDate := TO_DATE( SUBSTR( sFileName, 1, ( iCol - 1 ) ), 'YYYYMMDD' );                    -- 20160323.1
            EXCEPTION                                                                 -- 20160323.1
                WHEN VALUE_ERROR THEN                                                   -- 20160323.1
                    p_carrier_rec.PTC_FILEDATE := NULL;                                   -- 20160323.1
                    p_vehicle_rec.PTV_FILEDATE := NULL;                                   -- 20160323.1
                    dFileDate := NULL;                                                    -- 20160323.1
            END;                                                                      -- 20160323.1
         END IF;                                                                    -- 20160323.1
     END IF;                                                                      -- 20160128.1
--
    sEmailMsg  := 'Start IRP_READPRISMFILES,IRP_TARGETFILE( ' || sRunDirNm || ', ' || sRunFileNm || ' )  Run on ' ||
                                TO_CHAR( SYSDATE, 'MM/DD/YYYY' ) || ' at ' || TO_CHAR( SYSDATE, 'HH24:MI:SS' ) || sCRLF || sCRLF;
--
        F_PRISM_IN := UTL_FILE.FOPEN( sRunDirNm, sRunFileNm, 'R' );                 -- 20160203.1
--
        iRecCnt        := 0;
        iCarRecCnt     := 0;                                                        -- 20160129.1
        iInsCarCnt     := 0;
        iUpdCarCnt     := 0;
        iDupCarCnt     := 0;                                                        -- 20160129.1
        iRejCarCnt     := 0;                                                        -- 20160204.1
        iDelCarCnt     := 0;                                                        -- 20160323.1
        iVehRecCnt     := 0;                                                        -- 20160129.1
        iInsVehCnt     := 0;
        iUpdVehCnt     := 0;
        iDupVehCnt     := 0;                                                        -- 20160129.1
        iRejVehCnt     := 0;                                                        -- 20160204.1
        iDelVehCnt     := 0;                                                        -- 20160323.1
        iBlkCnt        := 0;
        iUnkCnt        := 0;
--
        LOOP
            BEGIN
                UTL_FILE.GET_LINE( F_PRISM_IN, PRISMline );
                IF LTRIM( RTRIM( SUBSTR( PRISMLine, 1, 100 ) ) ) IS NULL THEN
                    iBlkCnt := iBlkCnt + 1;
                ELSE
                    iRecCnt := iRecCnt + 1;                 -- Count all records          -- 20160129.1
                    iCol    := 1;                                                         -- 20160204.1
                    iLen    := 1;                                                         -- 20160204.1
                    p_carrier_rec.PTC_RECTYP        := SUBSTR( PRISMLine, 2, iLen );      -- 20160204.1
--
                    IF p_carrier_rec.PTC_RECTYP = 'C' THEN                                -- Carrier Target Record
                        iCarRecCnt := iCarRecCnt + 1;                                       -- 20160129.1
--
                        p_carrier_rec.PTC_FILTYP           := SUBSTR( PRISMLine, iCol, iLen );
                        iCol := iCol + iLen;                                                -- 20160204.1
                        iCol := iCol + 1;                  -- Adjust for RECORD TYPE read   -- 20160204.1
--
                        iLen := 7;
                        p_carrier_rec.PTC_USDOTNO          := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 55;
                        p_carrier_rec.PTC_COMPANY          := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
--            iLen := 55                       PLACEHOLDER                      -- 20160204.1
                        p_carrier_rec.PTC_DBA              := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 30;                                                         -- 20160204.1
                        p_carrier_rec.PTC_ADDSTREET        := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 25;                                                         -- 20160204.1
                        p_carrier_rec.PTC_ADDCITY          := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 3;                                                          -- 20160204.1
                        p_carrier_rec.PTC_ADDCOUNTY        := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 2;                                                          -- 20160204.1
                        p_carrier_rec.PTC_ADDSTATE         := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 5;                                                         -- 20160204.1
                        p_carrier_rec.PTC_ADDZIP           := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
--            iLen := 5;                       PLACEHOLDER                      -- 20160204.1
                        p_carrier_rec.PTC_ADDZIPEXT        := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 2;                                                         -- 20160204.1
                        IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL THEN
                            p_carrier_rec.PTC_MCSIPSTEP := NULL;
                        ELSE
                            p_carrier_rec.PTC_MCSIPSTEP := TO_NUMBER( SUBSTR( PRISMLine, iCol, iLen ) );
                        END IF;
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 8;                                                          -- 20160204.1
                        IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL THEN
                            p_carrier_rec.PTC_MCSIPDATE := NULL;
                        ELSE
                            p_carrier_rec.PTC_MCSIPDATE := TO_DATE( SUBSTR( PRISMLine, iCol, iLen ), 'YYYYMMDD' );
                        END IF;
                        iCol := iCol + iLen;                                                -- 20160204.1
                        iCol := iCol + 21;                    -- 21 character FILLER        -- 20160204.1
--
--            iLen := 8;                       PLACEHOLDER                      -- 20160204.1
                        IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL THEN
                            p_carrier_rec.PTC_PRSMCRFCREDAT := NULL;
                        ELSE
                            p_carrier_rec.PTC_PRSMCRFCREDAT := TO_DATE( SUBSTR( PRISMLine, iCol, iLen ), 'YYYYMMDD' );         -- 20151027
                        END IF;
                        iCol := iCol + iLen;                                                -- 20160204.1
--
--            iLen := 8;                       PLACEHOLDER                      -- 20160204.1
                        IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL THEN
                            p_carrier_rec.PTC_MCMISLSTUPD := NULL;
                        ELSE
                            p_carrier_rec.PTC_MCMISLSTUPD := TO_DATE( SUBSTR( PRISMLine, iCol, iLen ), 'YYYYMMDD' );
                        END IF;
                        iCol := iCol + iLen;                                                -- 20160204.1
--
--            iLen := 8;                       PLACEHOLDER                      -- 20160204.1
                        p_carrier_rec.PTC_MCMISLSTUSER     := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 1;                                                          -- 20160204.1
                        p_carrier_rec.PTC_CARTGTIND        := SUBSTR( PRISMLine, iCol, iLen );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 8;                                                          -- 20160204.1
                        IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL OR
                             LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) = '00000000' THEN
                            p_carrier_rec.PTC_CARTGTDATE := NULL;
                        ELSE
                            p_carrier_rec.PTC_CARTGTDATE := TO_DATE( SUBSTR( PRISMLine, iCol, iLen ), 'YYYYMMDD' );
                        END IF;
--
                        BEGIN
                            INSERT INTO IRP_PRSMLCLTGTCAR
                                ( IRP_PTC_FILTYP, IRP_PTC_RECTYP, IRP_PTC_USDOTNO,
                                    IRP_PTC_COMPANY, IRP_PTC_DBA, IRP_PTC_ADDSTREET,
                                    IRP_PTC_ADDCITY, IRP_PTC_ADDSTATE, IRP_PTC_ADDZIP,
                                    IRP_PTC_ADDZIPEXT, IRP_PTC_ADDCOUNTY, IRP_PTC_MCSIPSTEP,
                                    IRP_PTC_MCSIPDATE, IRP_PTC_PRSMCRFCREDAT, IRP_PTC_MCMISLSTUPD,
                                    IRP_PTC_MCMISLSTUSER, IRP_PTC_CARTGTIND, IRP_PTC_CARTGTDATE,
                                    IRP_PTC_FILEDATE )                                                                          -- 20160323.1
                            VALUES
                                ( p_carrier_rec.PTC_FILTYP, p_carrier_rec.PTC_RECTYP, p_carrier_rec.PTC_USDOTNO,
                                    p_carrier_rec.PTC_COMPANY, p_carrier_rec.PTC_DBA, p_carrier_rec.PTC_ADDSTREET,
                                    p_carrier_rec.PTC_ADDCITY, p_carrier_rec.PTC_ADDSTATE, p_carrier_rec.PTC_ADDZIP,
                                    p_carrier_rec.PTC_ADDZIPEXT, p_carrier_rec.PTC_ADDCOUNTY, p_carrier_rec.PTC_MCSIPSTEP,
                                    p_carrier_rec.PTC_MCSIPDATE, p_carrier_rec.PTC_PRSMCRFCREDAT, p_carrier_rec.PTC_MCMISLSTUPD,
                                    p_carrier_rec.PTC_MCMISLSTUSER, p_carrier_rec.PTC_CARTGTIND, p_carrier_rec.PTC_CARTGTDATE,
                                    p_carrier_rec.PTC_FILEDATE );                                                               -- 20160323.1
--
                            iInsCarCnt := iInsCarCnt + 1;
                        EXCEPTION
                            WHEN DUP_VAL_ON_INDEX THEN
                                BEGIN
                                    bUpdFlag := FALSE;
                                    SELECT IRP_PTC_FILTYP, IRP_PTC_RECTYP,
                                                 IRP_PTC_COMPANY, IRP_PTC_DBA, IRP_PTC_ADDSTREET,
                                                 IRP_PTC_ADDCITY, IRP_PTC_ADDSTATE, IRP_PTC_ADDZIP,
                                                 IRP_PTC_ADDZIPEXT, IRP_PTC_ADDCOUNTY, IRP_PTC_MCSIPSTEP,
                                                 IRP_PTC_MCSIPDATE, IRP_PTC_PRSMCRFCREDAT, IRP_PTC_MCMISLSTUPD,
                                                 IRP_PTC_MCMISLSTUSER, IRP_PTC_CARTGTIND, IRP_PTC_CARTGTDATE,
                                                 IRP_PTC_FILEDATE                                                                     -- 20160323.1
                                        INTO p_compcar_rec.PTC_FILTYP, p_compcar_rec.PTC_RECTYP,
                                                 p_compcar_rec.PTC_COMPANY, p_compcar_rec.PTC_DBA, p_compcar_rec.PTC_ADDSTREET,
                                                 p_compcar_rec.PTC_ADDCITY, p_compcar_rec.PTC_ADDSTATE, p_compcar_rec.PTC_ADDZIP,
                                                 p_compcar_rec.PTC_ADDZIPEXT, p_compcar_rec.PTC_ADDCOUNTY, p_compcar_rec.PTC_MCSIPSTEP,
                                                 p_compcar_rec.PTC_MCSIPDATE, p_compcar_rec.PTC_PRSMCRFCREDAT, p_compcar_rec.PTC_MCMISLSTUPD,
                                                 p_compcar_rec.PTC_MCMISLSTUSER, p_compcar_rec.PTC_CARTGTIND, p_compcar_rec.PTC_CARTGTDATE,
                                                 p_compcar_rec.PTC_FILEDATE                                                           -- 20160323.1
                                        FROM IRP_PRSMLCLTGTCAR
                                     WHERE IRP_PTC_USDOTNO = p_carrier_rec.PTC_USDOTNO;
--
                                    p_compcar_rec.PTC_USDOTNO       := p_carrier_rec.PTC_USDOTNO;
                                    IF p_carrier_rec.PTC_FILTYP        = p_compcar_rec.PTC_FILTYP AND
                                         p_carrier_rec.PTC_RECTYP        = p_compcar_rec.PTC_RECTYP AND
                                         p_carrier_rec.PTC_COMPANY       = p_compcar_rec.PTC_COMPANY AND
                                         p_carrier_rec.PTC_DBA           = p_compcar_rec.PTC_DBA AND
                                         p_carrier_rec.PTC_ADDSTREET     = p_compcar_rec.PTC_ADDSTREET AND
                                         p_carrier_rec.PTC_ADDCITY       = p_compcar_rec.PTC_ADDCITY AND
                                         p_carrier_rec.PTC_ADDSTATE      = p_compcar_rec.PTC_ADDSTATE AND
                                         p_carrier_rec.PTC_ADDZIP        = p_compcar_rec.PTC_ADDZIP AND
                                         p_carrier_rec.PTC_ADDZIPEXT     = p_compcar_rec.PTC_ADDZIPEXT AND
                                         p_carrier_rec.PTC_ADDCOUNTY     = p_compcar_rec.PTC_ADDCOUNTY AND
                                         p_carrier_rec.PTC_MCSIPSTEP     = p_compcar_rec.PTC_MCSIPSTEP AND
                                         p_carrier_rec.PTC_MCSIPDATE     = p_compcar_rec.PTC_MCSIPDATE AND
                                         p_carrier_rec.PTC_PRSMCRFCREDAT = p_compcar_rec.PTC_PRSMCRFCREDAT AND
                                         p_carrier_rec.PTC_MCMISLSTUPD   = p_compcar_rec.PTC_MCMISLSTUPD AND
                                         p_carrier_rec.PTC_MCMISLSTUSER  = p_compcar_rec.PTC_MCMISLSTUSER AND
                                         p_carrier_rec.PTC_CARTGTIND     = p_compcar_rec.PTC_CARTGTIND AND
                                         p_carrier_rec.PTC_CARTGTDATE    = p_compcar_rec.PTC_CARTGTDATE AND
                                         p_carrier_rec.PTC_FILEDATE      = p_compcar_rec.PTC_FILEDATE THEN         -- 20160323.1
--
                                        iDupCarCnt := iDupCarCnt + 1;                               -- 20160129.1
                                    ELSE
                                        UPDATE IRP_PRSMLCLTGTCAR
                                             SET IRP_PTC_FILTYP =  p_carrier_rec.PTC_FILTYP,
                                                     IRP_PTC_RECTYP = p_carrier_rec.PTC_RECTYP,
                                                     IRP_PTC_COMPANY = p_carrier_rec.PTC_COMPANY,
                                                     IRP_PTC_DBA = p_carrier_rec.PTC_DBA,
                                                     IRP_PTC_ADDSTREET = p_carrier_rec.PTC_ADDSTREET,
                                                     IRP_PTC_ADDCITY = p_carrier_rec.PTC_ADDCITY,
                                                     IRP_PTC_ADDSTATE = p_carrier_rec.PTC_ADDSTATE,
                                                     IRP_PTC_ADDZIP = p_carrier_rec.PTC_ADDZIP,
                                                     IRP_PTC_ADDZIPEXT = p_carrier_rec.PTC_ADDZIPEXT,
                                                     IRP_PTC_ADDCOUNTY = p_carrier_rec.PTC_ADDCOUNTY,
                                                     IRP_PTC_MCSIPSTEP = p_carrier_rec.PTC_MCSIPSTEP,
                                                     IRP_PTC_MCSIPDATE = p_carrier_rec.PTC_MCSIPDATE,
                                                     IRP_PTC_PRSMCRFCREDAT = p_carrier_rec.PTC_PRSMCRFCREDAT,
                                                     IRP_PTC_MCMISLSTUPD = p_carrier_rec.PTC_MCMISLSTUPD,
                                                     IRP_PTC_MCMISLSTUSER = p_carrier_rec.PTC_MCMISLSTUSER,
                                                     IRP_PTC_CARTGTIND = p_carrier_rec.PTC_CARTGTIND,
                                                     IRP_PTC_CARTGTDATE = p_carrier_rec.PTC_CARTGTDATE,
                                                     IRP_PTC_FILEDATE = p_carrier_rec.PTC_FILEDATE        -- 20160323.1
                                         WHERE IRP_PTC_USDOTNO = p_carrier_rec.PTC_USDOTNO;
                                        iUpdCarCnt := iUpdCarCnt + 1;
                                    END IF;
                                END;
                        END;
                    ELSIF p_carrier_rec.PTC_RECTYP = 'V' THEN                             -- Vehicle Target Record
                        iVehRecCnt := iVehRecCnt + 1;                                       -- 20160129.1
                        p_vehicle_rec.PTV_RECTYP           := p_carrier_rec.PTC_RECTYP;
--
                        p_vehicle_rec.PTV_FILTYP           := SUBSTR( PRISMLine, iCol, iLen );
                        iCol := iCol + iLen;                                                -- 20160204.1
                        iCol := iCol + 1;                  -- Adjust for RECORD TYPE read   -- 20160204.1
--
                        iLen := 7;                                                          -- 20160204.1
                        p_vehicle_rec.PTV_USDOTNO          := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 17;                                                         -- 20160204.1
                        p_vehicle_rec.PTV_VIN              := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 10;                                                         -- 20160204.1
                        p_vehicle_rec.PTV_PLATENBR         := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 2;                                                          -- 20160204.1
                        p_vehicle_rec.PTV_JURISCODE        := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 8;                                                          -- 20160204.1
                        IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL THEN
                            p_vehicle_rec.PTV_EFFECTIVEDATE := NULL;
                        ELSE
                            p_vehicle_rec.PTV_EFFECTIVEDATE := TO_DATE( SUBSTR( PRISMLine, iCol, iLen ), 'YYYYMMDD' );
                        END IF;
                        iCol := iCol + iLen;                                                -- 20160204.1
--
--            iLen := 8;                       PLACEHOLDER                      -- 20160204.1
                        IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL THEN
                            p_vehicle_rec.PTV_EXPIRATION := NULL;
                        ELSE
                            p_vehicle_rec.PTV_EXPIRATION := TO_DATE( SUBSTR( PRISMLine, iCol, iLen ), 'YYYYMMDD' );         -- 20151027
                        END IF;
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 4;                                                          -- 20160204.1
                        p_vehicle_rec.PTV_VEHMAKE          := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
--            iLen := 4;                       PLACEHOLDER                      -- 20160204.1
                        p_vehicle_rec.PTV_VEHYEAR         := LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 8;                                                          -- 20160204.1
                        IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL OR
                             LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) = '00000000' THEN
                            p_vehicle_rec.PTV_PRSMCRFCREDAT := NULL;
                        ELSE
                            p_vehicle_rec.PTV_PRSMCRFCREDAT := TO_DATE( SUBSTR( PRISMLine, iCol, iLen ), 'YYYYMMDD' );
                        END IF;
                        iCol := iCol + iLen;                                                -- 20160204.1
--
--            iLen := 8;                       PLACEHOLDER                      -- 20160204.1
                        IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL OR
                             LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) = '00000000' THEN
                            p_vehicle_rec.PTV_PRSMVHFCREDAT := NULL;
                        ELSE
                            p_vehicle_rec.PTV_PRSMVHFCREDAT := TO_DATE( SUBSTR( PRISMLine, iCol, iLen ), 'YYYYMMDD' );
                        END IF;
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 1;                                                          -- 20160204.1
                        p_vehicle_rec.PTV_VEHTGTIND        := SUBSTR( PRISMLine, iCol, iLen );
                        iCol := iCol + iLen;                                                -- 20160204.1
--
                        iLen := 8;                                                          -- 20160204.1
                        IF LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) IS NULL OR
                             LTRIM( RTRIM( SUBSTR( PRISMLine, iCol, iLen ) ) ) = '00000000' THEN
                            p_vehicle_rec.PTV_VEHTGTDATE := NULL;
                        ELSE
                            p_vehicle_rec.PTV_VEHTGTDATE := TO_DATE( SUBSTR( PRISMLine, iCol, iLen ), 'YYYYMMDD' );
                        END IF;
--
                        BEGIN
                            INSERT INTO IRP_PRSMLCLTGTVEH
                                ( IRP_PTV_FILTYP, IRP_PTV_RECTYP, IRP_PTV_USDOTNO,
                                    IRP_PTV_VIN, IRP_PTV_PLATENBR, IRP_PTV_JURISCODE,
                                    IRP_PTV_EFFECTIVEDATE, IRP_PTV_EXPIRATION, IRP_PTV_VEHMAKE,
                                    IRP_PTV_VEHYEAR, IRP_PTV_PRSMCRFCREDAT, IRP_PTV_PRSMVHFCREDAT,
                                    IRP_PTV_VEHTGTIND, IRP_PTV_VEHTGTDATE, IRP_PTV_FILEDATE )     -- 20160323.1
                            VALUES
                                ( p_vehicle_rec.PTV_FILTYP, p_vehicle_rec.PTV_RECTYP, p_vehicle_rec.PTV_USDOTNO,
                                    p_vehicle_rec.PTV_VIN, p_vehicle_rec.PTV_PLATENBR, p_vehicle_rec.PTV_JURISCODE,
                                    p_vehicle_rec.PTV_EFFECTIVEDATE, p_vehicle_rec.PTV_EXPIRATION, p_vehicle_rec.PTV_VEHMAKE,
                                    p_vehicle_rec.PTV_VEHYEAR, p_vehicle_rec.PTV_PRSMCRFCREDAT, p_vehicle_rec.PTV_PRSMVHFCREDAT,
                                    p_vehicle_rec.PTV_VEHTGTIND, p_vehicle_rec.PTV_VEHTGTDATE, p_vehicle_rec.PTV_FILEDATE );    -- 20160323.1
--
                            iInsVehCnt := iInsVehCnt + 1;
                        EXCEPTION
                            WHEN DUP_VAL_ON_INDEX THEN
                                BEGIN
                                    bUpdFlag := FALSE;
                                    SELECT IRP_PTV_FILTYP, IRP_PTV_RECTYP,
                                                 IRP_PTV_VIN, IRP_PTV_PLATENBR, IRP_PTV_JURISCODE,
                                                 IRP_PTV_EFFECTIVEDATE, IRP_PTV_EXPIRATION, IRP_PTV_VEHMAKE,
                                                 IRP_PTV_VEHYEAR, IRP_PTV_PRSMCRFCREDAT, IRP_PTV_PRSMVHFCREDAT,
                                                 IRP_PTV_VEHTGTIND, IRP_PTV_VEHTGTDATE, IRP_PTV_FILEDATE          -- 20160323.1
                                        INTO p_compveh_rec.PTV_FILTYP, p_compveh_rec.PTV_RECTYP,
                                                 p_compveh_rec.PTV_VIN, p_compveh_rec.PTV_PLATENBR, p_compveh_rec.PTV_JURISCODE,
                                                 p_compveh_rec.PTV_EFFECTIVEDATE, p_compveh_rec.PTV_EXPIRATION, p_compveh_rec.PTV_VEHMAKE,
                                                 p_compveh_rec.PTV_VEHYEAR, p_compveh_rec.PTV_PRSMCRFCREDAT, p_compveh_rec.PTV_PRSMVHFCREDAT,
                                                 p_compveh_rec.PTV_VEHTGTIND, p_compveh_rec.PTV_VEHTGTDATE, p_compveh_rec.PTV_FILEDATE     -- 20160323.1
                                        FROM IRP_PRSMLCLTGTVEH
                                     WHERE IRP_PTV_USDOTNO   = p_vehicle_rec.PTV_USDOTNO
                                         AND IRP_PTV_VIN       = p_vehicle_rec.PTV_VIN
                                         AND IRP_PTV_PLATENBR  = p_vehicle_rec.PTV_PLATENBR
                                         AND IRP_PTV_JURISCODE = p_vehicle_rec.PTV_JURISCODE;
--
                                    p_compveh_rec.PTV_USDOTNO       := p_vehicle_rec.PTV_USDOTNO;
                                    IF p_vehicle_rec.PTV_FILTYP        = p_compveh_rec.PTV_FILTYP AND
                                         p_vehicle_rec.PTV_RECTYP        = p_compveh_rec.PTV_RECTYP AND
                                         p_vehicle_rec.PTV_EFFECTIVEDATE = p_compveh_rec.PTV_EFFECTIVEDATE AND
                                         p_vehicle_rec.PTV_EXPIRATION    = p_compveh_rec.PTV_EXPIRATION AND
                                         p_vehicle_rec.PTV_VEHMAKE       = p_compveh_rec.PTV_VEHMAKE AND
                                         p_vehicle_rec.PTV_VEHYEAR       = p_compveh_rec.PTV_VEHYEAR AND
                                         p_vehicle_rec.PTV_PRSMCRFCREDAT = p_compveh_rec.PTV_PRSMCRFCREDAT AND
                                         p_vehicle_rec.PTV_PRSMVHFCREDAT = p_compveh_rec.PTV_PRSMVHFCREDAT AND
                                         p_vehicle_rec.PTV_VEHTGTIND     = p_compveh_rec.PTV_VEHTGTIND AND
                                         p_vehicle_rec.PTV_VEHTGTDATE    = p_compveh_rec.PTV_VEHTGTDATE AND
                                         p_vehicle_rec.PTV_FILEDATE      = p_compveh_rec.PTV_FILEDATE THEN         -- 20160323.1
--
                                        iDupVehCnt := iDupVehCnt + 1;                               -- 20160129.1
                                    ELSE
                                        UPDATE IRP_PRSMLCLTGTVEH
                                             SET IRP_PTV_FILTYP =  p_vehicle_rec.PTV_FILTYP,
                                                     IRP_PTV_RECTYP = p_vehicle_rec.PTV_RECTYP,
                                                     IRP_PTV_EFFECTIVEDATE = p_vehicle_rec.PTV_EFFECTIVEDATE,
                                                     IRP_PTV_EXPIRATION = p_vehicle_rec.PTV_EXPIRATION,
                                                     IRP_PTV_VEHMAKE = p_vehicle_rec.PTV_VEHMAKE,
                                                     IRP_PTV_VEHYEAR = p_vehicle_rec.PTV_VEHYEAR,
                                                     IRP_PTV_PRSMCRFCREDAT = p_vehicle_rec.PTV_PRSMCRFCREDAT,
                                                     IRP_PTV_PRSMVHFCREDAT = p_vehicle_rec.PTV_PRSMVHFCREDAT,
                                                     IRP_PTV_VEHTGTIND = p_vehicle_rec.PTV_VEHTGTIND,
                                                     IRP_PTV_VEHTGTDATE = p_vehicle_rec.PTV_VEHTGTDATE,
                                                     IRP_PTV_FILEDATE = p_vehicle_rec.PTV_FILEDATE        -- 20160323.1
                                         WHERE IRP_PTV_USDOTNO = p_vehicle_rec.PTV_USDOTNO
                                             AND IRP_PTV_VIN       = p_vehicle_rec.PTV_VIN
                                             AND IRP_PTV_PLATENBR  = p_vehicle_rec.PTV_PLATENBR
                                             AND IRP_PTV_JURISCODE = p_vehicle_rec.PTV_JURISCODE;
                                        iUpdVehCnt := iUpdVehCnt + 1;
                                    END IF;
                                END;
                        END;
                    ELSE
                        iUnkCnt := iUnkCnt + 1;
                    END IF;
                END IF;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
--          UTL_FILE.PUT_LINE( FILE_DEBUG, 'End-of-File on Input file: ' || sFileName || ', Records = ' || TO_CHAR( iRecCnt ) );
                    GOTO CLOSE_IN;
                WHEN OTHERS THEN
                    iErrCode := SQLCODE;
                    sErrTxt  := SUBSTR( SQLERRM, 1, 200 );
--
                    IF p_carrier_rec.PTC_RECTYP = 'C' THEN
                        sUSDOTno := 'USDOT #: (' || p_carrier_rec.PTC_RECTYP || ') ' || p_carrier_rec.PTC_USDOTNO;
                        iRejCarCnt  := iRejCarCnt + 1;                                      -- 20160204.1
                    ELSIF p_carrier_rec.PTC_RECTYP = 'V' THEN
                        sUSDOTno := 'USDOT #: (' || p_carrier_rec.PTC_RECTYP || ') ' || p_vehicle_rec.PTV_USDOTNO;
                        iRejVehCnt  := iRejVehCnt + 1;                                      -- 20160204.1
                    ELSE
                        sUSDOTno := 'Unknown Record Type: (' || p_carrier_rec.PTC_RECTYP || ') ';
                        iUnkCnt  := iUnkCnt + 1;                                      -- 20160204.1
                    END IF;
                    DBMS_OUTPUT.PUT_LINE( '*ERROR* on Line: ' || TO_CHAR( iRecCnt + iBlkCnt ) || ', Column: ' || TO_CHAR( iCol ) || ', Field Length: ' || TO_CHAR( iLen ) );
                    DBMS_OUTPUT.PUT_LINE( sUSDOTno || ', Errant Data: ' || SUBSTR( PRISMLine, iCol, iLen ) );
                    DBMS_OUTPUT.PUT_LINE( SUBSTR( PRISMLine, 1, 200 ) );
                    DBMS_OUTPUT.PUT_LINE( TO_CHAR( iErrCode ) );
                    DBMS_OUTPUT.PUT_LINE( sErrTxt );
--
                    IF LENGTH( sEmailMsg ) < iEmailMax THEN                               -- 20160203.1
--
                        sEmailMsg := sEmailMsg || '*ERROR* ' || TO_CHAR( iErrCode ) || ' on Line: ' || TO_CHAR( iRecCnt + iBlkCnt ) || ', Column: ' ||
                                                 TO_CHAR( iCol ) || ', Field Length: ' || TO_CHAR( iLen ) || sCRLF ||
                                                 sUSDOTno || ', Errant Data: ' || SUBSTR( PRISMLine, iCol, iLen ) || sCRLF ||
                                                 '[' || PRISMLine || ']' || sCRLF || sErrTxt || sCRLF || sCRLF;
                    ELSE                                        --                                            -- 20160203.1
                        DBMS_OUTPUT.PUT_LINE( '** Overflowed E-mail buffer' );                               -- 20160203.1
                    END IF;                                 --                                                -- 20160203.1
            END;
        END LOOP;
--
<<CLOSE_IN>>
        UTL_FILE.FCLOSE( F_PRISM_IN );
--
        IF dFileDate IS NOT NULL THEN                 -- Clear old target records               -- 20160323.1
            DELETE FROM IRP_PRSMLCLTGTCAR               --                                        -- 20160323.1
             WHERE IRP_PTC_FILEDATE < dFileDate;        --                                        -- 20160323.1
            iDelCarCnt := SQL%ROWCOUNT;                    -- Save number of records deleted         -- 20160323.1
--
            DELETE FROM IRP_PRSMLCLTGTVEH               --                                        -- 20160323.1
             WHERE IRP_PTV_FILEDATE < dFileDate;        --                                        -- 20160323.1
            iDelvehCnt := SQL%ROWCOUNT;                    -- Save number of records deleted         -- 20160323.1
        END IF;
--
        COMMIT;
        DBMS_OUTPUT.PUT_LINE( 'Total Records Read in          : ' || TO_CHAR( iRecCnt ) );
--
        DBMS_OUTPUT.PUT_LINE( 'Total Carrier Target Recs Read : ' || TO_CHAR( iCarRecCnt ) ); -- 20160129.1
        DBMS_OUTPUT.PUT_LINE( 'Carrier Target Records Inserted: ' || TO_CHAR( iInsCarCnt ) );
        DBMS_OUTPUT.PUT_LINE( 'Carrier Target Records Updated : ' || TO_CHAR( iUpdCarCnt ) );
        DBMS_OUTPUT.PUT_LINE( 'Duplicate Carrier Target Recs  : ' || TO_CHAR( iDupCarCnt ) ); -- 20160129.1
        DBMS_OUTPUT.PUT_LINE( 'REJECTED Carrier Target Recs   : ' || TO_CHAR( iRejCarCnt ) ); -- 20160129.1
        DBMS_OUTPUT.PUT_LINE( 'Carrier TARGET Records DELETED : ' || TO_CHAR( iDelCarCnt ) ); -- 20160323.1
--
        DBMS_OUTPUT.PUT_LINE( 'Total Vehicle Target Recs Read : ' || TO_CHAR( iVehRecCnt ) ); -- 20160129.1
        DBMS_OUTPUT.PUT_LINE( 'Vehicle Target Records Inserted: ' || TO_CHAR( iInsVehCnt ) ); -- 20160129.1
        DBMS_OUTPUT.PUT_LINE( 'Vehicle Target Records Updated : ' || TO_CHAR( iUpdVehCnt ) ); -- 20160129.1
        DBMS_OUTPUT.PUT_LINE( 'Duplicate Vehicle Target Recs  : ' || TO_CHAR( iDupVehCnt ) ); -- 20160129.1
        DBMS_OUTPUT.PUT_LINE( 'REJECTED Vehicle Target Recs   : ' || TO_CHAR( iRejVehCnt ) ); -- 20160129.1
        DBMS_OUTPUT.PUT_LINE( 'Vehicle TARGET Records DELETED : ' || TO_CHAR( iDelVehCnt ) ); -- 20160323.1
--
        DBMS_OUTPUT.PUT_LINE( 'Unknown Records Encountered    : ' || TO_CHAR( iUnkCnt ) );
        DBMS_OUTPUT.PUT_LINE( 'Blank Lines Read               : ' || TO_CHAR( iBlkCnt ) );
--
--
        IF LENGTH( sEmailMsg ) < iEmailMax THEN                               -- 20160203.1
            sEmailMsg := sEmailMsg || 'Records Read in : ' || TO_CHAR( iRecCnt ) || sCRLF || sCRLF ||
                                     'Carrier Target Records Read:     ' || TO_CHAR( iCarRecCnt ) || sCRLF ||
                                     'Carrier Target Records Inserted: ' || TO_CHAR( iInsCarCnt ) || sCRLF ||
                                     'Carrier Target Records Updated : ' || TO_CHAR( iUpdCarCnt ) || sCRLF ||
                                     'Duplicate Carrier Target Recs  : ' || TO_CHAR( iDupCarCnt ) || sCRLF ||
                                     'REJECTED Carrier Target Recs   : ' || TO_CHAR( iRejCarCnt ) || sCRLF ||
                                     'Carrier TARGET Records DELETED : ' || TO_CHAR( iDelCarCnt ) || sCRLF || sCRLF ||     -- 20160323.1
--
                                     'Total Vehicle Target Recs Read : ' || TO_CHAR( iVehRecCnt ) || sCRLF ||
                                     'Vehicle Target Records Inserted: ' || TO_CHAR( iInsVehCnt ) || sCRLF ||
                                     'Vehicle Target Records Updated : ' || TO_CHAR( iUpdVehCnt ) || sCRLF ||
                                     'Duplicate Vehicle Target Recs  : ' || TO_CHAR( iDupVehCnt ) || sCRLF ||
                                     'REJECTED Vehicle Target Recs   : ' || TO_CHAR( iRejVehCnt ) || sCRLF ||
                                     'Vehicle TARGET Records DELETED : ' || TO_CHAR( iDelVehCnt ) || sCRLF || sCRLF ||     -- 20160323.1
--
                                     'Unknown Records Encountered    : ' || TO_CHAR( iUnkCnt ) || sCRLF ||
                                     'Blank Lines Read: ' || TO_CHAR( iBlkCnt ) || sCRLF || sCRLF ||
                                     '*End* IRP_READPRISMFILES,IRP_IRP_TARGETFILE( ' || sRunDirNm || ', ' || sRunFileNm || ' )  Stop on ' ||
                                     TO_CHAR( SYSDATE, 'MM/DD/YYYY' ) || ' at ' || TO_CHAR( SYSDATE, 'HH24:MI:SS' ) || sCRLF;
        ELSE                                        --                                            -- 20160203.1
            DBMS_OUTPUT.PUT_LINE( '** Overflowed E-mail buffer' );                               -- 20160203.1
        END IF;                                 --                                                -- 20160203.1
--
--
        MAIL_TOOLS.sendmail( smtp_server => IRP_PRINT.GET_SMTP_SERVER,          -- 20150817.1
                                                 smtp_server_port => 25,                            -- 20150817.1
                                                 from_name => 'irp@marylandmva.com',                -- 20150817.1
                                                 to_name => 'sschneider1@mdot.state.md.us',      -- 20160202.1
                                                 cc_name => 'rlockard@mdot.state.md.us',                                   -- 20150817.1
                                                 bcc_name => NULL,                                  -- 20150817.1
                                                 subject => 'ReadPRISMFiles job status',              -- 20150817.1
                                                 MESSAGE => TO_CLOB( sEmailMsg ),  -- 20150817.1
                                                 priority => NULL,                                  -- 20150817.1
                                                 filename => NULL,                                  -- 20150817.1
                                                 binaryfile => EMPTY_BLOB ( ),                      -- 20150817.1
                                                 DEBUG      => 0 );                                 -- 20150817.1

        irp_process_target;                                                     -- 20160611.1
--
END IRP_TARGETFILE;

-- supporting functions

    FUNCTION set_db2flag (  sIrpNbr     IN VARCHAR2,
                            sTcar_id    IN VARCHAR2,
                            sVehcicleID IN VARCHAR2,
                            sTitleNbr   IN VARCHAR2,
                            sDelCode    IN VARCHAR2) RETURN VARCHAR2 IS

        -- do I populate irp_mvaflags before setting the flag in DB2?
        iID             NUMBER; -- this will be used by utility.log_stack to gather performace
                                -- information and error messages.
        sUserData         IRP_MVAFLAGS.IRP_MFG_USERDATA%TYPE;
        sTleFLgStatus   IRP_MVAFLAGS.IRP_MFG_STATUS%TYPE;
        sUnitCode         IRP_DELINQUENTCODE.IRP_DCO_UNITCODE%TYPE;
        sFlagDesc         IRP_DELINQUENTCODE.IRP_DCO_DELINQUENTNAME%TYPE;
        delseq        IRP_DELINQUENCIES.IRP_DEL_SEQ%TYPE;
        susp_src        IRP_DELINQUENCIES.IRP_DEL_SOURCE%TYPE;
        suspenddate     date := sysdate;
        iErrCode          number;
        sErrMsg           varchar2(254);
        iflagnbr          number; -- we are going to need to get
                                -- irp_mfg_flagnbr. There is a before insert
                                -- trigger on irp_mvaflags that increments the
                                -- nbr each time a duplicate vehicleid is inserted
                                -- into the table. ie: if vehicleid 123 has three
                                -- entries into irp_mvaflags, then irp_mfg_flagnbr
                                -- would = 3. We are going to use this to get the
                                -- max irp_mfg_flagnbr for a vehicleid and increment
                                -- by 1. Because the trigger tests to see if flagnbr
                                -- is null, we can safely populate this column with
                                -- no side effects.

    BEGIN
        BEGIN
            iID := utility.log_stack.create_entry(pUnit => $$PLSQL_UNIT,
                                                  pLine => $$PLSQL_LINE,
                                                  pStime => current_timestamp,
                                                  pParms => 'title# = ' || sTitleNbr || ' DelCode= ' || sDelCode);
        EXCEPTION WHEN OTHERS THEN
            -- send an error message
            sys.dbms_output.put_line('there is an error in log_stack');
        END;

        -- get sUnitCode and sFlagDesc if we fail to find the flag then
        -- return false.
        BEGIN
            SELECT IRP_DCO_UNITCODE, IRP_DCO_DELINQUENTNAME
            INTO sUnitCode, sFlagDesc
            FROM IRP.IRP_DELINQUENTCODE
            WHERE IRP_DCO_UNITCODE = sDelCode;
        EXCEPTION WHEN NO_DATA_FOUND THEN
            iErrCode := 1;
            sErrMsg  := '* ERROR * - Delinquency Code (' || sDelCode ||
                              ') not found in IRP_DELINQUENTCODE';
            utility.log_stack.end_entry(pLogId => iID,
                                        pEtime => current_timestamp,
                                        pResults => 'error on line ' || $$PLSQL_LINE || ' error ' || sErrMsg);
            RETURN( '-1' );
        WHEN OTHERS THEN
            iErrCode := SQLCODE;
            sErrMsg  := SUBSTR( SQLERRM, 1, 254 );
            utility.log_stack.end_entry(pLogId => iID,
                                        pEtime => current_timestamp,
                                        pResults => 'error on line ' || $$PLSQL_LINE || ' error ' || sErrMsg);
            RETURN( '-1' );
        END;


        -- set susp_src = 'F' for FMCSA
        susp_src := 'F';

        sUserData   := LPAD( TO_CHAR( sIrpNbr ), 6, '0' ) ||
                     ' ' || SUBSTR( LTRIM( RTRIM( NVL( susp_src, '?' ) ) ), 1, 1 );


        -- start the for loop to set the flag
        BEGIN
            IF  IRP_MVADB2.IRP_TLEFLAG_CREATE( vtitleno => sTitleNbr,
                                               dFlgDate => suspenddate,
                                               sUnitCode => sUnitCode,
                                               sUsrData => sUserData,
                                               sErrMsg => sErrMsg,
                                               vUid => user,
                                               vTerm => NULL ) THEN
              sTleFlgStatus := 'S';         -- how is this status flag used?
            ELSE
              sTleFlgStatus := 'T';         -- how is this status flag used?
            END IF;
        EXCEPTION WHEN OTHERS THEN
            sys.DBMS_OUTPUT.PUT_LINE('-1 ERROR IRP_MVADB2.IRP_TLEFLAG_CREATE' || sqlerrm);
            sErrMsg :=  substr(sqlerrm,1,254);
            utility.log_stack.end_entry(pLogId => iID,
                                        pResults => 'error on line ' || $$PLSQL_LINE || ' error ' || sErrMsg);
            RETURN '-1';
        END;
        --
        BEGIN

        -- step 1, get the max flag number for a vehicleid then
        -- increment iflagnbr

            begin
                select max(irp_mfg_flagnbr)
                into iflagnbr
                from irp_mvaflags
                where irp_mfg_vehicleid = sVehcicleID;

                iflagnbr := nvl(iflagnbr,0) + 1;        -- increment cnt. because max will
                                                        -- return null, we need to use nvl
            exception when no_data_found then
                iflagnbr := 0;          -- we did not find this vehicleid so
                                        -- set it to 0.
            end;

        -- insert the flag data.
            INSERT INTO IRP_MVAFLAGS
            ( IRP_MFG_VEHICLEID, IRP_MFG_FLAGNBR,
              IRP_MFG_UNITCODE,  IRP_MFG_FLAGDATE,
              IRP_MFG_USERDATA,  IRP_MFG_FLAGDESC,
              IRP_MFG_STATUS,    IRP_MFG_DELSEQ )
            ( select sVehcicleID,                   iflagnbr,
                    sUnitCode,                      suspenddate,
                    sUserData,                      sFlagDesc,
                    sTleFlgStatus,                  delseq
              from dual);

        -- if something goes wrong, so rollback the transactions
        -- and return false.
        EXCEPTION
          WHEN OTHERS THEN
            iErrCode := SQLCODE;
            sErrMsg  := SUBSTR( SQLERRM, 1, 254 );
            ROLLBACK;
            utility.log_stack.end_entry(pLogId => iID,
                                        pEtime => current_timestamp,
                                        pResults => 'error on line ' || $$PLSQL_LINE || ' error ' || sErrMsg);
            RETURN '-1';
        END;
        -- all done
        RETURN sTleFlgStatus;               -- we are all done, return Title flag status.
    END set_db2flag;                        -- function

function firp_veh_target return number IS
    x number; -- dummy variable
begin
    begin
        select count(*)
        into x
        from irp.irp_vehicles
        where irp_veh_target = 'Y';

    return x;
    exception when others then
        raise_application_error (-20001, 'fv add target raised error');
        return SQLCODE;
    end;
end;

function firp_remove_veh_target return number is
    begin
        update irp_vehicles vh
        set vh.irp_veh_target = 'N'
        where vh.irp_veh_target = 'Y'
            and vh.irp_veh_vin not in
                (select p.irp_ptv_vin
                 from irp.irp_prsmlcltgtveh p);
    return 0;
    exception when others then
        raise_application_error (-20002, 'fv add target raised error');
    return SQLCODE;
    end;

function firp_remove_veh_target(pTotalV number) RETURN number IS
    lCountTmp number;
    begin
        select count(*)
        into lCountTmp
        from irp_vehicles
        where irp_veh_target = 'Y';
        -- calc the number of vehicles removed
        return lCountTmp - pTotalV;
    exception when others then
     raise_application_error (-20003, 'fv add target raised error');
    return SQLCODE;
    end;

function firp_add_veh_target return number is
begin
/*
  update irp.irp_vehicles v1
  set irp_veh_target = 'Y'
  where irp_veh_vin in (
      select irp_ptv_vin
      from irp.IRP_PRSMLCLTGTVEH v,
         irp.IRP_PRSMLCLTGTCAR c,
         irp.IRP_PRISMMCSIP ps,
         irp.irp_vehicles vh
      where v.irp_ptv_usdotno = c.irp_ptc_usdotno
        and v.irp_ptv_vin = vh.irp_veh_vin
        and c.irp_ptc_mcsipstep = ps.irp_mcs_step
        and ps.irp_mcs_tgtroad = 'Y');
*/

    update irp.irp_vehicles set irp_veh_target = 'Y'
    where (irp_veh_vin, irp_veh_vinseq) IN
        (select irp_veh_vin, max(irp_veh_vinseq) irp_veh_vinseq
        from irp_vehicles v,
            irp_prsmlcltgtveh veh
        where v.irp_veh_vin = veh.irp_ptv_vin
        group by irp_veh_vin);

    --
    return 0;
exception when others then
    raise_application_error (-20004, 'fv add target raised error');
    return SQLCODE;
end;

function firp_add_veh_target(pTotalV number) return number is
    lCountTmp number;
    begin
        select count(*)
        into lCountTmp
        from irp_vehicles
        where irp_veh_target = 'Y';
        return lCountTmp - pTotalV;
    exception when others then
        /* log error */
        return SQLCODE;
    end;

    function firp_app_target return number is
    /*-- step 1: get a count of all irp_applicants*/
    /*-- that are targeted. for reporting*/
    -- 20160601.1
    lCountTmp number;
    BEGIN
        SELECT COUNT(*) INTO lCountTmp
    FROM irp_applicants WHERE irp_app_target = 'Y';
        return lCountTmp;
    EXCEPTION
    WHEN OTHERS THEN
    raise_application_error (-20005, 'fv add target raised error');
        return SQLCODE;
        /*-- log the error*/
    END;

    function firp_App_Removed return number is
    /*-- step 2: set all irp_app_target = 'N' where*/
    /*-- the usdot number does not exists in the prism*/
    /*-- target file. (if a usdot# is removed than this*/
    /*-- will clear it out.*/

  -- get a collection of carriers to be removed.
  cursor remove_cur is
    select irp_app_irpnbr
    from irp.irp_applicants
    where irp_app_target = 'Y'
      and irp_app_usdotno not in (
        select irp_ptc_usdotno
        from irp_prsmlcltgtcar);

    BEGIN                                                                       -- 20160601.1

    BEGIN
      for remove_rec in remove_cur
      loop
        update IRP_TARGETEDCARRIERS
        set irp_tcar_restoredate = sysdate,
            irp_tcar_activitydate = sysdate,
            irp_tcar_auduserid = user,
            irp_tcar_status = 'A'                                             -- carrier removed by DOT.
            where irp_tcar_irpnbr = remove_rec.irp_app_irpnbr;
      end loop;
    EXCEPTION when no_data_found then
      raise_application_error (-20006, 'firp_app_removed rasied error');
      return -1;
    END;

        UPDATE IRP_APPLICANTS a
        SET irp_app_target           = 'N'
        WHERE irp_app_target         = 'Y' /*-- this will limit the number of rows updated.*/
            AND a.irp_app_usdotno NOT IN
            ( SELECT irp_ptc_usdotno FROM IRP_PRSMLCLTGTCAR );
    RETURN 0;

    EXCEPTION
    WHEN OTHERS THEN
        raise_application_error (-20007, 'fv add target raised error');
        return SQLCODE;
    END;

    function firp_app_removed(pTotalApp number) return number is
    /*-- step 3: get a new count of all the targeted*/
    /*-- usdot#. this will tell us how many targeted*/
    /*-- applicants have been removed.*/

    lCountTmp number;
    BEGIN                                                                     -- 20160601.1
        SELECT COUNT(*) INTO lCountTmp FROM irp_applicants WHERE irp_app_target = 'Y';
        return lCountTmp - pTotalApp; /*-- calc the number removed.*/
    EXCEPTION
    WHEN OTHERS THEN
        /*-- log error*/
        return SQLCODE;
    END;

    function firp_add_app_target(pDelCode IN VARCHAR2) return number is
    /*-- step 4: for every targed usdot# set*/
    /*-- and regonly is = 'N' set targeted*/
    /*-- to 'Y'.*/
    sDelCode    VARCHAR2(4); --
    iMSCStep    NUMBER; -- we need the mscstep to record in irp.irp_targetedcarriers.
    lResults    VARCHAR2(2000);
    x           NUMBER;   -- just a dumb variable, it's only used to catch the retturn value
                            -- from utility.log_stack when an error is returned.
    BEGIN                                                                     -- 20160601.1
        sDelCode := pDelCode;
        UPDATE IRP_APPLICANTS a
        SET irp_app_target       = 'Y'
        WHERE a.irp_app_regonly  = 'N'
            AND a.irp_app_status  != 'T'
            AND a.irp_app_usdotno IN
            ( SELECT irp_ptc_usdotno FROM IRP_PRSMLCLTGTCAR
            );
        -- get a set of all new targed carries.
        -- call irp_targetedcarriers_api to insert carriers
        -- if there is an error return -1.
        for rec in (select irp_app_irpnbr, irp_app_usdotno
             from irp.irp_applicants
             where irp_app_target  = 'Y'
                 and irp_app_regonly = 'N'
                 and irp_app_status != 'T')         -- because we st the target flag to 'T' we don't need to
                                                    -- check irp_prsmlcltgtcar for targeted carriers.
        -- <FIXME> add in mscstep code. where am i going to get the code? in the 
        -- bellow line null is the holder for mscstep.
        
        LOOP
        
          -- GET THE MSCSTEP CODE.
          BEGIN
            SELECT irp_ptc_mcsipstep
            INTO iMSCStep
            FROM irp.irp_prsmlcltgtcar
            WHERE irp_ptc_usdotno = rec.irp_app_usdotno;
          END;
        
            if not IRP.IRP_TARGETEDCARRIERS_API.insert_targetedcarriers(rec.irp_app_irpnbr, iMSCStep, 'T') then
                -- log the error
                x := utility.log_stack.create_entry(pUnit  => 'IRP.IRP_TARGETEDCARRIERS_API.insert_targetedcarriers',
                                                                               pParms => rec.irp_app_irpnbr || ', T');
            end if;
        END LOOP;

        -- use the cursor tgt_cur to update db2.
        FOR tgt_rec in tgt_cur
        LOOP
            sys.dbms_output.put_line('running set_db2flag with ' || tgt_rec.irp_tcar_irpnbr);

            lResults := set_db2flag(sIrpNbr     => tgt_rec.irp_tcar_irpnbr,
                                    sTcar_id    => tgt_rec.irp_tcar_id,
                                    sVehcicleID => tgt_rec.irp_flv_vehicleid,
                                    sTitleNbr   => tgt_rec.IRP_VEH_TITLENBR,
                                    sDelCode    => sDelCode);

        END LOOP;

        -- Call irp0499 to run report. Once that is done, switch
        -- irp_app_status to 'T'
        -- CALL IRP0499 REPORT. Change parameters for production. Create
        -- a lookup table to grab parms.
        begin
            --
            -- set the flag that we are rready to run the report. Note, the report is
            -- setup to run in OEM.
            --
            update irp.irp_jobs
            set done = 'Y'
            where name = 'IRP_PROCESSTARGETED';
            commit;

        exception when others then

            dbms_output.put_line('error in running report irp0499 ' || SQLERRM);
        end;

        return 0;
    EXCEPTION
    WHEN OTHERS THEN
        raise_application_error (-20008, 'fv add target raised error');
        return -1;
    end;


    function firp_add_app_target(pTotalApp number, pTargetAppRemoved number) return number is
    /*-- Step 5: get a new count of all of the*/
    /*-- targed applicants. This will give us*/
    /*-- the number of new targed usdot#'s.*/
    lCountTmp number;
    BEGIN                                                                     -- 20160601.1
        SELECT COUNT(*) INTO lCountTmp FROM irp_applicants WHERE irp_app_target = 'Y';
        return lCountTmp - pTotalApp - pTargetAppRemoved; /*-- calc the number added.*/
    EXCEPTION
    WHEN OTHERS THEN
        raise_application_error (-20009, 'fv add target raised error');
        return SQLCODE;
    END;

    function firp_fv_target return number is
    /*-- step 6: now we are going to do the same thing for*/
    /*-- irp_fleetvehciles. get the total number targed*/
    /*-- in irp_fleetvehicles*/
    lTotalFV number;
    BEGIN                                                                     -- 20160601.1
        SELECT COUNT(*)
        INTO lTotalFV
        FROM IRP_FLEETVEHICLES
        WHERE irp_flv_target = 'Y';
    return lTotalFV;
    EXCEPTION
    WHEN OTHERS THEN
        raise_application_error (-20010, 'fv add target raised error');
        return SQLCODE;
    END;

    function firp_fv_removed return number is
    /*-- step 7: update irp_fleetvehciles set the targeted*/
    /*-- to 'N' where the usdot is no longer in the target file.*/
    /*-- we need to figure out the rule. if a usdot number is in FV*/
    /*-- but the vin is not in irp_prsmlcltgtveh, then is the vehicle targeted?*/
    /*-- for now, we will assume that if the VIN does not exist but the USDOT#*/
    /*-- does exists, then the Vehicle is targeted, through the USDOT#.*/
    BEGIN                                                                     -- 20160601.1
        UPDATE irp.IRP_FLEETVEHICLES f
        SET irp_flv_target           = 'N'
        WHERE f.irp_flv_target       = 'Y' /*-- limit the updates*/
            AND f.irp_flv_vehicleid NOT IN
                (select fv.irp_flv_vehicleid
                 from IRP.IRP_FLEETVEHICLES fv,
                            irp.irp_vehicles v,
                            irp.irp_prsmlcltgtveh p
                 where v.irp_veh_id = fv.IRP_FLV_VEHICLEID
                     and v.irp_veh_vin = p.irp_ptv_vin);
        return 0;
    EXCEPTION
    WHEN OTHERS THEN
        raise_application_error (-20011, 'fv add target raised error');
        return SQLCODE;
    END;

    function firp_fv_removed(pTotalFV number) return number is
    /*-- step 8: get the number of fleetvehicle targeted removed.*/
    lCountTmp number;
    BEGIN                                                                     -- 20160601.1
        SELECT COUNT(*)
        INTO lCountTmp
        FROM IRP_FLEETVEHICLES
        WHERE irp_flv_target = 'Y';
        return  lCountTmp - pTotalFV; /*-- calc the number targeed removed.*/
    EXCEPTION
    WHEN OTHERS THEN
        raise_application_error (-20012, 'fv add target raised error');
        return SQLCODE;
    END;

    function firp_add_fv_target return number is
    /*-- Step 9: update fleetvehicle with the targed usdot numbers.*/
      cursor t_veh_cur is
      select v.irp_veh_id, p.irp_ptv_usdotno, p.irp_ptv_vin
      from irp.IRP_PRSMLCLTGTVEH p
      ,    irp.irp_vehicles v
      where v.irp_veh_vin = p.irp_ptv_vin;

    BEGIN                                                                     -- 20160601.1
        UPDATE IRP_FLEETVEHICLES f
        SET irp_flv_target       = 'Y'
        WHERE f.irp_flv_vehicleid IN
                (select fv.IRP_FLV_VEHICLEID
                 from IRP.IRP_FLEETVEHICLES fv,
                            irp.irp_vehicles v,
                            irp.irp_prsmlcltgtveh p
                 where v.irp_veh_id = fv.IRP_FLV_VEHICLEID
                     and v.irp_veh_vin = p.irp_ptv_vin);
        -- now we are going to check by VIN. If VIN is targeted, then
        -- set the targeted flag to 'Y'.

        --update irp_fleetvehicles f
        --set irp_flv_target = 'Y'
        -- (select irp_ptv_vin FROM IRP_PRSMLCLTGTVEH);
        --where f.IRP_FLV_VEHICLEID IN
        begin
          for t_veh_rec in t_veh_cur
          loop
        update IRP_FLEETVEHICLES v
        set irp_flv_target = 'Y'
        where v.IRP_FLV_VEHICLEID = t_veh_rec.irp_veh_id;
          end loop;
        end;
    return 0;
    EXCEPTION
    WHEN no_data_found THEN
        raise_application_error (-20013, 'fv returned no data found');
        return 0;
    when others then
      raise_application_error (-20014, 'fv exception ' || sqlerrm);
      return 0;
    END;

    function firp_add_fv_target(pTotalFV number, pTargetFVRemoved number) return number is
    /*-- step 10: get the number of fleetvehicle added to targeted.*/
    lCountTmp number;
    BEGIN                                                                     -- 20160601.1
        SELECT COUNT(*)
        INTO lCountTmp
        FROM IRP_FLEETVEHICLES
        WHERE irp_flv_target = 'Y';
        return lCountTmp - pTotalFV - pTargetFVRemoved;
    EXCEPTION
    WHEN OTHERS THEN
        raise_application_error (-20015, 'fv add target raised error');
        return SQLCODE;
    END;

    /*
        Change log

        20160610.1: added host name and instance name to email subject.
        20160607.1: get the count of target carriers removed.
        20160607.2: add in the targeted carriers.
        20160607.3: get the count of targeted carriers added.
        20160607.4: get the total count of carriers targeted.
        20160607.5: get the count of fleetvehicles targeted, used for calc on remove and add.
        20160607.6: removed fleet vehicles that have been removed.
        20160607.7: get the number of fleetvehicle targeted removed
        20160607.8: set the targeted flag for fleet vehicles by USDOT# and VIN.
        20160607.9: get the number of fleetvehicle added to targeted

        20160606.1: adding irp_vehicles to targeted.
        20160606.2: get the count of vehicles that are targeted.
        20160606.3: set irp_vehicles.irp_veh_target = 'N' where
                                the vehicle is no longer a targed vehicle.
        20160606.4: get the count of vehicles that have been removed.
        20160606.5: add in any vehicles that have been targeted.
        20160606.6: get the count of vehicles that have added.
        20160606.7: get the new count of vehicles that have been targeted.
        20160606.8: get the count of carriers currently targeted.
        20160606.9: remove any targeted carriers that have been removed from the prism
                                file.

        20160603.1 We should add targed flag to the vehicle table. If a vehicle is transfered
        the fleetvehicle row will be moved to fleet vehicle history. Once this happens, we
        lost the targeted flag.

        20160601.1: set the targed flag on irp_applicants and irp_fleetvehciles. Pass 1 we
        are going to check to for any carriers and vehicles that have been targeted but
        removed from the targeted file. Once we have removed any carriers and vehicles
        that have had the targeting removed, we then pass through the targed file and
        add targeted carriers and targeted vehicles.
    */
    PROCEDURE IRP_PROCESS_TARGET IS
        sEmailMsg           VARCHAR2(32767);
        sCRLF               CONSTANT  VARCHAR2(5) := '<br>';
        lTotalV           NUMBER; /*The total number of vehicles that have been targeted. */
        lTargetVRemoved   NUMBER; /*The total number of target vehicles removed.*/
        lTargetVAdd       NUMBER; /*The total number of target vehicles added. */
        lTotalApp         NUMBER; /*-- the total number of irp_applicants that are targeted.*/
        lCountTmp         NUMBER; /*-- used as a temp hold for counts.*/
        lTargetAppRemoved NUMBER; /*-- the number of applicants where targeted was removed.*/
        lTargetAppAdd     NUMBER; /*-- the number of applicants where targeted was added.*/
        lTotalFV          NUMBER; /*-- the total number of irp_fleet_vehicles that are targ.*/
        lTargetFVRemoved  NUMBER; /*-- the total number of irp_fleet_vehicles where targed is removed.*/
        lTargetFVAdd      NUMBER; /*-- the total number of irp_fleet_vehicles that were added.*/
        sDelCode            irp.irp_delinquentcode.IRP_DCO_UNITCODE%TYPE;
        dummy             NUMBER; -- just a dummy variable to check success.
        lInstance         sys.v_$instance.instance_name%type;
        lHostName         sys.v_$instance.host_name%type;

        iLogId            number;  -- holds the id for logging actions.

    BEGIN

        BEGIN
            -- are we going to log the steps?
            iLogId := utility.log_stack.create_entry(pUnit => $$PLSQL_UNIT, pLine => to_number($$PLSQL_LINE));

            SELECT IRP_DCO_UNITCODE
            INTO sDelCode
            FROM irp.irp_delinquentcode
            WHERE IRP_DCO_DELINQUENTNAME = 'UNSAFE';
        EXCEPTION WHEN NO_DATA_FOUND THEN
            -- RAISE EXCEPTION
            utility.log_stack.end_entry(pLogId => iLogId, pEtime => current_timestamp, pResults => 'no_data_found at ' || $$PLSQL_LINE);
            RAISE_APPLICATION_ERROR(-20016, 'del code not found');
        END;

        BEGIN

            -- get the instance and host name
            select instance_name,
                         host_name
            into lInstance,
                     lHostName
            from sys.v_$instance;
            -- record the start time an initial message
            sEmailMsg := $$PLSQL_UNIT || '.irp_process_target started at  ' || to_char(current_timestamp) || sCRLF;

            -- this will process irp_vehicles.
            lTotalV := firp_veh_target;                          -- 20160606.2
            dummy := firp_remove_veh_target;                     -- 20160606.3  remove vehicles
            /*<FIXME>
            we need to get a collection of all vehicles that have been removed from targeted
            and remove the flags. From vehicle, get the titlenumber and vin.
            */
            lTargetVRemoved := firp_remove_veh_target(lTotalV);  -- 20160606.4
            dummy := firp_add_veh_target;                        -- 20160606.5  add vehicles
            lTargetVAdd := firp_add_veh_target(lTotalV);         -- 20160606.6
            lTotalV := firp_veh_target;                          -- 20160606.7

            -- now we are going to start into irp_applicants. for target carriers.
            lTotalApp := firp_app_target;                       -- 20160606.8
            dummy := firp_app_removed;                          -- 20160606.9  remove carriers
            /*<FIXME>
            for ever carrier that is removed, get a collection of their vehicles by titlenumber and VIN.
            pass to db2 remove the flags.
            */
            lTargetAppRemoved := firp_app_removed(lTotalApp);   -- 20160607.1
            dummy := firp_add_app_target(sDelCode);             -- 20160607.2  add carriers
            lTargetAppAdd := firp_add_app_target(lTotalApp, ltargetAppRemoved); -- 20160607.3
            lTotalApp := firp_app_target;                       -- 20160607.4
            -- new we are going to start into irp_fleetvehicles for targeted vehicles.

            lTotalFV := firp_fv_target;                         -- 20160607.5
            dummy := firp_fv_removed;                           -- 20160607.6  remove fleetvehicles
            /*<FIXME>
            for every fleet vehicle get a collection of all vehicles by titlenumber and VIN. pass to db2
            remove flags.

            so, looking at this - we should get a distinct list of all vehicles that are being removed
            from flagged and pass that one time.  So, to get a complete list of all vehicles that need
            to be removed, we need to check usdot# for carriers and usdot# for vehicles.

            */
            lTargetFVRemoved := firp_fv_removed(lTotalFV);      -- 20160607.7
            dummy := firp_add_fv_target;                        -- 20160607.8  add fleetvehicles
            lTargetFVAdd := firp_add_fv_target(lTotalFV, lTargetFVRemoved); -- 20160607.9
            -- commit the transaction
            dummy := clear_db2_flags;
            commit;
            -- something bad happened. rollback and record the error.  Add the error to the email subject line and body.
        EXCEPTION WHEN OTHERS THEN
            sEmailMsg := 'the following error occurred while adding and removing carriers / vehicles ' || sCRLF;
            sEmailMsg := sEmailMsg || SQLERRM;
            rollback;
        END;

        -- log the results.
        sEmailMsg := sEmailMsg || $$PLSQL_UNIT || '.irp_process_target finished at  ' || to_char(current_timestamp) || sCRLF;
        sEmailMsg := sEmailMsg || 'Total Targeted Applicants ' || to_char(lTotalApp) || sCRLF;
        sEmailMsg := sEmailMsg || 'Number of Targeted Applicants Removed ' || to_char(lTargetAppRemoved) || sCRLF;
        sEmailMsg := sEmailMsg || 'Number of Targeted Applicants Added ' || to_char(lTargetAppAdd) || sCRLF;
        sEmailMsg := sEmailMsg || 'Number of Targeted Vehicles Removed ' || to_char(lTargetVRemoved) || sCRLF;
        sEmailMsg := sEmailMsg || 'Number of Targeted Vehicles Added ' || to_char(lTargetVAdd) || sCRLF;
        sEmailMsg := sEmailMsg || 'Total Targeted Fleet Vehicles ' || to_char(lTotalFV) || sCRLF;
        sEmailMsg := sEmailMsg || 'Number of Targeted Fleet Vehicles Removed ' || to_char(lTargetFVRemoved) || sCRLF;
        sEmailMsg := sEmailMsg || 'Number of Targeted Fleet Vehicles Added ' || to_char(lTargetFVAdd) || sCRLF;

        sys.dbms_output.put_line('Total Targeted Applicants' || lTotalApp);
        sys.dbms_output.put_line('Number of Targeted Applicants Removed ' || lTargetAppRemoved);
        sys.dbms_output.put_line('Number of Targeted Applicants Added ' || lTargetAppAdd);
        sys.dbms_output.put_line('Total Targeted Vehicles ' || lTotalFV);
        sys.dbms_output.put_line('Number of Targeted Vehicles Removed ' || lTargetFVRemoved);
        sys.dbms_output.put_line('Number of Targeted Vehicles Added ' || lTargetFVAdd);



            MAIL_TOOLS.sendmail( smtp_server => IRP_PRINT.GET_SMTP_SERVER,          -- 20150817.1
                                                     smtp_server_port => 25,                            -- 20150817.1
                                                     from_name => 'irp@marylandmva.com',                -- 20150817.1
                                                     to_name => 'sschneider1@mdot.state.md.us',       -- 20160202.1
                                                     cc_name => 'rlockard@mdot.state.md.us',            -- 20150817.1
                                                     bcc_name => NULL,                                  -- 20150817.1
                                                     subject => lHostName || ' ' || lInstance || ' Target Post Process', --20160610.1
                                                     MESSAGE => TO_CLOB( sEmailMsg ),               -- 20150817.1
                                                     priority => NULL,                                  -- 20150817.1
                                                     filename => NULL,                                  -- 20150817.1
                                                     binaryfile => EMPTY_BLOB ( ),                      -- 20150817.1
                                                     DEBUG      => 0 );                                 -- 20150817.1

        utility.log_stack.end_entry(pLogId => iLogId, pEtime => current_timestamp, pResults => null);

    END;
    --
    --
    --
    PROCEDURE IRP_SCHEDULEJOB( sFMCSAfiletype IN VARCHAR2, sDownloadStatus IN VARCHAR2 ) IS
    --
        sUniqueJobName  VARCHAR2(75);
    --
    BEGIN
         sUniqueJobName := DBMS_SCHEDULER.GENERATE_JOB_NAME( prefix => 'PRISMJOB$_' );
    --

            -- create the job
            dbms_scheduler.create_job(job_name => sUniqueJobName,                                            -- 20100202.1
                                                             job_type => 'STORED_PROCEDURE',
                                                             job_action => 'IRP_PRISMFILELOAD',
                                                             number_of_arguments => 2);

            --

            -- set value 1
            DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE( job_name => sUniqueJobName,
                                                                                argument_position => 1,
                                                                                argument_value => sFMCSAfiletype );
            --  set value 2
            DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE( job_name => sUniqueJobName,
                                                                                argument_position => 2,
                                                                                argument_value => sDownloadStatus );
    --  enable the job
            DBMS_SCHEDULER.ENABLE( name => sUniqueJobName );
    --
    END IRP_SCHEDULEJOB;
    --
    -- check to see if an applicant or vehicle is targeted.
    FUNCTION irp_is_targeted(sUSDOTNO IN VARCHAR2 DEFAULT NULL, sVIN IN VARCHAR2 DEFAULT NULL) return boolean IS
    x number; -- dummy variable
    begin
            -- check applicant
            SELECT count(*)
            INTO x
            FROM irp.IRP_PRSMLCLTGTCAR
            WHERE IRP_PTC_USDOTNO = sUSDOTNO;

        IF x > 0 THEN
            -- we found an applicant that is
            -- targeted, return true.
            RETURN TRUE;
        -- we need a vin to check againt vehicles.
        ELSIF sVIN IS NOT NULL THEN
        -- Check vehicle
            SELECT count(*)
            INTO x
            FROM irp.IRP_PRSMLCLTGTVEH
            WHERE IRP_PTV_VIN = sVIN;
            IF x > 0 THEN
                -- we have a hit on the vehicle
                RETURN TRUE;
            END IF;
            -- whe have not found it, return false.
            RETURN FALSE;
        END IF;
        return false;
    end;
    --
END IRP_READPRISMFILES;

/
