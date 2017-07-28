--
-- 
--

create or replace FUNCTION irp.irp_next_part(pString IN VARCHAR2, pPos IN NUMBER, iNbrDelim IN NUMBER)
RETURN VARCHAR2 IS

iStart 		INTEGER;	-- Start read position
iEnd   		INTEGER;	-- End read postion
iNbrChar	INTEGER;	-- Number of charactors to read.
sString   VARCHAR2(2000);
BEGIN

	IF pPos = 0 THEN
		iStart := 1;
	ELSE
		iStart 		:= regexp_instr(pString, ' ', 1, pPos);
	END IF;
	--
	iEnd   		:= regexp_instr(pString, ' ', 1, pPos+1);
	--
	IF iEnd > 1 THEN
		iNbrChar := iEnd - iStart;
    sString := trim(substr(pString, iStart, iNbrChar));
	ELSE
		-- we are just going to read to the end of the string.
    sSTring := trim(substr(pString, iStart));
	END IF;

  RETURN sString;
	--RETURN substr(pString,1, regexp_instr(pString,' ', 1, pPos)-1);
END;
/

create or replace procedure   IRP.IRP_PARSE_NAME (pIRPNbr IN NUMBER DEFAULT NULL, 
                              pName   IN VARCHAR2 DEFAULT NULL,
                              pPhone  IN VARCHAR2 DEFAULT NULL,
                              pEmail  IN VARCHAR2 DEFAULT NULL,
                              pMobile IN VARCHAR2 DEFAULT NULL)
authid current_user
AS
PRAGMA AUTONOMOUS_TRANSACTION;

sString1        irp.irp_applicants.irp_app_conlname%TYPE;
sName           irp.irp_applicants.irp_app_conlname%TYPE;
sFname			    irp.irp_applicants.irp_app_conlname%TYPE;
sMname			    irp.irp_applicants.irp_app_conlname%TYPE;
sLname			    irp.irp_applicants.irp_app_conlname%TYPE;
sSuffix         VARCHAR2(10);
sTmp            INTEGER; -- just a dumb variable
i               INTEGER; -- loop index
j				        INTEGER; -- look index
iCnt			      INTEGER; -- the count of demilmiter.
--
BEGIN
  --
  --sys.dbms_output.put_line('NAME ' || trim(mult_name_rec.irp_app_conlname));
  --
  i := regexp_count(trim(pName),' ');
  IF i = 0 THEN
    i := 1;
  END IF;
  iCnt := i;
  --
  WHILE i > 0
    LOOP
      sString1 := pName;
      IF pName IS NOT NULL THEN
        sString1 := pName;
      END IF;
      --
      --
      -- check to see if there is a suffex in the name.
      --
      BEGIN      
        FOR sfx_rec IN (SELECT irp_lok_value
                        FROM irp.irp_lookups
                        WHERE irp_lok_type = 'NMSUFFEX')
        LOOP
          sTmp := regexp_instr(sString1, sfx_rec.irp_lok_value || '$');
          IF sTmp > 0 THEN
            -- get the suffex
            sSuffix := substr(sString1, sTmp);
            -- now that we have the suffex, we don't need that
            -- anymore, so lets lob it off.
            sString1 := trim(substr(sString1, 1, sTmp-1));
          END IF;
        END LOOP;
      END;
      -- okay we have broken apart the names. now we need to tokenize the names.
      -- sString1 := next_part(mult_name_rec.irp_app_conlname, i, iCnt);
      --
      -- get the number of ' ' in the string.
      j := nvl(regexp_count(trim(sString1), ' '),0);
      --
      --sys.dbms_output.put_line(sString1 || ' j= ' || to_char(j));
      WHILE j >= 0
      LOOP
        -- first case, fisrt middle last. there are two spaces.
        IF j = 2 THEN
          sLName := irp.irp_next_part(sString1, j, iCnt);
          sMName := irp.irp_next_part(sString1, j-1, iCnt);
          j := 0;     -- we got last name, and middle name so set j=0.
          --sys.dbms_output.put_line(sString1 || ' has 2 spaces');
        END IF;

        -- second case, first last. there is one space.
        IF j = 1 THEN
          sLname := irp.irp_next_part(sString1, j, iCnt);
          --sys.dbms_output.put_line(sString1 || ' as one space');
          j := 0;
        END IF;

        -- third case, first name only. there are no spaces
        IF j = 0 THEN
          sFname := irp.irp_next_part(sString1, j, iCnt);
          --sys.dbms_output.put_line(sString1 || ' as no spaces');
        END IF;
        -- decrement j
        j := j-1;
      END LOOP;
      -- decrement i
      i := i - 1;
      --
      --sys.dbms_output.put_Line('fist: ' || sFname || ' middle ' || sMname || ' last: ' || sLname);

      IF pName IS NOT NULL THEN
        -- insert the values into irp.irp_contacts
        BEGIN
          INSERT INTO irp.irp_contacts VALUES (
            irp.irp_contacts_seq.nextval,
            pIRPNbr,
            nvl(sFName,'x'),
            nvl(sMName,'x'),
            nvl(sLName,'x'),
            pPhone,
            upper(NVL(pEmail,'X')), 
            'Y',    -- irp_con_primary
            'x',    -- irp_con_mobile
            sSuffix
          );
        EXCEPTION WHEN dup_val_on_index THEN
          -- <FIXME>
          -- We tried to insert a duplicate person,
          -- we are just going to use null at this time
          NULL;
        END;
      END IF;
      -- reset variables
      sLName := NULL;
      sMName := NULL;
      sFName := NULL;
      sSuffix := NULL;
			--
  END LOOP;
  COMMIT;
    --
END;
/


-- CREATE PACKAGES
--------------------------------------------------------
--  DDL for Package PRISM_UTILITIES
--------------------------------------------------------

CREATE OR REPLACE EDITIONABLE PACKAGE "IRP_API"."PRISM_UTILITIES" 
AUTHID DEFINER
AS
  PROCEDURE pQuarter(pBeginQuarter OUT DATE,
                     pEndQuarter   OUT DATE);

  FUNCTION fCarrierCount(pMSCStep   IN VARCHAR2,
                         pBeginDate IN DATE,
                         pEndDate   IN DATE) RETURN NUMBER;

  FUNCTION fVehicleCount(pMSCStep   IN VARCHAR2,
                         pBeginDate IN DATE,
                         pEndDate   IN DATE) RETURN NUMBER;

END prism_utilities;
/

--------------------------------------------------------
--  DDL for Package Body PRISM_UTILITIES
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IRP_API"."PRISM_UTILITIES" AS

  -- return janurary 1 of the current year.
  FUNCTION fStartYear RETURN DATE IS
  sYear   VARCHAR2(4);
  BEGIN
    sYear := to_char(sysdate,'RRRR')-1;
    RETURN to_date('01-OCT-'||sYear);
  END fStartYear;

  -- get the begining quarter and end quarter for the previous quarter
  PROCEDURE pQuarter(pBeginQuarter  OUT DATE,
                     pEndQuarter    OUT DATE) IS
  dStart 	  DATE;	-- the start of the current year.
  dTmpStart	DATE;	-- this is to hold the previous start date.
  dTmpEnd	  DATE;	-- this is to hold the previous end date.
  BEGIN
    -- lets start with getting to the beginning of the current quarter. This 
    -- will be the end of the previous quarter.
      dStart   := fStartYear;
    -- find the previous quarter by looping through the quarters. Start 
    -- with current year - 1
    FOR REC IN (
        WITH q(qtr) AS (
          SELECT add_months(dStart, (level-1)*3)
          FROM dual
          CONNECT BY LEVEL <= 6   -- The reason we are using 6 quarters from the 
        )                         -- begining of the current year is because 
        SELECT qtr qStart,        -- two quarters minus current year will not
             last_day(add_months(qtr, 2)) qEnd -- match and we need that for the loop.
        FROM q
        ORDER BY 1
      )
    LOOP
      -- is the current date between qStart and qEnd?
      -- we know the first time through the loop the
      -- condition will never be met.
      IF sysdate BETWEEN rec.qStart AND rec.qEnd
      THEN
        pBeginQuarter := dTmpStart;
        pEndQuarter	  := dTmpEnd;
      END IF;
      -- store the date we just tested because we may
      -- use them the next time through the loop.
      dTmpStart := rec.qStart;
      dTmpEnd	  := rec.qEnd;
      --
    END LOOP;
  END pQuarter;

	-- count the number of carriers that have been targeted.
  FUNCTION fCarrierCount(pMSCStep   IN VARCHAR2,
                         pBeginDate IN DATE,
                         pEndDate   IN DATE) RETURN NUMBER IS
	knt NUMBER;
	BEGIN
		select count(*)
		INTO knt
		from irp.irp_targetedcarriers
    WHERE IRP_TCAR_LETTERDATE between pBeginDate and pEndDate;
		RETURN knt;
	END;
	-- count the number of vehicle that have been targeted.
  FUNCTION fVehicleCount(pMSCStep   IN VARCHAR2,
                         pBeginDate IN DATE,
                         pEndDate   IN DATE) RETURN NUMBER IS
	knt NUMBER; -- just a dumb variable
	BEGIN
    sys.dbms_output.put_line('MSCStep =' ||'''||pMSCStep||''');  
    SELECT count(*) 
    INTO knt
    FROM
      (SELECT c.irp_tcar_irpnbr, 
          c.irp_tcar_id, 
          v.IRP_FLV_VEHICLEID, 
          v.IRP_VEH_TITLENBR,
          v.IRP_PLC_MCSIPSTEP
      FROM 	irp.IRP_FLEETVEHICLE_VW v,
          irp.irp_targetedcarriers c,
          irp.irp_prismmcsip p
      WHERE 	v.irp_fle_irpnbr  = c.irp_tcar_irpnbr			-- irp number is the key for carriers (applicants)
        AND irp_mcs_step      = v.IRP_PLC_MCSIPSTEP		-- the mcsipstep code tells us the type of targeting
        AND irp_mcs_step IN '('||pMSCStep||')'
        AND irp_tcar_status   = 'T'						-- is the vehicle currently targeted.
        AND IRP_MCS_TGTROAD   = 'Y'					  -- is the vehicle to be targeted on the road?
        AND IRP_TCAR_LETTERDATE between pBeginDate and pEndDate);
    RETURN knt;
	END;
END prism_utilities;

/


--------------------------------------------------------
--  DDL for Package ERRORSTACK_PKG
--------------------------------------------------------

CREATE OR REPLACE EDITIONABLE PACKAGE "UTILITY"."ERRORSTACK_PKG" 
AUTHID DEFINER
AS
	PROCEDURE pMain(pErrorId 	OUT INTEGER);
END errorstack_pkg;

/
--------------------------------------------------------
--  DDL for Package GENERIC_PKG
--------------------------------------------------------

CREATE OR REPLACE EDITIONABLE PACKAGE "UTILITY"."GENERIC_PKG" 
AUTHID DEFINER
as
  FUNCTION fgetinstance RETURN VARCHAR2;
  FUNCTION fReserved(pString IN VARCHAR2) RETURN BOOLEAN;
END generic_pkg;

/
--------------------------------------------------------
--  DDL for Package LOG_STACK
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "UTILITY"."LOG_STACK" 
authid definer AS

  function create_entry(pUnit  VARCHAR2 default null,
					   pLine  number default null,
					   pStime timestamp default current_timestamp,
					   eEtime timestamp default null,
					   pParms clob      default null) return number;
					   
	procedure end_entry(pLogID number,
						pEtime timestamp default current_timestamp,
						pResults in clob default null);
end;

/
--------------------------------------------------------
--  DDL for Package Body ERRORSTACK_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "UTILITY"."ERRORSTACK_PKG" AS

	-- this procedure will get the stack values for each call in the stack.
	-- we are not exposing this to the specification because it will only
	-- be used interal to this package. Because of this, we are forward
	-- defining it.
	PROCEDURE pCallStackValues(pDepth 			IN 		  INTEGER,
								pOwner 				                OUT VARCHAR2,
								pLexical_depth		            OUT INTEGER,
								pUnit_Line			              OUT INTEGER,
								pSubProgram			              OUT VARCHAR2,
								pError_Number		              OUT INTEGER,
								pError_Msg		                OUT VARCHAR2) IS

	BEGIN
		-- get the values from the stack that we are going to be 
		-- putting into utility.error_lines.
		pOwner 			      := sys.utl_call_stack.owner(pDepth);
		pLexical_Depth		:= sys.utl_call_stack.lexical_depth(pDepth);
		pUnit_Line		  	:= sys.utl_call_stack.unit_line(pDepth);
		pSubProgram		    := sys.utl_call_stack.concatenate_subprogram(utl_call_stack.subprogram(pDepth));
    BEGIN
      pError_Number	  	:= sys.utl_call_stack.error_number(pDepth);
      pError_Msg		  	:= sys.utl_call_stack.error_msg(pDepth);
    -- This exception is to be expected when there are no errors.
    EXCEPTION WHEN OTHERS THEN
      pError_Number := 0;
      pError_Msg := NULL;
    END;
		
	END pCallStackValues;
	
	-- pass in the error number, this will gather the details of
	-- the error stack. Lets think about this, do we really need
	-- the ErrorId at this point?
	PROCEDURE pCallStackMain(pErrorId IN INTEGER) IS
	
		iLineId				  INTEGER;	      -- the error_line pk.
		sOwner 				  VARCHAR2(128);  -- populated by pCallStackValues
		iLexical_Depth	INTEGER;        -- populated by pCallStackValues
		iUnit_Line			INTEGER;        -- populated by pCallStackValues
		sSubProgram			VARCHAR2(256);  -- populated by pCallStackValues
		iError_Number		INTEGER;        -- populated by pCallStackValues
		sError_Msg			VARCHAR2(256);  -- populated by pCallStackValues
	BEGIN
		FOR i IN REVERSE 1 .. utl_call_stack.dynamic_depth()
		LOOP
			pCallStackValues(pDepth			=> i,
							 pOwner			        => sOwner,
							 pLexical_Depth	    => iLexical_Depth,
							 pUnit_Line		      => iUnit_Line,
							 pSubProgram	      => sSubProgram,
							 pError_Number	    => iError_Number,
							 pError_Msg	        => sError_Msg);
			-- get the next sequence number.
			SELECT utility.error_lines_seq.nextval
			INTO iLineId
			FROM dual;
			-- insert the line into utility.error_lines.
			INSERT INTO utility.error_lines VALUES (
					iLineId,		    -- primary key
					pErrorId,		    -- fk to utility.errors.
					i,				      -- dynamic_depth
					sOwner,			    -- pl/sql unit owner
					sSubProgram,  	-- pl/sql unit and sub program 1st value.
					iError_Number,	-- error number
					sError_Msg,		  -- error message
					iUnit_Line		  -- pl/sql line number
					);
		END LOOP;
		COMMIT;
	END pCallStackMain;

	-- the main calling procedure for the error stack package.
	PROCEDURE pMain (pErrorId OUT INTEGER)IS
	PRAGMA AUTONOMOUS_TRANSACTION;
	iErrorId 	INTEGER;		-- utility.errors pk.
	BEGIN
		-- get the next sequence for errors.
		SELECT utility.errors_seq.nextval
		INTO pErrorId
		FROM dual;
		-- create the base error in utility.errors table.
		INSERT INTO utility.errors VALUES (pErrorId, -- utility.errors pk
							sys_context('userenv', 'session_user'),
							sys_context('userenv', 'ip_address'),
							current_timestamp
							);
		-- populate the error_lines table using pCallStackMain.
    pCallStackMain(pErrorId => pErrorId);
		-- commit the transaction sense this is an AUTONOMOUS TRANSACTION
    -- we are not worried about the commit having side effects.
		COMMIT;
		-- return the error id.
		-- this is done through the OUT parameter pErrorId. so 
		-- there is not going to be a formal RETURN.
	END pMain;
END errorstack_pkg;

/

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "UTILITY"."GENERIC_PKG" as

  function fgetinstance return varchar2 as
  sInstanceName VARCHAR2(16);
  iLog INTEGER;
  begin
    SELECT INSTANCE_NAME
		INTO sInstanceName
		FROM SYS.V_$INSTANCE;
    RETURN sInstanceName;
	EXCEPTION WHEN OTHERS THEN
		utility.errorstack_pkg.pMain(pErrorId => iLog);
    RETURN null;
  END fgetinstance;
  
  FUNCTION fReserved(pString IN VARCHAR2) RETURN boolean IS
  iCnt INTEGER;
  BEGIN
    SELECT count(*)
    INTO icnt
    FROM sys.v_$reserved_words
    WHERE instr(' ' || upper(pString) || ' ', ' ' || keyword || ' ') > 0;
    IF iCnt > 0 THEN
      RETURN TRUE;
    ELSE 
      RETURN FALSE;
    END IF;
  END fReserved;
END generic_pkg;

/



  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "UTILITY"."LOG_STACK" AS
 
	-- this checks to see if we are dubugging. If we are 
	-- then we can start logging.
	function debug(pUnit varchar2,
				   pUser varchar2) return number IS
	x number; -- just a dumb variable
	begin
		-- check to see if we are debugging for a unit or a user.
		select count(*)
		into x
		from utility.debug
		where (unit = pUnit or unit = '*')
		  and (username = pUser or username = '*');

		-- test to see if anything was returned.
		return x;
	end;

	-- create a log entry.
	function create_entry(pUnit  VARCHAR2 default null,
					   pLine  number default null,
					   pStime timestamp default current_timestamp,
					   eEtime timestamp default null,
					   pParms clob      default null) return number IS
	PRAGMA AUTONOMOUS_TRANSACTION;

	iId number; -- temp holder of the primary key

	begin
    -- check to see if debugging is turned on
    if log_stack.debug(pUnit => pUnit, pUser => user) > 0 then
      -- if so, insert a row.
      -- grab the next id for the primary key.
      select log_stack_seq.nextval into iId from dual;

      insert into utility.app_log values (iId,
        			pUnit,
          			pLine,
            		user,
              		pStime,
                	null, 	-- we dont have the end time yet.
                  	pParms,
                    null);	-- we will wait for the results
      -- this is an atonomus transaction, so a commit is safe.
      commit;
      return iID;
    else
      return 0;
    end if;
	exception when others then
		rollback;					-- execute a rollback to be safe.
		return sqlcode * -1;		-- return the error code and flip the sign so we know if it's an error.
	end;

	procedure end_entry(pLogID number,
						pEtime timestamp default current_timestamp,
						pResults in clob default null) is
  PRAGMA AUTONOMOUS_TRANSACTION;
	BEGIN
    -- check to see if we are logging. if pLogId = 0 then logging did not 
    -- start.
    if pLogId != 0 then
		  update utility.app_log set etime = pEtime, results = pResults 
		  where id = pLogId;
	    commit;
    end if;
	END;

end;

/

--------------------------------------------------------
--  DDL for Package IRP_API_LOOKUPS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IRP"."IRP_API_LOOKUPS" AS
	FUNCTION fValue(pType IN VARCHAR2) RETURN NUMBER;
END;

/
--------------------------------------------------------
--  DDL for Package Body IRP_API_LOOKUPS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IRP"."IRP_API_LOOKUPS" AS
	FUNCTION fValue(pType IN VARCHAR2) RETURN NUMBER IS
    iTest irp.irp_lookups.irp_lok_value%type;
		BEGIN
			-- changed to a function to get to lookup value. this query is
			-- reused in multiple places.
			SELECT irp_lok_value
			INTO iTest
			FROM irp.irp_lookups
			WHERE irp_lok_type = pType;
			RETURN iTest;
		EXCEPTION WHEN NO_DATA_FOUND THEN
			RAISE_APPLICATION_ERROR(-2001,'there is no value for irp_lok_type ' || pType);
		END fValue;
END;

/




--------------------------------------------------------
--  DDL for Package IRP_CHAMELEON
--------------------------------------------------------

CREATE OR REPLACE EDITIONABLE PACKAGE "IRP"."IRP_CHAMELEON" 
AS
FUNCTION fInsChameleon(pIRPNbr 		IN NUMBER,
                        pusdotnbr 	IN VARCHAR2,
                        pvin        IN VARCHAR2 DEFAULT NULL,
                        pscore      IN NUMBER,
                        pAttr       IN VARCHAR2,
                        poverride   IN VARCHAR2 DEFAULT NULL) RETURN INTEGER;
FUNCTION fUpdChameleon(pChameleonId IN NUMBER,
                       pOverRide    IN VARCHAR2) RETURN NUMBER;
--
PROCEDURE pParseName( pName     IN 		VARCHAR2,
                        pFname    OUT 	VARCHAR2,
                        pMname    OUT 	VARCHAR2,
                        plname    OUT 	VARCHAR2,
                        psuffix   OUT   VARCHAR2);
--
FUNCTION irp_phone(pphone       IN     VARCHAR2,
                    pnewirpnbr  IN     NUMBER) RETURN NUMBER;
--
FUNCTION irp_new_usdotnbr(pusdotnbr IN VARCHAR2) RETURN NUMBER;
--
FUNCTION irp_address(pStreet 	IN VARCHAR2,
                    pstreet2 	IN VARCHAR2,
                    pcity		IN VARCHAR2,
                    pcounty	    IN VARCHAR2,
                    pstate		IN VARCHAR2,
                    pzip        IN VARCHAR2,
                    pirpnbr     IN INTEGER) RETURN NUMBER;
--
FUNCTION irp_email(pEmail   IN VARCHAR2,
                    pirpnbr IN INTEGER) RETURN NUMBER;
--
FUNCTION irp_name(pname   IN VARCHAR2) RETURN NUMBER;
--
FUNCTION irp_name(pfname    IN VARCHAR2,
                    pmname  IN VARCHAR2 DEFAULT NULL,
                    plname  IN VARCHAR2,
                    pirpnbr IN INTEGER) RETURN NUMBER;
--
FUNCTION irp_vehicle(pvin     IN VARCHAR2,
                     pirpnbr  IN INTEGER) RETURN NUMBER;
--
FUNCTION irp_override_chameleon(pChemId INTEGER,
                                pnote   CLOB) RETURN INTEGER;
--
FUNCTION fcheckvehicle(pvin        IN VARCHAR2,
                        pirpnbr    IN INTEGER) RETURN NUMBER;
--
FUNCTION fchameleonscorethreshold RETURN INTEGER;

END irp_chameleon;
/

create or replace PACKAGE BODY IRP.irp_chameleon
AS
  -- 20170228.1   initial version. the package is setup to test
  --              for phone number, address and email. this package
  --              depends on a few objects. execute on the package
  --              irp.irp_api_lookups, and select on irp.irp_applicants

	FUNCTION fInsChameleon(pIRPNbr 		IN NUMBER,
                            pusdotnbr 	IN VARCHAR2,
                            pVIN        IN VARCHAR2 DEFAULT NULL,
                            pscore      IN NUMBER,
                            pattr       IN VARCHAR2,
                            pOverRide   IN VARCHAR2 DEFAULT NULL) RETURN INTEGER IS
	PRAGMA AUTONOMOUS_TRANSACTION;

  iLog    INTEGER;   -- log id
  iChemId NUMBER;
  sError  VARCHAR2(255);
	BEGIN
    -- possible to get null with pOverRide, so use the NVL function.  
    IF nvl(pOverRide,'Y') != 'Y' AND pOverRide != 'N' THEN
      RAISE_APPLICATION_ERROR(-20003, 'Override must be Y or N');
    END IF;
  
    SELECT irp.irp_chameleon_seq.NEXTVAL
    INTO iChemId
    FROM dual;
  
		INSERT INTO irp.irp_chameleons VALUES (
                            iChemId,
                            pIRPNbr,
                            pUSDOTNBR,
                            pVIN,
                            pScore,
                            pattr,
                            NULL,           -- TITLE NUMBER. <FIXME> LOOK IT UP.
                            pOverRide);     -- OVERRIDE DEFUALTS TO 'N'
		COMMIT;
		RETURN iChemId;
	EXCEPTION WHEN OTHERS THEN
        sError := sqlerrm;
        UTILITY.ERRORSTACK_PKG.pMain(pErrorId => iLog);
        RAISE_APPLICATION_ERROR(-20000,'An error was raised, check the error ' 
                    || 'log #: ' || ilog);
        RETURN iLog*-1;
	END finschameleon;

    FUNCTION fupdchameleon(pchameleonid IN NUMBER,
                           poverride    IN VARCHAR2) RETURN NUMBER IS
    PRAGMA autonomous_transaction;
    ilog INTEGER;
    sError VARCHAR2(4000);
    BEGIN
        UPDATE irp.irp_chameleons
        SET irp_cha_override = poverride;
        COMMIT;
        RETURN pChameleonId;
    EXCEPTION WHEN OTHERS THEN
        sError := sqlerrm;
        UTILITY.ERRORSTACK_PKG.pMain(pErrorId => iLog);
        RAISE_APPLICATION_ERROR(-20000,'An error was raised, check the error ' 
                    || 'log #: ' || ilog);
        RETURN iLog*-1;
    END fUpdChameleon;

  --              we should also focuse on the name of the person who is
  --              the priniple.

 	PROCEDURE pParseName( pName IN VARCHAR2,
                        pFname    OUT VARCHAR2,
                        pMname    OUT VARCHAR2,
                        pLname    OUT VARCHAR2,
                        pSuffix   OUT VARCHAR2) IS

		iWhiteSpaceCount	INTEGER;	-- we are going to count the number of white spaces to determan if
									-- there is a middle name enbedded.
    iLog INTEGER;   -- log id
    sTemp VARCHAR2(65);

	BEGIN
		-- count the number of white spaces to determan if first, middle, last exists.
    sTemp := trim(pName);
    --<FIXME> we need to use the regualar expression, instr returns the position
    -- of the charactor, for this implanataion, we need the count of the 
    -- white spaces. Oracle 10G does not support this.
		iWhiteSpaceCount := regexp_count(sTemp, ' ');
		--
		IF iWhiteSpaceCount = 0 THEN	-- we are only dealing with last name
			pLname := trim(sTemp);
		ELSIF iWhiteSpaceCount = 1 THEN	-- we are dealing with first name, last name
			pFname := trim(substr(sTemp,1,instr(sTemp, ' ')));
			pLname := trim(substr(sTemp, instr(sTemp, ' ', 1)));
		ELSIF iWhiteSpaceCount = 2 THEN	  -- we are dealing with first, middle, last name
                                      -- and possibilly SR/JR, etc.
      sys.dbms_output.put_line('pName: ' || sTemp);
			pFname := trim(substr(sTemp,1,instr(sTemp, ' ')));
      sTemp := trim(substr(sTemp, regexp_instr(sTemp, ' ', 1, 1)));  --
      sys.dbms_output.put_line('sTemp: ' || sTemp);
			pMname := trim(substr(trim(sTemp), 1, regexp_instr(trim(sTemp), ' ')));
      sTemp := trim(substr(sTemp, regexp_instr(sTemp, ' ', 1, 1)));  --
			pLname := trim(substr(sTemp, regexp_instr(sTemp, ' ',1 , 2)));
    ELSIF iWhiteSpaceCount = 3 THEN -- this is a complex name.
      -- we are expecting a suffix, therefore lets strip out any
      -- potential ',' from the string.
      sTemp := replace(sTemp,',');  -- remove ',' from the string.
      pFname := trim(substr(sTemp,1,instr(sTemp, ' ')));
      sTemp := trim(substr(sTemp, regexp_instr(sTemp, ' ', 1, 1)));  --
			pMname := trim(substr(trim(sTemp), 1, regexp_instr(trim(sTemp), ' ')));
      sTemp := trim(substr(sTemp, regexp_instr(sTemp, ' ', 1, 1)));  --
			pLname := trim(substr(sTemp,1, regexp_instr(sTemp, ' ',1)));
      sTemp := trim(substr(sTemp, regexp_instr(sTemp, ' ', 1, 1)));  --
      pSuffix := trim(substr(sTemp, regexp_instr(sTemp, ' ',1 , 2)));
		END IF;
  EXCEPTION WHEN OTHERS THEN
    UTILITY.ERRORSTACK_PKG.pMain(pErrorId => iLog);
    RAISE_APPLICATION_ERROR(-20000,'An error was raised, check the error ' 
                || 'log #: ' || iLog);
	END pParseName;

	-- the test phone number function will look at the phone number
	-- the applicant gave us. if there phone number matches then
	-- we will return 1, else we will return 0.
		FUNCTION irp_phone(pPhone     IN     VARCHAR2,
                       pNewIRPNbr IN     NUMBER) RETURN NUMBER IS
      iTest NUMBER; 	-- the counter to test to see if the phone already exists.
                      -- we are reusing iTest to get the return value from irp_lookups.
      iLog INTEGER;   -- log id
      
	BEGIN

    -- test to see if the phone number exists and if the carrier is
    -- targeted. Note we are using regexp_replace to strip out anything
    -- that is not in 0-9. Also not, this is going to force a full tablescan.
    -- so, lets monitor the performace and if there is an issue with performance
    -- we can consider creating a bitmap index on irp_app_target.  Of course that
    -- would impact the performace of setting targeted carriers.
    BEGIN
      SELECT count(*)
      INTO iTest
      FROM irp.irp_contacts c
      WHERE regexp_replace(substr(irp_con_phone,1,10), '[^0-9]', '') = regexp_replace(substr(pphone,1,10), '[^0-9]', '')
        and c.irp_con_irpnbr != pNewIrpNbr;
        RETURN 1;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        RETURN 0;
      WHEN OTHERS THEN
        UTILITY.ERRORSTACK_PKG.pMain(pErrorId => iLog);
        RAISE_APPLICATION_ERROR(-20000,'An error was raised, check the error ' || 
                    ' log #: ' || iLog);
        RETURN 0;
    END;

  EXCEPTION WHEN OTHERS THEN
    utility.errorstack_pkg.pmain(perrorid => ilog);
    RAISE_APPLICATION_ERROR(-20000,'An error was raised, check the error ' || 'log #: ' || iLog);
	END irp_phone;

	-- test to see if the address the applicant gave us already exists.
	-- if the address exists and is targeted then return 1, else return 0
	FUNCTION irp_address(pstreet 	IN VARCHAR2,
						 pstreet2	  IN VARCHAR2,
						 pCity		  IN VARCHAR2,
						 pcounty	  IN VARCHAR2,
						 pstate		  IN VARCHAR2,
						 pzip		    IN VARCHAR2,
             pIrpNbr    IN INTEGER) RETURN NUMBER IS
	iTest NUMBER; 	-- the counter to test to see if the address already exists.
                  -- we are reusing iTest to get the return value from irp_lookups.
  iLog  INTEGER;  -- error log id
	BEGIN

	-- we should remove all the spaces from the address to make sure we
	-- catch multiple spaces or no spaces. we are also going to use the
	-- upper function to switch all letters to upper case. street2 is
	-- not always used, so we are going to put in nvl 'X' to force the
	-- true condition. note we using regexp_replace on both sides of the
    -- predicate to strip out any special charactors that may be in the data.
		SELECT count(*)
		INTO iTest
		FROM irp.irp_applicants
		WHERE regexp_replace(upper(irp_app_addstreet), '[^A-Z0-9]','')          = regexp_replace(upper(pstreet), '[^A-Z0-9]','')
		  AND regexp_replace(upper(nvl(irp_app_addline2,'X')), '[^A-Z0-9]','')  = regexp_replace(upper(nvl(pstreet2,'X')), '[^A-Z0-9]','')
		  AND regexp_replace(upper(irp_app_addcity), '[^A-Z0-9]','')            = regexp_replace(upper(pcity), '[^A-Z0-9]','')
		  AND regexp_replace(upper(irp_app_addstate), '[^A-Z0-9]','')           = regexp_replace(upper(pstate), '[^A-Z0-9]','')
		  AND regexp_replace(upper(irp_app_addzip), '[^A-Z0-9]','')             = regexp_replace(upper(pzip), '[^A-Z0-9]','')
      and irp_app_irpnbr != pIRPNbr;
		-- test to see if we have a hit. if there is s hit, then return 1, else return 0
		IF iTest > 0 THEN
			-- get the value (weight) for ccaddress.
			BEGIN
				-- change to a function to get to lookup value. once we have tested
        -- to see that the address exists, we need to get the weight of the 
        -- test from irp.irp_lookups sing the fValue function.
				iTest := IRP.IRP_API_LOOKUPS.fValue('CCADDRESS');
        --
        --
        RETURN iTest;
			END;
		ELSE
      --
			RETURN 0;
		END IF;
  EXCEPTION WHEN OTHERS THEN
    UTILITY.ERRORSTACK_PKG.pMain(pErrorId => iLog);
    RAISE_APPLICATION_ERROR(-20000,'An error was raised, check the error ' 
                || 'log #: ' || iLog);
	END irp_address;

	-- okay, if the carrier is using the same email address they are just plain stupid.
	FUNCTION irp_email(pEmail IN VARCHAR2,
                     pIrpNbr IN INTEGER) RETURN NUMBER IS
	iTest NUMBER; -- the counter to test to see if the email already exists.
                -- we are reusing iTest to get the return value from irp_lookups.
  iLog  INTEGER; -- error log id.
	BEGIN
  --
		SELECT count(*)
		INTO iTest
		FROM irp.irp_applicants
		WHERE regexp_replace(upper(irp_app_conemail), '[^A-Z0-9]','') = regexp_replace(upper(pemail), '[^A-Z0-9]','')
    AND irp_app_irpnbr != pIrpNbr;
		-- test to see if we have a hit. if we have a hit then
		-- return 1, else return 0.
		IF iTest > 0 THEN
			-- get the value (weight) for email.
			BEGIN
				-- change to a function to get to lookup value. this query is
				iTest := IRP.IRP_API_LOOKUPS.fValue('CCEMAIL');
        --
        RETURN iTest;
			END;
		ELSE
      --
      --
			RETURN 0;
		END IF;
  EXCEPTION WHEN OTHERS THEN
    UTILITY.ERRORSTACK_PKG.pMain(pErrorId => iLog);
    RAISE_APPLICATION_ERROR(-20000,'An error was raised, check the error ' 
                || 'log #: ' || iLog);
	END irp_email;

	-- this is to test to see if the person regestering the carrier has already
	FUNCTION irp_name(pName IN VARCHAR2) RETURN NUMBER IS
	iTest NUMBER;	-- the counter to test to see if the name already exists.
                -- we are reusing iTest to get the return value from irp_lookups.
  iLog  INTEGER;
	BEGIN
    --
		SELECT count(*)
		INTO iTest
		FROM irp.irp_applicants
		WHERE regexp_replace(upper(IRP_APP_CONLNAME), '[^A-Z0-9]','') = regexp_replace(upper(pname), '[^A-Z0-9]','');
		-- test to see if we have a hit. if we have a hit then
		-- return 1, else return 0.
		IF iTest > 0 THEN
			-- the the value (weight) for contact name
			iTest := irp_api_lookups.fValue('CCNAME');
      --
			RETURN iTest;
		ELSE
      --
			RETURN 0;
      --
		END IF;
    --
  EXCEPTION WHEN OTHERS THEN
    UTILITY.ERRORSTACK_PKG.pMain(pErrorId => iLog);
    RAISE_APPLICATION_ERROR(-20000,'An error was raised, check the error ' 
                || 'log #: ' || iLog);
	END irp_name;
  
  FUNCTION irp_name(pfname IN VARCHAR2,
                    pmname IN VARCHAR2 DEFAULT NULL,
                    plname IN VARCHAR2,
                    pIRPNbr IN INTEGER) RETURN NUMBER IS
  icnt          INTEGER;  -- the number of rows returns on a name.
  nprobability  NUMBER;   -- the probability of common names.
  nscore        NUMBER;   -- the score we are going to return.
  nTmp          NUMBER;   -- just a temporary variable.
  BEGIN
  
    -- validate the input. check to see the string does not
    -- contain any sql key words. if they do, then raise
    -- and error.
    -- <fixme> the validate function always returns true.
    /*
    IF utility.generic_pkg.fReserved(pFname) OR
       utility.generic_pkg.fReserved(pMname) OR
       utility.generic_pkg.fReserved(pLname) THEN
          raise_application_error(-20004,NULL);
    END IF;
    */
    -- get score baseline.
    BEGIN
      SELECT irp_lok_value 
      INTO nscore
      FROM irp.irp_lookups
      WHERE irp_lok_type = 'CCNAME';
    exception WHEN no_data_found THEN
          raise_application_error(-20005,NULL);
    END;
    -- is the first name a variation of a common name?
    BEGIN
      SELECT probability
      INTO nprobability
      FROM irp.irp_common_names
      WHERE name_variation = pfname;
      -- we had a match. 
      
    exception WHEN no_data_found THEN
      -- this is an expected condition
      nProbability := 1;
    WHEN too_many_rows THEN
      nprobability := 1;
    END;
    
    -- is the last name a common name?
    BEGIN
      SELECT probability 
      INTO nTmp
      FROM irp.irp_common_names
      WHERE NAME = plname;
      nprobability := nvl(nprobability,1) * ntmp;
      
    exception WHEN no_data_found THEN
      -- this is an expected condition.
      IF nvl(nprobability,1) = 1 THEN
        nprobability := 1;
      END IF;
      WHEN too_many_rows THEN
        nprobability := 1;
      WHEN others THEN
        -- <FIXME> insert error handler.
        raise_application_error(-20006, NULL);
    END;
    -- do we have a match on names?
    SELECT count(*)
    INTO iCnt
    FROM irp.irp_contacts
    WHERE irp_con_fname = pfname
      AND NVL(irp_con_mname,'X') = NVL(pMname,'X')
      AND irp_con_lname = plname
      AND irp_con_irpnbr != pIRPNbr;
    IF iCnt > 0 THEN
      nscore := nscore * nprobability;
      RETURN nscore;
    ELSE
      RETURN 0;
    END IF;
  END irp_name;

  -- test to see if the vehicle has been targeted. if it has then return the value
  -- from irp_lookups.
  FUNCTION irp_vehicle(pvin     IN VARCHAR2,
                       pIRPNbr  IN INTEGER) RETURN NUMBER IS
  iVINCnt INTEGER;
  iTest   NUMBER;     -- NOTE: because the score is in fractions, we need to use numbers.
  iLog    INTEGER;
  BEGIN
    --
    -- is the vehicle targeted. Use the package irp_chamilian
    SELECT COUNT(*)
    INTO iVINCnt
    FROM irp.irp_prsmlcltgtveh
    WHERE irp_ptv_vin = pvin;

    IF iVINCnt > 0 THEN
      -- get the score from irp.ipr_lookups.
      iTest := irp_api_lookups.fValue('VIN');
      --
      RETURN iTest;
    END IF;
    --
    -- if there is no hit, return 0
    RETURN 0;
  EXCEPTION WHEN OTHERS THEN
    UTILITY.ERRORSTACK_PKG.pMain(pErrorId => iLog);
    RAISE_APPLICATION_ERROR(-20000,'An error was raised, check the error ' 
                || 'log #: ' || iLog);
  END irp_vehicle;
  --

  FUNCTION irp_new_usdotnbr(pUSDOTNBR IN VARCHAR2) RETURN NUMBER IS
   dCreateDate  DATE;
   iDaysBetween INTEGER;
   iLog         INTEGER;
  BEGIN
    --
    --
    -- this function will be using irp.irp_prsmlclcensus table to get
    -- the date a usdot number was created.
    --
    BEGIN
      SELECT IRP_PLC_DATEADDED
      INTO dCreateDate
      FROM irp.irp_prsmlclcensus
      WHERE irp_plc_usdotno = pUSDOTNBR;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      --
      UTILITY.ERRORSTACK_PKG.pMain(pErrorId => iLog);
      RAISE_APPLICATION_ERROR(-20000,'An error was raised, check the error ' 
                  || 'log #: ' || iLog);
      RETURN -1;
    END;
    --
    -- We can do this a bit better. right now we are hard coding
    -- the newness of a carrier. lets think about what we can
    -- do to make this a bit more flexable.
    iDaysBetween := SYSDATE - dCreateDate;
    IF iDaysBetween < 30 THEN
      --
      RETURN 1;
    ELSE
      --
      RETURN 0;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    UTILITY.ERRORSTACK_PKG.pMain(pErrorId => iLog);
    RAISE_APPLICATION_ERROR(-20000,'An error was raised, check the error ' 
                || 'log #: ' || iLog);
  END irp_new_usdotnbr;

  FUNCTION irp_override_chameleon(pChemId INTEGER,
                                  pNote   CLOB) RETURN INTEGER IS
	PRAGMA AUTONOMOUS_TRANSACTION;
  iLog  INTEGER; -- ERROR LOG
  iNoteId INTEGER; 
  BEGIN
    UPDATE irp.irp_chameleons SET IRP_CHA_OVERRIDE = 'Y'
    WHERE irp_cha_id = pChemId;
    
    iNoteId := irp.irp_api_notes.pInsNote(pChemId => pChemId,
                                          pNote   => pNote);
    COMMIT;
    RETURN iNoteId;
  EXCEPTION WHEN OTHERS THEN
    ROLLBACK;
    UTILITY.ERRORSTACK_PKG.pMain(pErrorId => iLog);
    RAISE_APPLICATION_ERROR(-20000,'An error was raised, check the error ' 
                || 'log #: ' || iLog);
    RETURN -1;
  END irp_override_chameleon;
  
-- will return the number of vehicles that match the VIN.
  FUNCTION fcheckvehicle(pvin       IN VARCHAR2,
                         pirpnbr    IN INTEGER) RETURN NUMBER IS
    iCnt    INTEGER;
    iLog    INTEGER;
  BEGIN
    SELECT COUNT(*)
    INTO iCnt
    FROM IRP_VEHICLES,
      IRP_DECALS,
      IRP_BILLPAYMENTS,
      IRP_BILLS,
      IRP_FLEETS,
      IRP_FLEETVEHICLES,
      IRP_PLATES,
      IRP_APPLICANTS
    WHERE  IRP_PLA_SEQ = IRP_DEC_PLATESEQ
     AND IRP_BIL_REGID = IRP_DEC_REGID
     AND IRP_BLP_BILLID = IRP_BIL_ID
     AND IRP_VEH_ID = IRP_DEC_VEHICLEID
     AND IRP_FLE_ID = IRP_DEC_FLEETID
     AND IRP_FLV_FLEETID = IRP_DEC_FLEETID
     AND IRP_FLV_VEHICLEID = IRP_DEC_VEHICLEID
     AND IRP_FLV_REGYEAR = IRP_DEC_REGYEAR
     AND IRP_APP_IRPNBR = IRP_FLE_IRPNBR
     AND irp_veh_vin = pvin
     and irp_app_irpnbr != pIRPNbr
    ORDER BY irp_fle_irpnbr,
                            irp_fle_nbr,
                            IRP_FLV_REGYEAR,
                            irp_flv_ownereqpnbr;
    RETURN iCnt;
  EXCEPTION WHEN OTHERS THEN
    UTILITY.ERRORSTACK_PKG.pMain(pErrorId => iLog);
    RAISE_APPLICATION_ERROR(-20000,'An error was raised, check the error ' 
                || 'log #: ' || iLog);
  END fCheckVehicle;

  FUNCTION fchameleonscorethreshold RETURN INTEGER IS
  iScoreThreshold INTEGER;
  BEGIN
    SELECT irp_lok_value
    INTO iScoreThreshold
    FROM irp.irp_lookups
    WHERE irp_lok_type = 'THRESHOLD';
    RETURN iScoreThreshold;
  EXCEPTION WHEN too_many_rows THEN
    RAISE_APPLICATION_ERROR(-20001,NULL);
    WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20002,NULL);
  END fchameleonscorethreshold;

END irp_chameleon;
/


  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IRP"."IRP_API_NOTES" AS
  function finsnote(pchemid in number,
					pnote  IN CLOB) RETURN INTEGER;
  END;
/
--------------------------------------------------------
--  DDL for Package Body IRP_API_NOTES
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IRP"."IRP_API_NOTES" AS
  function finsnote(pchemid in number,
					pnote  IN CLOB) RETURN INTEGER AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  
  iNoteId INTEGER;
  BEGIN
    SELECT irp.irp_notes_seq.nextval
    INTO iNoteId
    FROM DUAL;

    INSERT INTO irp.irp_notes VALUES (
      iNoteId,
      sysdate,
      pNote,
      pChemId,
      user
    );
    COMMIT;
    RETURN iNoteId;
  EXCEPTION WHEN OTHERS THEN
    ROLLBACK;
    raise_application_error(-20002, 'error inserting note.');
    RETURN -1;
  END finsnote;

END;
/

