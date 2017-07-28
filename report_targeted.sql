--------------------------------------------------------
--  DDL for Package IRP_REPORTTARGETED
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IRP"."IRP_REPORTTARGETED" 
AUTHID current_user
--accessible BY (IRP.IRP_READPRISMFILES)			-- this is a version 12c feature. implement when move to 12c.
AS
	-- constant delorations
	lVersion	CONSTANT VARCHAR2(10) := '20161026.1';

	FUNCTION MAIN RETURN NUMBER;
	FUNCTION WHAT_VERSION RETURN VARCHAR2;

END;

/
--------------------------------------------------------
--  DDL for Package Body IRP_REPORTTARGETED
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IRP"."IRP_REPORTTARGETED" AS

/*
	modification history
	20161026.1 Initial version. the main function calls email_report that
	runs the report irp0499 and sends out a summary email on the processing.
*/

	FUNCTION WHAT_VERSION RETURN VARCHAR2 IS
	BEGIN
		RETURN lVersion;
	END;

	-- procedure email report.
	PROCEDURE EMAIL_REPORT(pStime IN TIMESTAMP) IS
	kount		NUMBER;				-- the number of letters generated.
	sFrom		IRP.IRP_EMAILPARAMS.IRP_EMP_LISTITEMS%TYPE;		-- email from line
	sTo			IRP.IRP_EMAILPARAMS.IRP_EMP_LISTITEMS%TYPE;		-- email to line
	sCC			IRP.IRP_EMAILPARAMS.IRP_EMP_LISTITEMS%TYPE := NULL;		-- cc list
	sBCC		IRP.IRP_EMAILPARAMS.IRP_EMP_LISTITEMS%TYPE := NULL;		-- bcc list
	sSubject	IRP.IRP_EMAILPARAMS.IRP_EMP_LISTITEMS%TYPE;		-- email subject line.
	sSMTPsvr	IRP.IRP_COMMISSIONER.IRP_COM_SMTPSERVER%TYPE;	-- server port
	sBody		CLOB;											-- email body.
	tEtime		TIMESTAMP;										-- end timestamp.
	crlf		VARCHAR2(4);									-- carrage return linefeed.
	-- 
	myPlist		system.SRW_PARAMLIST;
	myIdent		system.SRW.Job_Ident;
	selectDate	DATE;
	lDoneFlag	NUMBER := 0;

	BEGIN
		-- init variables
		crlf		:= '<br>';
		sFrom		:= 'irp@marylandmva.com';
		sTo			:= 'rlockard@mdot.state.md.us, sschneider1@mdot.state.md.us,lbanks1@mdot.state.md.us';
		sSubject	:= 'TESTING Letters of suspension for unsafe / unsat.';
		sBody		:= 'Running irp.irp_reporttargeted. ';
		-- Get a count of the number of letters we are generating.

		BEGIN
			WHILE lDoneFlag = 0
			LOOP
				select count(*)
				into lDoneFlag
				from irp.irp_jobs
				where name = 'IRP_PROCESSTARGETED'
				and done = 'Y';
				IF lDoneFlag = 0 THEN
					sys.dbms_lock.sleep(600);
				END IF;
			END LOOP;
		END;
--
		BEGIN
			select count(*)
			into kount
			from irp.irp_applicants a,
				irp.irp_prsmlcltgtcar p,
				IRP.IRP_PRISMMCSIP ps
			where a.irp_app_usdotno = p.irp_ptc_usdotno
			  and p.irp_ptc_mcsipstep = ps.irp_mcs_step
			  and ps.irp_mcs_tgtroad = 'Y'
			  and irp_app_target = 'Y'
			  and irp_app_status != 'T';
		EXCEPTION WHEN OTHERS THEN
			RAISE_APPLICATION_ERROR(-2,'Error checking to see if there are any letters to send ' || sqlerrm);
		END;

		sys.dbms_output.put_line('there are ' || to_char(kount) || ' letters to process');

		IF lDoneFlag > 0 AND kount > 0 then
			selectDate := TRUNC( sysdate ) - 1;
			myPlist := system.SRW_PARAMLIST(system.SRW_PARAMETER( '','' ) );
			system.srw.add_parameter( myPlist,'GATEWAY', 'http://mdotoradev1.otts.mdot.mdstate/reports/rwservlet' );
			system.srw.add_parameter( myPlist,'SERVER', 'rep_mdotoradev1_FRHome1' );
			system.srw.add_parameter( myPlist,'REPORT', 'IRP0499.rdf' );
			system.srw.add_parameter( myPlist,'USERID', 'IRP/IRP1@PIRP' );
			system.srw.add_parameter( myPlist,'AUTHID', 'IRP/IRP1@PIRP' );
			system.srw.add_parameter( myPlist,'SUBJECT', 'Targeted Carrier Letters'); 
			system.srw.add_parameter( myPlist,'DESTYPE', 'MAIL' );
			system.srw.add_parameter( myPlist,'DESFORMAT', 'PDF' );
			system.srw.add_parameter( myPlist,'DESNAME', 'MVA_IRP_Administration@mdot.state.md.us, rlockard@mdot.state.md.us,sschneider1@mdot.state.md.us' );
			myIdent := system.srw.run_report( myPlist );
	--
			tEtime	:= current_timestamp;
	--	there are no suspenion letters to process, send email stating so.
		ELSE
			tEtime	:= current_timestamp;
/*	because reports is sending out the report as an pdf email attachment and the 
	subjct line of that email contains the count of letters, we don't need to scheme
	out an additional email. */
			sBody	:= sBody || 'There are no suspension letters that need ' || crlf;
			sBody	:= sBody || 'to be printed and sent. ' || crlf;
			sBody	:= sBody || 'Start time = ' || pStime || '.' || crlf;
			sBody	:= sBody || 'End time = ' || tEtime || '.' || crlf;
	--
			MAIL_TOOLS.sendmail( smtp_server => IRP_PRINT.GET_SMTP_SERVER,
								smtp_server_port	=> 25,
								from_name			=> sFrom,
								to_name				=> sTo,
								cc_name				=> sCC,
								bcc_name			=> sBCC,
								subject				=> sSubject,
								MESSAGE				=> TO_CLOB( sBody ),
								priority			=> NULL,
								filename			=> NULL,
								binaryfile			=> EMPTY_BLOB ( ),
								DEBUG				=> 0 );

		END IF;
	END EMAIL_REPORT;

	-- this is to check to see if the report needs to be run.
	-- if there are reports to run, then x will be > zero. 
	-- if there are no report to run, then x will = zero.
	-- if there is an exception in the query than x = negative one.
	FUNCTION f_chk_report RETURN NUMBER IS
	x number;		-- dumb variable
	BEGIN
		select count(*)
		into x
		from irp.IRP_APPLICANTS a,
			 irp.IRP_PRSMLCLTGTCAR p,
			 irp.IRP_PRISMMCSIP ps
		where a.irp_app_usdotno = p.IRP_PTC_USDOTNO
		  and p.IRP_PTC_MCSIPSTEP = ps.IRP_MCS_STEP
		  and ps.IRP_MCS_TGTROAD = 'Y'
		  and irp_app_target = 'Y'
		  and irp_app_status != 'T';

		RETURN x;
	EXCEPTION WHEN OTHERS THEN
		RETURN -1;
	END f_chk_report;

	-- the main function is the entry point for this package. 
	-- calls email_report then updates ipr.irp_jobs and irp.irp_applicants.
	-- once completed if success it returns 0, if there is an exception
	-- then main will return -1.
	FUNCTION MAIN RETURN NUMBER IS
	iLogID NUMBER;
	x 		NUMBER; 		-- this is just a dumb variable.
	BEGIN

		iLogID := utility.log_stack.create_entry(pUnit => 'irp_reporttargeted.main',
												pLIne => $$PLSQL_LINE,
												pSTime => current_timestamp);


		-- check to see if we need to run the report.
		x := f_chk_report;
		-- if we got a return then run the report.
		if x > 0 then
			--email the summary report
			EMAIL_REPORT(pStime => current_timestamp);
			-- sleep for one minute to make sure the job is done. 
			sys.dbms_lock.sleep(10);
		end if;
		-- to set flags to mark complete. 
		-- update irp.irp_jobs. Once we are done, we are going to change irp.irp_jobs back to 'N'.
		update irp.irp_jobs
		set done = 'N'
		where name = 'IRP_PROCESSTARGETED';
		--
		-- update irp_applicants  
		UPDATE irp.irp_applicants
		SET irp_app_status = 'T'
		WHERE irp_app_target = 'Y'
		AND irp_app_status != 'T';
--
		commit;
		utility.log_stack.end_entry(pLogId => iLogID, pETime => current_timestamp, pResults => NULL);
		RETURN 0;
	EXCEPTION WHEN OTHERS THEN
		rollback;
		utility.log_stack.end_entry(pLogID => iLogID, pETime => current_timestamp, pResults => 'error ' || sqlerrm);
		RETURN -1;
		--raise_application_error(-3,'Error running irp_reporttargeted.main ' || sqlerrm);
	END MAIN;
END IRP_REPORTTARGETED;

/
