-- create types

CREATE OR REPLACE TYPE irp.ot_applicants AS OBJECT (
    irp_app_irpnbr         NUMBER(6),
    irp_app_company        VARCHAR2(55),
    irp_app_fein           VARCHAR2(9),
    irp_app_addstreet      VARCHAR2(40),
    irp_app_addline2       VARCHAR2(40),
    irp_app_addcity        VARCHAR2(25),
    irp_app_addcounty      VARCHAR2(15),
    irp_app_addstate       VARCHAR2(2),
    irp_app_addzip         VARCHAR2(10),
    irp_app_conlname       VARCHAR2(35),
    irp_app_confax         VARCHAR2(12),
    irp_app_prtsd          VARCHAR2(1),
    irp_app_remarks        VARCHAR2(80),
    irp_app_status         VARCHAR2(1),
    irp_app_userid         VARCHAR2(10),
    irp_app_activitydate   DATE,
    irp_app_usdotno        VARCHAR2(12),
    irp_app_ifta           VARCHAR2(9),
    irp_app_conphone       VARCHAR2(14),
    irp_app_conemail       VARCHAR2(250),
    irp_app_regonly        VARCHAR2(1),
    irp_app_prismchk       DATE,
    irp_app_prismstatus    VARCHAR2(1),
    irp_app_target         VARCHAR2(1)
)

final;
/


CREATE OR REPLACE TYPE irp.tt_applicants IS
    TABLE OF irp.ot_applicants;
/
