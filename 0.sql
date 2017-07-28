set echo on
spool prism_build.log
-- for pirpgb we don't need to run clean_up.sql
--@@clean_up.sql
@@create_tables.sql
@@create_packages.sql
@@create_types.sql
@@exec_grants.sql
@@insert_seed_data.sql
spool off

