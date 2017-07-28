-- clean_up.sql

-- this is the clean up.

-- tables
drop table utility.app_log;
drop table utility.debug;
drop table utility.error_lines;
drop table utility.errors;
drop table irp.IRP_CHAMELEONS;
drop table irp.irp_lookups;
drop table irp.irp_common_names;
drop table IRP.IRP_CONTACTS;
drop table IRP.IRP_NOTES;
-- sequences
drop sequence utility.error_lines_seq;
drop sequence utility.errors_seq;
drop sequence utility.log_stack_seq;
drop sequence irp.irp_notes_seq;
drop sequence irp.irp_chameleon_seq;
drop sequence irp.irp_lookups_seq;
drop sequence irp.irp_contacts_seq;
-- types
drop type irp.tt_applicants;
drop type irp.ot_applicants;
