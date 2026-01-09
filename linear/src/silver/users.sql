CREATE OR REPLACE MATERIALIZED VIEW users as
select 
id,
name,
display_name,
email,
active as active_flag,
admin as admin_flag,
guest as guest_flag,
  CAST(convert_timezone('UTC', 'America/Edmonton', created_at) AS DATE) as created_date,
  CAST(convert_timezone('UTC', 'America/Edmonton', updated_at) AS DATE) as updated_date
   from ${source_catalog}.linear.users
   where archived_at is null