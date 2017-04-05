drop table locker_transactions;

--postgres
create table locker_transactions(
  transaction_id varchar(100) not null,
  transaction_time timestamp not null,
  transaction_amount numeric,
  account_id varchar(100) not null,
  origination_point varchar(100),
  client varchar(512) not null,
  corp varchar(6) not null,
  programmer_id varchar(20),
  programmer_name varchar(100),
  rate_code varchar(20) not null,
  status varchar(512) not null,
  error_code varchar(512),
  error_message varchar(1000),
  primary key(transaction_id, transaction_time, error_code)
);