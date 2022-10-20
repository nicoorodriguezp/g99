CREATE TABLE IF NOT EXISTS tiempo_dim (
   id_tiempo SERIAL PRIMARY KEY,
   fecha int8,
   anio int8,
   mes int8,
   dia int8,
   dia_semana varchar(20)
);

CREATE TABLE IF NOT EXISTS divisa_dim (
   id_currency SERIAL PRIMARY KEY,
   currency_code varchar(3)
);

/* #insertar en la dimension los valores de las tablas Stagign#
insert into divisa_dim (currency_code) 
   select distinct c.currency_code from public.stg_chargebacks c union select distinct p.currency_code from public.stg_payments p; */


CREATE TABLE IF NOT EXISTS gateway_dim (
   id_gateway SERIAL PRIMARY KEY,
   gateway_code varchar(20)
);

/* #insertar en la dimension los valores de las tablas Stagign#
insert into gateway_dim (gateway_code) 
   select distinct c.gateway_code from public.stg_chargebacks c union select distinct p.gateway_code from public.stg_payments p; */

CREATE TABLE IF NOT EXISTS metodoPago_dim (
   id_paymentMethod SERIAL PRIMARY KEY,
   payment_method varchar(20)
);

-- Creation table payments 
   CREATE TABLE IF NOT EXISTS Fact_payments (
   payment_id int8 PRIMARY KEY,
   currency_code int8,
   gateway_code int8,
   payment_method int8,
   payment_date int8,
   token_customer VARCHAR(50),
   is_credit VARCHAR(1) NOT NULL,
   is_debit VARCHAR(1) NOT NULL,
   amount float8,
   FOREIGN KEY(payment_date) REFERENCES tiempo_dim (id_tiempo),
   FOREIGN KEY(currency_code) REFERENCES divisa_dim (id_currency),
   FOREIGN KEY (gateway_code) REFERENCES gateway_dim (id_gateway),
   FOREIGN KEY (payment_method) REFERENCES metodoPago_dim (id_paymentMethod)
);

-- Creation table chargebacks
CREATE TABLE IF NOT EXISTS Fact_chargebacks (
   chargebacks_id int8 PRIMARY KEY,
   payment_date int8,
   notification_date int8,
   debit_date int8,
   currency_code int8,
   token_customer VARCHAR(50),
   amount float8,
   is_fraud VARCHAR(1) NOT null,
   FOREIGN KEY(payment_date) REFERENCES tiempo_dim (id_tiempo),
   FOREIGN KEY(notification_date) REFERENCES tiempo_dim (id_tiempo),
   FOREIGN KEY(debit_date) REFERENCES tiempo_dim (id_tiempo),
   FOREIGN KEY(currency_code) REFERENCES divisa_dim (id_currency)
);
