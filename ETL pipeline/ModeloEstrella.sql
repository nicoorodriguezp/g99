CREATE TABLE tiempo_dim (
   id_tiempo int8  PRIMARY KEY,
   fecha int8,
   anio int8,
   mes int8,
   dia int8,
   dia_semana varchar(20)
);

CREATE TABLE divisa_dim (
   id_currency SERIAL PRIMARY KEY,
   currency_code varchar(3)
);

/* # insertar en la dimension los valores de las tablas Stagign#
insert into divisa_dim (currency_code) 
   select distinct c.currency_code from public.stg_chargebacks c union select distinct p.currency_code from public.stg_payments p; */


CREATE TABLE gateway_dim (
   id_gateway int8  PRIMARY KEY,
   gateway_code varchar(20)
);

CREATE TABLE metodoPago_dim (
   id_paymentMethod int8 PRIMARY KEY,
   payment_method varchar(20)
);

-- Creation table payments 
   CREATE TABLE Fact_payments (
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
CREATE TABLE Fact_chargebacks (
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