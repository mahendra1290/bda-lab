meds = LOAD './med.txt' USING
   PigStorage(',') as (id:int,name:chararray,price:int,symtoms:chararray);

order_by_price = ORDER meds by id DESC;

grouped_by_type = GROUP meds BY symtoms;

cold_meds = FILTER meds BY symtoms == 'cold';

-- Dump order_by_price;
STORE order_by_price into './order-by-price.txt';
STORE grouped_by_type into './grouped_by.txt';
STORE cold_meds into './cold_meds.txt';
-- Dump meds;
