

--create index
CREATE INDEX idx_product_category ON Products (category);
CREATE INDEX idx_product_price ON Product (price);
CREATE INDEX idx_customer_id ON Financial_Transactions(customer_id);

CREATE INDEX idx_transactions_order_customer_product
ON Financial_Transactions (order_id, customer_id, product_id, amount);

