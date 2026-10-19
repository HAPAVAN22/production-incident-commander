-- Customers
CREATE TABLE customers (
    customer_id UUID PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    customer_tier VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Payment Orders
CREATE TABLE payment_orders (
    transaction_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    payment_status VARCHAR(20) NOT NULL,
    payment_method VARCHAR(20),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_payment_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id)
);

-- Service Events
CREATE TABLE service_events (
    event_id UUID PRIMARY KEY,
    transaction_id UUID,
    service_name VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_status VARCHAR(20),
    latency_ms INTEGER,
    error_code VARCHAR(50),
    error_message TEXT,
    request_id UUID,
    trace_id UUID,
    event_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_event_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES payment_orders(transaction_id)
);
