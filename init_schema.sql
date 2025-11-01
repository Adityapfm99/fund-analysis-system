-- Create funds table
CREATE TABLE IF NOT EXISTS funds (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    gp_name VARCHAR(255),
    fund_type VARCHAR(100),
    vintage_year INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create capital_calls table
CREATE TABLE IF NOT EXISTS capital_calls (
    id SERIAL PRIMARY KEY,
    fund_id INTEGER REFERENCES funds(id),
    call_date DATE NOT NULL,
    call_type VARCHAR(100),
    amount DECIMAL(15, 2) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create distributions table
CREATE TABLE IF NOT EXISTS distributions (
    id SERIAL PRIMARY KEY,
    fund_id INTEGER REFERENCES funds(id),
    distribution_date DATE NOT NULL,
    distribution_type VARCHAR(100),
    is_recallable BOOLEAN DEFAULT FALSE,
    amount DECIMAL(15, 2) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create adjustments table
CREATE TABLE IF NOT EXISTS adjustments (
    id SERIAL PRIMARY KEY,
    fund_id INTEGER REFERENCES funds(id),
    adjustment_date DATE NOT NULL,
    adjustment_type VARCHAR(100),
    category VARCHAR(100),
    amount DECIMAL(15, 2) NOT NULL,
    is_contribution_adjustment BOOLEAN DEFAULT FALSE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create documents table
CREATE TABLE IF NOT EXISTS documents (
    id SERIAL PRIMARY KEY,
    fund_id INTEGER REFERENCES funds(id),
    file_name VARCHAR(255) NOT NULL,
    file_path VARCHAR(500),
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    parsing_status VARCHAR(50) DEFAULT 'pending',
    error_message TEXT
);
