DROP TABLE IF EXISTS Personal_information, Cards, Transactions, SKU_group, Product_grid, Checks, Stores, Date_of_analysis_formation CASCADE;

CREATE TABLE Personal_information
(
    Customer_ID            BIGINT PRIMARY KEY,
    Customer_Name          VARCHAR(255) NOT NULL CHECK ( Customer_Name ~ '^[A-ZА-Я][a-zа-я -]+$'),
    Customer_Surname       VARCHAR(255) NOT NULL CHECK ( Customer_Surname ~ '^[A-ZА-Я][a-zа-я -]+$'),
    Customer_Primary_Email VARCHAR(255) NOT NULL CHECK ( Customer_Primary_Email ~
                                                         '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9.-]+$'),
    Customer_Primary_Phone VARCHAR(255) NOT NULL CHECK ( Customer_Primary_Phone ~ '^[+][7][0-9]{10}' )
);

CREATE TABLE Cards
(
    Customer_Card_ID BIGINT PRIMARY KEY NOT NULL,
    Customer_ID      BIGINT             NOT NULL,

    CONSTRAINT fk_Cards_Customer_ID FOREIGN KEY (Customer_ID) REFERENCES Personal_information (Customer_ID)
);

CREATE TABLE Transactions
(
    Transaction_ID       BIGINT PRIMARY KEY UNIQUE,
    Customer_Card_ID     BIGINT  NOT NULL,
    Transaction_Summ     NUMERIC NOT NULL,
    Transaction_DateTime TIMESTAMP WITHOUT TIME ZONE,
    Transaction_Store_ID BIGINT  NOT NULL,

    CONSTRAINT fk_Transactions_Customer_Card_ID FOREIGN KEY (Customer_Card_ID) REFERENCES Cards (Customer_Card_ID)
);

CREATE TABLE SKU_group
(
    Group_ID   BIGINT PRIMARY KEY,
    Group_Name VARCHAR(255) NOT NULL CHECK ( Group_Name ~ '^[A-ZА-Яa-zа-я0-9!-\/:-@[-`{-~ ]+$' )
);

CREATE TABLE Product_grid
(
    SKU_ID   BIGINT PRIMARY KEY,
    SKU_Name VARCHAR(255) NOT NULL CHECK ( SKU_Name ~ '^[A-ZА-Яa-zа-я0-9!-\/:-@[-`{-~ ]+$' ),
    Group_ID BIGINT       NOT NULL,

    CONSTRAINT fk_Product_grid_Group_ID FOREIGN KEY (Group_ID) REFERENCES SKU_group (Group_ID)
);


CREATE TABLE Checks
(
    Transaction_ID BIGINT  NOT NULL,
    SKU_ID         BIGINT  NOT NULL,
    SKU_Amount     NUMERIC NOT NULL,
    SKU_Summ       NUMERIC NOT NULL,
    SKU_Summ_Paid  NUMERIC NOT NULL,
    SKU_Discount   NUMERIC NOT NULL,

    CONSTRAINT fk_Checks_Transaction_ID FOREIGN KEY (Transaction_ID) REFERENCES Transactions (Transaction_ID),
    CONSTRAINT fk_Checks_SKU_ID FOREIGN KEY (SKU_ID) REFERENCES Product_grid (SKU_ID)

);

CREATE TABLE Stores
(
    Transaction_Store_ID BIGINT  NOT NULL,
    SKU_ID               BIGINT  NOT NULL,
    SKU_Purchase_Price   NUMERIC NOT NULL CHECK ( SKU_Purchase_Price >= 0 ),
    SKU_Retail_Price     NUMERIC NOT NULL CHECK ( SKU_Retail_Price >= 0 ),

    CONSTRAINT fk_Stores_SKU_ID FOREIGN KEY (SKU_ID) REFERENCES Product_grid (SKU_ID)

);

CREATE TABLE Date_of_analysis_formation
(
    Analysis_Formation TIMESTAMP WITHOUT TIME ZONE
);

DROP PROCEDURE IF EXISTS Import(Tablename VARCHAR, Path TEXT, Separator CHAR);
DROP PROCEDURE IF EXISTS Import_mini(Tablename VARCHAR, Path TEXT, Separator CHAR);

CREATE PROCEDURE Import(Tablename VARCHAR, Path TEXT, Separator CHAR DEFAULT '\t')
    LANGUAGE plpgsql AS
$$
BEGIN
    IF (Separator = '\t') THEN
        EXECUTE CONCAT('COPY ', Tablename, ' FROM ''', Path, ''' DELIMITER E''\t''', ' CSV;');
    ELSE
        EXECUTE CONCAT('COPY ', Tablename, ' FROM ''', Path, ''' DELIMITER ''', Separator, ''' CSV;');
    END IF;
END;
$$;

CREATE PROCEDURE Import_mini(Tablename VARCHAR, Path TEXT, Separator CHAR DEFAULT '\t')
    LANGUAGE plpgsql AS
$$
BEGIN
    IF (Separator = '\t') THEN
        EXECUTE CONCAT('COPY ', Tablename, ' FROM ''', Path, ''' DELIMITER E''\t''', ' CSV;');
    ELSE
        EXECUTE CONCAT('COPY ', Tablename, ' FROM ''', Path, ''' DELIMITER ''', Separator, ''' CSV;');
    END IF;
END;
$$;

SET datestyle = "ISO, DMY";
-- SET Import_path.txt TO '/Users/bfile/Projects/SQL3_RetailAnalitycs_v1.0-1/datasets/';
SET Import_path.txt TO '/Users/polina/Desktop/SQL3_RetailAnalitycs_v1.0-1/datasets/';

-- CALL Import('Personal_information', (current_setting('Import_path.txt') || 'Personal_Data.tsv'));
-- CALL Import('Cards', (current_setting('Import_path.txt') || 'Cards.tsv'));
-- CALL Import('Transactions', (current_setting('Import_path.txt') || 'Transactions.tsv'));
-- CALL Import('SKU_group', (current_setting('Import_path.txt') || 'Groups_SKU.tsv'));
-- CALL Import('Product_grid', (current_setting('Import_path.txt') || 'SKU.tsv'));
-- CALL Import('Checks', (current_setting('Import_path.txt') || 'Checks.tsv'));
-- CALL Import('Stores', (current_setting('Import_path.txt') || 'Stores.tsv'));
-- CALL Import('Date_of_analysis_formation', (current_setting('Import_path.txt') || 'Date_Of_Analysis_Formation.tsv'));

CALL Import_mini('Personal_information', (current_setting('Import_path.txt') || 'Personal_Data_Mini.tsv'));
CALL Import_mini('Cards', (current_setting('Import_path.txt') || 'Cards_Mini.tsv'));
CALL Import_mini('Transactions', (current_setting('Import_path.txt') || 'Transactions_Mini.tsv'));
CALL Import_mini('SKU_group', (current_setting('Import_path.txt') || 'Groups_SKU_Mini.tsv'));
CALL Import_mini('Product_grid', (current_setting('Import_path.txt') || 'SKU_Mini.tsv'));
CALL Import_mini('Checks', (current_setting('Import_path.txt') || 'Checks_Mini.tsv'));
CALL Import_mini('Stores', (current_setting('Import_path.txt') || 'Stores_Mini.tsv'));
CALL Import_mini('Date_of_analysis_formation',
                 (current_setting('Import_path.txt') || 'Date_Of_Analysis_Formation.tsv'));

DROP PROCEDURE IF EXISTS Export(Tablename VARCHAR, Path TEXT, Separator CHAR);

CREATE PROCEDURE Export(Tablename VARCHAR, Path TEXT, Separator CHAR DEFAULT '\t')
    LANGUAGE plpgsql AS
$$
BEGIN
    IF (Separator = '\t') THEN
        EXECUTE CONCAT('COPY ', Tablename, ' TO ''', Path, ''' DELIMITER E''\t''', ' CSV;');
    ELSE
        EXECUTE CONCAT('COPY ', Tablename, ' TO ''', Path, ''' DELIMITER ''', Separator, ''' CSV;');
    END IF;
END;
$$;

-- SET Export_path.txt TO '/Users/bfile/Projects/SQL3_RetailAnalitycs_v1.0-1/src/export/';
SET Export_path.txt TO '/Users/polina/Desktop/SQL3_RetailAnalitycs_v1.0-1/src/export/';

CALL Export('Personal_information', (current_setting('Export_path.txt') || 'Personal_Data.tsv'), '\t');
CALL Export('Cards', (current_setting('Export_path.txt') || 'Cards.tsv'), '\t');
CALL Export('Transactions', (current_setting('Export_path.txt') || 'Transactions.tsv'), '\t');
CALL Export('SKU_group', (current_setting('Export_path.txt') || 'Groups_SKU.tsv'), '\t');
CALL Export('Product_grid', (current_setting('Export_path.txt') || 'SKU.tsv'), '\t');
CALL Export('Checks', (current_setting('Export_path.txt') || 'Checks.tsv'), '\t');
CALL Export('Stores', (current_setting('Export_path.txt') || 'Stores.tsv'), '\t');
CALL Export('Date_of_analysis_formation', (current_setting('Export_path.txt') || 'Date_Of_Analysis_Formation.tsv'),
            '\t');

CALL Export('Personal_information', (current_setting('Export_path.txt') || 'Personal_Data.csv'), '\t');
CALL Export('Cards', (current_setting('Export_path.txt') || 'Cards.csv'), '\t');
CALL Export('Transactions', (current_setting('Export_path.txt') || 'Transactions.csv'), '\t');
CALL Export('SKU_group', (current_setting('Export_path.txt') || 'Groups_SKU.csv'), '\t');
CALL Export('Product_grid', (current_setting('Export_path.txt') || 'SKU.csv'), '\t');
CALL Export('Checks', (current_setting('Export_path.txt') || 'Checks.csv'), '\t');
CALL Export('Stores', (current_setting('Export_path.txt') || 'Stores.csv'), '\t');
CALL Export('Date_of_analysis_formation', (current_setting('Export_path.txt') || 'Date_Of_Analysis_Formation.csv'),
            '\t');
