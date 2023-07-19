-- 2.1 Customers View
DROP VIEW IF EXISTS Customers_view, All_tables_data;
DROP TABLE IF EXISTS segment_status_average, segment_status_frequency, segment_status_churn CASCADE;

CREATE VIEW All_tables_data AS
SELECT pi.customer_id,
       customer_name,
       customer_surname,
       customer_primary_email,
       customer_primary_phone,
       c.customer_card_id,
       t.transaction_id,
       transaction_summ,
       transaction_datetime,
       transaction_store_id,
       sku_id,
       sku_amount,
       sku_summ,
       sku_summ_paid,
       sku_discount
FROM personal_information pi
         JOIN cards c ON pi.customer_id = c.customer_id
         JOIN transactions t ON c.customer_card_id = t.customer_card_id
         JOIN checks c2 ON t.transaction_id = c2.transaction_id;

CREATE TABLE segment_status_average
(
    status_Customer_Average_Check_Segment VARCHAR NOT NULL CHECK (status_Customer_Average_Check_Segment = 'High' OR
                                                                  status_Customer_Average_Check_Segment = 'Medium' OR
                                                                  status_Customer_Average_Check_Segment = 'Low')
);

CREATE TABLE segment_status_frequency
(
    status_Customer_Frequency_Segment VARCHAR NOT NULL CHECK (status_Customer_Frequency_Segment = 'Often' OR
                                                              status_Customer_Frequency_Segment = 'Occasionally' OR
                                                              status_Customer_Frequency_Segment = 'Rarely')
);

CREATE TABLE segment_status_churn
(
    status_Customer_Churn_Segment VARCHAR NOT NULL CHECK (status_Customer_Churn_Segment = 'Low' OR
                                                          status_Customer_Churn_Segment = 'Medium' OR
                                                          status_Customer_Churn_Segment = 'High')
);

INSERT INTO segment_status_average
VALUES ('High'),
       ('Medium'),
       ('Low');
INSERT INTO segment_status_frequency
VALUES ('Often'),
       ('Occasionally'),
       ('Rarely');
INSERT INTO segment_status_churn
VALUES ('Low'),
       ('Medium'),
       ('High');

CREATE OR REPLACE VIEW Segment AS
SELECT ROW_NUMBER() OVER () AS id,
       segment_status_average.status_Customer_Average_Check_Segment,
       segment_status_frequency.status_Customer_Frequency_Segment,
       segment_status_churn.status_Customer_Churn_Segment
FROM segment_status_average,
     segment_status_frequency,
     segment_status_churn
WHERE segment_status_frequency.status_Customer_Frequency_Segment <>
      segment_status_average.status_Customer_Average_Check_Segment
  AND segment_status_frequency.status_Customer_Frequency_Segment <> segment_status_churn.status_Customer_Churn_Segment;

CREATE VIEW Customers_view AS
WITH count_customers AS (SELECT ROW_NUMBER()
                                OVER (ORDER BY sum(sku_summ_paid) / count(transaction_id) DESC) AS count_cmr,
                                customer_id
                         FROM All_tables_data
                         GROUP BY customer_id),
     all_main_data AS (SELECT personal_information.Customer_id,
                              sum(t.transaction_summ) / count(t.transaction_id)                                                               AS Customer_Average_Check,
                              (MAX(t.transaction_datetime) -
                               MIN(t.transaction_datetime)) / (SELECT DISTINCT count(t.transaction_id)
                                                               FROM transactions
                                                                        JOIN cards c3 ON c3.customer_card_id = transactions.customer_card_id) AS Customer_Frequency,
                              current_date - MAX(t.transaction_datetime)                                                                      AS Customer_Inactive_Period,
                              extract(EPOCH FROM current_date - MAX(t.transaction_datetime)) /
                              extract(EPOCH FROM ((MAX(t.transaction_datetime) -
                                                   MIN(t.transaction_datetime)) /
                                                  (SELECT DISTINCT count(t.transaction_id)
                                                   FROM transactions
                                                            JOIN cards c3 ON c3.customer_card_id = transactions.customer_card_id)))           AS Customer_Churn_Rate

                       FROM personal_information
                                JOIN cards c ON personal_information.customer_id = c.customer_id
                                JOIN transactions t ON c.customer_card_id = t.customer_card_id
                                JOIN checks c2 ON t.transaction_id = c2.transaction_id
                       GROUP BY personal_information.Customer_id
                       ORDER BY Customer_Average_Check DESC, Customer_Frequency ASC),
     all_segments_statuses AS (SELECT all_main_data.customer_id,
                                      (CASE
                                           WHEN Customer_Churn_Rate BETWEEN 0.0 AND 2.0 THEN 'Low'
                                           WHEN Customer_Churn_Rate BETWEEN 2.0 AND 5.0 THEN 'Medium'
                                           WHEN Customer_Churn_Rate > 5.0 THEN 'High'
                                          END) AS Customer_Churn_Segment,
                                      (CASE
                                           WHEN (ROW_NUMBER() OVER (ORDER BY Customer_Average_Check DESC) * 1.0) <=
                                                (SELECT max(count_cmr) FROM count_customers) / 10.0 THEN 'High'
                                           WHEN (ROW_NUMBER() OVER (ORDER BY Customer_Average_Check DESC) * 1.0) <=
                                                (SELECT max(count_cmr) FROM count_customers) * 0.35 THEN 'Medium'
                                           WHEN (ROW_NUMBER() OVER (ORDER BY Customer_Average_Check DESC) * 1.0) <=
                                                (SELECT max(count_cmr) FROM count_customers) * 1.0 THEN 'Low'
                                          END) AS Customer_Average_Check_Segment,
                                      (CASE
                                           WHEN (ROW_NUMBER() OVER (ORDER BY Customer_Frequency ASC) *
                                                 1.0) <=
                                                (SELECT max(count_cmr) FROM count_customers) / 10.0 THEN 'Often'
                                           WHEN (ROW_NUMBER() OVER (ORDER BY Customer_Frequency ASC) *
                                                 1.0) <=
                                                (SELECT max(count_cmr) FROM count_customers) * 0.35 THEN 'Occasionally'
                                           WHEN (ROW_NUMBER() OVER (ORDER BY Customer_Frequency ASC) *
                                                 1.0) <=
                                                (SELECT max(count_cmr) FROM count_customers) * 1.0 THEN 'Rarely'
                                          END) AS Customer_Frequency_Segment
                               FROM all_main_data
                                        JOIN personal_information pi ON pi.customer_id = all_main_data.customer_id),
     the_share_of_transactions AS
         (SELECT pi.customer_id                                                                       AS c_id,
                 t.transaction_datetime                                                               AS tr_date,
                 t.transaction_store_id                                                               AS tr_s_id,
                 AVG(t.transaction_summ) OVER (PARTITION BY pi.customer_id),
                 ROW_NUMBER() OVER (PARTITION BY pi.customer_id ORDER BY t.transaction_datetime DESC) AS rn,
                 COUNT(*) OVER (PARTITION BY pi.customer_id, t.transaction_store_id)                  AS cnt

          FROM personal_information AS pi
                   JOIN cards AS c ON pi.customer_id = c.customer_id
                   JOIN transactions AS t ON t.customer_card_id = c.customer_card_id),
     main_store_1 AS (SELECT DISTINCT c_id,
                                      first_value(tr_s_id)
                                      OVER (PARTITION BY c_id ORDER BY cnt DESC, tr_date DESC)  AS main_store,
                                      first_value(tr_s_id) OVER (PARTITION BY c_id ORDER BY rn) AS last_store
                      FROM the_share_of_transactions),
     main_store_2 AS (SELECT c_id,
                             count(DISTINCT tr_s_id) last_3_cnt
                      FROM the_share_of_transactions
                      WHERE rn <= 3
                      GROUP BY c_id)
SELECT almd.customer_id,
       Customer_Average_Check,
       ass.Customer_Average_Check_Segment,
       (select extract(EPOCH FROM Customer_Frequency) / 3600.0 / 24.0)       as Customer_Frequency,
       ass.Customer_Frequency_Segment,
       (select extract(EPOCH FROM Customer_Inactive_Period) / 3600.0 / 24.0) as Customer_Inactive_Period,
       Customer_Churn_Rate,
       ass.Customer_Churn_Segment,
       s.id                                                                  AS Customer_Segment,
       CASE
           WHEN last_3_cnt = 1 THEN last_store
           ELSE main_store
           END                                                               AS Customer_Primary_Store
FROM all_main_data almd
         JOIN personal_information pi ON pi.customer_id = almd.customer_id
         JOIN all_segments_statuses ass ON ass.customer_id = almd.customer_id
         JOIN main_store_1 ON main_store_1.c_id = almd.customer_id
         JOIN main_store_2 ON main_store_2.c_id = almd.customer_id
         CROSS JOIN segment s
WHERE s.status_customer_average_check_segment = Customer_Average_Check_Segment
  AND s.status_customer_frequency_segment = Customer_Frequency_Segment
  AND s.status_customer_churn_segment = Customer_Churn_Segment
ORDER BY Customer_Average_Check DESC, Customer_Frequency ASC;

-- 2.2 Purchase history View
DROP VIEW IF EXISTS General CASCADE;

CREATE VIEW General AS
SELECT cards.Customer_ID,
       transactions.Transaction_id,
       transactions.Transaction_datetime,
       checks.SKU_ID,
       sku_group.Group_ID,
       group_name,
       SKU_Amount,
       SKU_Summ,
       SKU_Summ_Paid,
       SKU_Discount,
       SKU_Purchase_Price,
       SKU_Retail_Price
FROM transactions
         JOIN cards ON transactions.customer_card_id = cards.customer_card_id
         JOIN checks ON transactions.transaction_id = checks.transaction_id
         JOIN product_grid ON checks.sku_id = product_grid.sku_id
         JOIN stores ON product_grid.sku_id = stores.sku_id AND
                        transactions.transaction_store_id = stores.transaction_store_id
         JOIN sku_group ON product_grid.group_id = sku_group.group_id
ORDER BY 1, 2;

DROP VIEW IF EXISTS Purchase_history CASCADE;

CREATE VIEW Purchase_history AS
SELECT Customer_ID                                            AS Customer_ID,
       Transaction_id                                         AS Transaction_ID,
       Transaction_datetime                                   AS Transaction_DateTime,
       Group_ID                                               AS Group_ID,
       ROUND(SUM(SKU_Purchase_Price * General.SKU_Amount), 2) AS Group_Cost,
       ROUND(SUM(SKU_Summ), 2)                                AS Group_Summ,
       ROUND(SUM(SKU_Summ_Paid), 2)                           AS Group_Summ_Paid
FROM General
GROUP BY Customer_ID, Transaction_id, Transaction_datetime, Group_ID;

-- TEST
SELECT *
FROM Purchase_history;

SELECT *
FROM Purchase_history
WHERE Group_ID = 1;

SELECT *
FROM Purchase_history
WHERE Transaction_DateTime BETWEEN '2018-01-01' AND '2019-01-01';

-- 2.3 Periods View
DROP VIEW IF EXISTS Periods CASCADE;

CREATE VIEW Periods AS
SELECT Customer_ID               AS Customer_ID,
       Group_ID                  AS Group_ID,
       MIN(Transaction_DateTime) AS First_Group_Purchase_Date,
       MAX(Transaction_DateTime) AS Last_Group_Purchase_Date,
       COUNT(*)                  AS Group_Purchase,
       ROUND((MAX(Transaction_DateTime)::date - MIN(Transaction_DateTime)::date + 1):: NUMERIC / COUNT(*),
             2)                  AS Group_Frequency,
       ROUND(COALESCE(MIN(CASE WHEN SKU_Discount = 0 THEN NULL ELSE SKU_Discount / General.SKU_Summ END), 0),
             2)                  AS Group_Min_Discount
FROM General
GROUP BY Customer_ID, Group_ID;

-- TEST
SELECT *
FROM Periods;

SELECT *
FROM Periods
WHERE Group_Frequency < 100;

SELECT *
FROM Periods
WHERE Group_Min_Discount = 0;

-- 2.4 Groups View
CREATE
    OR REPLACE FUNCTION fnc_group_affinity_index(in_customer_id BIGINT, in_group_id BIGINT)
    RETURNS NUMERIC
    LANGUAGE plpgsql AS
$$
BEGIN
    RETURN ROUND((SELECT (SELECT Group_Purchase::NUMERIC / COUNT(DISTINCT Transaction_ID)
                          FROM Purchase_history
                          WHERE Customer_ID = in_customer_id
                            AND Transaction_DateTime >= First_Group_Purchase_Date
                            AND Transaction_DateTime <= Last_Group_Purchase_Date)
                  FROM Periods
                  WHERE Customer_ID = in_customer_id
                    AND Group_ID = in_group_id), 2);
END;
$$;

CREATE OR REPLACE FUNCTION fnc_group_churn_rate(in_customer_id BIGINT, in_group_id BIGINT)
    RETURNS NUMERIC
    LANGUAGE plpgsql AS
$$
BEGIN
    RETURN ROUND((SELECT EXTRACT(EPOCH FROM ((SELECT MAX(analysis_formation) FROM date_of_analysis_formation) -
                                             Last_Group_Purchase_Date)) / 86400.0 / Group_Frequency
                  FROM Periods
                  WHERE Customer_ID = in_customer_id
                    AND Group_ID = in_group_id), 2);
END;
$$;

CREATE
    OR REPLACE FUNCTION fnc_group_stability_index(in_customer_id BIGINT, in_group_id BIGINT)
    RETURNS NUMERIC
    LANGUAGE plpgsql AS
$$
DECLARE
    tmp_Group_Frequency NUMERIC := (SELECT Group_Frequency
                                    FROM Periods
                                    WHERE Customer_ID = in_customer_id
                                      AND Group_ID = in_group_id);
BEGIN
    RETURN ROUND((WITH tmp_cal AS
                           (SELECT COALESCE(EXTRACT(EPOCH FROM (Transaction_DateTime -
                                                                (LAG(Transaction_DateTime) OVER (ORDER BY Transaction_DateTime)))) /
                                            86400.0, 0) AS Interval
                            FROM Purchase_history
                            WHERE Customer_ID = in_customer_id
                              AND Group_ID = in_group_id)
                  SELECT COALESCE(AVG(ABS(Interval - tmp_Group_Frequency) / tmp_Group_Frequency),
                                  0)
                  FROM tmp_cal
                  WHERE Interval > 0), 2);
END;
$$;

CREATE OR REPLACE FUNCTION fnc_group_Margin_period(in_customer_id BIGINT, in_group_id BIGINT,
                                                   in_interval INTERVAL)
    RETURNS TABLE
            (
                id_Customer  BIGINT,
                id_Group     BIGINT,
                Margin_Group NUMERIC
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY WITH Group_Margin_period AS (SELECT Customer_ID, Group_ID, SUM(Group_Margin) AS RES
                                              FROM (SELECT general.customer_id,
                                                           general.group_id,
                                                           purchase_history.Group_Summ_Paid - purchase_history.Group_Cost AS Group_Margin
                                                    FROM general
                                                             INNER JOIN purchase_history ON general.customer_id = purchase_history.customer_id
                                                    WHERE general.group_id = purchase_history.group_id
                                                      AND general.transaction_id = purchase_history.transaction_id
                                                      AND purchase_history.Transaction_DateTime <=
                                                          (SELECT MAX(analysis_formation)
                                                           FROM date_of_analysis_formation)
                                                      AND purchase_history.Transaction_DateTime >=
                                                          (SELECT MAX(analysis_formation)
                                                           FROM date_of_analysis_formation) -
                                                          in_interval) AS period
                                              WHERE Customer_ID = in_customer_id
                                                AND Group_ID = in_group_id
                                              GROUP BY Group_ID, Customer_ID
                                              order by 1, 2)
                 SELECT *
                 FROM Group_Margin_period;
END;
$$;

CREATE
    OR REPLACE FUNCTION fnc_group_Margin_count_transactions(in_customer_id BIGINT, in_group_id BIGINT, in_count INT)
    RETURNS TABLE
            (
                id_Customer  BIGINT,
                id_Group     BIGINT,
                Margin_Group NUMERIC
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY WITH count_transactions AS (SELECT DISTINCT General.customer_id,
                                                             General.transaction_id,
                                                             General.Transaction_DateTime
                                             FROM General
                                                      INNER JOIN purchase_history ON General.customer_id = purchase_history.Customer_ID
                                             WHERE General.group_id = purchase_history.Group_ID
                                               AND General.Transaction_DateTime <= (SELECT MAX(analysis_formation)
                                                                                    FROM date_of_analysis_formation)
                                             ORDER BY 1 ASC, 3 DESC),
                      Group_Margin_count_transactions AS (SELECT customer_id, group_id, SUM(Group_Margin)
                                                          FROM (SELECT x.customer_id,
                                                                       General.group_id,
                                                                       x.transaction_id,
                                                                       purchase_history.Group_Summ_Paid - purchase_history.Group_Cost AS Group_Margin
                                                                FROM (SELECT ROW_NUMBER() OVER (PARTITION BY customer_id ) AS r,
                                                                             t.*
                                                                      FROM count_transactions t) x
                                                                         INNER JOIN General
                                                                                    ON (x.customer_id =
                                                                                        General.customer_id AND
                                                                                        x.transaction_id =
                                                                                        General.transaction_id)
                                                                         INNER JOIN purchase_history ON (x.customer_id =
                                                                                                         purchase_history.Customer_ID AND
                                                                                                         General.group_id =
                                                                                                         purchase_history.Group_ID AND
                                                                                                         x.transaction_id =
                                                                                                         purchase_history.Transaction_ID)
                                                                WHERE x.r <= in_count
                                                                  AND General.customer_id = in_customer_id
                                                                  AND General.group_id = in_group_id
                                                                ORDER BY 1, 2) AS tmp
                                                          GROUP BY Group_ID, Customer_ID
                                                          order by 1, 2)
                 SELECT *
                 FROM Group_Margin_count_transactions;
END;
$$;

CREATE OR REPLACE FUNCTION fnc_group_Margin(in_customer_id BIGINT, in_group_id BIGINT,
                                            in_method INT, in_interval INTERVAL,
                                            in_count INT)
    RETURNS NUMERIC
    LANGUAGE plpgsql AS
$$
DECLARE
    group_margin NUMERIC;
BEGIN
    IF (in_method = 1) THEN
        group_margin := (SELECT fnc_group_Margin_period.Margin_Group
                         FROM fnc_group_Margin_period(in_customer_id, in_group_id, in_interval));
    ELSEIF (in_method = 2) THEN
        group_margin := (SELECT fnc_group_Margin_count_transactions.Margin_Group
                         FROM fnc_group_Margin_count_transactions(in_customer_id, in_group_id, in_count));
    END IF;
    RETURN group_margin;
END;
$$;

CREATE OR REPLACE FUNCTION fnc_group_discount_share(in_customer_id BIGINT, in_group_id BIGINT)
    RETURNS NUMERIC
    LANGUAGE plpgsql AS
$$
DECLARE
    tmp_Group_Purchase NUMERIC := (SELECT Group_Purchase
                                   FROM periods
                                   WHERE Customer_ID = in_customer_id
                                     AND Group_ID = in_group_id);
BEGIN
    RETURN ROUND((SELECT COUNT(transaction_id)
                  FROM General AS tmp
                  WHERE sku_discount > 0
                    AND Customer_ID = in_customer_id
                    AND Group_ID = in_group_id):: NUMERIC / tmp_Group_Purchase, 2);
END;
$$;

CREATE OR REPLACE FUNCTION fnc_group_minimum_discount(in_customer_id BIGINT, in_group_id BIGINT)
    RETURNS NUMERIC
    LANGUAGE plpgsql AS
$$
BEGIN
    RETURN (SELECT Group_Min_Discount
            FROM Periods
            WHERE Customer_ID = in_customer_id
              AND Group_ID = in_group_id);
END;
$$;

CREATE OR REPLACE FUNCTION fnc_group_average_discount(in_customer_id BIGINT, in_group_id BIGINT)
    RETURNS NUMERIC
    LANGUAGE plpgsql AS
$$
BEGIN
    RETURN ROUND((SELECT SUM(Group_Summ_Paid) / SUM(Group_Summ)
                  FROM Purchase_history
                  WHERE Customer_ID = in_customer_id
                    AND Group_ID = in_group_id), 2);
END;
$$;

CREATE OR REPLACE FUNCTION fnc_create_group_view(in_method INT DEFAULT 1, in_interval INTERVAL DEFAULT '5000 day',
                                                 in_count INT DEFAULT 1000)
    RETURNS TABLE
            (
                Customer_ID            BIGINT,
                Group_ID               BIGINT,
                Group_Affinity_Index   NUMERIC,
                Group_Churn_Rate       NUMERIC,
                Group_Stability_Index  NUMERIC,
                Group_Margin           NUMERIC,
                Group_Discount_Share   NUMERIC,
                Group_Minimum_Discount NUMERIC,
                Group_Average_Discount NUMERIC
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
        SELECT Personal_information.Customer_ID,
               Product_grid.Group_ID,
               fnc_group_affinity_index(Personal_information.Customer_ID, product_grid.Group_ID),
               fnc_group_churn_rate(Personal_information.Customer_ID, product_grid.Group_ID),
               fnc_group_stability_index(Personal_information.Customer_ID, product_grid.Group_ID),
               COALESCE(
                       fnc_group_Margin(Personal_information.Customer_ID, product_grid.Group_ID, in_method, in_interval,
                                        in_count), 0),
               fnc_group_discount_share(Personal_information.Customer_ID, product_grid.Group_ID),
               fnc_group_minimum_discount(Personal_information.Customer_ID, product_grid.Group_ID),
               fnc_group_average_discount(Personal_information.Customer_ID, product_grid.Group_ID)

        FROM Personal_information
                 JOIN cards ON personal_information.customer_id = cards.customer_id
                 JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
                 JOIN checks ON transactions.transaction_id = checks.transaction_id
                 JOIN product_grid ON checks.sku_id = product_grid.sku_id
        GROUP BY Personal_information.Customer_ID, Product_grid.Group_ID
        ORDER BY Personal_information.Customer_ID, Product_grid.Group_ID;
END;
$$;

DROP VIEW IF EXISTS Groups CASCADE;

CREATE VIEW Groups AS
SELECT *
FROM fnc_create_group_view(1, '5000 day', 1000);

-- TEST
SELECT *
FROM Groups;

SELECT *
FROM Groups
WHERE Customer_ID = 1;

SELECT *
FROM Groups
WHERE Group_Affinity_Index = 1;
