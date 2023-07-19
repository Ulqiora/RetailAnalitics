CREATE ROLE Administrator WITH SUPERUSER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA PUBLIC TO Administrator;

CREATE ROLE Visitor;
GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO Visitor;

-- DELETE
-- REASSIGN OWNED BY Administrator TO postgres;
-- DROP OWNED BY Administrator;
-- DROP ROLE IF EXISTS Administrator;
--
-- REASSIGN OWNED BY Visitor TO postgres;
-- DROP OWNED BY Visitor;
-- DROP ROLE IF EXISTS Visitor;

-- TEST
-- psql -d retail
-- SELECT current_user;
-- SET ROLE visitor;
-- SELECT current_user;
-- SELECT COUNT(*) FROM periods;
-- INSERT INTO personal_information VALUES (100000, 'Alex', 'Romanov', 'imperator@mail.ru', '+79990001907');
-- DELETE FROM personal_information WHERE customer_id = 1;

-- SELECT current_user;
-- SET ROLE administrator;
-- SELECT current_user;
-- SELECT COUNT(*) FROM view_periods;
-- INSERT INTO personal_information VALUES (100000, 'Alex', 'Romanov', 'imperator@mail.ru', '+79990001907');
-- SELECT * FROM personal_information WHERE customer_id = 100000;
-- DELETE FROM personal_information WHERE customer_id = 100000;
-- SELECT * FROM personal_information WHERE customer_id = 100000;
