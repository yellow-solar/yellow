/* Check the charset - default for MySQL 8.0+ is utf8mb4  */
SELECT CCSA.character_set_name FROM information_schema.`TABLES` T,
       information_schema.`COLLATION_CHARACTER_SET_APPLICABILITY` CCSA
WHERE CCSA.collation_name = T.table_collation
  AND T.table_schema = "Angaza"
  AND T.table_name in ("clients","accounts");

  