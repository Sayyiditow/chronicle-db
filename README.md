# chronicle-db
Uses OpenHFT's Chronicle map as a DB implementation for a RESTful database.

WIP - but a lot of the features already allow to do basic db functionalities.

The aim of this is to build a raw data db that:
serves json responses when required,
can do sql like queries
can work with billions of records with cpu scaling

Data Types.
1. varchar, text, primary/foreign keys = String
2. dates/datetime = long (format yyyyMMddHHmmss)
3. currency amounts, numerical values = BigDecimal
4. fixed reference tables = enums
5. boolean for true/false
6. int for small numbers.
7. double if it fits the required precision ONLY.

The key of the map is always a String.