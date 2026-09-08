# Python Interview Problems — 210 Questions
**Level: Easy to Easy-Medium | First Round Interview Prep**

Topics covered: List · Dictionary · String · Tuple · Set · Data Cleaning ·
Functions · Comprehensions · File I/O · API / Requests · DB Connection ·
DSA (Arrays, Strings, Hashing, Two Pointers, Sliding Window, Sorting)

---

## SECTION 1 — LIST (30 Problems)

**L1.** Given `nums = [4, 2, 7, 1, 9, 3]`, return the list sorted in ascending order without modifying the original.

**L2.** Remove all duplicate values from `[1, 2, 2, 3, 4, 4, 5]` while preserving order.

**L3.** Given `[10, 20, 30, 40, 50]`, return the second largest element.

**L4.** Flatten `[[1, 2], [3, 4], [5, 6]]` into a single list `[1, 2, 3, 4, 5, 6]`.

**L5.** Given `nums = [1, 2, 3, 4, 5, 6]`, split into two halves. If odd length, the first half gets the extra element.

**L6.** Rotate `[1, 2, 3, 4, 5]` to the right by 2 positions → `[4, 5, 1, 2, 3]`.

**L7.** Count how many even numbers are in `[3, 6, 8, 11, 14, 17, 20]`.

**L8.** Given two lists `a = [1, 3, 5]` and `b = [2, 4, 6]`, merge them alternately → `[1, 2, 3, 4, 5, 6]`.

**L9.** Find the sum of all elements in a nested list `[[1, 2], [3, [4, 5]]]` (any depth).

**L10.** Given `records = [5, 3, 8, 1, 9, 2, 7]`, find the index of the maximum value without using `index(max(...))` directly — use a loop.

**L11.** Remove all `None` values from `[1, None, 2, None, 3, None]`.

**L12.** Given `[1, 2, 3, 4, 5]`, return a new list where every element is squared.

**L13.** Find the intersection (common elements) of `[1, 2, 3, 4]` and `[3, 4, 5, 6]` without using sets.

**L14.** Given a list of strings `["apple", "banana", "cherry"]`, return the longest string.

**L15.** Check if a list `[1, 2, 3, 2, 1]` is a palindrome (reads the same forwards and backwards).

**L16.** Given `[3, 1, 4, 1, 5, 9, 2, 6]`, move all zeros to the end while keeping other elements in order. *(There are no zeros here — modify the list to include some: `[3, 0, 1, 0, 5, 9, 0, 6]`)*

**L17.** Find the frequency of each element in `[1, 2, 2, 3, 3, 3, 4]` — return a dictionary.

**L18.** Given `prices = [100, 80, 60, 40, 20]`, find the maximum profit if you buy at one price and sell at a later (higher) price. Return 0 if no profit is possible.

**L19.** Split `[1, 2, 3, 4, 5, 6, 7, 8, 9]` into batches of size 3 → `[[1,2,3], [4,5,6], [7,8,9]]`.

**L20.** Given `["a", "b", "c", "d"]`, return all possible pairs as a list of tuples: `[("a","b"), ("a","c"), ...]`.

**L21.** Given `[1, 2, 3, 4, 5]`, find all elements that appear more than once — return an empty list if none.

**L22.** Reverse only the elements between index 1 and 4 (inclusive) in `[0, 1, 2, 3, 4, 5]` → `[0, 4, 3, 2, 1, 5]`.

**L23.** Given `nums = [2, 3, -1, 5, -4, 6]`, separate positives and negatives into two lists, preserving order.

**L24.** Given a list of employee dicts `[{"name": "Alice", "salary": 90000}, ...]`, sort by salary descending.

**L25.** Find the two numbers in `[1, 4, 5, 8, 3]` that add up to a target `T = 9`. Return their indices.

**L26.** Given `[1, 1, 2, 3, 3, 4, 5, 5]` (sorted), remove duplicates in-place and return the new length.

**L27.** Rotate a matrix (list of lists) 90 degrees clockwise:
```python
matrix = [[1,2,3],[4,5,6],[7,8,9]]
# expected: [[7,4,1],[8,5,2],[9,6,3]]
```

**L28.** Given `[4, 5, 6, 1, 2, 3]` (a rotated sorted array), find the index of value `1`.

**L29.** Implement a stack using a list with `push()`, `pop()`, and `peek()` methods.

**L30.** Given `temperatures = [30, 32, 28, 35, 36, 29]`, find the maximum temperature for each 3-day window (sliding window).

---

## SECTION 2 — DICTIONARY (30 Problems)

**D1.** Given `d = {"a": 1, "b": 2, "c": 3}`, invert it so keys become values and vice versa.

**D2.** Merge two dicts `d1 = {"x": 1, "y": 2}` and `d2 = {"y": 3, "z": 4}`. Where keys conflict, keep `d2`'s value.

**D3.** Given a list `["apple", "banana", "apple", "cherry", "banana", "apple"]`, count occurrences of each word using a dict.

**D4.** Find the key with the maximum value in `{"a": 10, "b": 25, "c": 18}`.

**D5.** Given `{"name": "Alice", "age": 30, "city": "Mumbai"}`, remove keys whose value is `None` or empty string.

**D6.** Group a list of strings by their first letter:
```python
words = ["apple", "ant", "banana", "ball", "cherry"]
# expected: {"a": ["apple", "ant"], "b": ["banana", "ball"], "c": ["cherry"]}
```

**D7.** Check if two dicts have the same keys (regardless of values): `{"a":1,"b":2}` and `{"a":3,"b":4}`.

**D8.** Given `config = {"host": "localhost", "port": 5432}`, access `"password"` safely, returning `"NOT SET"` if missing.

**D9.** Given a list of dicts (employee records), group employees by department:
```python
employees = [
    {"name": "Alice", "dept": "Eng"},
    {"name": "Bob",   "dept": "Sales"},
    {"name": "Carol", "dept": "Eng"},
]
# expected: {"Eng": ["Alice", "Carol"], "Sales": ["Bob"]}
```

**D10.** Count how many keys in a dict have integer values vs string values.

**D11.** Given `scores = {"Alice": 85, "Bob": 72, "Carol": 91, "Dave": 68}`, find all students who scored above 80.

**D12.** Build a dictionary from two lists — `keys = ["id", "name", "salary"]` and `values = [1, "Alice", 90000]` — using `zip()`.

**D13.** Given a nested dict:
```python
data = {"user": {"name": "Alice", "address": {"city": "Mumbai", "pin": "400001"}}}
```
Safely access `city`. If any key is missing at any level, return `"UNKNOWN"`.

**D14.** Remove all keys from a dict where the value is 0 or `False`.

**D15.** Given a dict `{"a": [1,2,3], "b": [4,5], "c": [6]}`, return the key with the longest list.

**D16.** Given pipeline run records:
```python
runs = [
    {"pipeline": "etl_a", "status": "SUCCESS", "rows": 1000},
    {"pipeline": "etl_b", "status": "FAILED",  "rows": 0},
    {"pipeline": "etl_a", "status": "SUCCESS", "rows": 1200},
]
```
Return a summary dict with total runs, success count, and total rows per pipeline.

**D17.** Flatten a nested dict one level deep:
```python
{"db": {"host": "localhost", "port": 5432}, "app": {"debug": True}}
# expected: {"db.host": "localhost", "db.port": 5432, "app.debug": True}
```

**D18.** Given `{"Jan": 100, "Feb": 80, "Mar": 120, "Apr": 90}`, sort by value descending and return as a list of `(month, value)` tuples.

**D19.** Check if a dict is a subset of another — all key-value pairs from `small` exist in `big`.

**D20.** Given a list of API response records (each is a dict), deduplicate by `"id"` keeping the last occurrence.

**D21.** Given `word_count = {"the": 50, "a": 30, "is": 20, "python": 15}`, return top 2 most frequent words.

**D22.** Build a frequency map from a string:
```python
s = "programming"
# expected: {"p":1, "r":2, "o":1, "g":2, "a":1, "m":2, "i":1, "n":1}
```

**D23.** Given two frequency dicts, find keys present in both and return the sum of their values.

**D24.** Convert a list of tuples `[(1,"a"), (2,"b"), (3,"c")]` into a dict `{1:"a", 2:"b", 3:"c"}`.

**D25.** Given a config dict, update only keys that currently have `None` values with provided defaults.

**D26.** Given `{"a": 1, "b": 2, "c": 3, "d": 4}`, filter to keep only keys `["a", "c"]`.

**D27.** From a list of order dicts, compute total revenue grouped by `category` field.

**D28.** Detect which keys are in `dict1` but NOT in `dict2` (like a schema diff).

**D29.** Given a dict where values are lists, return a new dict with each list deduplicated and sorted.

**D30.** Given records with a `"timestamp"` field, group them by date (strip time part) and count records per date.

---

## SECTION 3 — STRING (30 Problems)

**S1.** Check if a string is a palindrome: `"racecar"` → `True`, `"hello"` → `False`. Ignore case.

**S2.** Count the number of vowels in `"Hello World"`.

**S3.** Reverse each word in the sentence `"Hello World Python"` → `"olleH dlroW nohtyP"`.

**S4.** Given `"  hello world  "`, strip whitespace, capitalize first letter of each word.

**S5.** Check if two strings are anagrams: `"listen"` and `"silent"` → `True`.

**S6.** Find the first non-repeating character in `"aabbcdef"` → `"c"`.

**S7.** Count how many times the substring `"the"` appears in `"the cat sat on the mat and the hat"`.

**S8.** Given a CSV line `"Alice,30,Mumbai,90000"`, split it and return a dict with keys `["name","age","city","salary"]`.

**S9.** Remove all punctuation from `"Hello, World! How's it going?"` → `"Hello World Hows it going"`.

**S10.** Given a list of email strings, return only valid ones (must contain `@` and a `.` after `@`).

**S11.** Convert `"camelCaseString"` to `"camel_case_string"` (snake_case).

**S12.** Convert `"snake_case_string"` to `"SnakeCaseString"` (PascalCase).

**S13.** Given `"order_id=101&status=success&amount=250"`, parse it into a dict `{"order_id":"101","status":"success","amount":"250"}`.

**S14.** Compress consecutive identical characters: `"aaabbbccddddd"` → `"a3b3c2d5"`.

**S15.** Given a log line `"2024-01-15 ERROR Connection timeout after 30s"`, extract: date, level, and message.

**S16.** Check if a string contains only digits: `"12345"` → `True`, `"123a5"` → `False`. Do NOT use `.isdigit()`.

**S17.** Find the longest word in `"The quick brown fox jumped"`.

**S18.** Given `"banana"`, find the most frequent character.

**S19.** Remove duplicate words from `"the cat sat on the mat on the floor"` preserving order.

**S20.** Given a list of column names with mixed casing and spaces:
```python
["  Employee ID", "FIRST NAME", "last_name", "Hire-Date"]
```
Normalize each to snake_case lowercase.

**S21.** Check if all characters in `"Hello123"` are alphanumeric. Do not use `.isalnum()`.

**S22.** Given `"Python is great and Python is powerful"`, replace the second occurrence of `"Python"` with `"PySpark"` only.

**S23.** Find all words in a string that start with a capital letter.

**S24.** Given a string `"12 apples, 5 oranges, 3 bananas"`, extract all numbers as a list of integers.

**S25.** Given `name = "alice"`, `city = "mumbai"`, `score = 95.5`, format as: `"Name: Alice | City: Mumbai | Score: 95.50"`.

**S26.** Count words in a string without using `.split()` — use a loop and character checks.

**S27.** Given a string, return `True` if it has balanced parentheses: `"(a(b)c)"` → `True`, `"(a(b)"` → `False`.

**S28.** Truncate a string to 20 characters and append `"..."` if it was longer: `"Hello this is a long sentence"` → `"Hello this is a long..."`.

**S29.** Given a multi-line string (API response body), extract all lines that contain the word `"ERROR"`.

**S30.** Given `"2024-01-15"`, reformat it as `"15-Jan-2024"` using only string operations (no `datetime`).

---

## SECTION 4 — TUPLE & SET (20 Problems)

**T1.** Unpack `config = ("localhost", 5432, "warehouse", "admin")` into variables `host, port, db, user`.

**T2.** Given `t = (1, 2, 3, 2, 1, 4)`, count occurrences of `2` and find index of `3`.

**T3.** Write a function that returns multiple values as a tuple: min, max, and average of a list.

**T4.** Given a list of tuples `[(1,"Alice",90000),(2,"Bob",72000)]`, sort by the third element (salary) descending.

**T5.** Convert `t = (1, 2, 3)` to a list, append `4`, then convert back to a tuple.

**T6.** Swap values of two variables using tuple unpacking: `a, b = b, a`.

**T7.** Given `rows = [(1,"A"),(2,"B"),(3,"C")]`, unzip into two separate tuples: `(1,2,3)` and `("A","B","C")`.

**T8.** Write a function that accepts `*args` and returns only the unique values as a tuple.

**T9.** Check if a tuple `(1, 2, 3, 4)` contains the subsequence `(2, 3)`.

**T10.** Given `data = (("Alice", 90000), ("Bob", 72000), ("Carol", 85000))`, find the person with maximum salary.

**SE1.** Find unique elements in `[1, 2, 2, 3, 3, 3, 4]` using a set and return as a sorted list.

**SE2.** Given `a = {1,2,3,4}` and `b = {3,4,5,6}`, find: union, intersection, difference (a-b), symmetric difference.

**SE3.** Given a list of email addresses, find duplicates (emails appearing more than once).

**SE4.** Given `expected_cols = {"id","name","salary","dept"}` and `received_cols = {"id","name","phone","dept"}`, find missing and extra columns.

**SE5.** Check if `{"read","write"}` is a subset of `{"read","write","execute","delete"}`.

**SE6.** Given a list of tags from multiple articles, find tags that appear in ALL articles.

**SE7.** Remove duplicates from a list while preserving insertion order (sets don't preserve order — solve without using `set()` directly on the list).

**SE8.** Given two sets of user IDs — `active_users` and `premium_users` — find users who are active but NOT premium.

**SE9.** From `["python","java","python","go","java","rust","go"]`, find languages mentioned more than once using a set and dict together.

**SE10.** Given sets `a = {1,2,3}` and `b = {1,2,3,4,5}`, check if they are equal, if `a` is a proper subset of `b`, and if they are disjoint.

---

## SECTION 5 — DATA CLEANING (25 Problems)

**DC1.** Given a list of dicts with a `"name"` field, strip whitespace and convert to title case.

**DC2.** Replace all `None` values in a list `[1, None, 3, None, 5]` with `0`.

**DC3.** Given records with an `"age"` field, drop all records where age is `None`, negative, or greater than 120.

**DC4.** Normalize phone numbers — strip spaces, dashes, brackets: `"(91) 98765-43210"` → `"919876543210"`.

**DC5.** Given `"salary"` values as strings `["90,000", "1,20,000", "N/A", "75000"]`, convert to integers. Replace `"N/A"` with `0`.

**DC6.** Standardize status values — map `"success"`, `"SUCCESS"`, `"Success"` all to `"SUCCESS"`.

**DC7.** Given a list of email strings, validate each: must have `@`, a domain, and a `.` in the domain. Flag invalid ones.

**DC8.** Fill missing dictionary keys with defaults:
```python
records = [{"id": 1, "name": "Alice"}, {"id": 2}]
defaults = {"name": "UNKNOWN", "salary": 0, "dept": "UNASSIGNED"}
```
Every record must have all keys from `defaults`.

**DC9.** Deduplicate a list of dicts by `"id"`, keeping the record with the highest `"salary"`.

**DC10.** Given a list of date strings in mixed formats `["2024-01-15", "15/01/2024", "Jan 15 2024"]`, standardize all to `"YYYY-MM-DD"` using string parsing only.

**DC11.** Given records with `"price"` as a float, round all prices to 2 decimal places and remove records with price ≤ 0.

**DC12.** Strip HTML tags from a string: `"<p>Hello <b>World</b></p>"` → `"Hello World"`. Use string methods or simple parsing (no regex/library).

**DC13.** Given a CSV string with extra spaces around values: `"Alice , 30 , Mumbai , 90000"`, split and strip each field.

**DC14.** Detect and count missing fields in a list of records — a field is "missing" if its value is `None`, `""`, or `0` for required numeric fields.

**DC15.** Given employee records with a `"join_date"` string field, extract year and month and add them as separate keys.

**DC16.** Given a dict where some values are lists, some are strings, and some are `None` — flatten it: convert lists to comma-separated strings, keep strings, replace `None` with `""`.

**DC17.** From a list of transaction records, flag `"SUSPICIOUS"` if the same `customer_id` appears more than 3 times within the same `date`.

**DC18.** Given a list of product names with inconsistent casing and extra spaces, normalize and deduplicate.

**DC19.** Parse key=value pairs from a log line: `"host=localhost port=5432 db=warehouse timeout=30"` → dict.

**DC20.** Given records with a numeric `"score"` field that sometimes contains strings like `"N/A"` or `"pending"`, safely cast to float, defaulting to `-1` on failure.

**DC21.** Remove all records from a list where ALL of the following are true: status is `"FAILED"`, rows is `0`, and duration is less than `5` seconds.

**DC22.** Given a list of strings with trailing commas or semicolons, strip those characters from each.

**DC23.** Given records with `"start_time"` and `"end_time"` as `"HH:MM"` strings, compute duration in minutes for each record.

**DC24.** Validate that all `order_id` values in a list follow the pattern `"ORD-XXXX"` where X is a digit. Flag non-conforming ones.

**DC25.** Given a list of records where `"category"` can be `None` or `""`, fill with `"Uncategorized"` and group by category, counting records per group.

---

## SECTION 6 — API / REQUESTS (20 Problems)

**A1.** Make a GET request to `https://jsonplaceholder.typicode.com/users` and print the name and email of each user.

**A2.** Make a GET request to `https://jsonplaceholder.typicode.com/posts/1` and pretty-print the JSON response.

**A3.** Handle a failed API request (non-200 status). Print `"Request failed: <status_code>"` for anything other than 200.

**A4.** Make a GET request with query parameters: fetch posts by user — `https://jsonplaceholder.typicode.com/posts?userId=1`.

**A5.** Make a POST request with a JSON body to `https://jsonplaceholder.typicode.com/posts` — send `{"title": "test", "body": "hello", "userId": 1}` and print the created resource.

**A6.** Add headers to a request — set `Content-Type: application/json` and a fake `Authorization: Bearer TOKEN` header.

**A7.** Set a timeout of 5 seconds on a GET request. Handle `requests.Timeout` exception and print `"Request timed out"`.

**A8.** Fetch paginated data — keep calling `https://jsonplaceholder.typicode.com/posts?_page=N&_limit=10` (N=1,2,3) and collect all results into one list.

**A9.** Parse a JSON API response and extract only records where `"completed": True`:
```python
# GET https://jsonplaceholder.typicode.com/todos
```

**A10.** Write a `retry(url, max_retries=3)` function — retry the request up to 3 times if it fails, with a 1-second wait between attempts.

**A11.** Given the response from `https://jsonplaceholder.typicode.com/users`, group users by `company.name` field (nested).

**A12.** Make a GET request, save the response JSON to a local file `response.json`, then read it back and print one field.

**A13.** Catch `requests.ConnectionError` when the URL is unreachable and print a user-friendly message.

**A14.** Given a list of 5 user IDs, make individual GET requests to `https://jsonplaceholder.typicode.com/users/{id}` for each and collect names into a list.

**A15.** Parse the response headers from any request — print `Content-Type`, `Date`, and `X-Powered-By` if present.

**A16.** Make a request and check if the response is valid JSON before calling `.json()` — handle `ValueError` if it's not.

**A17.** Build a simple API client class with methods `get(endpoint)`, `post(endpoint, data)` — base URL is `https://jsonplaceholder.typicode.com`.

**A18.** Given a response JSON with nested lists, extract all unique `userId` values from `https://jsonplaceholder.typicode.com/posts`.

**A19.** Measure and print the response time (in milliseconds) of a GET request using `response.elapsed.total_seconds()`.

**A20.** Write a function that fetches data from an API and returns an empty list `[]` on any exception (network error, timeout, bad JSON) — never raise.

---

## SECTION 7 — DATABASE CONNECTION (15 Problems)

**DB1.** Connect to a PostgreSQL database using `psycopg2` and print the server version.

**DB2.** Fetch all rows from a `employees` table and print each row as a dict.

**DB3.** Insert a new employee record into the `employees` table and commit the transaction.

**DB4.** Use parameterized queries to insert employee data — never build SQL strings with f-strings for user input (SQL injection).

**DB5.** Fetch employees where salary > a given value — pass the value as a parameter, not embedded in the query string.

**DB6.** Update the salary of an employee by ID and commit. Print rows affected using `cursor.rowcount`.

**DB7.** Delete a record by ID. Handle the case where the ID doesn't exist — print `"No record found"`.

**DB8.** Use `cursor.fetchone()` vs `cursor.fetchall()` vs `cursor.fetchmany(5)` — demonstrate all three on the same query.

**DB9.** Wrap a multi-step insert in a transaction — if any step fails, rollback and print the error.

**DB10.** Use a context manager (`with psycopg2.connect(...) as conn`) to auto-close the connection.

**DB11.** Fetch results as dicts using `RealDictCursor` from `psycopg2.extras` instead of tuples.

**DB12.** Count total rows in a table and print: `"Total employees: N"`.

**DB13.** Connect using credentials stored in environment variables (`os.environ.get("DB_HOST")`) — never hardcode passwords.

**DB14.** Execute a `SELECT` with a `JOIN` in Python — employees joined with departments — and print combined results.

**DB15.** Batch-insert a list of 10 employee records using `cursor.executemany()` — more efficient than 10 separate `execute()` calls.

---

## SECTION 8 — DSA: ARRAYS & STRINGS (30 Problems)

**DSA1.** Two Sum — given `nums = [2, 7, 11, 15]` and `target = 9`, return indices of the two numbers that add up to target. *(Easy)*

**DSA2.** Find the single number in `[4, 1, 2, 1, 2]` where every other number appears twice. *(Easy — XOR trick)*

**DSA3.** Given `[0, 1, 0, 3, 12]`, move all zeros to the end without changing relative order of non-zeros. *(Easy)*

**DSA4.** Check if `[1, 2, 3, 1]` contains a duplicate. Return `True`/`False`. *(Easy)*

**DSA5.** Given a sorted array `[-4, -1, 0, 3, 10]`, return a sorted array of squares → `[0, 1, 9, 16, 100]`. *(Easy)*

**DSA6.** Given `nums = [1, 1, 2]` (sorted), remove duplicates in-place and return new length. *(Easy)*

**DSA7.** Best Time to Buy and Sell Stock — given `[7, 1, 5, 3, 6, 4]`, find the max profit (one transaction). *(Easy)*

**DSA8.** Plus One — given digits of a number `[1, 2, 3]`, add one and return `[1, 2, 4]`. Handle `[9, 9]` → `[1, 0, 0]`. *(Easy)*

**DSA9.** Find the missing number in `[3, 0, 1]` (contains 0..n with one missing). *(Easy)*

**DSA10.** Count the number of 1-bits in the binary representation of `11` (`1011` → 3). *(Easy)*

**DSA11.** Valid Anagram — check if `"anagram"` and `"nagaram"` are anagrams using a frequency dict. *(Easy)*

**DSA12.** First Unique Character — find first non-repeating char in `"loveleetcode"` → `"v"`. *(Easy)*

**DSA13.** Reverse a string in-place (swap characters from both ends). *(Easy)*

**DSA14.** Check if `"()[]{}` is a valid bracket sequence using a stack. *(Easy)*

**DSA15.** Given `strs = ["eat","tea","tan","ate","nat","bat"]`, group anagrams together. *(Easy-Medium)*

**DSA16.** Longest Common Prefix — find the longest prefix in `["flower","flow","flight"]` → `"fl"`. *(Easy)*

**DSA17.** Given a sorted array, find the first and last position of a target value using a linear scan. *(Easy)*

**DSA18.** Merge two sorted arrays `[1,3,5]` and `[2,4,6]` into one sorted array without using `sort()`. *(Easy)*

**DSA19.** Find all pairs in `[1,2,3,4,5]` whose sum equals 6 — return as list of tuples. *(Easy)*

**DSA20.** Given `"abcabcbb"`, find the length of the longest substring without repeating characters. *(Easy-Medium — sliding window)*

**DSA21.** Count and say — given `"1"`, generate the next 4 terms of the count-and-say sequence. *(Easy)*

**DSA22.** Roman to Integer — convert `"XIV"` → `14` using a dict for values. *(Easy)*

**DSA23.** Implement `strStr("hello", "ll")` — return the index of the first occurrence, or `-1`. *(Easy)*

**DSA24.** Given `[2, 3, 1, 1, 4]`, check if you can reach the last index (jump game). Each element = max jump length. *(Easy-Medium)*

**DSA25.** Find the maximum sum subarray of size `k=3` in `[2, 1, 5, 1, 3, 2]` using a sliding window. *(Easy-Medium)*

**DSA26.** Given a 2D matrix, return all elements in spiral order. *(Easy-Medium)*

**DSA27.** Given `[1, 2, 3, 4, 5]`, return the product of all elements except self without using division. *(Easy-Medium)*

**DSA28.** Find the majority element (appears more than n/2 times) in `[3, 2, 3]`. *(Easy)*

**DSA29.** Given `words = ["word","world","row"]` and `chars = "worldrow"`, find all words that can be formed using characters in `chars` (each char used once). *(Easy)*

**DSA30.** Implement binary search on a sorted list `[1, 3, 5, 7, 9, 11]` — return index of target or `-1`. *(Easy)*

---

## SECTION 9 — FUNCTIONS & COMPREHENSIONS (10 Problems)

**F1.** Write a function `is_prime(n)` that returns `True` if `n` is prime. Test on `[2, 7, 10, 13, 25]`.

**F2.** Write a lambda that takes a list of dicts and returns them sorted by `"salary"` key.

**F3.** Use `map()` to convert a list of strings `["1","2","3"]` to integers.

**F4.** Use `filter()` to get all words longer than 4 characters from `["hi","hello","hey","python","go"]`.

**F5.** Use `reduce()` from `functools` to compute the product of `[1, 2, 3, 4, 5]`.

**F6.** Write a list comprehension that produces `[(x, y) for x in range(3) for y in range(3) if x != y]`.

**F7.** Write a dict comprehension that squares keys: `{x: x**2 for x in range(1,6)}`.

**F8.** Write a generator function `fibonacci(n)` that yields the first `n` Fibonacci numbers.

**F9.** Write a decorator `timer` that prints how long a function took to execute. Apply it to a function that sums `range(1_000_000)`.

**F10.** Write a function `memoize(fn)` that caches results — calling `memoize(fib)(10)` should only compute each value once.

---

## SECTION 10 — FILE I/O (10 Problems)

**FI1.** Read a CSV file line by line using `open()` (no `csv` module) and print each row as a list.

**FI2.** Read `employees.csv` using the `csv` module and convert to a list of dicts (using `DictReader`).

**FI3.** Write a list of dicts to a CSV file using `csv.DictWriter`.

**FI4.** Read a JSON file and print the value of a specific nested key.

**FI5.** Write a Python dict to a JSON file with `indent=4` formatting.

**FI6.** Append a new log entry to a text file — do not overwrite existing content.

**FI7.** Read a file, count total lines, total words, and total characters.

**FI8.** Read a CSV file, filter rows where `salary > 80000`, and write matching rows to a new CSV file.

**FI9.** Handle `FileNotFoundError` gracefully when reading a file that may not exist — return an empty list.

**FI10.** Use a context manager (`with open(...)`) to safely read and write files — explain why this is better than `f = open(...)` without `with`.

---

*Total: 210 Problems*
*Difficulty: Easy to Easy-Medium — ideal for first-round Python interviews*
